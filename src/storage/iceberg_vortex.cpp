#include "storage/iceberg_vortex.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "common/iceberg_utils.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static string PhysicalColumnName(int32_t field_id) {
	return "__iceberg_vortex_v1_field_" + std::to_string(field_id);
}

string IcebergVortex::WriteFormat(const IcebergTableMetadata &metadata) {
	const auto &properties = metadata.GetTableProperties();
	auto entry = properties.find("write.format.default");
	auto format = entry == properties.end() ? "parquet" : StringUtil::Lower(entry->second);
	if (format != "parquet" && format != "vortex") {
		throw NotImplementedException("Unsupported Iceberg data format '%s'", format);
	}
	return format;
}

void IcebergVortex::ValidateTable(const IcebergTableMetadata &metadata) {
	if (metadata.iceberg_version != 2) {
		throw NotImplementedException("Vortex Iceberg data files currently require Iceberg v2");
	}
	if (metadata.GetSchemas().size() != 1) {
		throw NotImplementedException("Vortex Iceberg data files currently require a fixed schema");
	}
	for (const auto &spec : metadata.partition_specs) {
		if (!spec.second.fields.empty()) {
			throw NotImplementedException("Vortex Iceberg data files currently require an unpartitioned table");
		}
	}
	for (const auto &column : metadata.GetLatestSchema().columns) {
		if (!column->children.empty() || column->type.IsNested()) {
			throw NotImplementedException("Vortex Iceberg data files currently require primitive columns");
		}
	}
}

static bool ContainsVortexFiles(const vector<IcebergManifestEntry> &entries, idx_t start_index = 0) {
	for (idx_t index = start_index; index < entries.size(); index++) {
		const auto &entry = entries[index];
		if (entry.status != IcebergManifestEntryStatusType::DELETED &&
		    StringUtil::CIEquals(entry.data_file.file_format, "vortex")) {
			return true;
		}
	}
	return false;
}

void IcebergVortex::ValidateSchemaChange(const IcebergTransactionData &transaction_data) {
	const auto &metadata = transaction_data.table_info.table_metadata;
	auto reject = []() {
		throw NotImplementedException(
		    "Vortex Iceberg data files currently require a fixed schema; schema changes are not supported");
	};
	if (WriteFormat(metadata) == "vortex") {
		reject();
	}
	// Inserts earlier in this transaction have not been written to manifests yet.
	for (const auto &alter : transaction_data.alters) {
		for (const auto &manifest : alter.get().GetManifestFiles()) {
			if (ContainsVortexFiles(manifest.manifest_entries)) {
				reject();
			}
		}
	}
	// A property change or a Parquet-only current snapshot does not remove Vortex
	// files referenced by retained snapshots. Read each immutable manifest once.
	auto &context = transaction_data.context;
	auto &fs = FileSystem::GetFileSystem(context);
	IcebergOptions options;
	unordered_set<string> scanned_manifests;
	for (const auto &snapshot_entry : metadata.snapshots) {
		const auto &snapshot = snapshot_entry.second;
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = &snapshot;
		snapshot_info.schema_id = snapshot.GetSchemaId();
		vector<IcebergManifestListEntry> manifests;
		auto list_scan =
		    AvroScan::ScanManifestList(snapshot_info, metadata, context, snapshot.manifest_list, manifests);
		manifest_list::ManifestListReader list_reader(*list_scan);
		while (!list_reader.Finished()) {
			list_reader.Read();
		}
		for (auto &manifest : manifests) {
			if (manifest.file.content != IcebergManifestContentType::DATA ||
			    !scanned_manifests.insert(manifest.file.manifest_path).second) {
				continue;
			}
			// Open one manifest at a time so rejection does not read any later files.
			vector<IcebergManifestListEntry> data_manifests;
			data_manifests.push_back(std::move(manifest));
			auto scan = AvroScan::ScanManifest(snapshot_info, data_manifests, options, fs, "", metadata, context);
			manifest_file::ManifestReader reader(*scan);
			auto &entries = data_manifests[0].manifest_entries;
			while (!reader.Finished()) {
				auto start_index = entries.size();
				reader.Read();
				if (ContainsVortexFiles(entries, start_index)) {
					reject();
				}
			}
		}
	}
}

static TableFunction FindScan(ClientContext &context, const string &name, const LogicalType &argument) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	return entry.functions.GetFunctionByArguments(context, {argument});
}

static string VortexLocalPath(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.IsRemoteFile(path) || path.find("://") != string::npos) {
		throw NotImplementedException("Vortex Iceberg data files currently require literal local file paths");
	}
	auto absolute_path = fs.ExpandPath(path);
	if (!fs.IsPathAbsolute(absolute_path)) {
		absolute_path = fs.JoinPath(fs.GetWorkingDirectory(), absolute_path);
	}
	if (absolute_path.find_first_of("*?[") != string::npos) {
		throw NotImplementedException("Vortex Iceberg data files currently require literal local file paths");
	}
	// The pinned reader uses Url::path() without decoding it. Check the absolute
	// path so a relative destination cannot hide an escaped working directory.
	const string escaped_characters = "\"#%<>`{}";
	for (auto character : absolute_path) {
		auto byte = static_cast<uint8_t>(character);
		auto requires_encoding = byte <= 0x20 || byte >= 0x7f || escaped_characters.find(character) != string::npos;
#ifndef _WIN32
		requires_encoding = requires_encoding || character == '\\';
#endif
		if (requires_encoding) {
			throw NotImplementedException("Vortex Iceberg data paths cannot contain URL-escaped characters");
		}
	}
	return absolute_path;
}

static void VerifyVortexFileAccess(ClientContext &context, const string &path) {
	// Native Vortex I/O bypasses DuckDB's filesystem. Open through it first to
	// enforce the client's access policy and disabled-filesystem restrictions.
	auto handle = FileSystem::GetFileSystem(context).OpenFile(path, FileFlags::FILE_FLAGS_READ);
}

// Each file is claimed by one scan task. Other files can be scanned in parallel.
class IcebergVortexReader : public BaseFileReader {
public:
	IcebergVortexReader(ClientContext &context, const OpenFileInfo &file, const MultiFileBindData &iceberg_bind)
	    : BaseFileReader(file), scan(FindScan(context, "read_vortex", LogicalType::VARCHAR)) {
		scan_path = VortexLocalPath(context, file.path);
		VerifyVortexFileAccess(context, scan_path);
		vector<Value> inputs {Value(scan_path)};
		named_parameter_map_t parameters;
		vector<LogicalType> input_types;
		vector<string> input_names;
		TableFunctionRef ref;
		TableFunctionBindInput input(inputs, parameters, input_types, input_names, scan.function_info.get(), nullptr,
		                             scan, ref);
		vector<LogicalType> types;
		vector<string> names;
		bind_data = scan.bind(context, input, types, names);
		const auto &schema = iceberg_bind.reader_bind.schema;
		if (schema.size() != names.size()) {
			throw InvalidInputException("Vortex Iceberg file '%s' does not match the fixed table schema", file.path);
		}
		unordered_map<string, const MultiFileColumnDefinition *> fields;
		for (const auto &column : schema) {
			fields.emplace(PhysicalColumnName(column.identifier.GetValue<int32_t>()), &column);
		}
		for (idx_t index = 0; index < names.size(); index++) {
			auto field = fields.find(names[index]);
			if (field == fields.end() || field->second->type != types[index]) {
				throw InvalidInputException("Vortex Iceberg file '%s' has an incompatible field '%s'", file.path,
				                            names[index]);
			}
			MultiFileColumnDefinition column(names[index], types[index]);
			column.identifier = field->second->identifier;
			columns.push_back(std::move(column));
			fields.erase(field);
		}
	}

	void AddVirtualColumn(column_t id) override {
		if (id != COLUMN_IDENTIFIER_EMPTY) {
			throw NotImplementedException("Vortex Iceberg scans do not yet support virtual row columns");
		}
		columns.emplace_back("", LogicalType::BOOLEAN);
	}

	bool TryInitializeScan(ClientContext &, GlobalTableFunctionState &, LocalTableFunctionState &) override {
		if (claimed) {
			return false;
		}
		claimed = true;
		return true;
	}

	void PrepareScan(ClientContext &context, GlobalTableFunctionState &, LocalTableFunctionState &) override {
		// Access settings can change after a prepared query has bound this reader.
		VerifyVortexFileAccess(context, scan_path);
		if (deletion_filter) {
			throw NotImplementedException("Vortex Iceberg scans do not yet support delete files");
		}
		vector<column_t> ids;
		for (idx_t index = 0; index < column_ids.size(); index++) {
			auto id = column_ids[MultiFileLocalIndex(index)];
			ids.push_back(columns[id].name.empty() ? COLUMN_IDENTIFIER_EMPTY : id);
		}
		TableFunctionInitInput input(bind_data.get(), std::move(ids), {}, filters.get());
		global = scan.init_global(context, input);
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		local = scan.init_local(execution, input, global.get());
	}

	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &, LocalTableFunctionState &,
	                 DataChunk &chunk) override {
		TableFunctionInput input(bind_data.get(), local.get(), global.get());
		scan.function(context, input, chunk);
		if (input.async_result.GetResultType() == AsyncResultType::INVALID) {
			return chunk.size() ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
		}
		return std::move(input.async_result);
	}

	string GetReaderType() const override {
		return "Vortex";
	}

private:
	TableFunction scan;
	string scan_path;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
	bool claimed = false;
};

// Keep the Parquet bind/options/state contract used by iceberg_scan. Only the
// per-file factory changes, so a snapshot can contain both physical formats.
class IcebergFileInterface : public MultiFileReaderInterface {
public:
	explicit IcebergFileInterface(unique_ptr<MultiFileReaderInterface> parquet) : parquet(std::move(parquet)) {
	}

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override {
		return parquet->InitializeOptions(context, info);
	}
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &names, vector<LogicalType> &types) override {
		return parquet->ParseCopyOption(context, key, values, options, names, types);
	}
	bool ParseOption(ClientContext &context, const string &key, const Value &value, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override {
		return parquet->ParseOption(context, key, value, file_options, options);
	}
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override {
		return parquet->InitializeBindData(data, std::move(options));
	}
	void BindReader(ClientContext &context, vector<LogicalType> &types, vector<string> &names,
	                MultiFileBindData &data) override {
		parquet->BindReader(context, types, names, data);
	}
	optional_idx MaxThreads(const MultiFileBindData &data, const MultiFileGlobalState &state,
	                        FileExpandResult expand_result) override {
		return parquet->MaxThreads(data, state, expand_result);
	}
	void GetBindInfo(const TableFunctionData &data, BindInfo &info) override {
		parquet->GetBindInfo(data, info);
	}
	void GetVirtualColumns(ClientContext &context, MultiFileBindData &data, virtual_column_map_t &result) override {
		parquet->GetVirtualColumns(context, data, result);
	}
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context, const MultiFileBindData &data,
	                                          idx_t file_count) override {
		return parquet->GetCardinality(context, data, file_count);
	}
	void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                   LocalTableFunctionState &local_state) override {
		parquet->FinishReading(context, global_state, local_state);
	}
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &data,
	                                                           MultiFileGlobalState &state) override {
		return parquet->InitializeGlobalState(context, data, state);
	}
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &context,
	                                                         GlobalTableFunctionState &state) override {
		return parquet->InitializeLocalState(context, state);
	}
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &state,
	                                        BaseUnionData &data, const MultiFileBindData &bind) override {
		return parquet->CreateReader(context, state, data, bind);
	}
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &state,
	                                        const OpenFileInfo &file, idx_t index,
	                                        const MultiFileBindData &bind) override {
		if (file.extended_info) {
			auto format = file.extended_info->options.find("iceberg_file_format");
			if (format != file.extended_info->options.end() && format->second.GetValue<string>() == "vortex") {
				return make_shared_ptr<IcebergVortexReader>(context, file, bind);
			}
		}
		return parquet->CreateReader(context, state, file, index, bind);
	}
	unique_ptr<MultiFileReaderInterface> Copy() override {
		return make_uniq<IcebergFileInterface>(parquet->Copy());
	}

private:
	unique_ptr<MultiFileReaderInterface> parquet;
};

unique_ptr<FunctionData> IcebergVortex::BindScan(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &types, vector<string> &names) {
	auto parquet = FindScan(context, "parquet_scan", input.inputs[0].type());
	auto result = parquet.bind(context, input, types, names);
	auto &bind = result->Cast<MultiFileBindData>();
	bind.interface = make_uniq<IcebergFileInterface>(std::move(bind.interface));
	return result;
}

struct VortexCopyBind : public FunctionData {
	VortexCopyBind(CopyFunction function, unique_ptr<FunctionData> inner, vector<LogicalType> types,
	               vector<pair<idx_t, string>> required_columns)
	    : function(std::move(function)), inner(std::move(inner)), types(std::move(types)),
	      required_columns(std::move(required_columns)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VortexCopyBind>(function, inner->Copy(), types, required_columns);
	}
	bool Equals(const FunctionData &other) const override {
		auto &other_bind = other.Cast<VortexCopyBind>();
		return types == other_bind.types && required_columns == other_bind.required_columns &&
		       inner->Equals(*other_bind.inner);
	}
	CopyFunction function;
	unique_ptr<FunctionData> inner;
	vector<LogicalType> types;
	vector<pair<idx_t, string>> required_columns;
};

struct VortexCopyLocal : public LocalFunctionData {
	unique_ptr<LocalFunctionData> inner;
	DataChunk cast_chunk;
};

struct VortexCopyGlobal : public GlobalFunctionData {
	unique_ptr<GlobalFunctionData> inner;
	string path;
	atomic<idx_t> row_count {0};
	optional_ptr<CopyFunctionFileStatistics> statistics;
};

static void ValidateAppend(ClientContext &context, const IcebergTableMetadata &metadata) {
	auto snapshot = metadata.GetLatestSnapshot();
	if (!snapshot) {
		return;
	}
	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = snapshot;
	snapshot_info.schema_id = metadata.GetCurrentSchemaId();
	vector<IcebergManifestListEntry> manifests;
	auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, context, snapshot->manifest_list, manifests);
	manifest_list::ManifestListReader reader(*scan);
	while (!reader.Finished()) {
		reader.Read();
	}
	for (const auto &manifest : manifests) {
		if (manifest.file.content == IcebergManifestContentType::DELETE) {
			throw NotImplementedException("Vortex Iceberg appends do not yet support delete files");
		}
	}
}

template <class T>
static void ValidateTemporalRange(Vector &column, idx_t count, int64_t minimum, int64_t maximum,
                                  const char *type_name) {
	UnifiedVectorFormat format;
	column.ToUnifiedFormat(count, format);
	auto values = UnifiedVectorFormat::GetData<T>(format);
	for (idx_t row = 0; row < count; row++) {
		auto index = format.sel->get_index(row);
		if (!format.validity.RowIsValid(index)) {
			continue;
		}
		auto value = static_cast<int64_t>(values[index]);
		if (value < minimum || value > maximum) {
			throw InvalidInputException("Vortex Iceberg %s value is outside the supported range", type_name);
		}
	}
}

static void ValidateTemporalValues(DataChunk &chunk) {
	for (auto &column : chunk.data) {
		switch (column.GetType().id()) {
		case LogicalTypeId::TIME:
			// DuckDB permits 24:00:00, but the pinned Vortex/Jiff time scalar excludes it.
			ValidateTemporalRange<dtime_t>(column, chunk.size(), 0, Interval::MICROS_PER_DAY - 1, "time");
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			// The pinned Vortex writer validates scalars with jiff::Timestamp. Its
			// bounds are narrower than DuckDB's, and constructing a large Span can panic.
			ValidateTemporalRange<timestamp_t>(column, chunk.size(), -377705023201000000LL, 253402207200999999LL,
			                                   "timestamp");
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			// Exclude DuckDB's infinities and i64::MIN, which Jiff spans cannot represent.
			ValidateTemporalRange<timestamp_ns_t>(column, chunk.size(), -NumericLimits<int64_t>::Maximum() + 1,
			                                      NumericLimits<int64_t>::Maximum() - 1, "timestamp");
			break;
		default:
			break;
		}
	}
}

IcebergCopyOptions IcebergVortex::CopyOptions(ClientContext &context, const IcebergCopyInput &input) {
	ValidateTable(input.table_metadata);
	ValidateAppend(context, input.table_metadata);
	if (input.virtual_columns != IcebergInsertVirtualColumns::NONE) {
		throw NotImplementedException("Vortex Iceberg writes currently support append only");
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = VortexLocalPath(context, input.data_path);
	if (!input.options.empty()) {
		throw NotImplementedException("Vortex Iceberg writes do not yet support COPY options");
	}
	if (!fs.DirectoryExists(data_path)) {
		fs.CreateDirectoriesRecursive(data_path);
	}
	auto &inner = IcebergUtils::GetCopyFunction(context, "vortex").function;
	auto info = make_uniq<CopyInfo>();
	info->file_path = data_path;
	info->format = "vortex";
	info->is_from = false;
	vector<string> physical_names;
	vector<LogicalType> types;
	vector<pair<idx_t, string>> required_columns;
	for (const auto &column : input.schema.columns) {
		if (column->required) {
			required_columns.emplace_back(physical_names.size(), column->name);
		}
		physical_names.push_back(PhysicalColumnName(column->id));
		types.push_back(column->type);
	}
	CopyFunctionBindInput bind_input(*info);
	auto inner_bind = inner.copy_to_bind(context, bind_input, physical_names, types);
	auto bind = make_uniq<VortexCopyBind>(inner, std::move(inner_bind), std::move(types), std::move(required_columns));
	CopyFunction function("iceberg_vortex");
	function.extension = "vortex";
	function.copy_to_initialize_global = [](ClientContext &context, FunctionData &data, const string &path) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto result = make_uniq<VortexCopyGlobal>();
		result->path = path;
		result->inner = bind.function.copy_to_initialize_global(context, *bind.inner, path);
		return unique_ptr<GlobalFunctionData>(std::move(result));
	};
	function.copy_to_initialize_local = [](ExecutionContext &context, FunctionData &data) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto result = make_uniq<VortexCopyLocal>();
		result->inner = bind.function.copy_to_initialize_local(context, *bind.inner);
		result->cast_chunk.Initialize(context.client, bind.types);
		return unique_ptr<LocalFunctionData>(std::move(result));
	};
	function.copy_to_sink = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                           LocalFunctionData &local, DataChunk &chunk) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		auto &local_state = local.Cast<VortexCopyLocal>();
		auto &converted = local_state.cast_chunk;
		converted.Reset();
		D_ASSERT(chunk.ColumnCount() == bind.types.size());
		// COPY query output can differ from the Iceberg schema (e.g. SUM returns
		// HUGEINT, stored as DECIMAL(38,0)). The native writer uses the chunk types.
		for (idx_t column = 0; column < bind.types.size(); column++) {
			if (chunk.data[column].GetType() != bind.types[column]) {
				VectorOperations::Cast(context.client, chunk.data[column], converted.data[column], chunk.size());
			} else {
				converted.data[column].Reference(chunk.data[column]);
			}
		}
		converted.SetCardinality(chunk.size());
		for (const auto &column : bind.required_columns) {
			if (VectorOperations::HasNull(converted.data[column.first], converted.size())) {
				throw ConstraintException("NOT NULL constraint failed: %s", column.second);
			}
		}
		ValidateTemporalValues(converted);
		bind.function.copy_to_sink(context, *bind.inner, *state.inner, *local_state.inner, converted);
		state.row_count += chunk.size();
	};
	function.copy_to_combine = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                              LocalFunctionData &local) {
		auto &bind = data.Cast<VortexCopyBind>();
		if (bind.function.copy_to_combine) {
			bind.function.copy_to_combine(context, *bind.inner, *global.Cast<VortexCopyGlobal>().inner,
			                              *local.Cast<VortexCopyLocal>().inner);
		}
	};
	function.copy_to_get_written_statistics = [](ClientContext &, FunctionData &, GlobalFunctionData &global,
	                                             CopyFunctionFileStatistics &statistics) {
		global.Cast<VortexCopyGlobal>().statistics = statistics;
	};
	function.copy_to_finalize = [](ClientContext &context, FunctionData &data, GlobalFunctionData &global) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		bind.function.copy_to_finalize(context, *bind.inner, *state.inner);
		if (state.statistics) {
			state.statistics->row_count = state.row_count;
			auto file = FileSystem::GetFileSystem(context).OpenFile(state.path, FileFlags::FILE_FLAGS_READ);
			state.statistics->file_size_bytes = file->GetFileSize();
		}
	};
	function.execution_mode = [](bool, bool) {
		return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
	};
	IcebergCopyOptions result(std::move(info), std::move(function));
	result.bind_data = std::move(bind);
	result.file_path = data_path;
	result.file_extension = "vortex";
	result.filename_pattern.SetFilenamePattern("{uuidv7}");
	result.use_tmp_file = false;
	result.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	result.per_thread_output = true;
	result.rotate = false;
	result.hive_file_pattern = false;
	result.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	result.partition_output = false;
	result.write_partition_columns = true;
	// DuckDB's per-thread path creates files lazily; false would also initialize
	// the single-file writer against the output directory on the first chunk.
	result.write_empty_file = true;
	input.schema.GetColumnNamesAndTypes(result.names, result.expected_types);
	return result;
}

} // namespace duckdb
