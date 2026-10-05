#include "storage/iceberg_vortex.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "common/iceberg_utils.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
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

static TableFunction FindScan(ClientContext &context, const string &name, const LogicalType &argument) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	return entry.functions.GetFunctionByArguments(context, {argument});
}

// Each file is claimed by one scan task. Other files can be scanned in parallel.
class IcebergVortexReader : public BaseFileReader {
public:
	IcebergVortexReader(ClientContext &context, const OpenFileInfo &file, const MultiFileBindData &iceberg_bind)
	    : BaseFileReader(file), scan(FindScan(context, "read_vortex", LogicalType::VARCHAR)) {
		if (FileSystem::GetFileSystem(context).IsRemoteFile(file.path) ||
		    file.path.find_first_of("*?[") != string::npos) {
			throw NotImplementedException("Vortex Iceberg data files currently require literal local file paths");
		}
		vector<Value> inputs {Value(file.path)};
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
	VortexCopyBind(CopyFunction function, unique_ptr<FunctionData> inner, vector<pair<idx_t, string>> required_columns)
	    : function(std::move(function)), inner(std::move(inner)), required_columns(std::move(required_columns)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VortexCopyBind>(function, inner->Copy(), required_columns);
	}
	bool Equals(const FunctionData &other) const override {
		auto &other_bind = other.Cast<VortexCopyBind>();
		return required_columns == other_bind.required_columns && inner->Equals(*other_bind.inner);
	}
	CopyFunction function;
	unique_ptr<FunctionData> inner;
	vector<pair<idx_t, string>> required_columns;
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

static void ValidateTimestamps(DataChunk &chunk) {
	for (auto &column : chunk.data) {
		int64_t minimum;
		int64_t maximum;
		switch (column.GetType().id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			// The pinned Vortex writer validates scalars with jiff::Timestamp. Its
			// bounds are narrower than DuckDB's, and constructing a large Span can panic.
			minimum = -377705023201000000LL;
			maximum = 253402207200999999LL;
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			// Exclude DuckDB's infinities and i64::MIN, which Jiff spans cannot represent.
			minimum = -NumericLimits<int64_t>::Maximum() + 1;
			maximum = NumericLimits<int64_t>::Maximum() - 1;
			break;
		default:
			continue;
		}
		UnifiedVectorFormat format;
		column.ToUnifiedFormat(chunk.size(), format);
		auto values = UnifiedVectorFormat::GetData<timestamp_t>(format);
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto index = format.sel->get_index(row);
			if (format.validity.RowIsValid(index) && (values[index].value < minimum || values[index].value > maximum)) {
				throw InvalidInputException("Vortex Iceberg timestamp value is outside the supported range");
			}
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
	if (fs.IsRemoteFile(input.data_path) || input.data_path.find_first_of("*?[") != string::npos) {
		throw NotImplementedException("Vortex Iceberg data files currently require literal local file paths");
	}
	if (!input.options.empty()) {
		throw NotImplementedException("Vortex Iceberg writes do not yet support COPY options");
	}
	if (!fs.DirectoryExists(input.data_path)) {
		fs.CreateDirectoriesRecursive(input.data_path);
	}
	auto &inner = IcebergUtils::GetCopyFunction(context, "vortex").function;
	auto info = make_uniq<CopyInfo>();
	info->file_path = input.data_path;
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
	auto bind = make_uniq<VortexCopyBind>(inner, inner.copy_to_bind(context, bind_input, physical_names, types),
	                                      std::move(required_columns));
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
		return bind.function.copy_to_initialize_local(context, *bind.inner);
	};
	function.copy_to_sink = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                           LocalFunctionData &local, DataChunk &chunk) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		for (const auto &column : bind.required_columns) {
			if (VectorOperations::HasNull(chunk.data[column.first], chunk.size())) {
				throw ConstraintException("NOT NULL constraint failed: %s", column.second);
			}
		}
		ValidateTimestamps(chunk);
		bind.function.copy_to_sink(context, *bind.inner, *state.inner, local, chunk);
		state.row_count += chunk.size();
	};
	function.copy_to_combine = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                              LocalFunctionData &local) {
		auto &bind = data.Cast<VortexCopyBind>();
		if (bind.function.copy_to_combine) {
			bind.function.copy_to_combine(context, *bind.inner, *global.Cast<VortexCopyGlobal>().inner, local);
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
	result.file_path = input.data_path;
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
