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
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include <cmath>

#include "common/iceberg_utils.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "core/expression/iceberg_metrics.hpp"

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

static bool CanCopyVortexFilter(const TableFilter &filter, const LogicalType &type) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		return !type.IsNested();
	case TableFilterType::CONSTANT_COMPARISON: {
		if (type.id() != LogicalTypeId::INTEGER && type.id() != LogicalTypeId::BIGINT) {
			return false;
		}
		auto &comparison = filter.Cast<ConstantFilter>();
		if (comparison.constant.type() != type || comparison.constant.IsNull()) {
			return false;
		}
		switch (comparison.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return true;
		default:
			return false;
		}
	}
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.filter_type == TableFilterType::CONJUNCTION_AND
		                        ? static_cast<const ConjunctionFilter &>(filter.Cast<ConjunctionAndFilter>())
		                        : static_cast<const ConjunctionFilter &>(filter.Cast<ConjunctionOrFilter>());
		for (auto &child : conjunction.child_filters) {
			if (!CanCopyVortexFilter(*child, type)) {
				return false;
			}
		}
		return !conjunction.child_filters.empty();
	}
	case TableFilterType::OPTIONAL_FILTER: {
		// DuckDB wraps complete same-column OR clauses as pruning hints. Copy only
		// a fully validated disjunction, never runtime or arbitrary-expression hints.
		auto &optional = filter.Cast<OptionalFilter>();
		return optional.child_filter && optional.child_filter->filter_type == TableFilterType::CONJUNCTION_OR &&
		       CanCopyVortexFilter(*optional.child_filter, type);
	}
	default:
		return false;
	}
}

// Each file is claimed by one scan task. Other files can be scanned in parallel.
class IcebergVortexReader : public BaseFileReader {
public:
	IcebergVortexReader(ClientContext &context, const OpenFileInfo &file, const MultiFileBindData &iceberg_bind)
	    : BaseFileReader(file) {
		duckdb_filesystem = StringUtil::StartsWith(file.path, "s3://");
		if (duckdb_filesystem) {
#ifdef ICEBERG_VANE_DISTRIBUTED
			throw NotImplementedException("S3 Vortex Iceberg reads currently require native DuckDB execution");
#else
			if (file.path.find_first_of("*?[") != string::npos) {
				throw NotImplementedException("Vortex Iceberg data files require literal S3 object paths");
			}
			scan_path = file.path;
			scan = FindScan(context, "_read_vortex_file", LogicalType::VARCHAR);
#endif
		} else {
			scan_path = VortexLocalPath(context, file.path);
			scan = FindScan(context, "read_vortex", LogicalType::VARCHAR);
			VerifyVortexFileAccess(context, scan_path);
		}
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
			source_column_ids.push_back(index);
			fields.erase(field);
		}
		// These are immutable static predicates from metadata pruning. DuckDB keeps the
		// complete residual predicate; copy only operations whose Vortex semantics agree.
		auto &files = iceberg_bind.file_list->Cast<IcebergMultiFileList>();
		for (auto &entry : files.GetPruningFilters().filters) {
			if (entry.first >= schema.size()) {
				continue;
			}
			auto &column = schema[entry.first];
			if (!CanCopyVortexFilter(*entry.second, column.type)) {
				continue;
			}
			auto name = PhysicalColumnName(column.identifier.GetValue<int32_t>());
			for (idx_t index = 0; index < columns.size(); index++) {
				if (columns[index].name == name) {
					source_filters.PushFilter(ColumnIndex(index), entry.second->Copy());
					break;
				}
			}
		}
	}

	void AddVirtualColumn(column_t id) override {
		if (id != COLUMN_IDENTIFIER_EMPTY && id != MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			throw NotImplementedException("Vortex Iceberg scans do not support virtual column id %llu", id);
		}
		// The multi-file mapper has already appended this column to columns. Keep its
		// source identifier separately: it is not a field in the physical file schema.
		D_ASSERT(columns.size() == source_column_ids.size() + 1);
		source_column_ids.push_back(id);
		read_row_numbers |= id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
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
		// The DuckDB-backed reader reopens with the current credentials in init_global.
		if (!duckdb_filesystem) {
			VerifyVortexFileAccess(context, scan_path);
		}
		if (deletion_filter) {
			throw NotImplementedException("Vortex Iceberg scans do not yet support delete files");
		}
		vector<column_t> ids;
		vector<LogicalType> scan_types;
		scan_filters = filters ? filters->Copy() : nullptr;
		for (idx_t index = 0; index < column_ids.size(); index++) {
			auto id = column_ids[MultiFileLocalIndex(index)];
			auto source_id = source_column_ids[id];
			ids.push_back(source_id);
			// Vortex's row_idx() produces an unsigned position. Iceberg exposes BIGINT,
			// so read the native type and apply a checked cast after the scan.
			scan_types.push_back(source_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER ? LogicalType::UBIGINT
			                                                                                     : columns[id].type);
			auto filter = source_filters.filters.find(id);
			if (filter != source_filters.filters.end()) {
				if (!scan_filters) {
					scan_filters = make_uniq<TableFilterSet>();
				}
				scan_filters->PushFilter(ColumnIndex(index), filter->second->Copy());
			}
		}
		if (read_row_numbers) {
			scan_chunk.Initialize(context, scan_types);
		}
		TableFunctionInitInput input(bind_data.get(), std::move(ids), {}, scan_filters.get());
		global = scan.init_global(context, input);
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		local = scan.init_local(execution, input, global.get());
	}

	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &, LocalTableFunctionState &,
	                 DataChunk &chunk) override {
		TableFunctionInput input(bind_data.get(), local.get(), global.get());
		if (read_row_numbers) {
			scan_chunk.Reset();
			scan.function(context, input, scan_chunk);
			chunk.SetCardinality(scan_chunk.size());
			if (scan_chunk.size()) {
				for (idx_t index = 0; index < chunk.ColumnCount(); index++) {
					if (scan_chunk.data[index].GetType() == chunk.data[index].GetType()) {
						chunk.data[index].Reference(scan_chunk.data[index]);
					} else {
						VectorOperations::Cast(context, scan_chunk.data[index], chunk.data[index], scan_chunk.size());
					}
				}
			}
		} else {
			scan.function(context, input, chunk);
		}
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
	vector<column_t> source_column_ids;
	DataChunk scan_chunk;
	// Keep copied filter storage alive until all execution states have been destroyed.
	TableFilterSet source_filters;
	unique_ptr<TableFilterSet> scan_filters;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
	bool claimed = false;
	bool duckdb_filesystem = false;
	bool read_row_numbers = false;
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
	auto &files = bind.file_list->Cast<IcebergMultiFileList>();
	auto parquet_only = files.HasOnlyParquetDataFiles();
	input.table_function.filter_pushdown = parquet_only && parquet.filter_pushdown;
	files.SetRowFilterPushdown(input.table_function.filter_pushdown);
	input.table_function.pushdown_expression = parquet_only ? parquet.pushdown_expression : nullptr;
	return result;
}

struct VortexMetricsColumn {
	idx_t index;
	string name;
	bool bounds;
};

struct VortexColumnMetrics {
	idx_t null_count = 0;
	idx_t nan_count = 0;
	Value min;
	Value max;

	template <class T, bool FLOATING = false>
	void Update(Vector &vector, idx_t count, bool bounds) {
		UnifiedVectorFormat format;
		vector.ToUnifiedFormat(count, format);
		auto values = UnifiedVectorFormat::GetData<T>(format);
		bool have_bounds = !min.IsNull();
		T low = have_bounds ? min.GetValue<T>() : T();
		T high = have_bounds ? max.GetValue<T>() : T();
		for (idx_t row = 0; row < count; row++) {
			auto index = format.sel->get_index(row);
			if (!format.validity.RowIsValid(index)) {
				null_count++;
				continue;
			}
			auto value = values[index];
			if (FLOATING && Value::IsNan(value)) {
				nan_count++;
				continue;
			}
			if (!bounds) {
				continue;
			}
			// Iceberg bounds exclude NaNs and order -0 before +0.
			if (!have_bounds || value < low || (FLOATING && value == 0 && low == 0 && std::signbit(value))) {
				low = value;
			}
			if (!have_bounds || value > high || (FLOATING && value == 0 && high == 0 && !std::signbit(value))) {
				high = value;
			}
			have_bounds = true;
		}
		if (have_bounds) {
			min = Value::CreateValue<T>(low);
			max = Value::CreateValue<T>(high);
		}
	}
};

struct VortexCopyBind : public FunctionData {
	VortexCopyBind(CopyFunction function, unique_ptr<FunctionData> inner, vector<LogicalType> types,
	               vector<pair<idx_t, string>> required_columns, vector<VortexMetricsColumn> metrics_columns)
	    : function(std::move(function)), inner(std::move(inner)), types(std::move(types)),
	      required_columns(std::move(required_columns)), metrics_columns(std::move(metrics_columns)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VortexCopyBind>(function, inner->Copy(), types, required_columns, metrics_columns);
	}
	bool Equals(const FunctionData &other) const override {
		auto &other_bind = other.Cast<VortexCopyBind>();
		if (metrics_columns.size() != other_bind.metrics_columns.size()) {
			return false;
		}
		for (idx_t i = 0; i < metrics_columns.size(); i++) {
			auto &column = metrics_columns[i];
			auto &other_column = other_bind.metrics_columns[i];
			if (column.index != other_column.index || column.name != other_column.name ||
			    column.bounds != other_column.bounds) {
				return false;
			}
		}
		return types == other_bind.types && required_columns == other_bind.required_columns &&
		       inner->Equals(*other_bind.inner);
	}
	CopyFunction function;
	unique_ptr<FunctionData> inner;
	vector<LogicalType> types;
	vector<pair<idx_t, string>> required_columns;
	vector<VortexMetricsColumn> metrics_columns;
};

struct VortexCopyLocal : public LocalFunctionData {
	unique_ptr<LocalFunctionData> inner;
	DataChunk cast_chunk;
};

struct VortexCopyGlobal : public GlobalFunctionData {
	unique_ptr<GlobalFunctionData> inner;
	string path;
	atomic<idx_t> row_count {0};
	mutex metrics_lock;
	vector<VortexColumnMetrics> metrics;
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
	vector<VortexMetricsColumn> metrics_columns;
	auto default_metrics = GetDefaultMetricsConfig(input.table_metadata);
	for (const auto &column : input.schema.columns) {
		auto type = column->type.id();
		if (type == LogicalTypeId::INTEGER || type == LogicalTypeId::BIGINT || type == LogicalTypeId::FLOAT ||
		    type == LogicalTypeId::DOUBLE) {
			auto config = GetColumnMetricsConfig(input.table_metadata, default_metrics, column->name);
			if (config.mode != IcebergMetricsMode::NONE) {
				metrics_columns.push_back({types.size(), KeywordHelper::WriteQuoted(column->name, '"'),
				                           config.mode != IcebergMetricsMode::COUNTS});
			}
		}
		if (column->required) {
			required_columns.emplace_back(physical_names.size(), column->name);
		}
		physical_names.push_back(PhysicalColumnName(column->id));
		types.push_back(column->type);
	}
	CopyFunctionBindInput bind_input(*info);
	auto inner_bind = inner.copy_to_bind(context, bind_input, physical_names, types);
	auto bind = make_uniq<VortexCopyBind>(inner, std::move(inner_bind), std::move(types), std::move(required_columns),
	                                      std::move(metrics_columns));
	CopyFunction function("iceberg_vortex");
	function.extension = "vortex";
	function.copy_to_initialize_global = [](ClientContext &context, FunctionData &data, const string &path) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto result = make_uniq<VortexCopyGlobal>();
		result->metrics.resize(bind.metrics_columns.size());
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
		lock_guard<mutex> guard(state.metrics_lock);
		for (idx_t i = 0; i < bind.metrics_columns.size(); i++) {
			auto &column = bind.metrics_columns[i];
			auto &vector = converted.data[column.index];
			auto &metrics = state.metrics[i];
			switch (vector.GetType().id()) {
			case LogicalTypeId::INTEGER:
				metrics.Update<int32_t>(vector, converted.size(), column.bounds);
				break;
			case LogicalTypeId::BIGINT:
				metrics.Update<int64_t>(vector, converted.size(), column.bounds);
				break;
			case LogicalTypeId::FLOAT:
				metrics.Update<float, true>(vector, converted.size(), column.bounds);
				break;
			case LogicalTypeId::DOUBLE:
				metrics.Update<double, true>(vector, converted.size(), column.bounds);
				break;
			default:
				throw InternalException("Unexpected Vortex metrics type");
			}
		}
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
			for (idx_t i = 0; i < bind.metrics_columns.size(); i++) {
				auto &column = bind.metrics_columns[i];
				auto &metrics = state.metrics[i];
				auto &stats = state.statistics->column_statistics[column.name];
				stats["num_values"] = Value::UBIGINT(state.row_count);
				stats["null_count"] = Value::UBIGINT(metrics.null_count);
				auto type = bind.types[column.index].id();
				if (type == LogicalTypeId::FLOAT || type == LogicalTypeId::DOUBLE) {
					stats["nan_count"] = Value::UBIGINT(metrics.nan_count);
				}
				if (!metrics.min.IsNull()) {
					stats["min"] = metrics.min;
					stats["max"] = metrics.max;
				}
			}
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
