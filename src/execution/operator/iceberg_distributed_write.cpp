#include "execution/operator/iceberg_distributed_write.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/exchange/physical_repartition.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"
#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

void ValidateIcebergDistributedTargetPartitionSpec(const IcebergTableMetadata &metadata, const string &operation_name) {
	for (const auto &field : metadata.GetLatestPartitionSpec().fields) {
		if (field.transform.Type() == IcebergTransformType::VOID) {
			throw NotImplementedException("Distributed Iceberg %s does not support VOID partition transforms",
			                              operation_name);
		}
	}
}

void ValidateIcebergDistributedRowDeltaSourceBaseline(const IcebergMultiFileList &file_list,
                                                      const IcebergTableMetadata &target_metadata,
                                                      const string &operation_name) {
	if (!file_list.HasDistributedScanPlan()) {
		throw InvalidInputException("Distributed Iceberg %s requires a planned source scan", operation_name);
	}
	if (file_list.GetDistributedScanTableUUID() != target_metadata.table_uuid) {
		throw TransactionException(
		    "Iceberg table identity changed between the distributed %s source scan and write planning", operation_name);
	}
	if (file_list.GetSnapshot().schema_id != target_metadata.GetCurrentSchemaId()) {
		throw TransactionException(
		    "Iceberg table schema changed between the distributed %s source scan and write planning", operation_name);
	}
	auto target_snapshot = target_metadata.GetLatestSnapshot();
	auto source_has_snapshot = file_list.DistributedScanHasSnapshot();
	if (source_has_snapshot != (target_snapshot != nullptr) ||
	    (target_snapshot && file_list.GetDistributedScanSnapshotId() != target_snapshot->snapshot_id)) {
		throw TransactionException(
		    "Iceberg table snapshot changed between the distributed %s source scan and write planning", operation_name);
	}
}

void ValidateIcebergDistributedRowDeltaSourceSpecs(const IcebergMultiFileList &file_list,
                                                   int32_t expected_partition_spec_id, const string &operation_name) {
	auto file_count = file_list.GetTotalFileCount();
	for (idx_t file_index = 0; file_index < file_count; file_index++) {
		auto source_entry = file_list.GetManifestEntry(file_index);
		auto &source_manifest = file_list.GetManifestFileForEntry(source_entry, IcebergManifestContentType::DATA);
		if (source_manifest.partition_spec_id != expected_partition_spec_id) {
			throw NotImplementedException(
			    "Distributed Iceberg %s does not support source data written with partition spec %d when the current "
			    "default partition spec is %d",
			    operation_name, source_manifest.partition_spec_id, expected_partition_spec_id);
		}
	}
}

namespace {

static constexpr uint32_t ICEBERG_ROW_DELTA_PROTOCOL_VERSION = 4;
static const string ICEBERG_ROW_DELTA_FRAGMENT_CODEC = "iceberg.row-delta-fragment";
static constexpr uint32_t ICEBERG_MERGE_PROTOCOL_VERSION = 1;
static const string ICEBERG_MERGE_FRAGMENT_CODEC = "iceberg.merge-fragment";
static const string ICEBERG_MERGE_PARTITION_FUNCTION = "__iceberg_vane_merge_partition_hash";
static const DistributedPayloadCodec ICEBERG_DATA_FILE_CODEC {"iceberg.data-file", 1};
static const DistributedPayloadCodec ICEBERG_POSITION_DELETE_FILE_CODEC {"iceberg.position-delete-file", 1};
static const DistributedPayloadCodec ICEBERG_DELETION_VECTOR_FILE_CODEC {"iceberg.deletion-vector-puffin", 1};

struct IcebergMergePartitionLocalState : FunctionLocalState {
	idx_t row_offset = 0;
};

static unique_ptr<FunctionLocalState> IcebergMergePartitionInit(ExpressionState &, const BoundFunctionExpression &,
                                                                FunctionData *) {
	return make_uniq<IcebergMergePartitionLocalState>();
}

static void IcebergMergePartitionHash(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 0) {
		throw InternalException("Iceberg distributed MERGE partition hash requires a file-path column");
	}
	const auto count = args.size();
	const auto has_null_target_key = args.ColumnCount() > 1;
	Vector file_is_null(LogicalType::BOOLEAN, count);
	Vector file_hash(LogicalType::HASH, count);
	Vector null_target_hash(LogicalType::HASH, count);
	Vector partition_hash(LogicalType::HASH, count);
	VectorOperations::IsNull(args.data[0], file_is_null, count);
	VectorOperations::Hash(args.data[0], file_hash, count);
	if (has_null_target_key) {
		VectorOperations::Hash(args.data[1], null_target_hash, count);
		for (idx_t index = 2; index < args.ColumnCount(); index++) {
			VectorOperations::CombineHash(null_target_hash, args.data[index], count);
		}
	}

	file_is_null.Flatten(count);
	file_hash.Flatten(count);
	if (has_null_target_key) {
		null_target_hash.Flatten(count);
	}
	const auto file_is_null_values = FlatVector::GetData<bool>(file_is_null);
	const auto file_hash_values = FlatVector::GetData<hash_t>(file_hash);
	const auto null_target_hash_values = has_null_target_key ? FlatVector::GetData<hash_t>(null_target_hash) : nullptr;
	auto result_values = FlatVector::GetData<hash_t>(partition_hash);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<IcebergMergePartitionLocalState>();
	for (idx_t row = 0; row < count; row++) {
		if (!file_is_null_values[row]) {
			result_values[row] = file_hash_values[row];
			continue;
		}
		auto row_hash = Hash<idx_t>(local_state.row_offset + row);
		result_values[row] = has_null_target_key ? CombineHash(null_target_hash_values[row], row_hash) : row_hash;
	}
	local_state.row_offset += count;
	result.Reference(partition_hash);
}

static ScalarFunction IcebergDistributedMergePartitionFunction() {
	auto result = ScalarFunction(ICEBERG_MERGE_PARTITION_FUNCTION, {LogicalType::VARCHAR}, LogicalType::HASH,
	                             IcebergMergePartitionHash);
	result.varargs = LogicalType::ANY;
	result.SetInitStateCallback(IcebergMergePartitionInit);
	result.SetStability(FunctionStability::VOLATILE);
	result.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return result;
}

struct IcebergDistributedRowDeltaSourceState {
	string scan_file_path;
	string data_file_path;
	idx_t record_count = 0;
	//! Portable deletion-vector-v1 bytes. Keep historical deletes compressed in
	//! the shared worker bind and decode only a file that receives new deletes.
	string existing_delete_blob;
};

struct IcebergDistributedRowDeltaBind {
	IcebergDistributedRowDeltaKind kind = IcebergDistributedRowDeltaKind::DELETE;
	int32_t iceberg_version = 0;
	bool source_is_statically_empty = false;
	string data_path;
	string artifact_namespace;
	vector<idx_t> row_id_indexes;
	idx_t copy_column_count = 0;
	optional_idx copy_row_id_index;
	string copy_operator;
	vector<IcebergDistributedRowDeltaSourceState> delete_sources;
};

struct IcebergDistributedMergeWriterBind {
	string copy_operator;
	vector<unique_ptr<Expression>> projections;
};

struct IcebergDistributedMergeActionBind {
	MergeActionCondition condition = MergeActionCondition::WHEN_MATCHED;
	MergeActionType action_type = MergeActionType::MERGE_DO_NOTHING;
	unique_ptr<Expression> predicate;
	vector<unique_ptr<Expression>> expressions;
};

struct IcebergDistributedMergeBind {
	int32_t iceberg_version = 0;
	bool target_is_statically_empty = false;
	bool worker_plan_is_statically_empty = false;
	string data_path;
	string artifact_namespace;
	vector<LogicalType> input_types;
	idx_t row_id_start = 0;
	optional_idx source_marker;
	IcebergDistributedMergeWriterBind insert_writer;
	IcebergDistributedMergeWriterBind update_writer;
	vector<IcebergDistributedMergeActionBind> actions;
	vector<IcebergDistributedRowDeltaSourceState> delete_sources;
};

static idx_t CheckedAdd(idx_t left, idx_t right, const string &description) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InvalidInputException("Iceberg distributed write %s overflow", description);
	}
	return left + right;
}

static idx_t ValidateExistingDeleteBitmaps(const map<int32_t, roaring::Roaring> &bitmaps, idx_t record_count) {
	idx_t delete_count = 0;
	for (const auto &entry : bitmaps) {
		if (entry.first < 0 || entry.second.isEmpty()) {
			throw SerializationException("Iceberg distributed v3 row-delta contains an invalid existing delete bitmap");
		}
		auto cardinality = entry.second.cardinality();
		if (cardinality > NumericLimits<idx_t>::Maximum() - delete_count) {
			throw SerializationException("Iceberg distributed v3 row-delta existing delete count overflow");
		}
		delete_count += NumericCast<idx_t>(cardinality);
		auto maximum = (static_cast<uint64_t>(entry.first) << 32) | entry.second.maximum();
		if (maximum >= record_count) {
			throw SerializationException(
			    "Iceberg distributed v3 row-delta contains an out-of-range existing delete row");
		}
	}
	return delete_count;
}

static map<int32_t, roaring::Roaring> DecodeExistingDeleteBlob(const string &blob, idx_t record_count) {
	if (blob.empty()) {
		return {};
	}
	auto bitmaps = IcebergDeletionVectorData::DecodeBlob(blob);
	ValidateExistingDeleteBitmaps(bitmaps, record_count);
	return bitmaps;
}

static void AddExistingDeleteRow(map<int32_t, roaring::Roaring> &bitmaps, int64_t row, idx_t record_count) {
	if (row < 0 || NumericCast<idx_t>(row) >= record_count) {
		throw InvalidConfigurationException(
		    "Distributed Iceberg v3 row-delta source contains an out-of-range delete row");
	}
	auto high_bits = NumericCast<int32_t>(static_cast<uint64_t>(row) >> 32);
	auto low_bits = static_cast<uint32_t>(row & 0xFFFFFFFF);
	if (!bitmaps[high_bits].addChecked(low_bits)) {
		throw InvalidConfigurationException("Distributed Iceberg v3 row-delta source contains duplicate delete rows");
	}
}

static string EncodeExistingDeleteBlob(const IcebergDeleteData &delete_data, idx_t record_count) {
	map<int32_t, roaring::Roaring> bitmaps;
	if (delete_data.type == IcebergDeleteType::DELETION_VECTOR) {
		auto &deletion_vector = static_cast<const IcebergDeletionVectorData &>(delete_data);
		for (const auto &entry : deletion_vector.bitmaps) {
			if (!bitmaps.emplace(entry.first, entry.second).second) {
				throw InvalidConfigurationException(
				    "Distributed Iceberg v3 row-delta source contains duplicate deletion-vector bitmap keys");
			}
		}
	} else {
		auto &positional_deletes = static_cast<const IcebergPositionalDeleteData &>(delete_data);
		for (auto row : positional_deletes.invalid_rows) {
			AddExistingDeleteRow(bitmaps, row, record_count);
		}
	}
	if (bitmaps.empty()) {
		return {};
	}
	for (auto &entry : bitmaps) {
		entry.second.runOptimize();
	}
	ValidateExistingDeleteBitmaps(bitmaps, record_count);
	auto blob = IcebergDeletionVectorData::ToBlob(bitmaps);
	return string(reinterpret_cast<const char *>(blob.data()), blob.size());
}

static void PopulateDistributedRowDeltaSources(vector<IcebergDistributedRowDeltaSourceState> &delete_sources,
                                               const IcebergMultiFileList &file_list, const string &operation_name) {
	if (!file_list.HasDistributedScanPlan()) {
		throw InvalidInputException("Distributed Iceberg v3 %s requires a planned source scan", operation_name);
	}
	auto file_count = file_list.GetTotalFileCount();
	auto files = file_list.GetAllFiles();
	if (files.size() != file_count) {
		throw InternalException("Distributed Iceberg v3 %s source file count changed during planning", operation_name);
	}
	delete_sources.reserve(file_count);
	for (idx_t file_index = 0; file_index < file_count; file_index++) {
		auto manifest_entry = file_list.GetManifestEntry(file_index);
		if (manifest_entry.entry.data_file.record_count < 0) {
			throw InvalidConfigurationException(
			    "Distributed Iceberg v3 %s source data file '%s' has an invalid record count", operation_name,
			    files[file_index].path);
		}
		IcebergDistributedRowDeltaSourceState source;
		source.scan_file_path = files[file_index].path;
		source.data_file_path = manifest_entry.entry.data_file.file_path;
		source.record_count = NumericCast<idx_t>(manifest_entry.entry.data_file.record_count);
		auto existing_deletes = file_list.GetExistingPositionalDeleteData(source.data_file_path);
		if (existing_deletes) {
			source.existing_delete_blob = EncodeExistingDeleteBlob(*existing_deletes, source.record_count);
		}
		delete_sources.push_back(std::move(source));
	}
}

static void ValidateIcebergSignedFileCounts(idx_t row_count, idx_t file_size_bytes, const string &description) {
	const auto max_signed = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
	if (row_count > max_signed || file_size_bytes > max_signed) {
		throw InvalidInputException("Iceberg distributed %s statistics exceed signed 64-bit limits", description);
	}
}

static void ValidateIcebergDistributedDataFileValues(const distributed::DistributedCopyFileInfo &file) {
	auto expected_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	if (file.footer_size_bytes.IsNull() || file.footer_size_bytes.type() != expected_types[3] ||
	    UBigIntValue::Get(file.footer_size_bytes) > file.file_size_bytes) {
		throw InvalidInputException("Iceberg distributed write returned invalid data-file footer statistics");
	}
	if (file.column_statistics.IsNull() || file.column_statistics.type() != expected_types[4]) {
		throw InvalidInputException("Iceberg distributed write returned invalid data-file column statistics");
	}
	case_insensitive_set_t column_names;
	for (const auto &column_entry : MapValue::GetChildren(file.column_statistics)) {
		if (column_entry.IsNull()) {
			throw InvalidInputException("Iceberg distributed write returned a null column-statistics entry");
		}
		auto &column_children = StructValue::GetChildren(column_entry);
		if (column_children.size() != 2 || column_children[0].IsNull() || column_children[1].IsNull()) {
			throw InvalidInputException("Iceberg distributed write returned an invalid column-statistics entry");
		}
		auto &column_name = StringValue::Get(column_children[0]);
		if (column_name.empty() || !column_names.insert(column_name).second) {
			throw InvalidInputException(
			    "Iceberg distributed write returned an empty or duplicate column-statistics name");
		}
		case_insensitive_set_t statistic_names;
		for (const auto &stat_entry : MapValue::GetChildren(column_children[1])) {
			if (stat_entry.IsNull()) {
				throw InvalidInputException("Iceberg distributed write returned a null per-column statistic");
			}
			auto &stat_children = StructValue::GetChildren(stat_entry);
			if (stat_children.size() != 2 || stat_children[0].IsNull() || stat_children[1].IsNull()) {
				throw InvalidInputException("Iceberg distributed write returned an invalid per-column statistic");
			}
			auto &statistic_name = StringValue::Get(stat_children[0]);
			if (statistic_name.empty() || !statistic_names.insert(statistic_name).second) {
				throw InvalidInputException(
				    "Iceberg distributed write returned an empty or duplicate per-column statistic name");
			}
			StringValue::Get(stat_children[1]);
		}
	}
	if (file.partition_keys.type() != expected_types[5]) {
		throw InvalidInputException("Iceberg distributed write returned invalid data-file partition keys");
	}
}

static string BytesFromStream(MemoryStream &stream) {
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static MemoryStream StreamFromBytes(const string &bytes) {
	MemoryStream stream(Allocator::DefaultAllocator(), bytes.size());
	stream.WriteData(reinterpret_cast<const_data_ptr_t>(bytes.data()), bytes.size());
	stream.Rewind();
	return stream;
}

static string EncodePathComponent(const string &input) {
	static const char *HEX = "0123456789abcdef";
	string result;
	result.reserve(input.size() * 2);
	for (auto character : input) {
		auto byte = static_cast<uint8_t>(character);
		result.push_back(HEX[byte >> 4]);
		result.push_back(HEX[byte & 0x0f]);
	}
	return result;
}

static void ValidateDistributedRowRewriteCopyShape(const PhysicalCopyToFile &copy, const string &operation_name) {
	// Standalone callback finalization does not own DuckDB's pipeline Finalize context. Keep the worker COPY on the
	// multi-file paths used by Iceberg, which finalize their individual file states without the single-file lifecycle.
	auto partitioned = copy.partition_output && copy.write_empty_file && !copy.rotate && !copy.per_thread_output;
	auto rotating = !copy.partition_output && !copy.write_empty_file && copy.rotate && !copy.per_thread_output &&
	                copy.file_size_bytes.IsValid();
	if (!partitioned && !rotating) {
		throw NotImplementedException(
		    "Distributed Iceberg %s requires the canonical partitioned or rotating COPY writer", operation_name);
	}
	if (copy.use_tmp_file) {
		throw NotImplementedException("Distributed Iceberg %s does not support temporary COPY output", operation_name);
	}
	auto statistics_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	if (copy.return_type != CopyFunctionReturnType::WRITTEN_FILE_STATISTICS || copy.types != statistics_types) {
		throw SerializationException("Distributed Iceberg %s COPY must return written-file statistics", operation_name);
	}
	for (const auto &type : copy.expected_types) {
		if (TypeVisitor::Contains(type, LogicalTypeId::VARIANT)) {
			throw NotImplementedException("Distributed Iceberg %s does not support VARIANT columns because the Vane "
			                              "repartition transport cannot preserve raw VARIANT values",
			                              operation_name);
		}
	}
}

static optional_idx GetDistributedUpdateRowIdIndex(const PhysicalCopyToFile &copy, int32_t iceberg_version) {
	if (copy.names.size() != copy.expected_types.size()) {
		throw SerializationException("Distributed Iceberg UPDATE COPY names and types have different widths");
	}
	optional_idx row_id_index;
	for (idx_t index = 0; index < copy.names.size(); index++) {
		if (StringUtil::CIEquals(copy.names[index], "_last_updated_sequence_number")) {
			throw SerializationException(
			    "Distributed Iceberg UPDATE must derive _last_updated_sequence_number from the new data sequence");
		}
		if (!StringUtil::CIEquals(copy.names[index], "_row_id")) {
			continue;
		}
		if (row_id_index.IsValid() || copy.expected_types[index] != LogicalType::BIGINT) {
			throw SerializationException("Distributed Iceberg UPDATE COPY has an invalid _row_id column");
		}
		row_id_index = index;
	}
	if (iceberg_version == 2) {
		if (row_id_index.IsValid()) {
			throw SerializationException("Distributed Iceberg v2 UPDATE unexpectedly writes _row_id");
		}
		return optional_idx();
	}
	if (iceberg_version != 3 || !row_id_index.IsValid()) {
		throw SerializationException("Distributed Iceberg v3 UPDATE must preserve exactly one BIGINT _row_id column");
	}
	return row_id_index;
}

static string SerializeShallowCopy(const PhysicalCopyToFile &copy, const string &operation_name) {
	ValidateDistributedRowRewriteCopyShape(copy, operation_name);
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(100, "type", copy.type);
	serializer.WriteProperty(101, "types", copy.types);
	serializer.WriteProperty(102, "estimated_cardinality", copy.estimated_cardinality);
	copy.SerializeOperatorData(serializer);
	serializer.WriteList(198, "children", 0, [&](Serializer::List &, idx_t) {});
	serializer.End();
	return BytesFromStream(stream);
}

static string SerializeBind(const IcebergDistributedRowDeltaBind &bind) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "kind", static_cast<uint8_t>(bind.kind));
	serializer.WriteProperty(2, "data_path", bind.data_path);
	serializer.WriteProperty(3, "artifact_namespace", bind.artifact_namespace);
	serializer.WriteProperty(4, "row_id_indexes", bind.row_id_indexes);
	serializer.WriteProperty(5, "copy_column_count", bind.copy_column_count);
	serializer.WriteProperty(6, "copy_operator", bind.copy_operator);
	serializer.WriteProperty(7, "iceberg_version", bind.iceberg_version);
	serializer.WriteList(8, "delete_sources", bind.delete_sources.size(), [&](Serializer::List &list, idx_t index) {
		auto &source = bind.delete_sources[index];
		list.WriteObject([&](Serializer &object) {
			object.WriteProperty(1, "scan_file_path", source.scan_file_path);
			object.WriteProperty(2, "data_file_path", source.data_file_path);
			object.WriteProperty(3, "record_count", source.record_count);
			object.WriteProperty(4, "existing_delete_blob", source.existing_delete_blob);
		});
	});
	serializer.WriteProperty(9, "has_copy_row_id_index", bind.copy_row_id_index.IsValid());
	serializer.WriteProperty(10, "copy_row_id_index",
	                         bind.copy_row_id_index.IsValid() ? bind.copy_row_id_index.GetIndex() : 0);
	serializer.WriteProperty(11, "source_is_statically_empty", bind.source_is_statically_empty);
	serializer.End();
	return BytesFromStream(stream);
}

static IcebergDistributedRowDeltaBind DeserializeBind(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed row-delta bind data is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto kind = deserializer.ReadProperty<uint8_t>(1, "kind");
	if (kind > static_cast<uint8_t>(IcebergDistributedRowDeltaKind::UPDATE)) {
		throw SerializationException("Invalid Iceberg distributed row-delta kind %d", kind);
	}
	IcebergDistributedRowDeltaBind result;
	result.kind = static_cast<IcebergDistributedRowDeltaKind>(kind);
	result.data_path = deserializer.ReadProperty<string>(2, "data_path");
	result.artifact_namespace = deserializer.ReadProperty<string>(3, "artifact_namespace");
	result.row_id_indexes = deserializer.ReadProperty<vector<idx_t>>(4, "row_id_indexes");
	result.copy_column_count = deserializer.ReadProperty<idx_t>(5, "copy_column_count");
	result.copy_operator = deserializer.ReadProperty<string>(6, "copy_operator");
	result.iceberg_version = deserializer.ReadProperty<int32_t>(7, "iceberg_version");
	deserializer.ReadList(8, "delete_sources", [&](Deserializer::List &list, idx_t) {
		IcebergDistributedRowDeltaSourceState source;
		list.ReadObject([&](Deserializer &object) {
			source.scan_file_path = object.ReadProperty<string>(1, "scan_file_path");
			source.data_file_path = object.ReadProperty<string>(2, "data_file_path");
			source.record_count = object.ReadProperty<idx_t>(3, "record_count");
			source.existing_delete_blob = object.ReadProperty<string>(4, "existing_delete_blob");
		});
		result.delete_sources.push_back(std::move(source));
	});
	auto has_copy_row_id_index = deserializer.ReadProperty<bool>(9, "has_copy_row_id_index");
	auto copy_row_id_index = deserializer.ReadProperty<idx_t>(10, "copy_row_id_index");
	if (has_copy_row_id_index) {
		result.copy_row_id_index = copy_row_id_index;
	} else if (copy_row_id_index != 0) {
		throw SerializationException("Iceberg distributed row-delta bind has a non-canonical row-id index");
	}
	result.source_is_statically_empty = deserializer.ReadProperty<bool>(11, "source_is_statically_empty");
	deserializer.End();
	if (result.data_path.empty() || result.artifact_namespace.empty() || result.row_id_indexes.size() != 2) {
		throw SerializationException("Invalid Iceberg distributed row-delta bind data");
	}
	if (result.kind == IcebergDistributedRowDeltaKind::UPDATE &&
	    (result.copy_operator.empty() || result.copy_column_count == 0)) {
		throw SerializationException("Iceberg distributed UPDATE is missing its COPY writer");
	}
	if (result.kind == IcebergDistributedRowDeltaKind::DELETE &&
	    (!result.copy_operator.empty() || result.copy_column_count != 0)) {
		throw SerializationException("Iceberg distributed DELETE unexpectedly contains a COPY writer");
	}
	if (result.source_is_statically_empty &&
	    (result.kind != IcebergDistributedRowDeltaKind::UPDATE || !result.delete_sources.empty())) {
		throw SerializationException("Iceberg distributed row-delta bind has invalid statically-empty source state");
	}
	if (result.kind == IcebergDistributedRowDeltaKind::UPDATE && result.iceberg_version != 2 &&
	    result.iceberg_version != 3) {
		throw NotImplementedException("Distributed Iceberg UPDATE supports format-version 2 and 3 tables only");
	}
	if (result.kind == IcebergDistributedRowDeltaKind::DELETE && result.iceberg_version != 2 &&
	    result.iceberg_version != 3) {
		throw NotImplementedException("Distributed Iceberg DELETE supports format-version 2 and 3 tables only");
	}
	if (result.iceberg_version == 2 && !result.delete_sources.empty()) {
		throw SerializationException("Iceberg distributed v2 row-delta bind unexpectedly contains v3 source state");
	}
	if ((result.iceberg_version == 3) != result.copy_row_id_index.IsValid() &&
	    result.kind == IcebergDistributedRowDeltaKind::UPDATE) {
		throw SerializationException("Iceberg distributed UPDATE bind has invalid row-lineage state");
	}
	if (result.copy_row_id_index.IsValid() && result.copy_row_id_index.GetIndex() >= result.copy_column_count) {
		throw SerializationException("Iceberg distributed UPDATE bind has an out-of-range row-lineage column");
	}
	unordered_set<string> scan_paths;
	unordered_set<string> data_paths;
	for (const auto &source : result.delete_sources) {
		if (source.scan_file_path.empty() || source.data_file_path.empty() ||
		    !scan_paths.insert(source.scan_file_path).second || !data_paths.insert(source.data_file_path).second) {
			throw SerializationException("Iceberg distributed v3 row-delta contains invalid source-file state");
		}
		DecodeExistingDeleteBlob(source.existing_delete_blob, source.record_count);
	}
	return result;
}

static void SerializeMergeWriter(Serializer &serializer, const IcebergDistributedMergeWriterBind &writer) {
	serializer.WriteProperty(1, "copy_operator", writer.copy_operator);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(2, "projections", writer.projections);
}

static IcebergDistributedMergeWriterBind DeserializeMergeWriter(Deserializer &deserializer) {
	IcebergDistributedMergeWriterBind result;
	result.copy_operator = deserializer.ReadProperty<string>(1, "copy_operator");
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(2, "projections", result.projections);
	return result;
}

static string SerializeMergeBind(const IcebergDistributedMergeBind &bind) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "iceberg_version", bind.iceberg_version);
	serializer.WriteProperty(2, "target_is_statically_empty", bind.target_is_statically_empty);
	serializer.WriteProperty(3, "data_path", bind.data_path);
	serializer.WriteProperty(4, "artifact_namespace", bind.artifact_namespace);
	serializer.WriteProperty(5, "input_types", bind.input_types);
	serializer.WriteProperty(6, "row_id_start", bind.row_id_start);
	serializer.WriteProperty(7, "has_source_marker", bind.source_marker.IsValid());
	serializer.WriteProperty(8, "source_marker", bind.source_marker.IsValid() ? bind.source_marker.GetIndex() : 0);
	serializer.WriteObject(9, "insert_writer",
	                       [&](Serializer &object) { SerializeMergeWriter(object, bind.insert_writer); });
	serializer.WriteObject(10, "update_writer",
	                       [&](Serializer &object) { SerializeMergeWriter(object, bind.update_writer); });
	serializer.WriteList(11, "actions", bind.actions.size(), [&](Serializer::List &list, idx_t index) {
		auto &action = bind.actions[index];
		list.WriteObject([&](Serializer &object) {
			object.WriteProperty(1, "condition", action.condition);
			object.WriteProperty(2, "action_type", action.action_type);
			object.WritePropertyWithDefault<unique_ptr<Expression>>(3, "predicate", action.predicate);
			object.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(4, "expressions", action.expressions);
		});
	});
	serializer.WriteList(12, "delete_sources", bind.delete_sources.size(), [&](Serializer::List &list, idx_t index) {
		auto &source = bind.delete_sources[index];
		list.WriteObject([&](Serializer &object) {
			object.WriteProperty(1, "scan_file_path", source.scan_file_path);
			object.WriteProperty(2, "data_file_path", source.data_file_path);
			object.WriteProperty(3, "record_count", source.record_count);
			object.WriteProperty(4, "existing_delete_blob", source.existing_delete_blob);
		});
	});
	serializer.WriteProperty(13, "worker_plan_is_statically_empty", bind.worker_plan_is_statically_empty);
	serializer.End();
	return BytesFromStream(stream);
}

static IcebergDistributedMergeBind DeserializeMergeBind(ClientContext &context, const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed MERGE bind data is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(context);
	deserializer.Begin();
	IcebergDistributedMergeBind result;
	result.iceberg_version = deserializer.ReadProperty<int32_t>(1, "iceberg_version");
	result.target_is_statically_empty = deserializer.ReadProperty<bool>(2, "target_is_statically_empty");
	result.data_path = deserializer.ReadProperty<string>(3, "data_path");
	result.artifact_namespace = deserializer.ReadProperty<string>(4, "artifact_namespace");
	result.input_types = deserializer.ReadProperty<vector<LogicalType>>(5, "input_types");
	result.row_id_start = deserializer.ReadProperty<idx_t>(6, "row_id_start");
	auto has_source_marker = deserializer.ReadProperty<bool>(7, "has_source_marker");
	auto source_marker = deserializer.ReadProperty<idx_t>(8, "source_marker");
	if (has_source_marker) {
		result.source_marker = source_marker;
	} else if (source_marker != 0) {
		throw SerializationException("Iceberg distributed MERGE bind has a non-canonical source marker");
	}
	deserializer.ReadObject(9, "insert_writer",
	                        [&](Deserializer &object) { result.insert_writer = DeserializeMergeWriter(object); });
	deserializer.ReadObject(10, "update_writer",
	                        [&](Deserializer &object) { result.update_writer = DeserializeMergeWriter(object); });
	deserializer.ReadList(11, "actions", [&](Deserializer::List &list, idx_t) {
		IcebergDistributedMergeActionBind action;
		list.ReadObject([&](Deserializer &object) {
			action.condition = object.ReadProperty<MergeActionCondition>(1, "condition");
			action.action_type = object.ReadProperty<MergeActionType>(2, "action_type");
			object.ReadPropertyWithDefault<unique_ptr<Expression>>(3, "predicate", action.predicate);
			object.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(4, "expressions", action.expressions);
		});
		result.actions.push_back(std::move(action));
	});
	deserializer.ReadList(12, "delete_sources", [&](Deserializer::List &list, idx_t) {
		IcebergDistributedRowDeltaSourceState source;
		list.ReadObject([&](Deserializer &object) {
			source.scan_file_path = object.ReadProperty<string>(1, "scan_file_path");
			source.data_file_path = object.ReadProperty<string>(2, "data_file_path");
			source.record_count = object.ReadProperty<idx_t>(3, "record_count");
			source.existing_delete_blob = object.ReadProperty<string>(4, "existing_delete_blob");
		});
		result.delete_sources.push_back(std::move(source));
	});
	result.worker_plan_is_statically_empty = deserializer.ReadProperty<bool>(13, "worker_plan_is_statically_empty");
	deserializer.End();

	if ((result.iceberg_version != 2 && result.iceberg_version != 3) || result.data_path.empty() ||
	    result.artifact_namespace.empty() || result.actions.empty()) {
		throw SerializationException("Invalid Iceberg distributed MERGE bind data");
	}
	auto row_id_count = result.iceberg_version >= 3 ? 3 : 2;
	if (result.row_id_start > result.input_types.size() ||
	    row_id_count > result.input_types.size() - result.row_id_start) {
		throw SerializationException("Iceberg distributed MERGE row identifiers are out of bounds");
	}
	auto file_path_index = result.row_id_start + (result.iceberg_version >= 3 ? 1 : 0);
	if (result.input_types[file_path_index] != LogicalType::VARCHAR ||
	    result.input_types[file_path_index + 1] != LogicalType::BIGINT) {
		throw SerializationException("Iceberg distributed MERGE row identifiers have invalid types");
	}
	if (result.iceberg_version >= 3 && result.input_types[result.row_id_start] != LogicalType::BIGINT) {
		throw SerializationException("Iceberg distributed v3 MERGE _row_id has an invalid type");
	}
	if (result.source_marker.IsValid() &&
	    (result.source_marker.GetIndex() >= result.row_id_start ||
	     result.input_types[result.source_marker.GetIndex()] != LogicalType::INTEGER)) {
		throw SerializationException("Iceberg distributed MERGE source marker has an invalid position or type");
	}
	bool has_insert = false;
	bool has_update = false;
	bool has_row_delta = false;
	bool has_not_matched_by_source = false;
	for (const auto &action : result.actions) {
		switch (action.condition) {
		case MergeActionCondition::WHEN_MATCHED:
		case MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET:
			break;
		case MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE:
			has_not_matched_by_source = true;
			break;
		default:
			throw SerializationException("Iceberg distributed MERGE has an invalid match condition");
		}
		if (action.predicate && action.predicate->return_type != LogicalType::BOOLEAN) {
			throw SerializationException("Iceberg distributed MERGE action predicate is not BOOLEAN");
		}
		switch (action.action_type) {
		case MergeActionType::MERGE_INSERT:
			has_insert = true;
			if (action.condition != MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET || action.expressions.empty()) {
				throw SerializationException("Iceberg distributed MERGE has an invalid INSERT action");
			}
			break;
		case MergeActionType::MERGE_UPDATE:
			has_update = true;
			has_row_delta = true;
			if (action.condition == MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET || action.expressions.empty()) {
				throw SerializationException("Iceberg distributed MERGE has an invalid UPDATE action");
			}
			break;
		case MergeActionType::MERGE_DELETE:
			has_row_delta = true;
			if (action.condition == MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET || !action.expressions.empty()) {
				throw SerializationException("Iceberg distributed MERGE has an invalid DELETE action");
			}
			break;
		case MergeActionType::MERGE_ERROR:
			if (action.expressions.size() > 1 ||
			    (!action.expressions.empty() && action.expressions[0]->return_type != LogicalType::VARCHAR)) {
				throw SerializationException("Iceberg distributed MERGE has an invalid error expression");
			}
			break;
		case MergeActionType::MERGE_DO_NOTHING:
			if (!action.expressions.empty()) {
				throw SerializationException("Iceberg distributed MERGE DO NOTHING action has expressions");
			}
			break;
		default:
			throw SerializationException("Iceberg distributed MERGE has an invalid action type");
		}
	}
	if (has_not_matched_by_source != result.source_marker.IsValid()) {
		throw SerializationException("Iceberg distributed MERGE source marker does not match its actions");
	}
	if (has_insert != !result.insert_writer.copy_operator.empty() ||
	    has_update != !result.update_writer.copy_operator.empty()) {
		throw SerializationException("Iceberg distributed MERGE writer state does not match its actions");
	}
	if ((!has_insert && !result.insert_writer.projections.empty()) ||
	    (!has_update && !result.update_writer.projections.empty())) {
		throw SerializationException("Iceberg distributed MERGE has projections without a writer");
	}
	if (result.target_is_statically_empty && !result.delete_sources.empty()) {
		throw SerializationException("Iceberg distributed MERGE has delete sources for an empty target");
	}
	if (result.iceberg_version == 2 && !result.delete_sources.empty()) {
		throw SerializationException("Iceberg distributed v2 MERGE unexpectedly contains v3 source state");
	}
	if (!has_row_delta && !result.delete_sources.empty()) {
		throw SerializationException("Iceberg distributed MERGE contains unused delete-source state");
	}
	unordered_set<string> scan_paths;
	unordered_set<string> data_paths;
	for (const auto &source : result.delete_sources) {
		if (source.scan_file_path.empty() || source.data_file_path.empty() ||
		    !scan_paths.insert(source.scan_file_path).second || !data_paths.insert(source.data_file_path).second) {
			throw SerializationException("Iceberg distributed v3 MERGE contains invalid source-file state");
		}
		DecodeExistingDeleteBlob(source.existing_delete_blob, source.record_count);
	}
	return result;
}

static unique_ptr<PhysicalOperator> DeserializeShallowCopy(ClientContext &context, PhysicalPlan &physical_plan,
                                                           const string &bytes, const string &operation_name) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed %s COPY operator is empty", operation_name);
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(context);
	deserializer.Begin();
	auto result = PhysicalOperator::Deserialize(deserializer, physical_plan);
	deserializer.End();
	if (result->type != PhysicalOperatorType::COPY_TO_FILE || !result->children.empty()) {
		throw SerializationException("Iceberg distributed %s worker bind is not a shallow COPY operator",
		                             operation_name);
	}
	ValidateDistributedRowRewriteCopyShape(result->Cast<PhysicalCopyToFile>(), operation_name);
	return result;
}

static void ValidateDistributedMergeWriterShape(const PhysicalCopyToFile &copy,
                                                const vector<unique_ptr<Expression>> &projections,
                                                idx_t action_column_count, int32_t iceberg_version, bool is_update,
                                                const string &action_name) {
	auto input_column_count = action_column_count + (is_update && iceberg_version >= 3 ? 1 : 0);
	auto writer_column_count = projections.empty() ? input_column_count : projections.size();
	if (writer_column_count != copy.expected_types.size()) {
		throw SerializationException("Distributed Iceberg MERGE %s writer has an invalid input width", action_name);
	}
	if (!is_update) {
		return;
	}

	auto row_id_index = GetDistributedUpdateRowIdIndex(copy, iceberg_version);
	if (iceberg_version < 3) {
		return;
	}
	if (!row_id_index.IsValid() || row_id_index.GetIndex() != action_column_count) {
		throw SerializationException(
		    "Distributed Iceberg v3 MERGE UPDATE writer has an invalid row-lineage column position");
	}
	if (projections.empty()) {
		return;
	}
	auto &row_id_projection = *projections[row_id_index.GetIndex()];
	if (row_id_projection.GetExpressionClass() != ExpressionClass::BOUND_REF ||
	    row_id_projection.return_type != LogicalType::BIGINT ||
	    row_id_projection.Cast<BoundReferenceExpression>().index != action_column_count) {
		throw SerializationException(
		    "Distributed Iceberg v3 MERGE UPDATE projection does not preserve the target _row_id");
	}
}

static void SerializeCopyFile(Serializer &serializer, const distributed::DistributedCopyFileInfo &file) {
	serializer.WriteProperty(1, "staging_path", file.staging_path);
	serializer.WriteProperty(2, "final_path", file.final_path);
	serializer.WriteProperty(3, "row_count", file.row_count);
	serializer.WriteProperty(4, "file_size_bytes", file.file_size_bytes);
	serializer.WriteProperty(5, "footer_size_bytes", file.footer_size_bytes);
	serializer.WriteProperty(6, "column_statistics", file.column_statistics);
	serializer.WriteProperty(7, "partition_keys", file.partition_keys);
}

static distributed::DistributedCopyFileInfo DeserializeCopyFile(Deserializer &deserializer) {
	distributed::DistributedCopyFileInfo result;
	result.staging_path = deserializer.ReadProperty<string>(1, "staging_path");
	result.final_path = deserializer.ReadProperty<string>(2, "final_path");
	result.row_count = deserializer.ReadProperty<idx_t>(3, "row_count");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(4, "file_size_bytes");
	result.footer_size_bytes = deserializer.ReadProperty<Value>(5, "footer_size_bytes");
	result.column_statistics = deserializer.ReadProperty<Value>(6, "column_statistics");
	result.partition_keys = deserializer.ReadProperty<Value>(7, "partition_keys");
	return result;
}

static void SerializeDeleteFile(Serializer &serializer, const IcebergDistributedDeleteFileResult &file) {
	serializer.WriteProperty(1, "data_file_path", file.data_file_path);
	serializer.WriteProperty(2, "delete_file_path", file.delete_file_path);
	serializer.WriteProperty(3, "is_deletion_vector", file.is_deletion_vector);
	serializer.WriteProperty(4, "new_delete_count", file.new_delete_count);
	serializer.WriteProperty(5, "delete_count", file.delete_count);
	serializer.WriteProperty(6, "file_size_bytes", file.file_size_bytes);
	serializer.WriteProperty(7, "footer_size_bytes", file.footer_size_bytes);
	serializer.WriteProperty(8, "content_offset", file.content_offset);
	serializer.WriteProperty(9, "content_size_in_bytes", file.content_size_in_bytes);
	serializer.WriteProperty(10, "pos_min_value", file.pos_min_value);
	serializer.WriteProperty(11, "pos_max_value", file.pos_max_value);
}

static IcebergDistributedDeleteFileResult DeserializeDeleteFile(Deserializer &deserializer) {
	IcebergDistributedDeleteFileResult result;
	result.data_file_path = deserializer.ReadProperty<string>(1, "data_file_path");
	result.delete_file_path = deserializer.ReadProperty<string>(2, "delete_file_path");
	result.is_deletion_vector = deserializer.ReadProperty<bool>(3, "is_deletion_vector");
	result.new_delete_count = deserializer.ReadProperty<idx_t>(4, "new_delete_count");
	result.delete_count = deserializer.ReadProperty<idx_t>(5, "delete_count");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(6, "file_size_bytes");
	result.footer_size_bytes = deserializer.ReadProperty<idx_t>(7, "footer_size_bytes");
	result.content_offset = deserializer.ReadProperty<idx_t>(8, "content_offset");
	result.content_size_in_bytes = deserializer.ReadProperty<idx_t>(9, "content_size_in_bytes");
	result.pos_min_value = deserializer.ReadProperty<idx_t>(10, "pos_min_value");
	result.pos_max_value = deserializer.ReadProperty<idx_t>(11, "pos_max_value");
	return result;
}

static string SerializeFragmentPayload(IcebergDistributedRowDeltaKind kind,
                                       const vector<distributed::DistributedCopyFileInfo> &data_files,
                                       const vector<IcebergDistributedDeleteFileResult> &delete_files) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "kind", static_cast<uint8_t>(kind));
	serializer.WriteList(2, "data_files", data_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeCopyFile(object, data_files[index]); });
	});
	serializer.WriteList(3, "delete_files", delete_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeDeleteFile(object, delete_files[index]); });
	});
	serializer.End();
	return BytesFromStream(stream);
}

static IcebergDistributedRowDeltaResult DeserializeFragmentPayload(const string &bytes,
                                                                   IcebergDistributedRowDeltaKind expected_kind) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed row-delta fragment is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto kind = deserializer.ReadProperty<uint8_t>(1, "kind");
	if (kind != static_cast<uint8_t>(expected_kind)) {
		throw SerializationException("Iceberg distributed row-delta fragment has the wrong operation kind");
	}
	IcebergDistributedRowDeltaResult result;
	deserializer.ReadList(2, "data_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.data_files.push_back(DeserializeCopyFile(object)); });
	});
	deserializer.ReadList(3, "delete_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.delete_files.push_back(DeserializeDeleteFile(object)); });
	});
	deserializer.End();
	return result;
}

static string IcebergDistributedRowDeltaRoot(ClientContext &context, const string &data_path,
                                             const string &artifact_namespace) {
	auto &fs = FileSystem::GetFileSystem(context);
	return fs.JoinPath(data_path, "_vane_row_delta_" + EncodePathComponent(artifact_namespace));
}

static string IcebergDistributedMergeRoot(ClientContext &context, const string &data_path,
                                          const string &artifact_namespace) {
	auto &fs = FileSystem::GetFileSystem(context);
	return fs.JoinPath(data_path, "_vane_merge_" + EncodePathComponent(artifact_namespace));
}

class IcebergDistributedRowDeltaGlobalState final : public DistributedWriteGlobalState {
public:
	IcebergDistributedRowDeltaGlobalState(ClientContext &context, IcebergDistributedRowDeltaBind bind_p,
	                                      const DistributedWriteTaskContext &task)
	    : bind(std::move(bind_p)), copy_plan(Allocator::Get(context)) {
		auto &fs = FileSystem::GetFileSystem(context);
		operation_root = IcebergDistributedRowDeltaRoot(context, bind.data_path, bind.artifact_namespace);
		attempt_root = fs.JoinPath(operation_root, EncodePathComponent(task.task_attempt_id));
		if (!fs.IsRemoteFile(attempt_root)) {
			fs.CreateDirectoriesRecursive(attempt_root);
		}
		if (bind.kind == IcebergDistributedRowDeltaKind::UPDATE) {
			copy_holder = DeserializeShallowCopy(context, copy_plan, bind.copy_operator, "UPDATE");
			copy = &copy_holder->Cast<PhysicalCopyToFile>();
			copy->file_path = attempt_root;
			if (copy->expected_types.size() != bind.copy_column_count) {
				throw SerializationException("Iceberg distributed UPDATE COPY input width changed during transport");
			}
			auto row_id_index = GetDistributedUpdateRowIdIndex(*copy, bind.iceberg_version);
			if (row_id_index.IsValid() != bind.copy_row_id_index.IsValid() ||
			    (row_id_index.IsValid() && row_id_index.GetIndex() != bind.copy_row_id_index.GetIndex())) {
				throw SerializationException(
				    "Iceberg distributed UPDATE COPY row-lineage state changed during transport");
			}
		}
		for (idx_t index = 0; index < bind.delete_sources.size(); index++) {
			if (!delete_source_indexes.emplace(bind.delete_sources[index].scan_file_path, index).second) {
				throw SerializationException("Iceberg distributed v3 row-delta contains duplicate source-file state");
			}
		}
	}

	IcebergDistributedRowDeltaBind bind;
	PhysicalPlan copy_plan;
	unique_ptr<PhysicalOperator> copy_holder;
	optional_ptr<PhysicalCopyToFile> copy;
	string operation_root;
	string attempt_root;
	unordered_map<string, idx_t> delete_source_indexes;
	mutex copy_lock;
	bool copy_sink_initialized = false;
	mutex lock;
	unordered_map<string, vector<idx_t>> deleted_rows;
	idx_t affected_rows = 0;
};

class IcebergDistributedRowDeltaLocalState final : public DistributedWriteLocalState {
public:
	unique_ptr<LocalSinkState> copy_state;
	unordered_map<string, vector<idx_t>> deleted_rows;
	idx_t affected_rows = 0;
};

class IcebergDistributedMergeCopyWriter {
public:
	IcebergDistributedMergeCopyWriter(ClientContext &context, const string &copy_operator, string output_path_p)
	    : plan(Allocator::Get(context)), output_path(std::move(output_path_p)) {
		holder = DeserializeShallowCopy(context, plan, copy_operator, "MERGE");
		copy = &holder->Cast<PhysicalCopyToFile>();
		copy->file_path = output_path;
		auto &fs = FileSystem::GetFileSystem(context);
		if (!fs.IsRemoteFile(output_path)) {
			fs.CreateDirectoriesRecursive(output_path);
		}
	}

	PhysicalPlan plan;
	unique_ptr<PhysicalOperator> holder;
	optional_ptr<PhysicalCopyToFile> copy;
	string output_path;
	mutex lock;
	bool sink_initialized = false;
};

struct IcebergDistributedMergeCopyLocalState {
	unique_ptr<LocalSinkState> sink_state;
	unique_ptr<ExpressionExecutor> projection_executor;
	DataChunk projected_chunk;
	DataChunk cast_chunk;
};

struct IcebergDistributedMergeActionLocalState {
	unique_ptr<ExpressionExecutor> predicate_executor;
	unique_ptr<ExpressionExecutor> expression_executor;
	DataChunk expression_chunk;
	DataChunk update_chunk;
};

class IcebergDistributedMergeGlobalState final : public DistributedWriteGlobalState {
public:
	IcebergDistributedMergeGlobalState(ClientContext &context, IcebergDistributedMergeBind bind_p,
	                                   const DistributedWriteTaskContext &task)
	    : bind(std::move(bind_p)) {
		auto &fs = FileSystem::GetFileSystem(context);
		auto operation_root = IcebergDistributedMergeRoot(context, bind.data_path, bind.artifact_namespace);
		attempt_root = fs.JoinPath(operation_root, EncodePathComponent(task.task_attempt_id));
		if (!fs.IsRemoteFile(attempt_root)) {
			fs.CreateDirectoriesRecursive(attempt_root);
		}
		if (!bind.insert_writer.copy_operator.empty()) {
			insert_writer = make_uniq<IcebergDistributedMergeCopyWriter>(context, bind.insert_writer.copy_operator,
			                                                             fs.JoinPath(attempt_root, "insert"));
		}
		if (!bind.update_writer.copy_operator.empty()) {
			update_writer = make_uniq<IcebergDistributedMergeCopyWriter>(context, bind.update_writer.copy_operator,
			                                                             fs.JoinPath(attempt_root, "update"));
		}
		for (const auto &action : bind.actions) {
			if (action.action_type == MergeActionType::MERGE_INSERT) {
				ValidateDistributedMergeWriterShape(*insert_writer->copy, bind.insert_writer.projections,
				                                    action.expressions.size(), bind.iceberg_version, false, "INSERT");
			} else if (action.action_type == MergeActionType::MERGE_UPDATE) {
				ValidateDistributedMergeWriterShape(*update_writer->copy, bind.update_writer.projections,
				                                    action.expressions.size(), bind.iceberg_version, true, "UPDATE");
			}
		}
		for (idx_t index = 0; index < bind.delete_sources.size(); index++) {
			if (!delete_source_indexes.emplace(bind.delete_sources[index].scan_file_path, index).second) {
				throw SerializationException("Iceberg distributed v3 MERGE contains duplicate source-file state");
			}
		}
	}

	IcebergDistributedMergeBind bind;
	unique_ptr<IcebergDistributedMergeCopyWriter> insert_writer;
	unique_ptr<IcebergDistributedMergeCopyWriter> update_writer;
	string attempt_root;
	unordered_map<string, idx_t> delete_source_indexes;
	mutex lock;
	unordered_map<string, vector<idx_t>> deleted_rows;
	mutex modified_rows_lock;
	unordered_map<string, unordered_set<idx_t>> modified_rows;
	idx_t inserted_rows = 0;
	idx_t updated_rows = 0;
	idx_t deleted_row_count = 0;
};

class IcebergDistributedMergeLocalState final : public DistributedWriteLocalState {
public:
	IcebergDistributedMergeLocalState(ExecutionContext &context, const IcebergDistributedMergeGlobalState &global) {
		action_states.reserve(global.bind.actions.size());
		for (const auto &action : global.bind.actions) {
			auto state = make_uniq<IcebergDistributedMergeActionLocalState>();
			if (action.predicate) {
				state->predicate_executor = make_uniq<ExpressionExecutor>(context.client, *action.predicate);
			}
			if (!action.expressions.empty()) {
				state->expression_executor = make_uniq<ExpressionExecutor>(context.client, action.expressions);
				vector<LogicalType> expression_types;
				for (const auto &expression : action.expressions) {
					expression_types.push_back(expression->return_type);
				}
				state->expression_chunk.Initialize(context.client, expression_types);
				if (action.action_type == MergeActionType::MERGE_UPDATE && global.bind.iceberg_version >= 3) {
					expression_types.push_back(LogicalType::BIGINT);
					state->update_chunk.Initialize(context.client, expression_types);
				}
			}
			action_states.push_back(std::move(state));
		}
		InitializeCopyState(context, global.insert_writer.get(), global.bind.insert_writer, insert_copy);
		InitializeCopyState(context, global.update_writer.get(), global.bind.update_writer, update_copy);
	}

	static void InitializeCopyState(ExecutionContext &context, const IcebergDistributedMergeCopyWriter *writer,
	                                const IcebergDistributedMergeWriterBind &bind,
	                                IcebergDistributedMergeCopyLocalState &state) {
		if (!writer) {
			return;
		}
		if (!bind.projections.empty()) {
			state.projection_executor = make_uniq<ExpressionExecutor>(context.client, bind.projections);
			vector<LogicalType> projected_types;
			for (const auto &projection : bind.projections) {
				projected_types.push_back(projection->return_type);
			}
			state.projected_chunk.Initialize(context.client, projected_types);
		}
		state.cast_chunk.Initialize(context.client, writer->copy->expected_types);
	}

	vector<unique_ptr<IcebergDistributedMergeActionLocalState>> action_states;
	IcebergDistributedMergeCopyLocalState insert_copy;
	IcebergDistributedMergeCopyLocalState update_copy;
	unordered_map<string, vector<idx_t>> deleted_rows;
	idx_t inserted_rows = 0;
	idx_t updated_rows = 0;
	idx_t deleted_row_count = 0;
};

static unique_ptr<DistributedWriteGlobalState> IcebergMergeInitializeGlobal(ClientContext &context,
                                                                            const DistributedExtensionWriteInfo &info,
                                                                            const DistributedWriteTaskContext &task) {
	task.Validate();
	auto bind = DeserializeMergeBind(context, info.worker_bind_data);
	return make_uniq<IcebergDistributedMergeGlobalState>(context, std::move(bind), task);
}

static unique_ptr<DistributedWriteLocalState> IcebergMergeInitializeLocal(ExecutionContext &context,
                                                                          const DistributedExtensionWriteInfo &,
                                                                          const DistributedWriteTaskContext &,
                                                                          DistributedWriteGlobalState &global_state) {
	return make_uniq<IcebergDistributedMergeLocalState>(context,
	                                                    global_state.Cast<IcebergDistributedMergeGlobalState>());
}

static void SinkDistributedMergeCopy(ExecutionContext &context, IcebergDistributedMergeCopyWriter &writer,
                                     IcebergDistributedMergeCopyLocalState &local_state, DataChunk &input) {
	if (input.size() == 0) {
		return;
	}
	reference<DataChunk> copy_input = input;
	if (local_state.projection_executor) {
		local_state.projected_chunk.Reset();
		local_state.projection_executor->Execute(input, local_state.projected_chunk);
		copy_input = local_state.projected_chunk;
	}
	if (copy_input.get().ColumnCount() != writer.copy->expected_types.size()) {
		throw InvalidInputException("Iceberg distributed MERGE writer input has an invalid width");
	}
	local_state.cast_chunk.Reset();
	for (idx_t index = 0; index < copy_input.get().ColumnCount(); index++) {
		if (copy_input.get().data[index].GetType() != writer.copy->expected_types[index]) {
			VectorOperations::Cast(context.client, copy_input.get().data[index], local_state.cast_chunk.data[index],
			                       copy_input.get().size());
		} else {
			local_state.cast_chunk.data[index].Reference(copy_input.get().data[index]);
		}
	}
	local_state.cast_chunk.SetCardinality(copy_input.get().size());
	{
		lock_guard<mutex> guard(writer.lock);
		if (!writer.sink_initialized) {
			writer.copy->sink_state = writer.copy->GetGlobalSinkState(context.client);
			writer.sink_initialized = true;
		}
	}
	if (!local_state.sink_state) {
		local_state.sink_state = writer.copy->GetLocalSinkState(context);
	}
	InterruptState interrupt_state;
	OperatorSinkInput sink_input {*writer.copy->sink_state, *local_state.sink_state, interrupt_state};
	if (writer.copy->Sink(context, local_state.cast_chunk, sink_input) != SinkResultType::NEED_MORE_INPUT) {
		throw InternalException("Iceberg distributed MERGE COPY writer stopped before consuming its input");
	}
}

static void CombineDistributedMergeCopy(ExecutionContext &context, IcebergDistributedMergeCopyWriter *writer,
                                        IcebergDistributedMergeCopyLocalState &local_state) {
	if (!writer || !local_state.sink_state) {
		return;
	}
	InterruptState interrupt_state;
	OperatorSinkCombineInput combine_input {*writer->copy->sink_state, *local_state.sink_state, interrupt_state};
	if (writer->copy->Combine(context, combine_input) != SinkCombineResultType::FINISHED) {
		throw InternalException("Iceberg distributed MERGE COPY combine did not finish synchronously");
	}
}

static void CollectDistributedMergeDeletes(IcebergDistributedMergeGlobalState &global_state,
                                           IcebergDistributedMergeLocalState &local_state, DataChunk &input,
                                           MergeActionType action_type) {
	auto file_path_index = global_state.bind.row_id_start + (global_state.bind.iceberg_version >= 3 ? 1 : 0);
	auto row_position_index = file_path_index + 1;
	lock_guard<mutex> modified_rows_guard(global_state.modified_rows_lock);
	for (idx_t row = 0; row < input.size(); row++) {
		auto file_value = input.GetValue(file_path_index, row);
		auto position_value = input.GetValue(row_position_index, row);
		if (file_value.IsNull() || position_value.IsNull()) {
			throw InvalidInputException("Iceberg distributed MERGE received a NULL target row identifier");
		}
		auto file_path = file_value.GetValue<string>();
		auto position = position_value.GetValue<int64_t>();
		if (file_path.empty() || position < 0) {
			throw InvalidInputException("Iceberg distributed MERGE received an invalid target row identifier");
		}
		if (global_state.bind.iceberg_version >= 3) {
			auto source = global_state.delete_source_indexes.find(file_path);
			if (source == global_state.delete_source_indexes.end() ||
			    NumericCast<idx_t>(position) >= global_state.bind.delete_sources[source->second].record_count) {
				throw InvalidInputException(
				    "Iceberg distributed v3 MERGE received a target row outside its planned source files");
			}
			if (action_type == MergeActionType::MERGE_UPDATE) {
				auto row_id = input.GetValue(global_state.bind.row_id_start, row);
				if (row_id.IsNull() || row_id.GetValue<int64_t>() < 0) {
					throw InvalidInputException("Iceberg distributed v3 MERGE UPDATE received an invalid _row_id");
				}
			}
		}
		if (!global_state.modified_rows[file_path].insert(NumericCast<idx_t>(position)).second) {
			throw InvalidInputException("The same Iceberg row was modified multiple times in one distributed MERGE; "
			                            "eliminate duplicate matches");
		}
		local_state.deleted_rows[std::move(file_path)].push_back(NumericCast<idx_t>(position));
	}
}

static void ExecuteDistributedMergeAction(ExecutionContext &context, IcebergDistributedMergeGlobalState &global_state,
                                          IcebergDistributedMergeLocalState &local_state, idx_t action_index,
                                          DataChunk &input) {
	auto &action = global_state.bind.actions[action_index];
	auto &action_state = *local_state.action_states[action_index];
	switch (action.action_type) {
	case MergeActionType::MERGE_INSERT:
		action_state.expression_chunk.Reset();
		action_state.expression_executor->Execute(input, action_state.expression_chunk);
		SinkDistributedMergeCopy(context, *global_state.insert_writer, local_state.insert_copy,
		                         action_state.expression_chunk);
		local_state.inserted_rows =
		    CheckedAdd(local_state.inserted_rows, input.size(), "MERGE worker inserted row count");
		return;
	case MergeActionType::MERGE_UPDATE: {
		CollectDistributedMergeDeletes(global_state, local_state, input, action.action_type);
		action_state.expression_chunk.Reset();
		action_state.expression_executor->Execute(input, action_state.expression_chunk);
		reference<DataChunk> copy_input = action_state.expression_chunk;
		if (global_state.bind.iceberg_version >= 3) {
			action_state.update_chunk.Reset();
			for (idx_t column = 0; column < action_state.expression_chunk.ColumnCount(); column++) {
				action_state.update_chunk.data[column].Reference(action_state.expression_chunk.data[column]);
			}
			action_state.update_chunk.data.back().Reference(input.data[global_state.bind.row_id_start]);
			action_state.update_chunk.SetCardinality(input.size());
			copy_input = action_state.update_chunk;
		}
		SinkDistributedMergeCopy(context, *global_state.update_writer, local_state.update_copy, copy_input);
		local_state.updated_rows = CheckedAdd(local_state.updated_rows, input.size(), "MERGE worker updated row count");
		return;
	}
	case MergeActionType::MERGE_DELETE:
		CollectDistributedMergeDeletes(global_state, local_state, input, action.action_type);
		local_state.deleted_row_count =
		    CheckedAdd(local_state.deleted_row_count, input.size(), "MERGE worker deleted row count");
		return;
	case MergeActionType::MERGE_ERROR: {
		string merge_condition = MergeIntoStatement::ActionConditionToString(action.condition);
		if (action.predicate) {
			merge_condition += " AND " + action.predicate->ToString();
		}
		if (action_state.expression_executor) {
			action_state.expression_chunk.Reset();
			action_state.expression_executor->Execute(input, action_state.expression_chunk);
			merge_condition += ": " + action_state.expression_chunk.GetValue(0, 0).ToString();
		}
		throw ConstraintException("Merge error condition %s", merge_condition);
	}
	case MergeActionType::MERGE_DO_NOTHING:
		return;
	default:
		throw InternalException("Unsupported Iceberg distributed MERGE action");
	}
}

static void ExecuteDistributedMergeCondition(ExecutionContext &context,
                                             IcebergDistributedMergeGlobalState &global_state,
                                             IcebergDistributedMergeLocalState &local_state,
                                             MergeActionCondition condition, DataChunk &input,
                                             const SelectionVector &condition_selection, idx_t condition_count) {
	if (condition_count == 0) {
		return;
	}
	DataChunk condition_chunk;
	condition_chunk.Initialize(context.client, global_state.bind.input_types);
	condition_chunk.Slice(input, condition_selection, condition_count);
	SelectionVector current_selection;
	SelectionVector selected_selection(STANDARD_VECTOR_SIZE);
	SelectionVector remaining_selection(STANDARD_VECTOR_SIZE);
	idx_t current_count = condition_count;
	for (idx_t action_index = 0; action_index < global_state.bind.actions.size(); action_index++) {
		auto &action = global_state.bind.actions[action_index];
		if (action.condition != condition || current_count == 0) {
			continue;
		}
		auto &action_state = *local_state.action_states[action_index];
		idx_t selected_count;
		if (action_state.predicate_executor) {
			selected_count = action_state.predicate_executor->SelectExpression(
			    condition_chunk, selected_selection, remaining_selection, current_selection, current_count);
			if (selected_count == 0) {
				continue;
			}
			current_count -= selected_count;
			current_selection.Initialize(remaining_selection);
		} else {
			selected_selection.Initialize(current_selection);
			selected_count = current_count;
			current_count = 0;
		}
		DataChunk action_chunk;
		action_chunk.Initialize(context.client, global_state.bind.input_types);
		action_chunk.Slice(condition_chunk, selected_selection, selected_count);
		ExecuteDistributedMergeAction(context, global_state, local_state, action_index, action_chunk);
	}
}

static void IcebergMergeSink(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                             const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                             DistributedWriteLocalState &local_state_p, DataChunk &input) {
	auto &global_state = global_state_p.Cast<IcebergDistributedMergeGlobalState>();
	auto &local_state = local_state_p.Cast<IcebergDistributedMergeLocalState>();
	if (global_state.bind.worker_plan_is_statically_empty) {
		if (input.size() != 0) {
			throw InvalidInputException("Iceberg distributed MERGE received rows from a statically empty worker plan");
		}
		return;
	}
	if (input.GetTypes() != global_state.bind.input_types) {
		throw InvalidInputException("Iceberg distributed MERGE worker input schema changed during transport");
	}
	SelectionVector matched(STANDARD_VECTOR_SIZE);
	SelectionVector not_matched_by_target(STANDARD_VECTOR_SIZE);
	SelectionVector not_matched_by_source(STANDARD_VECTOR_SIZE);
	idx_t matched_count = 0;
	idx_t not_matched_by_target_count = 0;
	idx_t not_matched_by_source_count = 0;
	UnifiedVectorFormat target_marker_data;
	input.data[global_state.bind.row_id_start].ToUnifiedFormat(input.size(), target_marker_data);
	if (global_state.bind.source_marker.IsValid()) {
		UnifiedVectorFormat source_marker_data;
		input.data[global_state.bind.source_marker.GetIndex()].ToUnifiedFormat(input.size(), source_marker_data);
		for (idx_t row = 0; row < input.size(); row++) {
			auto source_index = source_marker_data.sel->get_index(row);
			auto target_index = target_marker_data.sel->get_index(row);
			if (!source_marker_data.validity.RowIsValid(source_index)) {
				not_matched_by_source.set_index(not_matched_by_source_count++, row);
			} else if (!target_marker_data.validity.RowIsValid(target_index)) {
				not_matched_by_target.set_index(not_matched_by_target_count++, row);
			} else {
				matched.set_index(matched_count++, row);
			}
		}
	} else {
		for (idx_t row = 0; row < input.size(); row++) {
			auto target_index = target_marker_data.sel->get_index(row);
			if (target_marker_data.validity.RowIsValid(target_index)) {
				matched.set_index(matched_count++, row);
			} else {
				not_matched_by_target.set_index(not_matched_by_target_count++, row);
			}
		}
	}
	if (global_state.bind.target_is_statically_empty && (matched_count != 0 || not_matched_by_source_count != 0)) {
		throw InvalidInputException(
		    "Iceberg distributed MERGE received target rows from a statically empty target scan");
	}
	ExecuteDistributedMergeCondition(context, global_state, local_state, MergeActionCondition::WHEN_MATCHED, input,
	                                 matched, matched_count);
	ExecuteDistributedMergeCondition(context, global_state, local_state,
	                                 MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET, input, not_matched_by_target,
	                                 not_matched_by_target_count);
	ExecuteDistributedMergeCondition(context, global_state, local_state,
	                                 MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE, input, not_matched_by_source,
	                                 not_matched_by_source_count);
}

static void IcebergMergeCombine(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                                DistributedWriteLocalState &local_state_p) {
	auto &global_state = global_state_p.Cast<IcebergDistributedMergeGlobalState>();
	auto &local_state = local_state_p.Cast<IcebergDistributedMergeLocalState>();
	CombineDistributedMergeCopy(context, global_state.insert_writer.get(), local_state.insert_copy);
	CombineDistributedMergeCopy(context, global_state.update_writer.get(), local_state.update_copy);
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : local_state.deleted_rows) {
		auto &target = global_state.deleted_rows[entry.first];
		target.insert(target.end(), entry.second.begin(), entry.second.end());
	}
	global_state.inserted_rows =
	    CheckedAdd(global_state.inserted_rows, local_state.inserted_rows, "MERGE worker inserted row count");
	global_state.updated_rows =
	    CheckedAdd(global_state.updated_rows, local_state.updated_rows, "MERGE worker updated row count");
	global_state.deleted_row_count =
	    CheckedAdd(global_state.deleted_row_count, local_state.deleted_row_count, "MERGE worker deleted row count");
}

static unique_ptr<DistributedWriteGlobalState>
IcebergRowDeltaInitializeGlobal(ClientContext &context, const DistributedExtensionWriteInfo &info,
                                const DistributedWriteTaskContext &task) {
	task.Validate();
	auto bind = DeserializeBind(info.worker_bind_data);
	return make_uniq<IcebergDistributedRowDeltaGlobalState>(context, std::move(bind), task);
}

static unique_ptr<DistributedWriteLocalState> IcebergRowDeltaInitializeLocal(ExecutionContext &,
                                                                             const DistributedExtensionWriteInfo &,
                                                                             const DistributedWriteTaskContext &,
                                                                             DistributedWriteGlobalState &) {
	return make_uniq<IcebergDistributedRowDeltaLocalState>();
}

static void IcebergRowDeltaSink(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                                DistributedWriteLocalState &local_state_p, DataChunk &input) {
	auto &global_state = global_state_p.Cast<IcebergDistributedRowDeltaGlobalState>();
	auto &local_state = local_state_p.Cast<IcebergDistributedRowDeltaLocalState>();
	if (global_state.bind.source_is_statically_empty) {
		if (input.size() != 0) {
			throw InvalidInputException("Iceberg distributed UPDATE received rows from a statically empty source");
		}
		return;
	}
	for (auto index : global_state.bind.row_id_indexes) {
		if (index >= input.ColumnCount()) {
			throw InvalidInputException("Iceberg distributed row-delta row identifier index is out of bounds");
		}
	}
	if (input.data[global_state.bind.row_id_indexes[0]].GetType() != LogicalType::VARCHAR ||
	    input.data[global_state.bind.row_id_indexes[1]].GetType() != LogicalType::BIGINT) {
		throw InvalidInputException("Iceberg distributed row-delta row identifiers have invalid types");
	}

	if (global_state.copy) {
		if (global_state.bind.copy_column_count > input.ColumnCount() ||
		    input.ColumnCount() - global_state.bind.copy_column_count != 2 ||
		    global_state.bind.row_id_indexes[0] != global_state.bind.copy_column_count ||
		    global_state.bind.row_id_indexes[1] != global_state.bind.copy_column_count + 1) {
			throw InvalidInputException("Iceberg distributed UPDATE input does not match its worker column contract");
		}
		for (idx_t index = 0; index < global_state.bind.copy_column_count; index++) {
			if (input.data[index].GetType() != global_state.copy->expected_types[index]) {
				throw InvalidInputException("Iceberg distributed UPDATE input does not match its COPY column types");
			}
		}
		{
			lock_guard<mutex> guard(global_state.copy_lock);
			if (!global_state.copy_sink_initialized) {
				global_state.copy->sink_state = global_state.copy->GetGlobalSinkState(context.client);
				global_state.copy_sink_initialized = true;
			}
		}
		if (!local_state.copy_state) {
			local_state.copy_state = global_state.copy->GetLocalSinkState(context);
		}
		DataChunk copy_chunk;
		copy_chunk.InitializeEmpty(global_state.copy->expected_types);
		for (idx_t index = 0; index < global_state.bind.copy_column_count; index++) {
			copy_chunk.data[index].Reference(input.data[index]);
		}
		copy_chunk.SetCardinality(input.size());
		InterruptState interrupt_state;
		OperatorSinkInput sink_input {*global_state.copy->sink_state, *local_state.copy_state, interrupt_state};
		auto sink_result = global_state.copy->Sink(context, copy_chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("Iceberg distributed UPDATE COPY writer stopped before consuming its input");
		}
	}

	for (idx_t row = 0; row < input.size(); row++) {
		auto file_value = input.GetValue(global_state.bind.row_id_indexes[0], row);
		auto position_value = input.GetValue(global_state.bind.row_id_indexes[1], row);
		if (file_value.IsNull() || position_value.IsNull()) {
			throw InvalidInputException("Iceberg distributed row-delta received a NULL row identifier");
		}
		auto file_path = file_value.GetValue<string>();
		auto position = position_value.GetValue<int64_t>();
		if (position < 0) {
			throw InvalidInputException("Iceberg distributed row-delta received a negative row position");
		}
		if (global_state.bind.copy_row_id_index.IsValid()) {
			auto row_id_value = input.GetValue(global_state.bind.copy_row_id_index.GetIndex(), row);
			if (row_id_value.IsNull() || row_id_value.GetValue<int64_t>() < 0) {
				throw InvalidInputException("Iceberg distributed v3 UPDATE received an invalid _row_id");
			}
		}
		if (global_state.bind.iceberg_version >= 3) {
			auto source = global_state.delete_source_indexes.find(file_path);
			if (source == global_state.delete_source_indexes.end() ||
			    NumericCast<idx_t>(position) >= global_state.bind.delete_sources[source->second].record_count) {
				throw InvalidInputException(
				    "Iceberg distributed v3 row-delta received a row identifier outside its planned source files");
			}
		}
		local_state.deleted_rows[std::move(file_path)].push_back(NumericCast<idx_t>(position));
	}
	local_state.affected_rows = CheckedAdd(local_state.affected_rows, input.size(), "worker affected row count");
}

static void IcebergRowDeltaCombine(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                   const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                                   DistributedWriteLocalState &local_state_p) {
	auto &global_state = global_state_p.Cast<IcebergDistributedRowDeltaGlobalState>();
	auto &local_state = local_state_p.Cast<IcebergDistributedRowDeltaLocalState>();
	if (local_state.copy_state) {
		InterruptState interrupt_state;
		OperatorSinkCombineInput combine_input {*global_state.copy->sink_state, *local_state.copy_state,
		                                        interrupt_state};
		if (global_state.copy->Combine(context, combine_input) != SinkCombineResultType::FINISHED) {
			throw InternalException("Iceberg distributed UPDATE COPY combine did not finish synchronously");
		}
	}
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : local_state.deleted_rows) {
		auto &target = global_state.deleted_rows[entry.first];
		target.insert(target.end(), entry.second.begin(), entry.second.end());
	}
	global_state.affected_rows =
	    CheckedAdd(global_state.affected_rows, local_state.affected_rows, "worker affected row count");
}

static IcebergDistributedDeleteFileResult WritePositionDeleteFile(ClientContext &context, const string &output_path,
                                                                  const string &data_file_path,
                                                                  const vector<idx_t> &row_positions) {
	set<idx_t> sorted_positions(row_positions.begin(), row_positions.end());
	if (sorted_positions.size() != row_positions.size()) {
		throw NotImplementedException(
		    "The same Iceberg row was modified multiple times in one distributed write; eliminate duplicate matches");
	}
	if (sorted_positions.empty()) {
		throw InternalException("Cannot write an empty Iceberg positional-delete file");
	}

	auto info = make_uniq<CopyInfo>();
	info->file_path = output_path;
	info->format = "parquet";
	info->is_from = false;
	child_list_t<Value> field_ids;
	field_ids.emplace_back("file_path", Value::INTEGER(MultiFileReader::DELETE_FILE_PATH_FIELD_ID));
	field_ids.emplace_back("pos", Value::INTEGER(MultiFileReader::DELETE_POS_FIELD_ID));
	info->options["field_ids"].push_back(Value::STRUCT(std::move(field_ids)));

	vector<string> names {"file_path", "pos"};
	vector<LogicalType> types {LogicalType::VARCHAR, LogicalType::BIGINT};
	auto &copy_function = IcebergUtils::GetCopyFunction(context, "parquet").function;
	CopyFunctionBindInput bind_input(*info);
	auto bind_data = copy_function.copy_to_bind(context, bind_input, names, types);
	auto global_state = copy_function.copy_to_initialize_global(context, *bind_data, output_path);
	CopyFunctionFileStatistics statistics;
	copy_function.copy_to_get_written_statistics(context, *bind_data, *global_state, statistics);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto local_state = copy_function.copy_to_initialize_local(execution_context, *bind_data);
	DataChunk chunk;
	chunk.Initialize(context, types);
	Value data_file_value(data_file_path);
	chunk.data[0].Reference(data_file_value);
	auto positions = FlatVector::GetData<int64_t>(chunk.data[1]);
	idx_t count = 0;
	try {
		for (auto position : sorted_positions) {
			positions[count++] = NumericCast<int64_t>(position);
			if (count == STANDARD_VECTOR_SIZE) {
				chunk.SetCardinality(count);
				copy_function.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, chunk);
				count = 0;
			}
		}
		if (count != 0) {
			chunk.SetCardinality(count);
			copy_function.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, chunk);
		}
		copy_function.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
		copy_function.copy_to_finalize(context, *bind_data, *global_state);
	} catch (...) {
		FileSystem::GetFileSystem(context).TryRemoveFile(output_path);
		throw;
	}

	auto position_stats = statistics.column_statistics.find("\"pos\"");
	if (position_stats == statistics.column_statistics.end()) {
		throw InternalException("Iceberg distributed positional-delete writer did not return position statistics");
	}
	auto min_position = position_stats->second.find("min");
	auto max_position = position_stats->second.find("max");
	if (min_position == position_stats->second.end() || max_position == position_stats->second.end() ||
	    statistics.footer_size_bytes.IsNull()) {
		throw InternalException("Iceberg distributed positional-delete writer returned incomplete statistics");
	}
	IcebergDistributedDeleteFileResult result;
	result.data_file_path = data_file_path;
	result.delete_file_path = output_path;
	result.new_delete_count = statistics.row_count;
	result.delete_count = statistics.row_count;
	result.file_size_bytes = statistics.file_size_bytes;
	result.footer_size_bytes = statistics.footer_size_bytes.GetValue<idx_t>();
	result.pos_min_value = min_position->second.GetValue<idx_t>();
	result.pos_max_value = max_position->second.GetValue<idx_t>();
	return result;
}

static IcebergDistributedDeleteFileResult WriteDeletionVectorFile(ClientContext &context, const string &output_path,
                                                                  const IcebergDistributedRowDeltaSourceState &source,
                                                                  const vector<idx_t> &new_row_positions) {
	if (new_row_positions.empty()) {
		throw NotImplementedException(
		    "The same Iceberg row was modified multiple times in one distributed write; eliminate duplicate matches");
	}

	auto bitmaps = DecodeExistingDeleteBlob(source.existing_delete_blob, source.record_count);
	auto existing_delete_count = ValidateExistingDeleteBitmaps(bitmaps, source.record_count);
	for (auto row : new_row_positions) {
		if (row >= source.record_count) {
			throw InvalidInputException(
			    "Iceberg distributed v3 row-delta attempted to replace an invalid or already-deleted row");
		}
		auto high_bits = NumericCast<int32_t>(static_cast<uint64_t>(row) >> 32);
		auto low_bits = static_cast<uint32_t>(row & 0xFFFFFFFF);
		if (!bitmaps[high_bits].addChecked(low_bits)) {
			throw InvalidInputException(
			    "Iceberg distributed v3 row-delta attempted to replace an invalid or already-deleted row");
		}
	}
	for (auto &entry : bitmaps) {
		entry.second.runOptimize();
	}
	auto delete_count = CheckedAdd(existing_delete_count, new_row_positions.size(), "deletion-vector row count");
	auto blob = IcebergDeletionVectorData::ToBlob(bitmaps);
	auto puffin = IcebergDeletionVectorData::ToPuffinFile(blob, source.data_file_path, delete_count);
	auto &fs = FileSystem::GetFileSystem(context);
	try {
		auto handle = fs.OpenFile(output_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(puffin.data(), puffin.size());
		handle->Close();
	} catch (...) {
		fs.TryRemoveFile(output_path);
		throw;
	}

	IcebergDistributedDeleteFileResult result;
	result.data_file_path = source.data_file_path;
	result.delete_file_path = output_path;
	result.is_deletion_vector = true;
	result.new_delete_count = new_row_positions.size();
	result.delete_count = delete_count;
	result.file_size_bytes = puffin.size();
	result.content_offset = 4;
	result.content_size_in_bytes = blob.size();
	result.pos_min_value =
	    NumericCast<idx_t>((static_cast<uint64_t>(bitmaps.begin()->first) << 32) | bitmaps.begin()->second.minimum());
	result.pos_max_value =
	    NumericCast<idx_t>((static_cast<uint64_t>(bitmaps.rbegin()->first) << 32) | bitmaps.rbegin()->second.maximum());
	return result;
}

static vector<distributed::DistributedCopyFileInfo>
FinalizeCopyAndReadStatistics(ClientContext &context, IcebergDistributedRowDeltaGlobalState &global_state) {
	vector<distributed::DistributedCopyFileInfo> result;
	if (!global_state.copy) {
		return result;
	}
	if (!global_state.copy_sink_initialized || !global_state.copy->sink_state) {
		throw InternalException("Iceberg distributed UPDATE received rows without initializing its COPY writer");
	}
	if (global_state.copy->FinalizeInternal(context, *global_state.copy->sink_state) != SinkFinalizeType::READY) {
		throw InternalException("Iceberg distributed UPDATE COPY finalization did not finish synchronously");
	}
	auto source_global = global_state.copy->GetGlobalSourceState(context);
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto source_local = global_state.copy->GetLocalSourceState(execution_context, *source_global);
	InterruptState interrupt_state;
	OperatorSourceInput source_input {*source_global, *source_local, interrupt_state};
	while (true) {
		DataChunk chunk;
		chunk.Initialize(context, global_state.copy->types);
		auto state = global_state.copy->GetDataInternal(execution_context, chunk, source_input);
		for (idx_t row = 0; row < chunk.size(); row++) {
			distributed::DistributedCopyFileInfo file;
			file.final_path = chunk.GetValue(0, row).GetValue<string>();
			file.row_count = chunk.GetValue(1, row).GetValue<idx_t>();
			file.file_size_bytes = chunk.GetValue(2, row).GetValue<idx_t>();
			file.footer_size_bytes = chunk.GetValue(3, row);
			file.column_statistics = chunk.GetValue(4, row);
			file.partition_keys = chunk.GetValue(5, row);
			result.push_back(std::move(file));
		}
		if (state == SourceResultType::FINISHED) {
			break;
		}
		if (state != SourceResultType::HAVE_MORE_OUTPUT) {
			throw InternalException("Iceberg distributed UPDATE COPY statistics source blocked unexpectedly");
		}
	}
	return result;
}

static vector<DistributedWriteFragment> IcebergRowDeltaFinalize(ClientContext &context,
                                                                const DistributedExtensionWriteInfo &,
                                                                const DistributedWriteTaskContext &task,
                                                                DistributedWriteGlobalState &global_state_p) {
	auto &global_state = global_state_p.Cast<IcebergDistributedRowDeltaGlobalState>();
	idx_t affected_rows;
	unordered_map<string, vector<idx_t>> deleted_rows;
	{
		lock_guard<mutex> guard(global_state.lock);
		affected_rows = global_state.affected_rows;
		deleted_rows = std::move(global_state.deleted_rows);
	}
	if (affected_rows == 0) {
		return {};
	}

	auto data_files = FinalizeCopyAndReadStatistics(context, global_state);
	vector<IcebergDistributedDeleteFileResult> delete_files;
	for (auto &entry : deleted_rows) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (global_state.bind.iceberg_version >= 3) {
			auto source_index = global_state.delete_source_indexes.find(entry.first);
			if (source_index == global_state.delete_source_indexes.end()) {
				throw InvalidInputException(
				    "Iceberg distributed v3 row-delta produced rows for an unplanned source data file");
			}
			auto file_name = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.puffin";
			auto output_path = fs.JoinPath(global_state.attempt_root, file_name);
			delete_files.push_back(WriteDeletionVectorFile(
			    context, output_path, global_state.bind.delete_sources[source_index->second], entry.second));
		} else {
			auto file_name = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.parquet";
			auto output_path = fs.JoinPath(global_state.attempt_root, file_name);
			delete_files.push_back(WritePositionDeleteFile(context, output_path, entry.first, entry.second));
		}
	}

	idx_t data_rows = 0;
	idx_t byte_count = 0;
	for (auto &file : data_files) {
		data_rows = CheckedAdd(data_rows, file.row_count, "worker data row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "worker byte count");
	}
	idx_t delete_rows = 0;
	for (auto &file : delete_files) {
		delete_rows = CheckedAdd(delete_rows, file.new_delete_count, "worker delete row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "worker byte count");
	}
	if (delete_rows != affected_rows ||
	    (global_state.bind.kind == IcebergDistributedRowDeltaKind::UPDATE && data_rows != affected_rows)) {
		throw InternalException("Iceberg distributed row-delta worker produced inconsistent affected-row counts");
	}

	DistributedWriteFragment fragment;
	fragment.fragment_id = task.query_id + "/" + task.task_attempt_id;
	fragment.payload = SerializeFragmentPayload(global_state.bind.kind, data_files, delete_files);
	fragment.row_count = affected_rows;
	fragment.byte_count = byte_count;
	for (idx_t index = 0; index < data_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "data:" + std::to_string(index);
		artifact.uri =
		    data_files[index].final_path.empty() ? data_files[index].staging_path : data_files[index].final_path;
		artifact.codec = ICEBERG_DATA_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	for (idx_t index = 0; index < delete_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "delete:" + std::to_string(index);
		artifact.uri = delete_files[index].delete_file_path;
		artifact.codec = delete_files[index].is_deletion_vector ? ICEBERG_DELETION_VECTOR_FILE_CODEC
		                                                        : ICEBERG_POSITION_DELETE_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	return {std::move(fragment)};
}

static vector<distributed::DistributedCopyFileInfo>
FinalizeMergeCopyAndReadStatistics(ClientContext &context, IcebergDistributedMergeCopyWriter *writer,
                                   idx_t expected_rows, const string &action_name) {
	vector<distributed::DistributedCopyFileInfo> result;
	if (expected_rows == 0) {
		if (writer && writer->sink_initialized) {
			throw InternalException("Iceberg distributed MERGE %s writer was initialized without rows", action_name);
		}
		return result;
	}
	if (!writer || !writer->sink_initialized || !writer->copy->sink_state) {
		throw InternalException("Iceberg distributed MERGE %s rows are missing their COPY writer", action_name);
	}
	if (writer->copy->FinalizeInternal(context, *writer->copy->sink_state) != SinkFinalizeType::READY) {
		throw InternalException("Iceberg distributed MERGE %s COPY finalization did not finish synchronously",
		                        action_name);
	}
	auto source_global = writer->copy->GetGlobalSourceState(context);
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto source_local = writer->copy->GetLocalSourceState(execution_context, *source_global);
	InterruptState interrupt_state;
	OperatorSourceInput source_input {*source_global, *source_local, interrupt_state};
	while (true) {
		DataChunk chunk;
		chunk.Initialize(context, writer->copy->types);
		auto state = writer->copy->GetDataInternal(execution_context, chunk, source_input);
		for (idx_t row = 0; row < chunk.size(); row++) {
			distributed::DistributedCopyFileInfo file;
			file.final_path = chunk.GetValue(0, row).GetValue<string>();
			file.row_count = chunk.GetValue(1, row).GetValue<idx_t>();
			file.file_size_bytes = chunk.GetValue(2, row).GetValue<idx_t>();
			file.footer_size_bytes = chunk.GetValue(3, row);
			file.column_statistics = chunk.GetValue(4, row);
			file.partition_keys = chunk.GetValue(5, row);
			result.push_back(std::move(file));
		}
		if (state == SourceResultType::FINISHED) {
			break;
		}
		if (state != SourceResultType::HAVE_MORE_OUTPUT) {
			throw InternalException("Iceberg distributed MERGE %s COPY statistics source blocked unexpectedly",
			                        action_name);
		}
	}
	return result;
}

static string SerializeMergeFragmentPayload(const IcebergDistributedMergeResult &result) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "inserted_rows", result.inserted_rows);
	serializer.WriteProperty(2, "updated_rows", result.updated_rows);
	serializer.WriteProperty(3, "deleted_rows", result.deleted_rows);
	serializer.WriteList(4, "data_files", result.data_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeCopyFile(object, result.data_files[index]); });
	});
	serializer.WriteList(5, "delete_files", result.delete_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeDeleteFile(object, result.delete_files[index]); });
	});
	serializer.End();
	return BytesFromStream(stream);
}

static IcebergDistributedMergeResult DeserializeMergeFragmentPayload(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed MERGE fragment is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	IcebergDistributedMergeResult result;
	result.inserted_rows = deserializer.ReadProperty<idx_t>(1, "inserted_rows");
	result.updated_rows = deserializer.ReadProperty<idx_t>(2, "updated_rows");
	result.deleted_rows = deserializer.ReadProperty<idx_t>(3, "deleted_rows");
	deserializer.ReadList(4, "data_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.data_files.push_back(DeserializeCopyFile(object)); });
	});
	deserializer.ReadList(5, "delete_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.delete_files.push_back(DeserializeDeleteFile(object)); });
	});
	deserializer.End();
	return result;
}

static vector<DistributedWriteFragment> IcebergMergeFinalize(ClientContext &context,
                                                             const DistributedExtensionWriteInfo &,
                                                             const DistributedWriteTaskContext &task,
                                                             DistributedWriteGlobalState &global_state_p) {
	auto &global_state = global_state_p.Cast<IcebergDistributedMergeGlobalState>();
	IcebergDistributedMergeResult result;
	unordered_map<string, vector<idx_t>> deleted_rows;
	{
		lock_guard<mutex> guard(global_state.lock);
		result.inserted_rows = global_state.inserted_rows;
		result.updated_rows = global_state.updated_rows;
		result.deleted_rows = global_state.deleted_row_count;
		deleted_rows = std::move(global_state.deleted_rows);
	}
	auto affected_rows = CheckedAdd(result.inserted_rows, result.updated_rows, "MERGE worker affected row count");
	affected_rows = CheckedAdd(affected_rows, result.deleted_rows, "MERGE worker affected row count");
	if (affected_rows == 0) {
		return {};
	}

	auto insert_files =
	    FinalizeMergeCopyAndReadStatistics(context, global_state.insert_writer.get(), result.inserted_rows, "INSERT");
	auto update_files =
	    FinalizeMergeCopyAndReadStatistics(context, global_state.update_writer.get(), result.updated_rows, "UPDATE");
	result.data_files.reserve(insert_files.size() + update_files.size());
	for (auto &file : insert_files) {
		result.data_files.push_back(std::move(file));
	}
	for (auto &file : update_files) {
		result.data_files.push_back(std::move(file));
	}

	for (auto &entry : deleted_rows) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (global_state.bind.iceberg_version >= 3) {
			auto source_index = global_state.delete_source_indexes.find(entry.first);
			if (source_index == global_state.delete_source_indexes.end()) {
				throw InvalidInputException(
				    "Iceberg distributed v3 MERGE produced rows for an unplanned source data file");
			}
			auto file_name = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.puffin";
			auto output_path = fs.JoinPath(global_state.attempt_root, file_name);
			result.delete_files.push_back(WriteDeletionVectorFile(
			    context, output_path, global_state.bind.delete_sources[source_index->second], entry.second));
		} else {
			auto file_name = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.parquet";
			auto output_path = fs.JoinPath(global_state.attempt_root, file_name);
			result.delete_files.push_back(WritePositionDeleteFile(context, output_path, entry.first, entry.second));
		}
	}

	idx_t data_rows = 0;
	idx_t delete_row_count = 0;
	idx_t byte_count = 0;
	for (const auto &file : result.data_files) {
		data_rows = CheckedAdd(data_rows, file.row_count, "MERGE worker data row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "MERGE worker byte count");
	}
	for (const auto &file : result.delete_files) {
		delete_row_count = CheckedAdd(delete_row_count, file.new_delete_count, "MERGE worker delete row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "MERGE worker byte count");
	}
	auto expected_data_rows = CheckedAdd(result.inserted_rows, result.updated_rows, "MERGE worker data row count");
	auto expected_delete_rows = CheckedAdd(result.updated_rows, result.deleted_rows, "MERGE worker delete row count");
	if (data_rows != expected_data_rows || delete_row_count != expected_delete_rows) {
		throw InternalException("Iceberg distributed MERGE worker produced inconsistent action counts");
	}

	DistributedWriteFragment fragment;
	fragment.fragment_id = task.query_id + "/" + task.task_attempt_id;
	fragment.payload = SerializeMergeFragmentPayload(result);
	fragment.row_count = affected_rows;
	fragment.byte_count = byte_count;
	for (idx_t index = 0; index < result.data_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "data:" + std::to_string(index);
		artifact.uri = result.data_files[index].final_path.empty() ? result.data_files[index].staging_path
		                                                           : result.data_files[index].final_path;
		artifact.codec = ICEBERG_DATA_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	for (idx_t index = 0; index < result.delete_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "delete:" + std::to_string(index);
		artifact.uri = result.delete_files[index].delete_file_path;
		artifact.codec = result.delete_files[index].is_deletion_vector ? ICEBERG_DELETION_VECTOR_FILE_CODEC
		                                                               : ICEBERG_POSITION_DELETE_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	return {std::move(fragment)};
}

static string CanonicalIcebergDistributedPath(FileSystem &fs, const string &path, const string &description) {
	auto canonical = distributed::CanonicalDistributedCopyBasePath(fs, path);
	if (canonical.is_err()) {
		throw InvalidInputException("Invalid Iceberg distributed %s path: %s", description, canonical.error().what());
	}
	return std::move(canonical).value();
}

static void ValidateIcebergDistributedArtifactPathInRoot(ClientContext &context, const string &allowed_root,
                                                         const string &path, idx_t expected_size,
                                                         const string &description) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto canonical_root = CanonicalIcebergDistributedPath(fs, allowed_root, description + " root");
	auto canonical_path = CanonicalIcebergDistributedPath(fs, path, description);
	auto separator = fs.PathSeparator(canonical_root);
	if (separator.empty() || canonical_path == canonical_root ||
	    !distributed::DistributedCopyPathIsInDirectory(canonical_path, canonical_root, separator)) {
		throw InvalidInputException("Iceberg distributed %s path is outside its allowed storage root", description);
	}

	auto relative_path = canonical_path.substr(canonical_root.size());
	while (StringUtil::StartsWith(relative_path, separator)) {
		relative_path.erase(0, separator.size());
	}
	if (relative_path.empty()) {
		throw InvalidInputException("Iceberg distributed %s path does not name a file", description);
	}
	idx_t component_start = 0;
	while (component_start <= relative_path.size()) {
		auto component_end = relative_path.find(separator, component_start);
		auto component = relative_path.substr(
		    component_start, component_end == string::npos ? string::npos : component_end - component_start);
		if (component.empty() || component == "." || component == "..") {
			throw InvalidInputException("Iceberg distributed %s path contains an invalid component", description);
		}
		if (component_end == string::npos) {
			break;
		}
		component_start = component_end + separator.size();
	}

	try {
		auto handle = fs.OpenFile(canonical_path, FileFlags::FILE_FLAGS_READ);
		auto actual_size = handle->GetFileSize();
		if (actual_size != expected_size) {
			throw InvalidInputException(
			    "Iceberg distributed %s size mismatch for '%s' (worker reported %llu bytes, found %llu)", description,
			    path, static_cast<unsigned long long>(expected_size), static_cast<unsigned long long>(actual_size));
		}
	} catch (const InvalidInputException &) {
		throw;
	} catch (const std::exception &ex) {
		throw IOException("Failed to validate Iceberg distributed %s artifact '%s': %s", description, path, ex.what());
	}
}

static void ValidateIcebergDistributedAttemptArtifactPath(ClientContext &context, const string &operation_root,
                                                          const string &task_attempt_id, const string &path,
                                                          idx_t expected_size, const string &description) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto attempt_root = fs.JoinPath(operation_root, EncodePathComponent(task_attempt_id));
	ValidateIcebergDistributedArtifactPathInRoot(context, attempt_root, path, expected_size, description);
}

} // namespace

string CreateIcebergDistributedArtifactNamespace() {
	return UUID::ToString(UUID::GenerateRandomUUID());
}

PhysicalOperator &PlanIcebergDistributedRowDeltaRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                            idx_t file_path_index) {
	if (file_path_index >= input.types.size() || input.types[file_path_index] != LogicalType::VARCHAR) {
		throw InternalException("Iceberg distributed row-delta file-path column is invalid");
	}
	vector<ExprRef> partition_by;
	partition_by.emplace_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, file_path_index));
	auto repartition_spec = RepartitionSpec::create_hash(0, std::move(partition_by));
	auto &result =
	    planner.Make<PhysicalRepartition>(input.types, std::move(repartition_spec), input.estimated_cardinality);
	result.children.push_back(input);
	return result;
}

PhysicalOperator &PlanIcebergDistributedMergeRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                         idx_t file_path_index,
                                                         const vector<idx_t> &null_target_partition_indexes) {
	if (file_path_index >= input.types.size() || input.types[file_path_index] != LogicalType::VARCHAR) {
		throw InternalException("Iceberg distributed MERGE file-path column is invalid");
	}
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, file_path_index));
	for (auto partition_index : null_target_partition_indexes) {
		if (partition_index >= input.types.size() || partition_index == file_path_index) {
			throw InternalException("Iceberg distributed MERGE null-target partition column is invalid");
		}
		if (input.types[partition_index].id() == LogicalTypeId::SQLNULL) {
			continue;
		}
		arguments.push_back(make_uniq<BoundReferenceExpression>(input.types[partition_index], partition_index));
	}
	vector<ExprRef> partition_by;
	partition_by.emplace_back(make_uniq<BoundFunctionExpression>(
	    LogicalType::HASH, IcebergDistributedMergePartitionFunction(), std::move(arguments), nullptr));
	auto repartition_spec = RepartitionSpec::create_hash(0, std::move(partition_by));
	auto &result =
	    planner.Make<PhysicalRepartition>(input.types, std::move(repartition_spec), input.estimated_cardinality);
	result.children.push_back(input);
	return result;
}

string BuildIcebergDistributedDeleteBind(ClientContext &context, const IcebergTableEntry &table,
                                         const IcebergMultiFileList &file_list, const vector<idx_t> &row_id_indexes,
                                         const string &artifact_namespace) {
	auto &metadata = table.table_info.table_metadata;
	IcebergDistributedRowDeltaBind bind;
	bind.kind = IcebergDistributedRowDeltaKind::DELETE;
	bind.iceberg_version = metadata.iceberg_version;
	bind.data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	bind.artifact_namespace = artifact_namespace;
	bind.row_id_indexes = row_id_indexes;
	if (metadata.iceberg_version >= 3) {
		PopulateDistributedRowDeltaSources(bind.delete_sources, file_list, "DELETE");
	}
	return SerializeBind(bind);
}

static string BuildIcebergDistributedUpdateBindInternal(ClientContext &context, const IcebergTableEntry &table,
                                                        optional_ptr<const IcebergMultiFileList> file_list,
                                                        const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                                        idx_t file_path_index, idx_t row_position_index,
                                                        const string &artifact_namespace,
                                                        bool source_is_statically_empty) {
	if ((source_is_statically_empty && file_list) || (!source_is_statically_empty && !file_list)) {
		throw InternalException("Iceberg distributed UPDATE source state is inconsistent");
	}
	auto &metadata = table.table_info.table_metadata;
	IcebergDistributedRowDeltaBind bind;
	bind.kind = IcebergDistributedRowDeltaKind::UPDATE;
	bind.iceberg_version = metadata.iceberg_version;
	bind.source_is_statically_empty = source_is_statically_empty;
	bind.data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	bind.artifact_namespace = artifact_namespace;
	bind.row_id_indexes = {file_path_index, row_position_index};
	bind.copy_column_count = copy_column_count;
	bind.copy_row_id_index = GetDistributedUpdateRowIdIndex(copy, metadata.iceberg_version);
	bind.copy_operator = SerializeShallowCopy(copy, "UPDATE");
	if (metadata.iceberg_version >= 3 && file_list) {
		PopulateDistributedRowDeltaSources(bind.delete_sources, *file_list, "UPDATE");
	}
	return SerializeBind(bind);
}

string BuildIcebergDistributedUpdateBind(ClientContext &context, const IcebergTableEntry &table,
                                         const IcebergMultiFileList &file_list, const PhysicalCopyToFile &copy,
                                         idx_t copy_column_count, idx_t file_path_index, idx_t row_position_index,
                                         const string &artifact_namespace) {
	return BuildIcebergDistributedUpdateBindInternal(context, table, &file_list, copy, copy_column_count,
	                                                 file_path_index, row_position_index, artifact_namespace, false);
}

string BuildIcebergDistributedEmptyUpdateBind(ClientContext &context, const IcebergTableEntry &table,
                                              const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                              idx_t file_path_index, idx_t row_position_index,
                                              const string &artifact_namespace) {
	return BuildIcebergDistributedUpdateBindInternal(context, table, nullptr, copy, copy_column_count, file_path_index,
	                                                 row_position_index, artifact_namespace, true);
}

static vector<unique_ptr<Expression>> CopyExpressionList(const vector<unique_ptr<Expression>> &expressions) {
	vector<unique_ptr<Expression>> result;
	result.reserve(expressions.size());
	for (const auto &expression : expressions) {
		result.push_back(expression->Copy());
	}
	return result;
}

static void ConfigureDistributedMergeWriter(IcebergDistributedMergeWriterBind &target, const PhysicalCopyToFile &copy,
                                            const vector<unique_ptr<Expression>> &projections,
                                            idx_t action_column_count, int32_t iceberg_version, bool is_update,
                                            const string &action_name) {
	ValidateDistributedMergeWriterShape(copy, projections, action_column_count, iceberg_version, is_update,
	                                    action_name);
	auto serialized_copy = SerializeShallowCopy(copy, "MERGE");
	if (target.copy_operator.empty()) {
		target.copy_operator = std::move(serialized_copy);
		target.projections = CopyExpressionList(projections);
		return;
	}
	if (target.copy_operator != serialized_copy || !Expression::ListEquals(target.projections, projections)) {
		throw NotImplementedException("Distributed Iceberg MERGE requires one canonical %s writer", action_name);
	}
}

string BuildIcebergDistributedMergeBind(ClientContext &context, const IcebergTableEntry &table,
                                        optional_ptr<const IcebergMultiFileList> target_file_list,
                                        const vector<IcebergDistributedMergePlanAction> &actions,
                                        const vector<LogicalType> &input_types, idx_t row_id_start,
                                        optional_idx source_marker, bool target_is_statically_empty,
                                        bool worker_plan_is_statically_empty, const string &artifact_namespace) {
	auto &metadata = table.table_info.table_metadata;
	IcebergDistributedMergeBind bind;
	bind.iceberg_version = metadata.iceberg_version;
	bind.target_is_statically_empty = target_is_statically_empty;
	bind.worker_plan_is_statically_empty = worker_plan_is_statically_empty;
	bind.data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	bind.artifact_namespace = artifact_namespace;
	bind.input_types = input_types;
	bind.row_id_start = row_id_start;
	bind.source_marker = source_marker;

	bool has_row_delta = false;
	for (const auto &planned_action : actions) {
		IcebergDistributedMergeActionBind action;
		action.condition = planned_action.match_condition;
		action.action_type = planned_action.action_type;
		if (planned_action.condition) {
			action.predicate = planned_action.condition->Copy();
		}
		action.expressions = CopyExpressionList(planned_action.expressions);
		switch (planned_action.action_type) {
		case MergeActionType::MERGE_INSERT:
			if (!planned_action.copy) {
				throw InternalException("Iceberg MERGE INSERT action is missing its physical writer");
			}
			ConfigureDistributedMergeWriter(bind.insert_writer, *planned_action.copy, planned_action.projections,
			                                action.expressions.size(), metadata.iceberg_version, false, "INSERT");
			break;
		case MergeActionType::MERGE_UPDATE:
			if (!planned_action.copy) {
				throw InternalException("Iceberg MERGE UPDATE action is missing its physical writer");
			}
			ConfigureDistributedMergeWriter(bind.update_writer, *planned_action.copy, planned_action.projections,
			                                action.expressions.size(), metadata.iceberg_version, true, "UPDATE");
			has_row_delta = true;
			break;
		case MergeActionType::MERGE_DELETE:
			has_row_delta = true;
			break;
		case MergeActionType::MERGE_ERROR:
		case MergeActionType::MERGE_DO_NOTHING:
			break;
		default:
			throw InternalException("Unsupported Iceberg MERGE action");
		}
		bind.actions.push_back(std::move(action));
	}
	if (has_row_delta && !target_is_statically_empty) {
		if (!target_file_list || !target_file_list->HasDistributedScanPlan()) {
			throw InvalidInputException("Distributed Iceberg MERGE requires a planned target scan");
		}
		if (metadata.iceberg_version >= 3) {
			PopulateDistributedRowDeltaSources(bind.delete_sources, *target_file_list, "MERGE");
		}
	}
	return SerializeMergeBind(bind);
}

DistributedExtensionWriteCallbacks IcebergDistributedRowDeltaCallbacks() {
	DistributedExtensionWriteCallbacks callbacks;
	callbacks.initialize_global = IcebergRowDeltaInitializeGlobal;
	callbacks.initialize_local = IcebergRowDeltaInitializeLocal;
	callbacks.sink = IcebergRowDeltaSink;
	callbacks.combine = IcebergRowDeltaCombine;
	callbacks.finalize = IcebergRowDeltaFinalize;
	return callbacks;
}

static DistributedExtensionWriteCallbacks IcebergDistributedMergeCallbacks() {
	DistributedExtensionWriteCallbacks callbacks;
	callbacks.initialize_global = IcebergMergeInitializeGlobal;
	callbacks.initialize_local = IcebergMergeInitializeLocal;
	callbacks.sink = IcebergMergeSink;
	callbacks.combine = IcebergMergeCombine;
	callbacks.finalize = IcebergMergeFinalize;
	return callbacks;
}

void ValidateIcebergDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                 const vector<distributed::DistributedCopyFileInfo> &files) {
	if (data_path.empty()) {
		throw InvalidInputException("Iceberg distributed data path cannot be empty");
	}
	idx_t total_rows = 0;
	for (const auto &file : files) {
		ValidateIcebergDistributedDataFileValues(file);
		const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
		if (path.empty() || file.file_size_bytes == 0) {
			throw InvalidInputException("Iceberg distributed write returned an empty data-file artifact");
		}
		ValidateIcebergSignedFileCounts(file.row_count, file.file_size_bytes, "data-file");
		total_rows = CheckedAdd(total_rows, file.row_count, "data row count");
		ValidateIcebergDistributedArtifactPathInRoot(context, data_path, path, file.file_size_bytes, "data-file");
	}
	if (total_rows > NumericCast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Iceberg distributed write row count exceeds signed 64-bit limits");
	}
}

IcebergDistributedRowDeltaResult DecodeIcebergDistributedRowDeltaResults(
    ClientContext &context, const string &data_path, const string &artifact_namespace,
    const DistributedExtensionWriteInfo &info, const vector<DistributedWriteTaskResult> &results,
    IcebergDistributedRowDeltaKind expected_kind, int32_t expected_iceberg_version) {
	info.Validate();
	if (info.mode != DistributedWriteMode::CALLBACK ||
	    info.fragment_codec !=
	        DistributedPayloadCodec {ICEBERG_ROW_DELTA_FRAGMENT_CODEC, ICEBERG_ROW_DELTA_PROTOCOL_VERSION}) {
		throw InvalidInputException("Iceberg distributed row-delta coordinator resolved the wrong worker protocol");
	}
	if (expected_iceberg_version != 2 && expected_iceberg_version != 3) {
		throw InvalidInputException("Iceberg distributed row-delta coordinator has an unsupported table version");
	}

	IcebergDistributedRowDeltaResult combined;
	set<string> task_attempt_ids;
	set<string> fragment_ids;
	set<string> artifact_paths;
	set<string> referenced_data_paths;
	string query_id;
	auto operation_root = IcebergDistributedRowDeltaRoot(context, data_path, artifact_namespace);
	for (const auto &task_result : results) {
		task_result.Validate();
		if (task_result.capability != info.capability || task_result.fragment_codec != info.fragment_codec) {
			throw InvalidInputException("Iceberg distributed row-delta received a mismatched task result protocol");
		}
		if (query_id.empty()) {
			query_id = task_result.query_id;
		} else if (task_result.query_id != query_id) {
			throw InvalidInputException("Iceberg distributed row-delta received results from multiple queries");
		}
		if (!task_attempt_ids.insert(task_result.task_attempt_id).second) {
			throw InvalidInputException("Iceberg distributed row-delta selected task attempt '%s' more than once",
			                            task_result.task_attempt_id);
		}
		if (task_result.fragments.size() > 1) {
			throw InvalidInputException(
			    "Iceberg distributed row-delta task attempt '%s' returned more than one fragment",
			    task_result.task_attempt_id);
		}
		for (const auto &fragment : task_result.fragments) {
			if (!fragment_ids.insert(fragment.fragment_id).second) {
				throw InvalidInputException("Iceberg distributed row-delta selected fragment '%s' more than once",
				                            fragment.fragment_id);
			}
			if (fragment.fragment_id != task_result.query_id + "/" + task_result.task_attempt_id) {
				throw InvalidInputException("Iceberg distributed row-delta fragment has a non-canonical identity");
			}
			if (fragment.row_count == 0) {
				throw InvalidInputException("Iceberg distributed row-delta returned an empty fragment");
			}
			auto decoded = DeserializeFragmentPayload(fragment.payload, expected_kind);
			if (expected_kind == IcebergDistributedRowDeltaKind::DELETE && !decoded.data_files.empty()) {
				throw InvalidInputException("Iceberg distributed DELETE returned an unexpected data-file artifact");
			}
			idx_t row_count = 0;
			idx_t byte_count = 0;
			vector<pair<string, DistributedPayloadCodec>> expected_artifacts;
			vector<string> expected_artifact_ids;
			idx_t data_artifact_index = 0;
			for (auto &file : decoded.data_files) {
				const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
				if (path.empty() || file.row_count == 0 || file.file_size_bytes == 0 ||
				    !artifact_paths.insert(path).second) {
					throw InvalidInputException(
					    "Iceberg distributed UPDATE returned a duplicate or empty data-file path");
				}
				ValidateIcebergSignedFileCounts(file.row_count, file.file_size_bytes, "data-file");
				ValidateIcebergDistributedDataFileValues(file);
				ValidateIcebergDistributedAttemptArtifactPath(context, operation_root, task_result.task_attempt_id,
				                                              path, file.file_size_bytes, "data-file");
				row_count = CheckedAdd(row_count, file.row_count, "row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "byte count");
				expected_artifacts.emplace_back(path, ICEBERG_DATA_FILE_CODEC);
				expected_artifact_ids.push_back("data:" + std::to_string(data_artifact_index++));
				combined.data_files.push_back(std::move(file));
			}
			idx_t delete_row_count = 0;
			idx_t delete_artifact_index = 0;
			for (auto &file : decoded.delete_files) {
				auto expects_deletion_vector = expected_iceberg_version >= 3;
				if (file.data_file_path.empty() || file.delete_file_path.empty() || file.new_delete_count == 0 ||
				    file.delete_count == 0 || file.new_delete_count > file.delete_count || file.file_size_bytes == 0 ||
				    file.pos_min_value > file.pos_max_value ||
				    file.delete_count - 1 > file.pos_max_value - file.pos_min_value ||
				    file.is_deletion_vector != expects_deletion_vector ||
				    !artifact_paths.insert(file.delete_file_path).second) {
					throw InvalidInputException("Iceberg distributed row-delta returned invalid delete-file metadata");
				}
				if (file.is_deletion_vector) {
					if (file.footer_size_bytes != 0 || file.content_offset != 4 || file.content_size_in_bytes < 12 ||
					    CheckedAdd(file.content_offset, file.content_size_in_bytes, "deletion-vector content range") >=
					        file.file_size_bytes) {
						throw InvalidInputException(
						    "Iceberg distributed v3 row-delta returned invalid deletion-vector metadata");
					}
				} else if (file.new_delete_count != file.delete_count || file.footer_size_bytes == 0 ||
				           file.footer_size_bytes > file.file_size_bytes || file.content_offset != 0 ||
				           file.content_size_in_bytes != 0) {
					throw InvalidInputException(
					    "Iceberg distributed v2 row-delta returned invalid positional-delete metadata");
				}
				ValidateIcebergSignedFileCounts(file.delete_count, file.file_size_bytes, "delete-file");
				ValidateIcebergDistributedAttemptArtifactPath(context, operation_root, task_result.task_attempt_id,
				                                              file.delete_file_path, file.file_size_bytes,
				                                              "delete-file");
				if (!referenced_data_paths.insert(file.data_file_path).second) {
					throw NotImplementedException(
					    "Distributed Iceberg row-delta produced multiple delete files for one data file");
				}
				delete_row_count = CheckedAdd(delete_row_count, file.new_delete_count, "delete row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "byte count");
				expected_artifacts.emplace_back(file.delete_file_path, file.is_deletion_vector
				                                                           ? ICEBERG_DELETION_VECTOR_FILE_CODEC
				                                                           : ICEBERG_POSITION_DELETE_FILE_CODEC);
				expected_artifact_ids.push_back("delete:" + std::to_string(delete_artifact_index++));
				combined.delete_files.push_back(std::move(file));
			}
			if (delete_row_count != fragment.row_count ||
			    (expected_kind == IcebergDistributedRowDeltaKind::UPDATE && row_count != fragment.row_count) ||
			    byte_count != fragment.byte_count || fragment.artifacts.size() != expected_artifacts.size()) {
				throw InvalidInputException("Iceberg distributed row-delta fragment counts are inconsistent");
			}
			for (idx_t index = 0; index < fragment.artifacts.size(); index++) {
				auto &artifact = fragment.artifacts[index];
				if (artifact.artifact_id != expected_artifact_ids[index] ||
				    artifact.uri != expected_artifacts[index].first ||
				    artifact.codec != expected_artifacts[index].second || !artifact.payload.empty()) {
					throw InvalidInputException("Iceberg distributed row-delta fragment has invalid artifact metadata");
				}
				combined.selected_artifact_paths.insert(artifact.uri);
			}
			combined.affected_rows = CheckedAdd(combined.affected_rows, fragment.row_count, "affected row count");
			if (combined.affected_rows > NumericCast<idx_t>(NumericLimits<int64_t>::Maximum())) {
				throw InvalidInputException(
				    "Iceberg distributed row-delta affected row count exceeds signed 64-bit limits");
			}
		}
	}
	return combined;
}

IcebergDistributedMergeResult DecodeIcebergDistributedMergeResults(ClientContext &context, const string &data_path,
                                                                   const string &artifact_namespace,
                                                                   const DistributedExtensionWriteInfo &info,
                                                                   const vector<DistributedWriteTaskResult> &results,
                                                                   int32_t expected_iceberg_version,
                                                                   bool worker_plan_is_statically_empty) {
	info.Validate();
	if (info.mode != DistributedWriteMode::CALLBACK ||
	    info.fragment_codec != DistributedPayloadCodec {ICEBERG_MERGE_FRAGMENT_CODEC, ICEBERG_MERGE_PROTOCOL_VERSION}) {
		throw InvalidInputException("Iceberg distributed MERGE coordinator resolved the wrong worker protocol");
	}
	if (expected_iceberg_version != 2 && expected_iceberg_version != 3) {
		throw InvalidInputException("Iceberg distributed MERGE coordinator has an unsupported table version");
	}
	if (results.empty() && !worker_plan_is_statically_empty) {
		throw InvalidInputException("Iceberg distributed MERGE returned no selected task results");
	}

	IcebergDistributedMergeResult combined;
	set<string> task_attempt_ids;
	set<string> fragment_ids;
	set<string> artifact_paths;
	set<string> referenced_data_paths;
	string query_id;
	auto operation_root = IcebergDistributedMergeRoot(context, data_path, artifact_namespace);
	for (const auto &task_result : results) {
		task_result.Validate();
		if (task_result.capability != info.capability || task_result.fragment_codec != info.fragment_codec) {
			throw InvalidInputException("Iceberg distributed MERGE received a mismatched task result protocol");
		}
		if (query_id.empty()) {
			query_id = task_result.query_id;
		} else if (task_result.query_id != query_id) {
			throw InvalidInputException("Iceberg distributed MERGE received results from multiple queries");
		}
		if (!task_attempt_ids.insert(task_result.task_attempt_id).second) {
			throw InvalidInputException("Iceberg distributed MERGE selected task attempt '%s' more than once",
			                            task_result.task_attempt_id);
		}
		if (task_result.fragments.size() > 1) {
			throw InvalidInputException("Iceberg distributed MERGE task attempt '%s' returned more than one fragment",
			                            task_result.task_attempt_id);
		}
		for (const auto &fragment : task_result.fragments) {
			if (!fragment_ids.insert(fragment.fragment_id).second) {
				throw InvalidInputException("Iceberg distributed MERGE selected fragment '%s' more than once",
				                            fragment.fragment_id);
			}
			if (fragment.fragment_id != task_result.query_id + "/" + task_result.task_attempt_id ||
			    fragment.row_count == 0) {
				throw InvalidInputException("Iceberg distributed MERGE fragment has an invalid identity or row count");
			}
			auto decoded = DeserializeMergeFragmentPayload(fragment.payload);
			auto affected_rows = CheckedAdd(decoded.inserted_rows, decoded.updated_rows, "MERGE affected row count");
			affected_rows = CheckedAdd(affected_rows, decoded.deleted_rows, "MERGE affected row count");
			if (affected_rows != fragment.row_count) {
				throw InvalidInputException("Iceberg distributed MERGE fragment has inconsistent action counts");
			}

			idx_t data_row_count = 0;
			idx_t delete_row_count = 0;
			idx_t byte_count = 0;
			vector<pair<string, DistributedPayloadCodec>> expected_artifacts;
			vector<string> expected_artifact_ids;
			idx_t data_artifact_index = 0;
			for (auto &file : decoded.data_files) {
				const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
				if (path.empty() || file.row_count == 0 || file.file_size_bytes == 0 ||
				    !artifact_paths.insert(path).second) {
					throw InvalidInputException(
					    "Iceberg distributed MERGE returned a duplicate or empty data-file path");
				}
				ValidateIcebergSignedFileCounts(file.row_count, file.file_size_bytes, "MERGE data-file");
				ValidateIcebergDistributedDataFileValues(file);
				ValidateIcebergDistributedAttemptArtifactPath(context, operation_root, task_result.task_attempt_id,
				                                              path, file.file_size_bytes, "MERGE data-file");
				data_row_count = CheckedAdd(data_row_count, file.row_count, "MERGE data row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "MERGE byte count");
				expected_artifacts.emplace_back(path, ICEBERG_DATA_FILE_CODEC);
				expected_artifact_ids.push_back("data:" + std::to_string(data_artifact_index++));
				combined.data_files.push_back(std::move(file));
			}
			idx_t delete_artifact_index = 0;
			for (auto &file : decoded.delete_files) {
				auto expects_deletion_vector = expected_iceberg_version >= 3;
				if (file.data_file_path.empty() || file.delete_file_path.empty() || file.new_delete_count == 0 ||
				    file.delete_count == 0 || file.new_delete_count > file.delete_count || file.file_size_bytes == 0 ||
				    file.pos_min_value > file.pos_max_value ||
				    file.delete_count - 1 > file.pos_max_value - file.pos_min_value ||
				    file.is_deletion_vector != expects_deletion_vector ||
				    !artifact_paths.insert(file.delete_file_path).second) {
					throw InvalidInputException("Iceberg distributed MERGE returned invalid delete-file metadata");
				}
				if (file.is_deletion_vector) {
					if (file.footer_size_bytes != 0 || file.content_offset != 4 || file.content_size_in_bytes < 12 ||
					    CheckedAdd(file.content_offset, file.content_size_in_bytes,
					               "MERGE deletion-vector content range") >= file.file_size_bytes) {
						throw InvalidInputException(
						    "Iceberg distributed v3 MERGE returned invalid deletion-vector metadata");
					}
				} else if (file.new_delete_count != file.delete_count || file.footer_size_bytes == 0 ||
				           file.footer_size_bytes > file.file_size_bytes || file.content_offset != 0 ||
				           file.content_size_in_bytes != 0) {
					throw InvalidInputException(
					    "Iceberg distributed v2 MERGE returned invalid positional-delete metadata");
				}
				ValidateIcebergSignedFileCounts(file.delete_count, file.file_size_bytes, "MERGE delete-file");
				ValidateIcebergDistributedAttemptArtifactPath(context, operation_root, task_result.task_attempt_id,
				                                              file.delete_file_path, file.file_size_bytes,
				                                              "MERGE delete-file");
				if (!referenced_data_paths.insert(file.data_file_path).second) {
					throw NotImplementedException(
					    "Distributed Iceberg MERGE produced multiple delete files for one data file");
				}
				delete_row_count = CheckedAdd(delete_row_count, file.new_delete_count, "MERGE delete row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "MERGE byte count");
				expected_artifacts.emplace_back(file.delete_file_path, file.is_deletion_vector
				                                                           ? ICEBERG_DELETION_VECTOR_FILE_CODEC
				                                                           : ICEBERG_POSITION_DELETE_FILE_CODEC);
				expected_artifact_ids.push_back("delete:" + std::to_string(delete_artifact_index++));
				combined.delete_files.push_back(std::move(file));
			}
			auto expected_data_rows = CheckedAdd(decoded.inserted_rows, decoded.updated_rows, "MERGE data row count");
			auto expected_delete_rows =
			    CheckedAdd(decoded.updated_rows, decoded.deleted_rows, "MERGE delete row count");
			if (data_row_count != expected_data_rows || delete_row_count != expected_delete_rows ||
			    byte_count != fragment.byte_count || fragment.artifacts.size() != expected_artifacts.size()) {
				throw InvalidInputException("Iceberg distributed MERGE fragment counts are inconsistent");
			}
			for (idx_t index = 0; index < fragment.artifacts.size(); index++) {
				auto &artifact = fragment.artifacts[index];
				if (artifact.artifact_id != expected_artifact_ids[index] ||
				    artifact.uri != expected_artifacts[index].first ||
				    artifact.codec != expected_artifacts[index].second || !artifact.payload.empty()) {
					throw InvalidInputException("Iceberg distributed MERGE fragment has invalid artifact metadata");
				}
				combined.selected_artifact_paths.insert(artifact.uri);
			}
			combined.inserted_rows =
			    CheckedAdd(combined.inserted_rows, decoded.inserted_rows, "MERGE inserted row count");
			combined.updated_rows = CheckedAdd(combined.updated_rows, decoded.updated_rows, "MERGE updated row count");
			combined.deleted_rows = CheckedAdd(combined.deleted_rows, decoded.deleted_rows, "MERGE deleted row count");
		}
	}
	auto affected_rows = CheckedAdd(combined.inserted_rows, combined.updated_rows, "MERGE affected row count");
	affected_rows = CheckedAdd(affected_rows, combined.deleted_rows, "MERGE affected row count");
	if (worker_plan_is_statically_empty && affected_rows != 0) {
		throw InvalidInputException("Iceberg distributed MERGE returned mutations for a statically empty worker plan");
	}
	if (affected_rows > NumericCast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Iceberg distributed MERGE affected row count exceeds signed 64-bit limits");
	}
	combined.affected_rows = affected_rows;
	return combined;
}

static void CleanupIcebergDistributedWriteRoot(ClientContext &context, const string &root,
                                               const unordered_set<string> *paths_to_keep,
                                               const string &operation_name) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!paths_to_keep || paths_to_keep->empty()) {
		distributed::RemoveDistributedCopyDirectoryTree(fs, root);
	} else {
		unordered_set<string> canonical_paths_to_keep;
		canonical_paths_to_keep.reserve(paths_to_keep->size());
		for (const auto &path : *paths_to_keep) {
			canonical_paths_to_keep.insert(CanonicalIcebergDistributedPath(fs, path, "retained artifact"));
		}
		vector<string> files;
		distributed::ListDistributedCopyFilesRecursive(fs, root, files);
		for (auto &file : files) {
			if (canonical_paths_to_keep.find(CanonicalIcebergDistributedPath(fs, file, "listed artifact")) ==
			    canonical_paths_to_keep.end()) {
				fs.TryRemoveFile(file);
			}
		}

		vector<string> remaining_files;
		distributed::ListDistributedCopyFilesRecursive(fs, root, remaining_files);
		for (const auto &file : remaining_files) {
			if (canonical_paths_to_keep.find(CanonicalIcebergDistributedPath(fs, file, "remaining artifact")) ==
			    canonical_paths_to_keep.end()) {
				throw IOException("Iceberg distributed %s cleanup left artifact '%s'", operation_name, file);
			}
		}
		return;
	}

	vector<string> remaining_files;
	distributed::ListDistributedCopyFilesRecursive(fs, root, remaining_files);
	for (const auto &file : remaining_files) {
		if (!paths_to_keep || paths_to_keep->find(file) == paths_to_keep->end()) {
			throw IOException("Iceberg distributed %s cleanup left artifact '%s'", operation_name, file);
		}
	}
}

void CleanupIcebergDistributedRowDelta(ClientContext &context, const string &data_path,
                                       const string &artifact_namespace, const unordered_set<string> *paths_to_keep) {
	if (data_path.empty() || artifact_namespace.empty()) {
		return;
	}
	CleanupIcebergDistributedWriteRoot(context, IcebergDistributedRowDeltaRoot(context, data_path, artifact_namespace),
	                                   paths_to_keep, "row-delta");
}

void CleanupIcebergDistributedMerge(ClientContext &context, const string &data_path, const string &artifact_namespace,
                                    const unordered_set<string> *paths_to_keep) {
	if (data_path.empty() || artifact_namespace.empty()) {
		return;
	}
	CleanupIcebergDistributedWriteRoot(context, IcebergDistributedMergeRoot(context, data_path, artifact_namespace),
	                                   paths_to_keep, "MERGE");
}

void RegisterIcebergDistributedWrites(ExtensionLoader &loader) {
	loader.RegisterFunction(IcebergDistributedMergePartitionFunction());

	auto register_file_write = [&](const string &name) {
		DistributedWriteOperatorExtension extension;
		extension.name = name;
		extension.protocol_version = 1;
		extension.mode = DistributedWriteMode::FILE_ARTIFACT;
		extension.fragment_codec = {distributed::DISTRIBUTED_FILE_WRITE_FRAGMENT_CODEC,
		                            distributed::DISTRIBUTED_FILE_WRITE_FRAGMENT_CODEC_VERSION};
		DistributedWriteOperatorExtension::Register(loader, std::move(extension));
	};
	register_file_write("insert");
	register_file_write("ctas");

	auto register_row_delta = [&](const string &name) {
		DistributedWriteOperatorExtension extension;
		extension.name = name;
		extension.protocol_version = ICEBERG_ROW_DELTA_PROTOCOL_VERSION;
		extension.mode = DistributedWriteMode::CALLBACK;
		extension.fragment_codec = {ICEBERG_ROW_DELTA_FRAGMENT_CODEC, ICEBERG_ROW_DELTA_PROTOCOL_VERSION};
		extension.callbacks = IcebergDistributedRowDeltaCallbacks();
		DistributedWriteOperatorExtension::Register(loader, std::move(extension));
	};
	register_row_delta("delete");
	register_row_delta("update");

	DistributedWriteOperatorExtension merge;
	merge.name = "merge";
	merge.protocol_version = ICEBERG_MERGE_PROTOCOL_VERSION;
	merge.mode = DistributedWriteMode::CALLBACK;
	merge.fragment_codec = {ICEBERG_MERGE_FRAGMENT_CODEC, ICEBERG_MERGE_PROTOCOL_VERSION};
	merge.callbacks = IcebergDistributedMergeCallbacks();
	DistributedWriteOperatorExtension::Register(loader, std::move(merge));
}

} // namespace duckdb
