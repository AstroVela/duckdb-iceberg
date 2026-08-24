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
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
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
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"
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

static const string ICEBERG_ROW_DELTA_FRAGMENT_CODEC = "iceberg.row-delta-fragment";
static const DistributedPayloadCodec ICEBERG_DATA_FILE_CODEC {"iceberg.data-file", 1};
static const DistributedPayloadCodec ICEBERG_POSITION_DELETE_FILE_CODEC {"iceberg.position-delete-file", 1};

struct IcebergDistributedRowDeltaBind {
	IcebergDistributedRowDeltaKind kind = IcebergDistributedRowDeltaKind::DELETE;
	int32_t iceberg_version = 0;
	string data_path;
	string artifact_namespace;
	vector<idx_t> row_id_indexes;
	idx_t copy_column_count = 0;
	string copy_operator;
};

static idx_t CheckedAdd(idx_t left, idx_t right, const string &description) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InvalidInputException("Iceberg distributed row-delta %s overflow", description);
	}
	return left + right;
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

static void ValidateDistributedUpdateCopyShape(const PhysicalCopyToFile &copy) {
	// Standalone callback finalization does not own DuckDB's pipeline Finalize context. Keep the worker COPY on the
	// multi-file paths used by Iceberg, which finalize their individual file states without the single-file lifecycle.
	auto partitioned = copy.partition_output && copy.write_empty_file && !copy.rotate && !copy.per_thread_output;
	auto rotating = !copy.partition_output && !copy.write_empty_file && copy.rotate && !copy.per_thread_output &&
	                copy.file_size_bytes.IsValid();
	if (!partitioned && !rotating) {
		throw NotImplementedException(
		    "Distributed Iceberg UPDATE requires the canonical partitioned or rotating COPY writer");
	}
	if (copy.use_tmp_file) {
		throw NotImplementedException("Distributed Iceberg UPDATE does not support temporary COPY output");
	}
	auto statistics_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	if (copy.return_type != CopyFunctionReturnType::WRITTEN_FILE_STATISTICS || copy.types != statistics_types) {
		throw SerializationException("Distributed Iceberg UPDATE COPY must return written-file statistics");
	}
}

static string SerializeShallowCopy(const PhysicalCopyToFile &copy) {
	ValidateDistributedUpdateCopyShape(copy);
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
	if (result.iceberg_version != 2) {
		throw NotImplementedException(
		    "Distributed Iceberg row-delta writes currently support format-version 2 tables only");
	}
	return result;
}

static unique_ptr<PhysicalOperator> DeserializeShallowCopy(ClientContext &context, PhysicalPlan &physical_plan,
                                                           const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Iceberg distributed UPDATE COPY operator is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(context);
	deserializer.Begin();
	auto result = PhysicalOperator::Deserialize(deserializer, physical_plan);
	deserializer.End();
	if (result->type != PhysicalOperatorType::COPY_TO_FILE || !result->children.empty()) {
		throw SerializationException("Iceberg distributed UPDATE worker bind is not a shallow COPY operator");
	}
	ValidateDistributedUpdateCopyShape(result->Cast<PhysicalCopyToFile>());
	return result;
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
	serializer.WriteProperty(3, "delete_count", file.delete_count);
	serializer.WriteProperty(4, "file_size_bytes", file.file_size_bytes);
	serializer.WriteProperty(5, "footer_size_bytes", file.footer_size_bytes);
	serializer.WriteProperty(6, "pos_min_value", file.pos_min_value);
	serializer.WriteProperty(7, "pos_max_value", file.pos_max_value);
}

static IcebergDistributedDeleteFileResult DeserializeDeleteFile(Deserializer &deserializer) {
	IcebergDistributedDeleteFileResult result;
	result.data_file_path = deserializer.ReadProperty<string>(1, "data_file_path");
	result.delete_file_path = deserializer.ReadProperty<string>(2, "delete_file_path");
	result.delete_count = deserializer.ReadProperty<idx_t>(3, "delete_count");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(4, "file_size_bytes");
	result.footer_size_bytes = deserializer.ReadProperty<idx_t>(5, "footer_size_bytes");
	result.pos_min_value = deserializer.ReadProperty<idx_t>(6, "pos_min_value");
	result.pos_max_value = deserializer.ReadProperty<idx_t>(7, "pos_max_value");
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
			copy_holder = DeserializeShallowCopy(context, copy_plan, bind.copy_operator);
			copy = &copy_holder->Cast<PhysicalCopyToFile>();
			copy->file_path = attempt_root;
			if (copy->expected_types.size() != bind.copy_column_count) {
				throw SerializationException("Iceberg distributed UPDATE COPY input width changed during transport");
			}
		}
	}

	IcebergDistributedRowDeltaBind bind;
	PhysicalPlan copy_plan;
	unique_ptr<PhysicalOperator> copy_holder;
	optional_ptr<PhysicalCopyToFile> copy;
	string operation_root;
	string attempt_root;
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
		auto position = position_value.GetValue<int64_t>();
		if (position < 0) {
			throw InvalidInputException("Iceberg distributed row-delta received a negative row position");
		}
		local_state.deleted_rows[file_value.GetValue<string>()].push_back(NumericCast<idx_t>(position));
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
	result.delete_count = statistics.row_count;
	result.file_size_bytes = statistics.file_size_bytes;
	result.footer_size_bytes = statistics.footer_size_bytes.GetValue<idx_t>();
	result.pos_min_value = min_position->second.GetValue<idx_t>();
	result.pos_max_value = max_position->second.GetValue<idx_t>();
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
		auto file_name = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.parquet";
		auto output_path = fs.JoinPath(global_state.attempt_root, file_name);
		delete_files.push_back(WritePositionDeleteFile(context, output_path, entry.first, entry.second));
	}

	idx_t data_rows = 0;
	idx_t byte_count = 0;
	for (auto &file : data_files) {
		data_rows = CheckedAdd(data_rows, file.row_count, "worker data row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "worker byte count");
	}
	idx_t delete_rows = 0;
	for (auto &file : delete_files) {
		delete_rows = CheckedAdd(delete_rows, file.delete_count, "worker delete row count");
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
		artifact.codec = ICEBERG_POSITION_DELETE_FILE_CODEC;
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

string BuildIcebergDistributedDeleteBind(ClientContext &context, const IcebergTableEntry &table,
                                         const vector<idx_t> &row_id_indexes, const string &artifact_namespace) {
	auto &metadata = table.table_info.table_metadata;
	IcebergDistributedRowDeltaBind bind;
	bind.kind = IcebergDistributedRowDeltaKind::DELETE;
	bind.iceberg_version = metadata.iceberg_version;
	bind.data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	bind.artifact_namespace = artifact_namespace;
	bind.row_id_indexes = row_id_indexes;
	return SerializeBind(bind);
}

string BuildIcebergDistributedUpdateBind(ClientContext &context, const IcebergTableEntry &table,
                                         const PhysicalCopyToFile &copy, idx_t copy_column_count, idx_t file_path_index,
                                         idx_t row_position_index, const string &artifact_namespace) {
	auto &metadata = table.table_info.table_metadata;
	IcebergDistributedRowDeltaBind bind;
	bind.kind = IcebergDistributedRowDeltaKind::UPDATE;
	bind.iceberg_version = metadata.iceberg_version;
	bind.data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	bind.artifact_namespace = artifact_namespace;
	bind.row_id_indexes = {file_path_index, row_position_index};
	bind.copy_column_count = copy_column_count;
	bind.copy_operator = SerializeShallowCopy(copy);
	return SerializeBind(bind);
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

IcebergDistributedRowDeltaResult
DecodeIcebergDistributedRowDeltaResults(ClientContext &context, const string &data_path,
                                        const string &artifact_namespace, const DistributedExtensionWriteInfo &info,
                                        const vector<DistributedWriteTaskResult> &results,
                                        IcebergDistributedRowDeltaKind expected_kind) {
	info.Validate();
	if (info.mode != DistributedWriteMode::CALLBACK ||
	    info.fragment_codec != DistributedPayloadCodec {ICEBERG_ROW_DELTA_FRAGMENT_CODEC, 1}) {
		throw InvalidInputException("Iceberg distributed row-delta coordinator resolved the wrong worker protocol");
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
				if (file.data_file_path.empty() || file.delete_file_path.empty() || file.delete_count == 0 ||
				    file.file_size_bytes == 0 || file.footer_size_bytes > file.file_size_bytes ||
				    file.pos_min_value > file.pos_max_value ||
				    file.delete_count - 1 > file.pos_max_value - file.pos_min_value ||
				    !artifact_paths.insert(file.delete_file_path).second) {
					throw InvalidInputException("Iceberg distributed row-delta returned invalid delete-file metadata");
				}
				ValidateIcebergSignedFileCounts(file.delete_count, file.file_size_bytes, "delete-file");
				ValidateIcebergDistributedAttemptArtifactPath(context, operation_root, task_result.task_attempt_id,
				                                              file.delete_file_path, file.file_size_bytes,
				                                              "delete-file");
				if (!referenced_data_paths.insert(file.data_file_path).second) {
					throw NotImplementedException(
					    "Distributed Iceberg row-delta produced multiple delete files for one data file");
				}
				delete_row_count = CheckedAdd(delete_row_count, file.delete_count, "delete row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "byte count");
				expected_artifacts.emplace_back(file.delete_file_path, ICEBERG_POSITION_DELETE_FILE_CODEC);
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

void CleanupIcebergDistributedRowDelta(ClientContext &context, const string &data_path,
                                       const string &artifact_namespace, const unordered_set<string> *paths_to_keep) {
	if (data_path.empty() || artifact_namespace.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto root = IcebergDistributedRowDeltaRoot(context, data_path, artifact_namespace);
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
				throw IOException("Iceberg distributed row-delta cleanup left artifact '%s'", file);
			}
		}
		return;
	}

	vector<string> remaining_files;
	distributed::ListDistributedCopyFilesRecursive(fs, root, remaining_files);
	for (const auto &file : remaining_files) {
		if (!paths_to_keep || paths_to_keep->find(file) == paths_to_keep->end()) {
			throw IOException("Iceberg distributed row-delta cleanup left artifact '%s'", file);
		}
	}
}

void RegisterIcebergDistributedWrites(ExtensionLoader &loader) {
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
		extension.protocol_version = 1;
		extension.mode = DistributedWriteMode::CALLBACK;
		extension.fragment_codec = {ICEBERG_ROW_DELTA_FRAGMENT_CODEC, 1};
		extension.callbacks = IcebergDistributedRowDeltaCallbacks();
		DistributedWriteOperatorExtension::Register(loader, std::move(extension));
	};
	register_row_delta("delete");
	register_row_delta("update");
}

} // namespace duckdb
