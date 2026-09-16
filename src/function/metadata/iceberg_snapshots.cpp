#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/function/distributed_table_function.hpp"
#endif

#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

#include <string>

namespace duckdb {

static string SnapshotOperationToString(IcebergSnapshotOperationType type) {
	switch (type) {
	case IcebergSnapshotOperationType::APPEND:
		return "append";
	case IcebergSnapshotOperationType::REPLACE:
		return "replace";
	case IcebergSnapshotOperationType::OVERWRITE:
		return "overwrite";
	case IcebergSnapshotOperationType::DELETE:
		return "delete";
	default:
		return "unknown";
	}
}

struct IcebergSnapshotsBindData : public TableFunctionData {
	//! Iceberg metadata files are immutable. Resolve the catalog/version hint
	//! only during binding.
	string metadata_path;
	string metadata_compression_codec;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<IcebergSnapshotsBindData>();
		result->metadata_path = metadata_path;
		result->metadata_compression_codec = metadata_compression_codec;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IcebergSnapshotsBindData>();
		return metadata_path == other.metadata_path && metadata_compression_codec == other.metadata_compression_codec;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction &) {
		auto &data = bind_data->Cast<IcebergSnapshotsBindData>();
		serializer.WriteProperty(100, "metadata_path", data.metadata_path);
		serializer.WriteProperty(101, "metadata_compression_codec", data.metadata_compression_codec);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &) {
		auto result = make_uniq<IcebergSnapshotsBindData>();
		result->metadata_path = deserializer.ReadProperty<string>(100, "metadata_path");
		result->metadata_compression_codec = deserializer.ReadProperty<string>(101, "metadata_compression_codec");
		return std::move(result);
	}
};

struct IcebergSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergSnapshotsBindData>();
		auto global_state = make_uniq<IcebergSnapshotGlobalTableFunctionState>();

		auto &fs = FileSystem::GetFileSystem(context);
		auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);

		auto table_metadata =
		    IcebergTableMetadata::Parse(bind_data.metadata_path, *caching_fs, bind_data.metadata_compression_codec);
		global_state->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

		auto &info = global_state->metadata;
		global_state->snapshot_it = info.snapshots.begin();
		return std::move(global_state);
	}

	IcebergTableMetadata metadata;
	unordered_map<int64_t, IcebergSnapshot>::iterator snapshot_it;
};

static unique_ptr<FunctionData> IcebergSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IcebergSnapshotsBindData>();
	IcebergOptions options;
	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(kv.second);
		} else if (loption == "version") {
			options.table_version = StringValue::Get(kv.second);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException("'version_name_format' has to contain two "
				                            "occurrences of '%%s' in it, found %d",
				                            string_substitutions);
			}
			options.version_name_format = value;
		}
	}
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);
	auto &fs = FileSystem::GetFileSystem(context);
	bind_data->metadata_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	bind_data->metadata_compression_codec = options.metadata_compression_codec;

	names.emplace_back("sequence_number");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp_ms");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("manifest_list");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("operation");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

// Snapshots function
static void IcebergSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergSnapshotGlobalTableFunctionState>();
	idx_t i = 0;
	auto &it = global_state.snapshot_it;
	auto end = global_state.metadata.snapshots.end();
	for (; it != end; it++) {
		if (i >= STANDARD_VECTOR_SIZE) {
			break;
		}

		auto &snapshot = it->second;
		FlatVector::GetData<uint64_t>(output.data[0])[i] = snapshot.sequence_number;
		FlatVector::GetData<uint64_t>(output.data[1])[i] = snapshot.snapshot_id;
		FlatVector::GetData<timestamp_t>(output.data[2])[i] = snapshot.timestamp_ms;
		string_t manifest_string_t = StringVector::AddString(output.data[3], string_t(snapshot.manifest_list));
		FlatVector::GetData<string_t>(output.data[3])[i] = manifest_string_t;
		auto operation_str = SnapshotOperationToString(snapshot.operation);
		FlatVector::GetData<string_t>(output.data[4])[i] = StringVector::AddString(output.data[4], operation_str);
		i++;
	}
	output.SetCardinality(i);
}

TableFunctionSet IcebergFunctions::GetIcebergSnapshotsFunction() {
	TableFunctionSet function_set("iceberg_snapshots");
	TableFunction table_function({LogicalType::VARCHAR}, IcebergSnapshotsFunction, IcebergSnapshotsBind,
	                             IcebergSnapshotGlobalTableFunctionState::Init);
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["version"] = LogicalType::VARCHAR;
	table_function.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	table_function.serialize = IcebergSnapshotsBindData::Serialize;
	table_function.deserialize = IcebergSnapshotsBindData::Deserialize;
#ifdef ICEBERG_VANE_DISTRIBUTED
	table_function.SetDistributedScanCallbacks(MakeDistributedSingletonSourceCallbacks());
#endif
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
