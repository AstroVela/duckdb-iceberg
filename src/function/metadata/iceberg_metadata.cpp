#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/function/distributed_table_function.hpp"
#endif

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include <numeric>

namespace duckdb {

static vector<LogicalType> IcebergManifestEntryTypes() {
	return {
	    //! status
	    LogicalType::VARCHAR,
	    //! content
	    LogicalType::VARCHAR,
	    //! file_path
	    LogicalType::VARCHAR,
	    //! file_format
	    LogicalType::VARCHAR,
	    //! record_count
	    LogicalType::BIGINT,
	};
}

static vector<string> IcebergManifestEntryNames() {
	return {"status", "content", "file_path", "file_format", "record_count"};
}

static vector<LogicalType> IcebergManifestTypes() {
	return {
	    //! manifest_path
	    LogicalType::VARCHAR,
	    //! manifest_sequence_number
	    LogicalType::BIGINT,
	    //! manifest_content
	    LogicalType::VARCHAR,
	};
}

static vector<string> IcebergManifestNames() {
	return {"manifest_path", "manifest_sequence_number", "manifest_content"};
}

struct IcebergMetaDataBindData : public TableFunctionData {
	string filename;
	string metadata_path;
	string metadata_compression_codec;
	bool allow_moved_paths = false;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	int32_t schema_id = 0;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<IcebergMetaDataBindData>();
		result->filename = filename;
		result->metadata_path = metadata_path;
		result->metadata_compression_codec = metadata_compression_codec;
		result->allow_moved_paths = allow_moved_paths;
		result->has_snapshot = has_snapshot;
		result->snapshot_id = snapshot_id;
		result->schema_id = schema_id;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IcebergMetaDataBindData>();
		return filename == other.filename && metadata_path == other.metadata_path &&
		       metadata_compression_codec == other.metadata_compression_codec &&
		       allow_moved_paths == other.allow_moved_paths && has_snapshot == other.has_snapshot &&
		       snapshot_id == other.snapshot_id && schema_id == other.schema_id;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction &) {
		auto &data = bind_data->Cast<IcebergMetaDataBindData>();
		serializer.WriteProperty(100, "filename", data.filename);
		serializer.WriteProperty(101, "metadata_path", data.metadata_path);
		serializer.WriteProperty(102, "metadata_compression_codec", data.metadata_compression_codec);
		serializer.WriteProperty(103, "allow_moved_paths", data.allow_moved_paths);
		serializer.WriteProperty(104, "has_snapshot", data.has_snapshot);
		serializer.WriteProperty(105, "snapshot_id", data.snapshot_id);
		serializer.WriteProperty(106, "schema_id", data.schema_id);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &) {
		auto result = make_uniq<IcebergMetaDataBindData>();
		result->filename = deserializer.ReadProperty<string>(100, "filename");
		result->metadata_path = deserializer.ReadProperty<string>(101, "metadata_path");
		result->metadata_compression_codec = deserializer.ReadProperty<string>(102, "metadata_compression_codec");
		result->allow_moved_paths = deserializer.ReadProperty<bool>(103, "allow_moved_paths");
		result->has_snapshot = deserializer.ReadProperty<bool>(104, "has_snapshot");
		result->snapshot_id = deserializer.ReadProperty<int64_t>(105, "snapshot_id");
		result->schema_id = deserializer.ReadProperty<int32_t>(106, "schema_id");
		return std::move(result);
	}
};

struct IcebergMetaDataGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto result = make_uniq<IcebergMetaDataGlobalTableFunctionState>();
		auto &bind_data = input.bind_data->Cast<IcebergMetaDataBindData>();
		if (!bind_data.has_snapshot) {
			return std::move(result);
		}
		auto &fs = FileSystem::GetFileSystem(context);
		auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
		auto table_metadata =
		    IcebergTableMetadata::Parse(bind_data.metadata_path, *caching_fs, bind_data.metadata_compression_codec);
		auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
		IcebergOptions options;
		options.allow_moved_paths = bind_data.allow_moved_paths;
		IcebergSnapshotScanInfo snapshot;
		snapshot.snapshot = metadata.GetSnapshotById(bind_data.snapshot_id);
		snapshot.schema_id = bind_data.schema_id;
		result->iceberg_table = IcebergManifestList::Load(bind_data.filename, metadata, snapshot, context, options);
		return std::move(result);
	}

	unique_ptr<IcebergManifestList> iceberg_table;
	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
};

static unique_ptr<FunctionData> IcebergMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergMetaDataBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);

	IcebergOptions options;
	auto &snapshot_lookup = options.snapshot_lookup;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			options.allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			options.table_version = StringValue::Get(val);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException("'version_name_format' has to contain two "
				                            "occurrences of '%%s' in it, found %d",
				                            string_substitutions);
			}
			options.version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException("Can't use 'snapshot_from_id' in combination with "
				                            "'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_ID);
			snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		} else if (loption == "snapshot_from_timestamp") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException("Can't use 'snapshot_from_id' in combination with "
				                            "'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_TIMESTAMP);
			snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		}
	}

	//! Keep the immutable metadata file and selected snapshot, never a catalog
	//! lookup or mutable version hint.
	ret->filename = filename;
	ret->metadata_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	ret->metadata_compression_codec = options.metadata_compression_codec;
	ret->allow_moved_paths = options.allow_moved_paths;
	auto table_metadata =
	    IcebergTableMetadata::Parse(ret->metadata_path, *caching_fs, options.metadata_compression_codec);
	auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

	auto snapshot_to_scan = metadata.GetSnapshot(options.snapshot_lookup);

	if (snapshot_to_scan.snapshot) {
		ret->has_snapshot = true;
		ret->snapshot_id = snapshot_to_scan.snapshot->snapshot_id;
		ret->schema_id = snapshot_to_scan.schema_id;
	}

	auto manifest_types = IcebergManifestTypes();
	return_types.insert(return_types.end(), manifest_types.begin(), manifest_types.end());
	auto manifest_entry_types = IcebergManifestEntryTypes();
	return_types.insert(return_types.end(), manifest_entry_types.begin(), manifest_entry_types.end());

	auto manifest_names = IcebergManifestNames();
	names.insert(names.end(), manifest_names.begin(), manifest_names.end());
	auto manifest_entry_names = IcebergManifestEntryNames();
	names.insert(names.end(), manifest_entry_names.begin(), manifest_entry_names.end());

	D_ASSERT(manifest_types.size() == manifest_names.size());

	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void IcebergMetaDataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergMetaDataGlobalTableFunctionState>();

	if (!global_state.iceberg_table) {
		//! Table is empty
		return;
	}

	idx_t out = 0;
	auto &table_entries = global_state.iceberg_table->GetManifestFilesConst();
	for (; global_state.current_manifest_idx < table_entries.size(); global_state.current_manifest_idx++) {
		auto &table_entry = table_entries[global_state.current_manifest_idx];
		auto &entries = table_entry.manifest_entries;
		for (; global_state.current_manifest_entry_idx < entries.size(); global_state.current_manifest_entry_idx++) {
			if (out >= STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out);
				return;
			}
			auto &manifest = table_entry.file;
			auto &manifest_entry = entries[global_state.current_manifest_entry_idx];
			auto &data_file = manifest_entry.data_file;

			//! manifest_path
			AddString(output.data[0], out, string_t(manifest.manifest_path));
			//! manifest_sequence_number
			FlatVector::GetData<int64_t>(output.data[1])[out] = manifest.sequence_number;
			//! manifest_content
			AddString(output.data[2], out, string_t(IcebergManifestContentTypeToString(manifest.content)));

			//! status
			AddString(output.data[3], out, string_t(IcebergManifestEntryStatusTypeToString(manifest_entry.status)));
			//! content
			AddString(output.data[4], out, string_t(IcebergManifestEntryContentTypeToString(data_file.content)));
			//! file_path
			AddString(output.data[5], out, string_t(data_file.file_path));
			//! file_format
			AddString(output.data[6], out, string_t(data_file.file_format));
			//! record_count
			FlatVector::GetData<int64_t>(output.data[7])[out] = data_file.record_count;
			out++;
		}
		global_state.current_manifest_entry_idx = 0;
	}
	output.SetCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergMetadataFunction() {
	TableFunctionSet function_set("iceberg_metadata");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergMetaDataFunction, IcebergMetaDataBind,
	                         IcebergMetaDataGlobalTableFunctionState::Init);
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	fun.serialize = IcebergMetaDataBindData::Serialize;
	fun.deserialize = IcebergMetaDataBindData::Deserialize;
#ifdef ICEBERG_VANE_DISTRIBUTED
	fun.SetDistributedScanCallbacks(MakeDistributedSingletonSourceCallbacks());
#endif
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
