#include "function/scan/iceberg_distributed_scan.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

#include "parquet_multi_file_info.hpp"

#include <algorithm>

namespace duckdb {

namespace {

static constexpr const char *ICEBERG_DISTRIBUTED_SCAN_TASK_CODEC = "iceberg.scan-file";

static string SerializeWorkerMetadata(const IcebergTableMetadata &metadata) {
	return metadata.ToDistributedScanJSON();
}

static shared_ptr<IcebergScanInfo> CreateOwnedDistributedScanInfo(const string &metadata_json, int32_t schema_id) {
	auto metadata_doc =
	    unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(metadata_json.c_str(), metadata_json.size(), 0));
	if (!metadata_doc) {
		throw SerializationException("Failed to deserialize Iceberg table metadata");
	}
	auto metadata_root = yyjson_doc_get_root(metadata_doc.get());
	auto parsed_metadata = rest_api_objects::TableMetadata::FromJSON(metadata_root);
	auto temporary_data = make_uniq<IcebergScanTemporaryData>();
	temporary_data->metadata = IcebergTableMetadata::FromTableMetadata(parsed_metadata);
	if (temporary_data->metadata.GetSchemas().find(schema_id) == temporary_data->metadata.GetSchemas().end()) {
		throw SerializationException("Distributed Iceberg worker bind references unknown schema id %d", schema_id);
	}

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.schema_id = schema_id;
	snapshot_info.snapshot = nullptr;
	auto schema = temporary_data->metadata.GetSchemaFromId(schema_id);
	return make_shared_ptr<IcebergScanInfo>(string(), std::move(temporary_data), snapshot_info, *schema);
}

static void SerializeDataFile(Serializer &serializer, const IcebergDataFile &file) {
	vector<uint64_t> partition_field_ids;
	vector<Value> partition_values;
	partition_field_ids.reserve(file.partition_info.size());
	partition_values.reserve(file.partition_info.size());
	for (const auto &partition : file.partition_info) {
		partition_field_ids.push_back(partition.field_id);
		partition_values.push_back(partition.value);
	}

	serializer.WriteProperty(1, "content", static_cast<uint8_t>(file.content));
	serializer.WriteProperty(2, "file_path", file.file_path);
	serializer.WriteProperty(3, "file_format", file.file_format);
	serializer.WriteProperty(4, "partition_field_ids", partition_field_ids);
	serializer.WriteProperty(5, "partition_values", partition_values);
	serializer.WriteProperty(6, "record_count", file.record_count);
	serializer.WriteProperty(7, "file_size_in_bytes", file.file_size_in_bytes);
	serializer.WriteProperty(8, "column_sizes", file.column_sizes);
	serializer.WriteProperty(9, "value_counts", file.value_counts);
	serializer.WriteProperty(10, "null_value_counts", file.null_value_counts);
	serializer.WriteProperty(11, "nan_value_counts", file.nan_value_counts);
	serializer.WriteProperty(12, "lower_bounds", file.lower_bounds);
	serializer.WriteProperty(13, "upper_bounds", file.upper_bounds);
	serializer.WriteProperty(14, "equality_ids", file.equality_ids);
	serializer.WriteProperty(15, "split_offsets", file.split_offsets);
	serializer.WriteProperty(16, "has_sort_order_id", file.has_sort_order_id);
	serializer.WriteProperty(17, "sort_order_id", file.has_sort_order_id ? file.sort_order_id : 0);
	serializer.WriteProperty(18, "referenced_data_file", file.referenced_data_file);
	serializer.WriteProperty(19, "content_offset", file.content_offset);
	serializer.WriteProperty(20, "content_size_in_bytes", file.content_size_in_bytes);
	serializer.WriteProperty(21, "has_first_row_id", file.HasFirstRowId());
	serializer.WriteProperty(22, "first_row_id", file.HasFirstRowId() ? file.GetFirstRowId() : 0);
}

static IcebergDataFile DeserializeDataFile(Deserializer &deserializer) {
	IcebergDataFile result;
	auto content = deserializer.ReadProperty<uint8_t>(1, "content");
	if (content != static_cast<uint8_t>(IcebergManifestEntryContentType::DATA)) {
		throw SerializationException("Distributed Iceberg scan task does not describe a data file");
	}
	result.content = IcebergManifestEntryContentType::DATA;
	result.file_path = deserializer.ReadProperty<string>(2, "file_path");
	result.file_format = deserializer.ReadProperty<string>(3, "file_format");
	auto partition_field_ids = deserializer.ReadProperty<vector<uint64_t>>(4, "partition_field_ids");
	auto partition_values = deserializer.ReadProperty<vector<Value>>(5, "partition_values");
	if (partition_field_ids.size() != partition_values.size()) {
		throw SerializationException("Distributed Iceberg scan task has mismatched partition fields and values");
	}
	result.partition_info.reserve(partition_field_ids.size());
	for (idx_t index = 0; index < partition_field_ids.size(); index++) {
		result.partition_info.push_back({partition_field_ids[index], std::move(partition_values[index])});
	}
	result.record_count = deserializer.ReadProperty<int64_t>(6, "record_count");
	result.file_size_in_bytes = deserializer.ReadProperty<int64_t>(7, "file_size_in_bytes");
	result.column_sizes = deserializer.ReadProperty<unordered_map<int32_t, int64_t>>(8, "column_sizes");
	result.value_counts = deserializer.ReadProperty<unordered_map<int32_t, int64_t>>(9, "value_counts");
	result.null_value_counts = deserializer.ReadProperty<unordered_map<int32_t, int64_t>>(10, "null_value_counts");
	result.nan_value_counts = deserializer.ReadProperty<unordered_map<int32_t, int64_t>>(11, "nan_value_counts");
	result.lower_bounds = deserializer.ReadProperty<unordered_map<int32_t, Value>>(12, "lower_bounds");
	result.upper_bounds = deserializer.ReadProperty<unordered_map<int32_t, Value>>(13, "upper_bounds");
	result.equality_ids = deserializer.ReadProperty<vector<int32_t>>(14, "equality_ids");
	result.split_offsets = deserializer.ReadProperty<vector<int64_t>>(15, "split_offsets");
	result.has_sort_order_id = deserializer.ReadProperty<bool>(16, "has_sort_order_id");
	auto sort_order_id = deserializer.ReadProperty<int32_t>(17, "sort_order_id");
	if (!result.has_sort_order_id && sort_order_id != 0) {
		throw SerializationException("Distributed Iceberg scan task has a non-canonical absent sort-order id");
	}
	result.sort_order_id = sort_order_id;
	result.referenced_data_file = deserializer.ReadProperty<string>(18, "referenced_data_file");
	result.content_offset = deserializer.ReadProperty<Value>(19, "content_offset");
	result.content_size_in_bytes = deserializer.ReadProperty<Value>(20, "content_size_in_bytes");
	auto has_first_row_id = deserializer.ReadProperty<bool>(21, "has_first_row_id");
	auto first_row_id = deserializer.ReadProperty<int64_t>(22, "first_row_id");
	if (has_first_row_id) {
		result.SetFirstRowId(first_row_id);
	} else if (first_row_id != 0) {
		throw SerializationException("Distributed Iceberg scan task has a non-canonical absent first-row id");
	}
	if (result.file_path.empty() || !StringUtil::CIEquals(result.file_format, "parquet") || result.record_count < 0 ||
	    result.file_size_in_bytes < 0) {
		throw SerializationException("Distributed Iceberg scan task contains invalid data-file metadata");
	}
	return result;
}

static void SerializeManifestFile(Serializer &serializer, const IcebergManifestFile &file) {
	serializer.WriteProperty(1, "partition_spec_id", file.partition_spec_id);
	serializer.WriteProperty(2, "has_first_row_id", file.has_first_row_id);
	serializer.WriteProperty(3, "first_row_id", file.has_first_row_id ? file.first_row_id : 0);
	serializer.WriteProperty(4, "content", static_cast<uint8_t>(file.content));
	serializer.WriteProperty(5, "sequence_number", file.sequence_number);
	serializer.WriteProperty(6, "has_min_sequence_number", file.has_min_sequence_number);
	serializer.WriteProperty(7, "min_sequence_number", file.has_min_sequence_number ? file.min_sequence_number : 0);
	serializer.WriteProperty(8, "added_snapshot_id", file.added_snapshot_id);
	serializer.WriteProperty(9, "added_files_count", file.added_files_count);
	serializer.WriteProperty(10, "existing_files_count", file.existing_files_count);
	serializer.WriteProperty(11, "deleted_files_count", file.deleted_files_count);
	serializer.WriteProperty(12, "added_rows_count", file.added_rows_count);
	serializer.WriteProperty(13, "existing_rows_count", file.existing_rows_count);
	serializer.WriteProperty(14, "deleted_rows_count", file.deleted_rows_count);
}

static IcebergManifestFile DeserializeManifestFile(Deserializer &deserializer) {
	IcebergManifestFile result {string()};
	result.manifest_length = 0;
	result.partition_spec_id = deserializer.ReadProperty<int32_t>(1, "partition_spec_id");
	result.has_first_row_id = deserializer.ReadProperty<bool>(2, "has_first_row_id");
	result.first_row_id = deserializer.ReadProperty<sequence_number_t>(3, "first_row_id");
	if (!result.has_first_row_id && result.first_row_id != 0) {
		throw SerializationException("Distributed Iceberg scan task has a non-canonical absent first-row id");
	}
	auto content = deserializer.ReadProperty<uint8_t>(4, "content");
	if (content != static_cast<uint8_t>(IcebergManifestContentType::DATA)) {
		throw SerializationException("Distributed Iceberg scan task does not describe a data manifest");
	}
	result.content = IcebergManifestContentType::DATA;
	result.sequence_number = deserializer.ReadProperty<sequence_number_t>(5, "sequence_number");
	result.has_min_sequence_number = deserializer.ReadProperty<bool>(6, "has_min_sequence_number");
	result.min_sequence_number = deserializer.ReadProperty<sequence_number_t>(7, "min_sequence_number");
	if (!result.has_min_sequence_number && result.min_sequence_number != 0) {
		throw SerializationException(
		    "Distributed Iceberg scan task has a non-canonical absent minimum sequence number");
	}
	result.added_snapshot_id = deserializer.ReadProperty<int64_t>(8, "added_snapshot_id");
	result.added_files_count = deserializer.ReadProperty<idx_t>(9, "added_files_count");
	result.existing_files_count = deserializer.ReadProperty<idx_t>(10, "existing_files_count");
	result.deleted_files_count = deserializer.ReadProperty<idx_t>(11, "deleted_files_count");
	result.added_rows_count = deserializer.ReadProperty<idx_t>(12, "added_rows_count");
	result.existing_rows_count = deserializer.ReadProperty<idx_t>(13, "existing_rows_count");
	result.deleted_rows_count = deserializer.ReadProperty<idx_t>(14, "deleted_rows_count");
	return result;
}

static void SerializeManifestEntry(Serializer &serializer, const BoundIcebergManifestEntry &bound_entry,
                                   const IcebergManifestFile &manifest_file) {
	auto &entry = bound_entry.entry;
	serializer.WriteProperty(1, "status", static_cast<uint8_t>(entry.status));
	serializer.WriteObject(2, "data_file", [&](Serializer &object) { SerializeDataFile(object, entry.data_file); });
	serializer.WriteProperty(3, "has_snapshot_id", entry.HasSnapshotId());
	serializer.WriteProperty(4, "snapshot_id", entry.HasSnapshotId() ? entry.GetSnapshotId() : 0);
	serializer.WriteProperty(5, "sequence_number", entry.GetSequenceNumber(manifest_file));
	serializer.WriteProperty(6, "file_sequence_number", entry.GetFileSequenceNumber(manifest_file));
	serializer.WriteProperty(7, "has_first_row_id", bound_entry.HasFirstRowId());
	serializer.WriteProperty(8, "first_row_id", bound_entry.HasFirstRowId() ? bound_entry.GetFirstRowId() : 0);
}

static IcebergManifestEntry DeserializeManifestEntry(Deserializer &deserializer) {
	IcebergManifestEntry result {};
	auto status = deserializer.ReadProperty<uint8_t>(1, "status");
	if (status > static_cast<uint8_t>(IcebergManifestEntryStatusType::DELETED)) {
		throw SerializationException("Distributed Iceberg scan task has invalid manifest-entry status %d", status);
	}
	result.status = static_cast<IcebergManifestEntryStatusType>(status);
	deserializer.ReadObject(2, "data_file",
	                        [&](Deserializer &object) { result.data_file = DeserializeDataFile(object); });
	auto has_snapshot_id = deserializer.ReadProperty<bool>(3, "has_snapshot_id");
	auto snapshot_id = deserializer.ReadProperty<int64_t>(4, "snapshot_id");
	if (has_snapshot_id) {
		result.SetSnapshotId(snapshot_id);
	} else if (snapshot_id != 0) {
		throw SerializationException("Distributed Iceberg scan task has a non-canonical absent snapshot id");
	}
	result.SetSequenceNumber(deserializer.ReadProperty<sequence_number_t>(5, "sequence_number"));
	result.SetFileSequenceNumber(deserializer.ReadProperty<sequence_number_t>(6, "file_sequence_number"));
	auto has_first_row_id = deserializer.ReadProperty<bool>(7, "has_first_row_id");
	auto first_row_id = deserializer.ReadProperty<int64_t>(8, "first_row_id");
	if (has_first_row_id) {
		if (result.data_file.HasFirstRowId() && result.data_file.GetFirstRowId() != first_row_id) {
			throw SerializationException("Distributed Iceberg scan task has inconsistent first-row ids");
		}
		result.data_file.SetFirstRowId(first_row_id);
	} else if (first_row_id != 0 || result.data_file.HasFirstRowId()) {
		throw SerializationException("Distributed Iceberg scan task has a non-canonical absent first-row id");
	}
	if (result.status == IcebergManifestEntryStatusType::DELETED) {
		throw SerializationException("Distributed Iceberg scan task describes a deleted data file");
	}
	return result;
}

struct IcebergDistributedEqualityDeleteRow {
	unordered_map<int32_t, Value> values;
};

static Value GetEqualityDeleteValue(const Expression &expression) {
	if (expression.type == ExpressionType::OPERATOR_IS_NOT_NULL &&
	    expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &is_not_null = expression.Cast<BoundOperatorExpression>();
		if (is_not_null.children.size() != 1 ||
		    is_not_null.children[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
			throw SerializationException("Iceberg equality-delete NULL predicate has an invalid shape");
		}
		return Value(is_not_null.children[0]->return_type);
	}
	if (expression.type != ExpressionType::COMPARE_NOTEQUAL ||
	    expression.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		throw SerializationException("Iceberg equality-delete predicate has an unsupported shape");
	}
	auto &comparison = expression.Cast<BoundComparisonExpression>();
	if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_REF ||
	    comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		throw SerializationException("Iceberg equality-delete comparison has an invalid shape");
	}
	auto &constant = comparison.right->Cast<BoundConstantExpression>().value;
	if (constant.IsNull()) {
		throw SerializationException("Iceberg equality-delete comparison has an invalid constant");
	}
	return constant;
}

static void SerializeEqualityDeletes(Serializer &serializer,
                                     const vector<reference<const IcebergEqualityDeleteRow>> &delete_rows) {
	serializer.WriteList(5, "equality_delete_rows", delete_rows.size(), [&](Serializer::List &list, idx_t row_index) {
		auto &row = delete_rows[row_index].get();
		vector<int32_t> field_ids;
		field_ids.reserve(row.filters.size());
		for (const auto &entry : row.filters) {
			field_ids.push_back(entry.first);
		}
		std::sort(field_ids.begin(), field_ids.end());
		list.WriteObject([&](Serializer &row_serializer) {
			row_serializer.WriteList(
			    1, "filters", field_ids.size(), [&](Serializer::List &filters, idx_t filter_index) {
				    auto field_id = field_ids[filter_index];
				    auto filter = row.filters.find(field_id);
				    D_ASSERT(filter != row.filters.end());
				    filters.WriteObject([&](Serializer &filter_serializer) {
					    filter_serializer.WriteProperty(1, "field_id", field_id);
					    filter_serializer.WriteProperty(2, "value", GetEqualityDeleteValue(*filter->second));
				    });
			    });
		});
	});
}

static vector<IcebergDistributedEqualityDeleteRow> DeserializeEqualityDeletes(Deserializer &deserializer) {
	vector<IcebergDistributedEqualityDeleteRow> result;
	deserializer.ReadList(5, "equality_delete_rows", [&](Deserializer::List &list, idx_t) {
		IcebergDistributedEqualityDeleteRow row;
		list.ReadObject([&](Deserializer &row_deserializer) {
			row_deserializer.ReadList(1, "filters", [&](Deserializer::List &filters, idx_t) {
				filters.ReadObject([&](Deserializer &filter_deserializer) {
					auto field_id = filter_deserializer.ReadProperty<int32_t>(1, "field_id");
					auto value = filter_deserializer.ReadProperty<Value>(2, "value");
					auto type_id = value.type().id();
					if (field_id <= 0 || type_id == LogicalTypeId::INVALID || type_id == LogicalTypeId::SQLNULL ||
					    type_id == LogicalTypeId::UNKNOWN || type_id == LogicalTypeId::ANY ||
					    type_id == LogicalTypeId::UNBOUND || !row.values.emplace(field_id, std::move(value)).second) {
						throw SerializationException(
						    "Distributed Iceberg scan task contains an invalid equality delete");
					}
				});
			});
		});
		if (row.values.empty()) {
			throw SerializationException("Distributed Iceberg scan task contains an empty equality-delete row");
		}
		result.push_back(std::move(row));
	});
	return result;
}

struct IcebergDistributedDecodedTask {
	string scan_set_id;
	string resolved_path;
	IcebergManifestFile manifest_file {""};
	IcebergManifestEntry manifest_entry;
	vector<int64_t> positional_delete_rows;
	vector<IcebergDistributedEqualityDeleteRow> equality_delete_rows;
};

static string SerializeTask(const string &scan_set_id, const string &resolved_path,
                            const IcebergManifestFile &manifest_file, const BoundIcebergManifestEntry &manifest_entry,
                            const vector<int64_t> &positional_delete_rows,
                            const vector<reference<const IcebergEqualityDeleteRow>> &equality_delete_rows) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "resolved_path", resolved_path);
	serializer.WriteObject(2, "manifest_file",
	                       [&](Serializer &object) { SerializeManifestFile(object, manifest_file); });
	serializer.WriteObject(3, "manifest_entry",
	                       [&](Serializer &object) { SerializeManifestEntry(object, manifest_entry, manifest_file); });
	serializer.WriteProperty(4, "positional_delete_rows", positional_delete_rows);
	SerializeEqualityDeletes(serializer, equality_delete_rows);
	serializer.WriteProperty(6, "scan_set_id", scan_set_id);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static IcebergDistributedDecodedTask DeserializeTask(const string &payload) {
	if (payload.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed Iceberg scan task");
	}
	vector<data_t> buffer(payload.begin(), payload.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	IcebergDistributedDecodedTask result;
	result.resolved_path = deserializer.ReadProperty<string>(1, "resolved_path");
	deserializer.ReadObject(2, "manifest_file",
	                        [&](Deserializer &object) { result.manifest_file = DeserializeManifestFile(object); });
	deserializer.ReadObject(3, "manifest_entry",
	                        [&](Deserializer &object) { result.manifest_entry = DeserializeManifestEntry(object); });
	result.positional_delete_rows = deserializer.ReadProperty<vector<int64_t>>(4, "positional_delete_rows");
	result.equality_delete_rows = DeserializeEqualityDeletes(deserializer);
	result.scan_set_id = deserializer.ReadProperty<string>(6, "scan_set_id");
	deserializer.End();
	if (result.scan_set_id.empty() || result.resolved_path.empty()) {
		throw SerializationException("Distributed Iceberg scan task has an empty scan-set identity or resolved path");
	}
	return result;
}

static bool IsCanonicalTaskId(const string &task_id) {
	if (task_id.empty() || (task_id.size() > 1 && task_id[0] == '0')) {
		return false;
	}
	for (auto character : task_id) {
		if (character < '0' || character > '9') {
			return false;
		}
	}
	return true;
}

static void SerializeWorkerColumn(Serializer &serializer, const MultiFileColumnDefinition &column) {
	serializer.WriteProperty(1, "name", column.name);
	serializer.WriteProperty(2, "type", column.type);
	serializer.WriteProperty(3, "identifier", column.identifier);
	serializer.WriteProperty(4, "has_default", column.default_expression != nullptr);
	if (column.default_expression) {
		serializer.WriteProperty(5, "default_value", column.GetDefaultValue());
	}
	serializer.WriteList(6, "children", column.children.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeWorkerColumn(object, column.children[index]); });
	});
}

static MultiFileColumnDefinition DeserializeWorkerColumn(Deserializer &deserializer) {
	auto name = deserializer.ReadProperty<string>(1, "name");
	auto type = deserializer.ReadProperty<LogicalType>(2, "type");
	auto identifier = deserializer.ReadProperty<Value>(3, "identifier");
	auto has_default = deserializer.ReadProperty<bool>(4, "has_default");
	MultiFileColumnDefinition result(name, type);
	result.identifier = std::move(identifier);
	if (has_default) {
		auto default_value = deserializer.ReadProperty<Value>(5, "default_value");
		if (default_value.type() != type) {
			throw SerializationException("Distributed Iceberg worker column '%s' has an invalid default type", name);
		}
		result.default_expression = make_uniq<ConstantExpression>(std::move(default_value));
	}
	deserializer.ReadList(6, "children", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.children.push_back(DeserializeWorkerColumn(object)); });
	});
	if (result.name.empty() || result.identifier.IsNull() || result.identifier.type().id() != LogicalTypeId::INTEGER) {
		throw SerializationException("Distributed Iceberg worker column has an invalid field-id mapping");
	}
	return result;
}

static const vector<MultiFileColumnDefinition> &GetReaderSchema(const MultiFileBindData &bind_data) {
	return bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
}

static vector<string> MaterializeCoordinatorTaskPayloads(const MultiFileBindData &bind_data,
                                                         const string &scan_set_id) {
	if (scan_set_id.empty()) {
		throw InternalException("Distributed Iceberg scan set identity cannot be empty");
	}
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	auto file_count = file_list.GetTotalFileCount();
	auto files = file_list.GetAllFiles();
	if (files.size() != file_count) {
		throw InternalException("Iceberg distributed scan returned inconsistent file counts while materializing tasks");
	}

	auto global_columns = GetReaderSchema(bind_data);
	unordered_set<int32_t> present_field_ids;
	for (const auto &column : global_columns) {
		if (!column.identifier.IsNull() && column.identifier.type().id() == LogicalTypeId::INTEGER) {
			present_field_ids.insert(column.identifier.GetValue<int32_t>());
		}
	}
	auto delete_entries = file_list.GetDeleteManifestEntries();
	set<int32_t> required_field_ids;
	for (const auto &entry : delete_entries) {
		if (entry.entry.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
			continue;
		}
		for (auto field_id : entry.entry.data_file.equality_ids) {
			required_field_ids.insert(field_id);
		}
	}
	for (auto field_id : required_field_ids) {
		if (present_field_ids.find(field_id) != present_field_ids.end()) {
			continue;
		}
		auto column = file_list.GetMetadata().FindColumnByFieldId(field_id);
		if (!column) {
			throw InvalidConfigurationException("Iceberg equality-delete field id %d is absent from every table schema",
			                                    field_id);
		}
		auto worker_column = column->GetMultiFileColumnDefinition();
		if (!worker_column.default_expression) {
			worker_column.default_expression = make_uniq<ConstantExpression>(Value(column->type));
		}
		global_columns.push_back(std::move(worker_column));
		present_field_ids.insert(field_id);
	}

	vector<ColumnIndex> column_ids;
	column_ids.reserve(global_columns.size());
	for (idx_t column_index = 0; column_index < global_columns.size(); column_index++) {
		column_ids.emplace_back(column_index);
	}
	file_list.ProcessDeletes(global_columns, column_ids, {});

	vector<string> payloads;
	payloads.reserve(files.size());
	for (idx_t file_index = 0; file_index < files.size(); file_index++) {
		auto manifest_entry = file_list.GetManifestEntry(file_index);
		auto &manifest_file = file_list.GetManifestFileForEntry(manifest_entry, IcebergManifestContentType::DATA);
		vector<int64_t> positional_delete_rows;
		auto positional_delete_data =
		    file_list.GetExistingPositionalDeleteData(manifest_entry.entry.data_file.file_path);
		if (positional_delete_data) {
			set<idx_t> sorted_rows;
			positional_delete_data->ToSet(sorted_rows);
			positional_delete_rows.reserve(sorted_rows.size());
			for (auto row : sorted_rows) {
				positional_delete_rows.push_back(NumericCast<int64_t>(row));
			}
		}
		auto equality_delete_rows = file_list.GetEqualityDeletesForFile(manifest_entry);
		payloads.push_back(SerializeTask(scan_set_id, files[file_index].path, manifest_file, manifest_entry,
		                                 positional_delete_rows, equality_delete_rows));
	}
	return payloads;
}

struct IcebergWorkerEqualityDeleteMapping {
	unordered_map<int32_t, idx_t> output_indexes;
	unordered_map<int32_t, LogicalType> types;
	idx_t output_column_count = 0;
};

static IcebergWorkerEqualityDeleteMapping
BuildWorkerEqualityDeleteMapping(const TableFunctionDistributedScanInput &input) {
	auto &bind_data = input.bind_data.Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	if (!file_list.HasDistributedCoordinatorScanTasks()) {
		throw InvalidInputException("Distributed Iceberg worker bind requires a materialized coordinator task set");
	}
	auto &global_columns = GetReaderSchema(bind_data);
	IcebergWorkerEqualityDeleteMapping result;
	result.output_column_count = input.projection_ids.empty() ? input.column_ids.size() : input.projection_ids.size();
	auto required_field_ids = file_list.GetDistributedEqualityDeleteFieldIds();
	unordered_set<int32_t> required_field_id_set(required_field_ids.begin(), required_field_ids.end());
	for (idx_t result_index = 0; result_index < input.column_ids.size(); result_index++) {
		auto &column_id = input.column_ids[result_index];
		if (column_id.IsVirtualColumn() || column_id.GetPrimaryIndex() >= global_columns.size()) {
			continue;
		}
		auto &column = global_columns[column_id.GetPrimaryIndex()];
		if (column.identifier.IsNull() || column.identifier.type().id() != LogicalTypeId::INTEGER) {
			continue;
		}
		idx_t output_index = result_index;
		if (!input.projection_ids.empty()) {
			auto projection = std::find(input.projection_ids.begin(), input.projection_ids.end(), result_index);
			if (projection == input.projection_ids.end()) {
				continue;
			}
			output_index = NumericCast<idx_t>(projection - input.projection_ids.begin());
		}
		auto field_id = column.identifier.GetValue<int32_t>();
		if (required_field_id_set.find(field_id) == required_field_id_set.end()) {
			continue;
		}
		if (!result.output_indexes.emplace(field_id, output_index).second ||
		    !result.types.emplace(field_id, column.type).second) {
			throw InvalidInputException("Distributed Iceberg scan projects field id %d more than once", field_id);
		}
	}
	for (auto field_id : required_field_ids) {
		if (result.output_indexes.find(field_id) == result.output_indexes.end()) {
			throw InvalidInputException("Distributed Iceberg scan did not project equality-delete field id %d",
			                            field_id);
		}
	}
	return result;
}

class IcebergDistributedWorkerBindData : public TableFunctionData {
public:
	explicit IcebergDistributedWorkerBindData(const MultiFileBindData &source) {
		if (!source.multi_file_reader || !source.file_list || !source.interface || !source.bind_data) {
			throw InvalidInputException("Iceberg distributed scan requires complete multi-file bind state");
		}
		auto &file_list = source.file_list->Cast<IcebergMultiFileList>();
		if (!file_list.HasDistributedCoordinatorScanTasks()) {
			throw InvalidInputException("Distributed Iceberg worker bind requires frozen coordinator scan state");
		}
		types = source.types;
		names = source.names;
		table_columns = source.table_columns;
		reader_bind = source.reader_bind;
		parquet_options =
		    ParquetMultiFileInfo::SerializeBindData(source, initial_file_row_groups, initial_file_cardinality);
		if (parquet_options.parquet_options.encryption_config) {
			throw NotImplementedException(
			    "Distributed Iceberg scans do not transport Parquet encryption keys; configure worker credentials "
			    "independently");
		}
		if (parquet_options.parquet_options.explicit_cardinality != 0) {
			throw NotImplementedException(
			    "Distributed Iceberg scans do not support the parquet explicit_cardinality option");
		}
		if (parquet_options.parquet_options.variant_legacy_encoding) {
			throw NotImplementedException("Distributed Iceberg scans do not support legacy Parquet VARIANT decoding");
		}
		schema_id = file_list.GetSnapshot().schema_id;
		metadata_json = SerializeWorkerMetadata(file_list.GetMetadata());
		scan_set_id = file_list.GetDistributedCoordinatorScanSetId();
		table_uuid = file_list.GetDistributedCoordinatorTableUUID();
		has_snapshot = file_list.DistributedCoordinatorHasSnapshot();
		snapshot_id = file_list.GetDistributedCoordinatorSnapshotId();
		if (scan_set_id.empty() || table_uuid != file_list.GetMetadata().table_uuid ||
		    (!has_snapshot && snapshot_id != 0)) {
			throw InvalidInputException("Distributed Iceberg scan has an inconsistent coordinator identity");
		}
	}
	explicit IcebergDistributedWorkerBindData(const TableFunctionDistributedScanInput &input)
	    : IcebergDistributedWorkerBindData(input.bind_data.Cast<MultiFileBindData>()) {
		auto mapping = BuildWorkerEqualityDeleteMapping(input);
		vector<int32_t> sorted_field_ids;
		sorted_field_ids.reserve(mapping.output_indexes.size());
		for (const auto &entry : mapping.output_indexes) {
			sorted_field_ids.push_back(entry.first);
		}
		std::sort(sorted_field_ids.begin(), sorted_field_ids.end());
		for (auto field_id : sorted_field_ids) {
			equality_delete_field_ids.push_back(field_id);
			equality_delete_output_indexes.push_back(mapping.output_indexes.at(field_id));
			equality_delete_types.push_back(mapping.types.at(field_id));
		}
		output_column_count = mapping.output_column_count;
	}

	IcebergDistributedWorkerBindData() = default;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IcebergDistributedWorkerBindData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		return this == &other;
	}

	bool SupportStatementCache() const override {
		return false;
	}

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty(1, "types", types);
		serializer.WriteProperty(2, "names", names);
		serializer.WriteProperty(3, "table_columns", table_columns);
		serializer.WriteProperty(4, "reader_bind", reader_bind);
		serializer.WriteProperty(5, "parquet_options", parquet_options);
		serializer.WriteProperty(6, "initial_file_row_groups", initial_file_row_groups);
		serializer.WriteProperty(7, "initial_file_cardinality", initial_file_cardinality);
		serializer.WriteProperty(8, "schema_id", schema_id);
		serializer.WriteProperty(9, "metadata_json", metadata_json);
		serializer.WriteList(10, "reader_schema", reader_bind.schema.size(), [&](Serializer::List &list, idx_t index) {
			list.WriteObject([&](Serializer &object) { SerializeWorkerColumn(object, reader_bind.schema[index]); });
		});
		serializer.WriteProperty(11, "reader_mapping", static_cast<uint8_t>(reader_bind.mapping));
		serializer.WriteProperty(12, "equality_delete_field_ids", equality_delete_field_ids);
		serializer.WriteProperty(13, "equality_delete_output_indexes", equality_delete_output_indexes);
		serializer.WriteProperty(14, "equality_delete_types", equality_delete_types);
		serializer.WriteProperty(15, "output_column_count", output_column_count);
		serializer.WriteProperty(16, "variant_legacy_encoding",
		                         parquet_options.parquet_options.variant_legacy_encoding);
		serializer.WriteProperty(17, "table_uuid", table_uuid);
		serializer.WriteProperty(18, "has_snapshot", has_snapshot);
		serializer.WriteProperty(19, "snapshot_id", has_snapshot ? snapshot_id : 0);
		serializer.WriteProperty(20, "scan_set_id", scan_set_id);
	}

	static IcebergDistributedWorkerBindData Deserialize(Deserializer &deserializer) {
		IcebergDistributedWorkerBindData result;
		result.types = deserializer.ReadProperty<vector<LogicalType>>(1, "types");
		result.names = deserializer.ReadProperty<vector<string>>(2, "names");
		result.table_columns = deserializer.ReadProperty<vector<string>>(3, "table_columns");
		result.reader_bind = deserializer.ReadProperty<MultiFileReaderBindData>(4, "reader_bind");
		result.parquet_options = deserializer.ReadProperty<ParquetOptionsSerialization>(5, "parquet_options");
		if (result.parquet_options.parquet_options.encryption_config) {
			throw SerializationException("Distributed Iceberg worker bind must not contain Parquet encryption keys");
		}
		if (result.parquet_options.parquet_options.explicit_cardinality != 0) {
			throw SerializationException(
			    "Distributed Iceberg worker bind must not contain parquet explicit cardinality");
		}
		if (result.parquet_options.parquet_options.variant_legacy_encoding) {
			throw SerializationException(
			    "Distributed Iceberg worker bind must not contain legacy Parquet VARIANT decoding");
		}
		result.initial_file_row_groups = deserializer.ReadProperty<idx_t>(6, "initial_file_row_groups");
		result.initial_file_cardinality = deserializer.ReadProperty<idx_t>(7, "initial_file_cardinality");
		result.schema_id = deserializer.ReadProperty<int32_t>(8, "schema_id");
		result.metadata_json = deserializer.ReadProperty<string>(9, "metadata_json");
		deserializer.ReadList(10, "reader_schema", [&](Deserializer::List &list, idx_t) {
			list.ReadObject(
			    [&](Deserializer &object) { result.reader_bind.schema.push_back(DeserializeWorkerColumn(object)); });
		});
		auto mapping = deserializer.ReadProperty<uint8_t>(11, "reader_mapping");
		if (mapping != static_cast<uint8_t>(MultiFileColumnMappingMode::BY_FIELD_ID)) {
			throw SerializationException("Distributed Iceberg worker bind must map columns by Iceberg field id");
		}
		result.reader_bind.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
		result.equality_delete_field_ids = deserializer.ReadProperty<vector<int32_t>>(12, "equality_delete_field_ids");
		result.equality_delete_output_indexes =
		    deserializer.ReadProperty<vector<idx_t>>(13, "equality_delete_output_indexes");
		result.equality_delete_types = deserializer.ReadProperty<vector<LogicalType>>(14, "equality_delete_types");
		result.output_column_count = deserializer.ReadProperty<idx_t>(15, "output_column_count");
		auto variant_legacy_encoding = deserializer.ReadProperty<bool>(16, "variant_legacy_encoding");
		if (variant_legacy_encoding || result.parquet_options.parquet_options.variant_legacy_encoding) {
			throw SerializationException(
			    "Distributed Iceberg worker bind must not contain legacy Parquet VARIANT decoding");
		}
		result.table_uuid = deserializer.ReadProperty<string>(17, "table_uuid");
		result.has_snapshot = deserializer.ReadProperty<bool>(18, "has_snapshot");
		result.snapshot_id = deserializer.ReadProperty<int64_t>(19, "snapshot_id");
		result.scan_set_id = deserializer.ReadProperty<string>(20, "scan_set_id");
		if (!result.has_snapshot && result.snapshot_id != 0) {
			throw SerializationException("Distributed Iceberg worker bind has a non-canonical absent snapshot id");
		}
		if (result.types.size() != result.names.size() || result.metadata_json.empty() || result.table_uuid.empty() ||
		    result.scan_set_id.empty()) {
			throw SerializationException("Distributed Iceberg worker bind is incomplete");
		}
		if (result.equality_delete_field_ids.size() != result.equality_delete_output_indexes.size() ||
		    result.equality_delete_field_ids.size() != result.equality_delete_types.size() ||
		    !std::is_sorted(result.equality_delete_field_ids.begin(), result.equality_delete_field_ids.end()) ||
		    std::adjacent_find(result.equality_delete_field_ids.begin(), result.equality_delete_field_ids.end()) !=
		        result.equality_delete_field_ids.end()) {
			throw SerializationException("Distributed Iceberg worker bind has an invalid equality-delete mapping");
		}
		return result;
	}

	vector<LogicalType> types;
	vector<string> names;
	vector<string> table_columns;
	MultiFileReaderBindData reader_bind;
	ParquetOptionsSerialization parquet_options;
	idx_t initial_file_row_groups = 0;
	idx_t initial_file_cardinality = 0;
	int32_t schema_id = 0;
	string metadata_json;
	vector<int32_t> equality_delete_field_ids;
	vector<idx_t> equality_delete_output_indexes;
	vector<LogicalType> equality_delete_types;
	idx_t output_column_count = 0;
	string table_uuid;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	string scan_set_id;
};

enum class IcebergDistributedBindKind : uint8_t { COORDINATOR = 1, WORKER = 2 };

static void IcebergDistributedScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                            const TableFunction &) {
	if (!bind_data) {
		throw SerializationException("Cannot serialize empty distributed Iceberg scan bind data");
	}
	auto worker_bind = dynamic_cast<const IcebergDistributedWorkerBindData *>(bind_data.get());
	if (worker_bind) {
		serializer.WriteProperty(1, "bind_kind", static_cast<uint8_t>(IcebergDistributedBindKind::WORKER));
		serializer.WriteObject(2, "selected_state", [&](Serializer &object) { worker_bind->Serialize(object); });
		serializer.WriteProperty(3, "coordinator_tasks", vector<string> {});
		return;
	}
	auto &coordinator_bind = bind_data->CastNoConst<MultiFileBindData>();
	auto &coordinator_file_list = coordinator_bind.file_list->Cast<IcebergMultiFileList>();
	if (coordinator_file_list.HasDistributedWorkerScanTasks()) {
		throw SerializationException("Distributed Iceberg worker bind cannot be serialized as coordinator state");
	}
	vector<string> coordinator_tasks;
	if (!coordinator_file_list.HasDistributedCoordinatorScanTasks()) {
		auto scan_set_id = UUID::ToString(UUID::GenerateRandomUUID());
		coordinator_tasks = MaterializeCoordinatorTaskPayloads(coordinator_bind, scan_set_id);
		auto &snapshot = coordinator_file_list.GetSnapshot();
		auto &metadata = coordinator_file_list.GetMetadata();
		auto schema_id = snapshot.schema_id;
		auto metadata_json = SerializeWorkerMetadata(metadata);
		auto has_snapshot = snapshot.snapshot != nullptr;
		auto snapshot_id = has_snapshot ? snapshot.snapshot->snapshot_id : 0;
		auto owned_scan_info = CreateOwnedDistributedScanInfo(metadata_json, schema_id);
		coordinator_file_list.InstallDistributedCoordinatorScanTasks(coordinator_tasks, std::move(owned_scan_info),
		                                                             std::move(scan_set_id), metadata.table_uuid,
		                                                             has_snapshot, snapshot_id);
	} else {
		auto task_count = coordinator_bind.file_list->GetTotalFileCount();
		coordinator_tasks.reserve(task_count);
		for (idx_t task_index = 0; task_index < task_count; task_index++) {
			coordinator_tasks.push_back(coordinator_file_list.GetDistributedScanTaskPayload(task_index));
		}
	}
	IcebergDistributedWorkerBindData selected_state(coordinator_bind);
	serializer.WriteProperty(1, "bind_kind", static_cast<uint8_t>(IcebergDistributedBindKind::COORDINATOR));
	serializer.WriteObject(2, "selected_state", [&](Serializer &object) { selected_state.Serialize(object); });
	serializer.WriteProperty(3, "coordinator_tasks", coordinator_tasks);
}

static unique_ptr<FunctionData> IcebergDistributedScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto bind_kind_value = deserializer.ReadProperty<uint8_t>(1, "bind_kind");
	if (bind_kind_value != static_cast<uint8_t>(IcebergDistributedBindKind::COORDINATOR) &&
	    bind_kind_value != static_cast<uint8_t>(IcebergDistributedBindKind::WORKER)) {
		throw SerializationException("Distributed Iceberg scan bind has invalid kind %d", bind_kind_value);
	}
	IcebergDistributedWorkerBindData state;
	deserializer.ReadObject(2, "selected_state", [&](Deserializer &object) {
		state = IcebergDistributedWorkerBindData::Deserialize(object);
	});
	auto coordinator_tasks = deserializer.ReadProperty<vector<string>>(3, "coordinator_tasks");
	auto bind_kind = static_cast<IcebergDistributedBindKind>(bind_kind_value);
	if (bind_kind == IcebergDistributedBindKind::WORKER && !coordinator_tasks.empty()) {
		throw SerializationException("Distributed Iceberg worker bind contains coordinator tasks");
	}
	if (bind_kind == IcebergDistributedBindKind::COORDINATOR &&
	    (!state.equality_delete_field_ids.empty() || !state.equality_delete_output_indexes.empty() ||
	     !state.equality_delete_types.empty() || state.output_column_count != 0)) {
		throw SerializationException("Distributed Iceberg coordinator bind contains worker output mappings");
	}
	auto scan_info = CreateOwnedDistributedScanInfo(state.metadata_json, state.schema_id);
	if (scan_info->metadata.table_uuid != state.table_uuid) {
		throw SerializationException("Distributed Iceberg scan identity does not match its table metadata");
	}
	function.function_info = scan_info;

	auto multi_file_reader = MultiFileReader::Create(function);
	IcebergOptions worker_options;
	multi_file_reader->Cast<IcebergMultiFileReader>().options = worker_options;
	auto file_list = make_shared_ptr<IcebergMultiFileList>(context, scan_info, string(), worker_options);
	if (bind_kind == IcebergDistributedBindKind::COORDINATOR) {
		file_list->InstallDistributedCoordinatorScanTasks(std::move(coordinator_tasks), scan_info,
		                                                  std::move(state.scan_set_id), std::move(state.table_uuid),
		                                                  state.has_snapshot, state.snapshot_id);
	} else {
		unordered_map<int32_t, idx_t> equality_delete_mapping;
		unordered_map<int32_t, LogicalType> equality_delete_types;
		for (idx_t index = 0; index < state.equality_delete_field_ids.size(); index++) {
			auto field_id = state.equality_delete_field_ids[index];
			auto output_index = state.equality_delete_output_indexes[index];
			auto &type = state.equality_delete_types[index];
			if (field_id <= 0 || output_index >= state.output_column_count || type.id() == LogicalTypeId::INVALID ||
			    !equality_delete_mapping.emplace(field_id, output_index).second ||
			    !equality_delete_types.emplace(field_id, type).second) {
				throw SerializationException("Distributed Iceberg worker bind has an invalid equality-delete mapping");
			}
		}
		file_list->ConfigureDistributedWorkerEqualityDeleteMapping(
		    std::move(equality_delete_mapping), std::move(equality_delete_types), std::move(state.scan_set_id));
	}
	auto interface = make_uniq<ParquetMultiFileInfo>();
	interface->InitializeInterface(context, *multi_file_reader, *file_list);

	auto result = make_uniq<MultiFileBindData>();
	result->multi_file_reader = std::move(multi_file_reader);
	result->file_list = std::move(file_list);
	result->interface = std::move(interface);
	ParquetMultiFileInfo::DeserializeBindData(*result, std::move(state.parquet_options), state.initial_file_row_groups,
	                                          state.initial_file_cardinality);
	result->types = std::move(state.types);
	result->names = std::move(state.names);
	result->table_columns = std::move(state.table_columns);
	result->reader_bind = std::move(state.reader_bind);
	if (result->reader_bind.mapping != MultiFileColumnMappingMode::BY_FIELD_ID) {
		throw SerializationException("Distributed Iceberg worker bind must map columns by Iceberg field id");
	}
	result->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(result->names, result->types);
	result->virtual_columns = IcebergTableEntry::VirtualColumns();
	result->interface->FinalizeBindData(*result);
	return std::move(result);
}

static vector<DistributedScanTask> IcebergPlanDistributedScan(const TableFunctionDistributedScanInput &input) {
	auto &bind_data = input.bind_data.Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	if (!file_list.HasDistributedCoordinatorScanTasks()) {
		throw InvalidInputException("Distributed Iceberg scan planning requires a materialized coordinator task set");
	}
	auto file_count = file_list.GetTotalFileCount();
	auto files = file_list.GetAllFiles();
	if (files.size() != file_count) {
		throw InternalException("Iceberg distributed scan expanded %llu files but returned %llu",
		                        static_cast<unsigned long long>(file_count),
		                        static_cast<unsigned long long>(files.size()));
	}
	vector<DistributedScanTask> result;
	result.reserve(files.size());
	for (idx_t file_index = 0; file_index < files.size(); file_index++) {
		auto manifest_entry = file_list.GetManifestEntry(file_index);
		DistributedScanTask task;
		task.task_id = std::to_string(file_index);
		task.payload = file_list.GetDistributedScanTaskPayload(file_index);
		auto record_count = manifest_entry.entry.data_file.record_count;
		auto file_size = manifest_entry.entry.data_file.file_size_in_bytes;
		if (record_count < 0 || file_size < 0) {
			throw InvalidInputException("Iceberg data file '%s' has invalid size metadata",
			                            manifest_entry.entry.data_file.file_path);
		}
		task.estimated_cardinality = optional_idx(NumericCast<idx_t>(record_count));
		task.estimated_bytes = optional_idx(NumericCast<idx_t>(file_size));
		result.push_back(std::move(task));
	}
	return result;
}

static unique_ptr<FunctionData> IcebergCreateDistributedWorkerBind(const TableFunctionDistributedScanInput &input) {
	return make_uniq<IcebergDistributedWorkerBindData>(input);
}

static void IcebergApplyDistributedScanTasks(FunctionData &worker_bind_data, const vector<DistributedScanTask> &tasks) {
	unordered_set<string> task_ids;
	vector<string> payloads;
	payloads.reserve(tasks.size());
	for (const auto &task : tasks) {
		if (!IsCanonicalTaskId(task.task_id) || task.payload.empty()) {
			throw InvalidInputException("Invalid distributed Iceberg scan task '%s'", task.task_id);
		}
		if (!task_ids.insert(task.task_id).second) {
			throw InvalidInputException("Duplicate distributed Iceberg scan task id '%s'", task.task_id);
		}
		payloads.push_back(task.payload);
	}
	auto &bind_data = worker_bind_data.Cast<MultiFileBindData>();
	bind_data.file_list->Cast<IcebergMultiFileList>().InstallDistributedWorkerScanTasks(std::move(payloads));
}

static unique_ptr<GlobalTableFunctionState> IcebergDistributedScanInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	if (bind_data.file_list->Cast<IcebergMultiFileList>().HasDistributedCoordinatorScanTasks()) {
		throw InvalidInputException("A frozen distributed Iceberg coordinator scan cannot be executed directly; "
		                            "create a worker bind and assign its scan tasks");
	}
	return MultiFileFunction<ParquetMultiFileInfo>::MultiFileInitGlobal(context, input);
}

static vector<IcebergEqualityDeleteRow>
BindEqualityDeleteRows(const vector<IcebergDistributedEqualityDeleteRow> &rows,
                       const unordered_map<int32_t, idx_t> &field_id_to_output_index,
                       const unordered_map<int32_t, LogicalType> &field_id_to_type) {
	vector<IcebergEqualityDeleteRow> result;
	result.reserve(rows.size());
	for (const auto &row : rows) {
		IcebergEqualityDeleteRow bound_row;
		for (const auto &entry : row.values) {
			auto output_index = field_id_to_output_index.find(entry.first);
			auto expected_type = field_id_to_type.find(entry.first);
			if (output_index == field_id_to_output_index.end() || expected_type == field_id_to_type.end()) {
				throw SerializationException(
				    "Distributed Iceberg worker task references unprojected equality-delete field id %d", entry.first);
			}
			auto &value = entry.second;
			auto &source_type = value.type();
			auto &target_type = expected_type->second;
			bool supported_promotion = source_type == target_type;
			if (!supported_promotion) {
				// Keep this allowlist aligned with the schema evolutions accepted by
				// IcebergSchemaEntry. Arbitrary DuckDB casts are not a valid Iceberg
				// field evolution contract.
				switch (source_type.id()) {
				case LogicalTypeId::INTEGER:
					supported_promotion = target_type.id() == LogicalTypeId::BIGINT;
					break;
				case LogicalTypeId::FLOAT:
					supported_promotion = target_type.id() == LogicalTypeId::DOUBLE;
					break;
				case LogicalTypeId::DECIMAL: {
					if (target_type.id() == LogicalTypeId::DECIMAL) {
						uint8_t source_width;
						uint8_t source_scale;
						uint8_t target_width;
						uint8_t target_scale;
						source_type.GetDecimalProperties(source_width, source_scale);
						target_type.GetDecimalProperties(target_width, target_scale);
						supported_promotion = source_scale == target_scale && source_width <= target_width;
					}
					break;
				}
				case LogicalTypeId::DATE:
					supported_promotion =
					    target_type.id() == LogicalTypeId::TIMESTAMP || target_type.id() == LogicalTypeId::TIMESTAMP_NS;
					break;
				default:
					break;
				}
			}
			if (!supported_promotion) {
				throw SerializationException(
				    "Distributed Iceberg worker task cannot promote equality-delete field id %d from %s to %s",
				    entry.first, source_type.ToString(), target_type.ToString());
			}
			Value promoted_value;
			if (value.IsNull()) {
				promoted_value = Value(target_type);
			} else {
				string error_message;
				if (!value.DefaultTryCastAs(target_type, promoted_value, &error_message)) {
					throw SerializationException(
					    "Distributed Iceberg worker task cannot cast equality-delete field id %d from %s to %s: %s",
					    entry.first, source_type.ToString(), target_type.ToString(), error_message);
				}
			}
			auto reference = make_uniq<BoundReferenceExpression>(target_type, output_index->second);
			unique_ptr<Expression> predicate;
			if (promoted_value.IsNull()) {
				auto is_not_null =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null->children.push_back(std::move(reference));
				predicate = std::move(is_not_null);
			} else {
				predicate =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(reference),
				                                         make_uniq<BoundConstantExpression>(std::move(promoted_value)));
			}
			bound_row.filters.emplace(entry.first, std::move(predicate));
		}
		result.push_back(std::move(bound_row));
	}
	return result;
}

} // namespace

struct IcebergDistributedScanState::FileState {
	FileState(idx_t file_index, string task_payload_p, IcebergDistributedDecodedTask task,
	          optional_ptr<const unordered_map<int32_t, idx_t>> field_id_to_output_index,
	          optional_ptr<const unordered_map<int32_t, LogicalType>> field_id_to_type)
	    : manifest_list_entry(std::move(task.manifest_file)), resolved_path(std::move(task.resolved_path)),
	      task_payload(std::move(task_payload_p)), equality_delete_values(std::move(task.equality_delete_rows)) {
		if (field_id_to_output_index) {
			if (!field_id_to_type) {
				throw InternalException("Distributed Iceberg worker equality-delete types are missing");
			}
			equality_delete_rows =
			    BindEqualityDeleteRows(equality_delete_values, *field_id_to_output_index, *field_id_to_type);
		}
		manifest_list_entry.manifest_entries.push_back(std::move(task.manifest_entry));
		bound_manifest = make_uniq<BoundIcebergManifestListEntry>(file_index, manifest_list_entry);
		bound_entry =
		    make_uniq<BoundIcebergManifestEntry>(bound_manifest->BindEntry(manifest_list_entry.manifest_entries[0]));

		file = OpenFileInfo(resolved_path);
		file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file.extended_info->options["file_size"] = Value::UBIGINT(NumericCast<uint64_t>(DataFile().file_size_in_bytes));
		file.extended_info->options["record_count"] = Value::UBIGINT(NumericCast<uint64_t>(DataFile().record_count));
		file.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file.extended_info->options["etag"] = Value("");
		file.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		if (bound_entry->HasFirstRowId()) {
			file.extended_info->options["first_row_id"] = Value::BIGINT(bound_entry->GetFirstRowId());
		}
		file.extended_info->options["sequence_number"] =
		    Value::BIGINT(bound_entry->entry.GetSequenceNumber(manifest_list_entry.file));
		file.extended_info->options["has_deletes"] =
		    Value::BOOLEAN(!task.positional_delete_rows.empty() || !equality_delete_values.empty());

		if (!task.positional_delete_rows.empty()) {
			positional_deletes = make_shared_ptr<IcebergPositionalDeleteData>(*bound_entry);
			for (auto row : task.positional_delete_rows) {
				if (row < 0 || row >= DataFile().record_count || !positional_deletes->invalid_rows.insert(row).second) {
					throw SerializationException(
					    "Distributed Iceberg scan task contains an invalid positional-delete row");
				}
			}
		}
	}

	const IcebergDataFile &DataFile() const {
		return bound_entry->entry.data_file;
	}

	IcebergManifestListEntry manifest_list_entry;
	unique_ptr<BoundIcebergManifestListEntry> bound_manifest;
	unique_ptr<BoundIcebergManifestEntry> bound_entry;
	string resolved_path;
	string task_payload;
	OpenFileInfo file;
	shared_ptr<IcebergPositionalDeleteData> positional_deletes;
	vector<IcebergDistributedEqualityDeleteRow> equality_delete_values;
	vector<IcebergEqualityDeleteRow> equality_delete_rows;
};

IcebergDistributedScanState::IcebergDistributedScanState() = default;

IcebergDistributedScanState::~IcebergDistributedScanState() = default;

void IcebergDistributedScanState::InstallTasksInternal(
    vector<string> payloads, const string &expected_scan_set_id,
    optional_ptr<const unordered_map<int32_t, idx_t>> field_id_to_output_index,
    optional_ptr<const unordered_map<int32_t, LogicalType>> field_id_to_type) {
	if (expected_scan_set_id.empty()) {
		throw InternalException("Distributed Iceberg scan set identity cannot be empty");
	}
	vector<unique_ptr<FileState>> installed;
	installed.reserve(payloads.size());
	unordered_set<string> paths;
	for (idx_t index = 0; index < payloads.size(); index++) {
		auto decoded = DeserializeTask(payloads[index]);
		if (decoded.scan_set_id != expected_scan_set_id) {
			throw InvalidInputException("Distributed Iceberg scan received a task from another frozen scan set");
		}
		if (!paths.insert(decoded.resolved_path).second) {
			throw InvalidInputException("Distributed Iceberg scan assigned data file '%s' more than once",
			                            decoded.resolved_path);
		}
		installed.push_back(make_uniq<FileState>(index, std::move(payloads[index]), std::move(decoded),
		                                         field_id_to_output_index, field_id_to_type));
	}
	files = std::move(installed);
}

void IcebergDistributedScanState::InstallCoordinatorTasks(vector<string> payloads,
                                                          shared_ptr<IcebergScanInfo> scan_info, string scan_set_id,
                                                          string table_uuid, bool has_snapshot, int64_t snapshot_id) {
	if (!scan_info || !scan_info->owned_temp_data || scan_info->transaction_data) {
		throw InternalException(
		    "Distributed Iceberg coordinator scan requires owned metadata without transaction data");
	}
	if (scan_set_id.empty() || table_uuid.empty() || scan_info->metadata.table_uuid != table_uuid ||
	    (!has_snapshot && snapshot_id != 0)) {
		throw InternalException("Distributed Iceberg coordinator scan has an invalid source identity");
	}
	InstallTasksInternal(std::move(payloads), scan_set_id, nullptr, nullptr);
	coordinator_task_set = true;
	coordinator_scan_info = std::move(scan_info);
	coordinator_scan_set_id = std::move(scan_set_id);
	coordinator_table_uuid = std::move(table_uuid);
	coordinator_has_snapshot = has_snapshot;
	coordinator_snapshot_id = snapshot_id;
	worker_mapping_configured = false;
	worker_tasks_installed = false;
	worker_scan_set_id.clear();
	worker_field_id_to_output_index.clear();
	worker_field_id_to_type.clear();
}

void IcebergDistributedScanState::ConfigureWorkerEqualityDeleteMapping(
    unordered_map<int32_t, idx_t> field_id_to_output_index, unordered_map<int32_t, LogicalType> field_id_to_type,
    string scan_set_id) {
	if (field_id_to_output_index.size() != field_id_to_type.size() || scan_set_id.empty()) {
		throw InternalException("Distributed Iceberg worker equality-delete mapping is inconsistent");
	}
	coordinator_task_set = false;
	coordinator_scan_info.reset();
	coordinator_scan_set_id.clear();
	coordinator_table_uuid.clear();
	coordinator_has_snapshot = false;
	coordinator_snapshot_id = 0;
	worker_mapping_configured = true;
	worker_tasks_installed = false;
	worker_scan_set_id = std::move(scan_set_id);
	worker_field_id_to_output_index = std::move(field_id_to_output_index);
	worker_field_id_to_type = std::move(field_id_to_type);
	files.clear();
}

void IcebergDistributedScanState::InstallWorkerTasks(vector<string> payloads) {
	if (!worker_mapping_configured || coordinator_task_set) {
		throw InternalException("Distributed Iceberg worker scan received tasks before its output mapping");
	}
	InstallTasksInternal(std::move(payloads), worker_scan_set_id, worker_field_id_to_output_index,
	                     worker_field_id_to_type);
	worker_tasks_installed = true;
}

bool IcebergDistributedScanState::IsCoordinatorTaskSet() const {
	return coordinator_task_set;
}

bool IcebergDistributedScanState::HasCoordinatorScanInfo() const {
	return coordinator_task_set && coordinator_scan_info != nullptr;
}

const IcebergTableMetadata &IcebergDistributedScanState::GetCoordinatorMetadata() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator metadata is unavailable");
	}
	return coordinator_scan_info->metadata;
}

const IcebergSnapshotScanInfo &IcebergDistributedScanState::GetCoordinatorSnapshot() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator snapshot is unavailable");
	}
	return coordinator_scan_info->snapshot_info;
}

const IcebergTableSchema &IcebergDistributedScanState::GetCoordinatorSchema() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator schema is unavailable");
	}
	return coordinator_scan_info->schema;
}

const string &IcebergDistributedScanState::GetCoordinatorScanSetId() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator identity is unavailable");
	}
	return coordinator_scan_set_id;
}

const string &IcebergDistributedScanState::GetCoordinatorTableUUID() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator identity is unavailable");
	}
	return coordinator_table_uuid;
}

bool IcebergDistributedScanState::CoordinatorHasSnapshot() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator identity is unavailable");
	}
	return coordinator_has_snapshot;
}

int64_t IcebergDistributedScanState::GetCoordinatorSnapshotId() const {
	if (!HasCoordinatorScanInfo()) {
		throw InternalException("Distributed Iceberg coordinator identity is unavailable");
	}
	return coordinator_snapshot_id;
}

bool IcebergDistributedScanState::IsWorkerTaskSet() const {
	return worker_mapping_configured;
}

string IcebergDistributedScanState::GetTaskPayload(idx_t file_id) const {
	if (!coordinator_task_set || file_id >= files.size()) {
		throw InternalException("Distributed Iceberg coordinator task index %llu is unavailable",
		                        static_cast<unsigned long long>(file_id));
	}
	return files[file_id]->task_payload;
}

vector<int32_t> IcebergDistributedScanState::GetEqualityDeleteFieldIds() const {
	RequireInstalledTasks();
	set<int32_t> field_ids;
	for (const auto &file : files) {
		for (const auto &row : file->equality_delete_values) {
			for (const auto &entry : row.values) {
				field_ids.insert(entry.first);
			}
		}
	}
	return vector<int32_t>(field_ids.begin(), field_ids.end());
}

void IcebergDistributedScanState::RequireInstalledTasks() const {
	if (worker_mapping_configured && !worker_tasks_installed) {
		throw InvalidInputException("Distributed Iceberg worker scan has no explicit task assignment");
	}
}

vector<OpenFileInfo> IcebergDistributedScanState::GetAllFiles() const {
	RequireInstalledTasks();
	vector<OpenFileInfo> result;
	result.reserve(files.size());
	for (const auto &file : files) {
		result.push_back(file->file);
	}
	return result;
}

FileExpandResult IcebergDistributedScanState::GetExpandResult() const {
	RequireInstalledTasks();
	if (files.empty()) {
		return FileExpandResult::NO_FILES;
	}
	if (files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergDistributedScanState::GetTotalFileCount() const {
	RequireInstalledTasks();
	return files.size();
}

unique_ptr<NodeStatistics> IcebergDistributedScanState::GetCardinality() const {
	RequireInstalledTasks();
	idx_t cardinality = 0;
	for (const auto &file : files) {
		auto file_cardinality = NumericCast<idx_t>(file->DataFile().record_count);
		if (file_cardinality > NumericLimits<idx_t>::Maximum() - cardinality) {
			throw InvalidInputException("Distributed Iceberg scan cardinality exceeds idx_t");
		}
		cardinality += file_cardinality;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

OpenFileInfo IcebergDistributedScanState::GetFile(idx_t file_id) const {
	RequireInstalledTasks();
	if (file_id >= files.size()) {
		return OpenFileInfo();
	}
	return files[file_id]->file;
}

BoundIcebergManifestEntry IcebergDistributedScanState::GetManifestEntry(idx_t file_id) const {
	RequireInstalledTasks();
	if (file_id >= files.size()) {
		throw InternalException("Distributed Iceberg scan file index %llu is out of bounds",
		                        static_cast<unsigned long long>(file_id));
	}
	return *files[file_id]->bound_entry;
}

const IcebergManifestFile &IcebergDistributedScanState::GetManifestFile(const BoundIcebergManifestEntry &entry) const {
	RequireInstalledTasks();
	if (entry.manifest_file_idx >= files.size()) {
		throw InternalException("Distributed Iceberg manifest index %llu is out of bounds",
		                        static_cast<unsigned long long>(entry.manifest_file_idx));
	}
	auto &file = *files[entry.manifest_file_idx];
	if (entry.entry.data_file.file_path != file.bound_entry->entry.data_file.file_path) {
		throw InternalException("Distributed Iceberg manifest lookup received an unrelated entry");
	}
	return file.manifest_list_entry.file;
}

vector<IcebergPartitionInfo> IcebergDistributedScanState::GetPartitionInfo(const string &file_path) const {
	RequireInstalledTasks();
	for (const auto &file : files) {
		if (file->resolved_path == file_path || file->DataFile().file_path == file_path) {
			return file->DataFile().partition_info;
		}
	}
	throw InternalException("Could not find distributed Iceberg data file '%s'", file_path);
}

unique_ptr<DeleteFilter> IcebergDistributedScanState::GetPositionalDeletes(const string &file_path) const {
	RequireInstalledTasks();
	auto delete_data = GetPositionalDeleteData(file_path);
	if (!delete_data) {
		return nullptr;
	}
	return delete_data->ToFilter();
}

shared_ptr<IcebergDeleteData> IcebergDistributedScanState::GetPositionalDeleteData(const string &file_path) const {
	RequireInstalledTasks();
	for (const auto &file : files) {
		if (file->resolved_path == file_path || file->DataFile().file_path == file_path) {
			return file->positional_deletes;
		}
	}
	return nullptr;
}

vector<reference<const IcebergEqualityDeleteRow>>
IcebergDistributedScanState::GetEqualityDeletes(const BoundIcebergManifestEntry &entry) const {
	RequireInstalledTasks();
	if (entry.manifest_file_idx >= files.size()) {
		throw InternalException("Distributed Iceberg manifest index %llu is out of bounds",
		                        static_cast<unsigned long long>(entry.manifest_file_idx));
	}
	auto &file = *files[entry.manifest_file_idx];
	if (entry.entry.data_file.file_path != file.bound_entry->entry.data_file.file_path) {
		throw InternalException("Distributed Iceberg equality-delete lookup received an unrelated entry");
	}
	vector<reference<const IcebergEqualityDeleteRow>> result;
	result.reserve(file.equality_delete_rows.size());
	for (const auto &row : file.equality_delete_rows) {
		result.emplace_back(row);
	}
	return result;
}

void ConfigureIcebergDistributedScan(TableFunction &function) {
	function.serialize = IcebergDistributedScanSerialize;
	function.deserialize = IcebergDistributedScanDeserialize;
	function.init_global = IcebergDistributedScanInitGlobal;
	TableFunctionDistributedScanCallbacks callbacks;
	callbacks.protocol_version = 1;
	callbacks.task_codec = {ICEBERG_DISTRIBUTED_SCAN_TASK_CODEC, 1};
	callbacks.plan = IcebergPlanDistributedScan;
	callbacks.create_worker_bind = IcebergCreateDistributedWorkerBind;
	callbacks.apply_tasks = IcebergApplyDistributedScanTasks;
	function.SetDistributedScanCallbacks(std::move(callbacks));
}

} // namespace duckdb
