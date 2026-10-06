#include "function/copy/iceberg_copy_function.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"

#include "execution/operator/copy/iceberg_copy.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#ifdef ICEBERG_ENABLE_VORTEX
#include "storage/iceberg_vortex.hpp"
#endif

namespace duckdb {

static BoundStatement IcebergCopyPlan(Binder &binder, CopyStatement &stmt) {
	auto &copy_info = *stmt.info;
	// bind the select statement
	auto node_copy = copy_info.select_statement->Copy();
	auto child_statement = binder.Bind(*node_copy);

	// Create bind data with metadata and schema
	auto bind_data = make_uniq<CopyIcebergBindData>(copy_info, std::move(child_statement.names),
	                                                std::move(child_statement.types), binder.context);

	// Create logical copy operator
	auto logical_copy = make_uniq<IcebergLogicalCopy>();
	logical_copy->bind_data = std::move(bind_data);
	logical_copy->children.push_back(std::move(child_statement.plan));

	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};
	result.plan = std::move(logical_copy);
	return result;
}

CopyIcebergBindData::CopyIcebergBindData(const vector<string> &names, const vector<LogicalType> &types,
                                         const string &file_path, unique_ptr<IcebergTableMetadata> table_metadata,
                                         unique_ptr<IcebergTableSchema> table_schema)
    : names(names), types(types), file_path(file_path), table_metadata(std::move(table_metadata)),
      table_schema(std::move(table_schema)) {
}

CopyIcebergBindData::CopyIcebergBindData(const CopyInfo &info, vector<string> &&names_p, vector<LogicalType> &&types_p,
                                         ClientContext &context)
    : names(std::move(names_p)), types(std::move(types_p)) {
	file_path = info.file_path;

	// Create IcebergTableMetadata
	table_metadata = make_uniq<IcebergTableMetadata>();
	table_metadata->table_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	table_metadata->location = file_path;
	table_metadata->iceberg_version = 2;
	table_metadata->SetCurrentSchemaId(0);

	// Create ColumnList from query output
	ColumnList columns;
	for (idx_t i = 0; i < names.size(); i++) {
		columns.AddColumn(ColumnDefinition(names[i], types[i]));
	}

	int32_t last_column_id;
	table_schema =
	    IcebergCreateTableRequest::CreateIcebergSchema(context, *table_metadata, columns, nullptr, last_column_id);
	table_schema->schema_id = 0;
	auto &result_schema = table_metadata->AddSchemaOrGetExisting(table_schema);
	if (result_schema.schema_id != 0) {
		throw InternalException("Iceberg COPY created non-0 schema id (%d)", result_schema.schema_id);
	}
	table_metadata->SetCurrentSchemaId(0);
	//! FIXME: adapt when we have partitioning support
	table_metadata->partition_specs.emplace(0, IcebergPartitionSpec(0));
	table_metadata->default_spec_id = 0;
	table_metadata->last_column_id = last_column_id;
	table_metadata->last_partition_field_id = 0;
	table_metadata->default_sort_order_id = 0;

#ifdef ICEBERG_ENABLE_VORTEX
	// Catalog appends resolve the default sort order from this map.
	IcebergSortOrder sort_order;
	sort_order.sort_order_id = 0;
	table_metadata->sort_specs.emplace(0, std::move(sort_order));
#endif
	for (const auto &option : info.options) {
		if (StringUtil::CIEquals(option.first, "data_format")) {
			if (option.second.size() != 1 || option.second[0].IsNull()) {
				throw BinderException("Iceberg DATA_FORMAT requires one format name");
			}
			auto format = StringUtil::Lower(option.second[0].GetValue<string>());
			if (format != "parquet" && format != "vortex") {
				throw NotImplementedException("Unsupported Iceberg data format '%s'", format);
			}
#ifndef ICEBERG_ENABLE_VORTEX
			if (format == "vortex") {
				throw NotImplementedException("Vortex support is not enabled; rebuild with ICEBERG_ENABLE_VORTEX=ON");
			}
#endif
			table_metadata->table_properties["write.format.default"] = format;
		} else {
			throw BinderException("Unsupported Iceberg COPY option '%s'", option.first);
		}
	}
#ifdef ICEBERG_ENABLE_VORTEX
	if (IcebergVortex::WriteFormat(*table_metadata) == "vortex") {
		IcebergVortex::ValidateTable(*table_metadata);
	}
#endif
}

unique_ptr<FunctionData> CopyIcebergBindData::Copy() const {
	throw NotImplementedException("Can't copy CopyIcebergBindData!");
	// return make_uniq<CopyIcebergBindData>(names, types, file_path, table_metadata->Copy(), table_schema->Copy());
}

bool CopyIcebergBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<CopyIcebergBindData>();
	if (names.size() != other.names.size()) {
		return false;
	}
	if (types.size() != other.types.size()) {
		return false;
	}
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < types.size(); i++) {
		if (types[i] != other.types[i]) {
			return false;
		}
		if (names[i] != other.names[i]) {
			return false;
		}
	}

	//! TODO: compare table metadata and table schema ???
	return true;
}

CopyFunction IcebergCopyFunction::Create() {
	auto res = CopyFunction("iceberg");
	res.plan = IcebergCopyPlan;
	return res;
}

} // namespace duckdb
