#include "planning/iceberg_optimizer.hpp"

#include "iceberg_logging.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

GuaranteeEqualityDeleteColumnsOptimizer::GuaranteeEqualityDeleteColumnsOptimizer(ClientContext &context)
    : context(context) {
}

#ifdef ICEBERG_VANE_DISTRIBUTED
static idx_t AddDistributedEqualityDeleteColumn(LogicalGet &get, MultiFileBindData &bind_data,
                                                const MultiFileColumnDefinition &column) {
	if (bind_data.reader_bind.mapping != MultiFileColumnMappingMode::BY_FIELD_ID ||
	    bind_data.types.size() != bind_data.names.size() || bind_data.types.size() != bind_data.columns.size() ||
	    get.returned_types.size() != get.names.size() || get.returned_types.size() != bind_data.types.size()) {
		throw InternalException("Distributed Iceberg equality-delete column state is inconsistent");
	}

	auto reader_schema_index = bind_data.reader_bind.schema.size();
	auto filename_index = bind_data.reader_bind.filename_idx;
	bool has_materialized_filename =
	    filename_index.IsValid() && !ColumnIndex(filename_index.GetIndex()).IsVirtualColumn();
	if (!has_materialized_filename) {
		if (bind_data.types.size() != reader_schema_index) {
			throw NotImplementedException(
			    "Distributed Iceberg equality deletes do not support additional materialized multi-file columns");
		}
		get.returned_types.push_back(column.type);
		get.names.push_back(column.name);
		bind_data.types.push_back(column.type);
		bind_data.names.push_back(column.name);
		bind_data.columns.push_back(column);
		bind_data.reader_bind.schema.push_back(column);
		return reader_schema_index;
	}

	if (filename_index.GetIndex() != reader_schema_index || bind_data.types.size() != reader_schema_index + 1) {
		throw InternalException("Distributed Iceberg filename column is not in its canonical bind position");
	}
	for (auto &column_id : get.GetMutableColumnIds()) {
		if (column_id.IsVirtualColumn() || column_id.GetPrimaryIndex() != reader_schema_index) {
			continue;
		}
		if (column_id.IsPushdownExtract()) {
			throw InternalException("Distributed Iceberg filename column cannot be a pushed-down child extraction");
		}
		ColumnIndex shifted(reader_schema_index + 1, column_id.GetChildIndexes());
		if (column_id.HasType()) {
			shifted.SetType(column_id.GetType());
		}
		column_id = std::move(shifted);
	}
	auto filename_filter = get.table_filters.filters.find(reader_schema_index);
	if (filename_filter != get.table_filters.filters.end()) {
		auto filter = std::move(filename_filter->second);
		get.table_filters.filters.erase(filename_filter);
		if (!get.table_filters.filters.emplace(reader_schema_index + 1, std::move(filter)).second) {
			throw InternalException("Distributed Iceberg filename filter index is duplicated");
		}
	}

	get.returned_types.insert(get.returned_types.begin() + reader_schema_index, column.type);
	get.names.insert(get.names.begin() + reader_schema_index, column.name);
	bind_data.types.insert(bind_data.types.begin() + reader_schema_index, column.type);
	bind_data.names.insert(bind_data.names.begin() + reader_schema_index, column.name);
	bind_data.columns.insert(bind_data.columns.begin() + reader_schema_index, column);
	bind_data.reader_bind.schema.push_back(column);
	bind_data.reader_bind.filename_idx = optional_idx(reader_schema_index + 1);
	return reader_schema_index;
}
#endif

void GuaranteeEqualityDeleteColumnsOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto &child = op->children[child_index];
		if (child->type != LogicalOperatorType::LOGICAL_GET) {
			VisitOperator(child);
			continue;
		}
		auto &get = child->Cast<LogicalGet>();
		// Identify our iceberg scan by the multi file reader it installs, not by
		// function name alone. Other extensions might create their own
		// iceberg_scan function or overload ours, so we cannot just depend on
		// the name. We avoid dynamic_cast here because it does not behave
		// reliably across the extension linking boundary; instead the function
		// pointer uniquely identifies our scan, which guarantees the bind data
		// and file list are the iceberg types we expect.
		if (get.function.name != "iceberg_scan" ||
		    get.function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance || !get.bind_data) {
			VisitOperator(child);
			continue;
		}
		auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
		if (!mfbd.file_list) {
			continue;
		}
		auto &iceberg_list = mfbd.file_list->Cast<IcebergMultiFileList>();
		unordered_set<int32_t> required_field_ids;
#ifdef ICEBERG_VANE_DISTRIBUTED
		if (iceberg_list.HasDistributedScanPlan()) {
			for (auto field_id : iceberg_list.GetDistributedEqualityDeleteFieldIds()) {
				required_field_ids.insert(field_id);
			}
		} else {
#endif
			auto delete_manifest_entries = iceberg_list.GetDeleteManifestEntries();
			for (auto &entry : delete_manifest_entries) {
				auto &mft = entry.entry;
				if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
					continue;
				}
				for (auto fid : mft.data_file.equality_ids) {
					required_field_ids.insert(fid);
				}
			}
#ifdef ICEBERG_VANE_DISTRIBUTED
		}
#endif

		if (required_field_ids.empty()) {
			continue;
		}

		auto &schema_columns = iceberg_list.GetSchema().columns;
		LogicalType col_type;
		vector<unique_ptr<Expression>> args;
		for (auto fid : required_field_ids) {
			idx_t schema_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < schema_columns.size(); i++) {
				if (schema_columns[i]->id == fid) {
					schema_idx = i;
					col_type = schema_columns[schema_idx]->type;
					break;
				}
			}
			if (schema_idx == DConstants::INVALID_INDEX) {
				// column was deleted and exists most likely in an old schemas
				// TODO: if the type of the equality delete column was evolved, then grabbing just any schema could be a
				// problem
#ifdef ICEBERG_VANE_DISTRIBUTED
				optional_ptr<const IcebergColumnDefinition> col_info;
				if (iceberg_list.HasDistributedScanPlan()) {
					col_info = iceberg_list.GetMetadata().FindColumnByFieldId(fid);
				} else {
					auto table = iceberg_list.GetTable();
					D_ASSERT(table);
					col_info = table->table_info.table_metadata.FindColumnByFieldId(fid);
				}
#else
				auto table = iceberg_list.GetTable();
				D_ASSERT(table);
				auto col_info = table->table_info.table_metadata.FindColumnByFieldId(fid);
#endif
				if (!col_info) {
					throw InvalidConfigurationException(
					    "column %d must apply equality deletes, but no schema has a column with that field id", fid);
				}
				DUCKDB_LOG(context, IcebergLogType, "Detected deleted column with equality delete: %s", col_info->name);
				col_type = col_info->type;

				auto new_col = col_info->GetMultiFileColumnDefinition();
				if (!new_col.default_expression) {
					// set default expression to null.
					new_col.default_expression = make_uniq<ConstantExpression>(Value(col_type));
				}
				new_col.identifier = col_info->id;
#ifdef ICEBERG_VANE_DISTRIBUTED
				if (iceberg_list.HasDistributedScanPlan()) {
					schema_idx = AddDistributedEqualityDeleteColumn(get, mfbd, new_col);
				} else {
#endif
					schema_idx = col_info->id;
					// modify the returned types of the get to add a column
					get.returned_types.push_back(col_type);

					// modify the multi file reader bind data to add the extra column
					mfbd.types.push_back(col_type);
					mfbd.names.push_back(col_info->name);
					mfbd.columns.push_back(new_col);
					// also push back the info to the reader_bind.schema
					mfbd.reader_bind.schema.push_back(new_col);
#ifdef ICEBERG_VANE_DISTRIBUTED
				}
#endif
			}
			idx_t local_idx = DConstants::INVALID_INDEX;
			const auto &col_ids = get.GetColumnIds();
			for (idx_t i = 0; i < col_ids.size(); i++) {
				if (!col_ids[i].IsVirtualColumn() && col_ids[i].GetPrimaryIndex() == schema_idx) {
					local_idx = i;
					break;
				}
			}
			if (local_idx == DConstants::INVALID_INDEX) {
				get.AddColumnId(schema_idx);
				local_idx = get.GetColumnIds().size() - 1;
			}
			auto bindings = get.GetColumnBindings();
			args.push_back(make_uniq<BoundColumnRefExpression>(col_type, bindings[local_idx]));
		}
		if (args.empty()) {
			continue;
		}

		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &fn_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "iceberg_verify_equality_deletes");
		FunctionBinder function_binder(context);
		vector<LogicalType> arg_types;
		for (auto &a : args) {
			arg_types.push_back(a->return_type);
		}
		auto fn = fn_entry.functions.GetFunctionByArguments(context, arg_types);
		auto bound_call = function_binder.BindScalarFunction(fn, std::move(args));

		auto filter = make_uniq<LogicalFilter>();
		filter->expressions.push_back(std::move(bound_call));
		filter->children.push_back(std::move(op->children[child_index]));
		op->children[child_index] = std::move(filter);
	}
}

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	GuaranteeEqualityDeleteColumnsOptimizer guarantee_equality_delete_columns_optimizer(input.context);
	if (plan->children.size() == 0) {
		return;
	}
	guarantee_equality_delete_columns_optimizer.VisitOperator(plan);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
