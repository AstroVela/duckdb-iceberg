
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "execution/operator/merge_into/iceberg_merge_insert.hpp"
#include "execution/operator/merge_into/iceberg_merge_update.hpp"
#include "execution/operator/merge_into/iceberg_merge_into.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "execution/operator/iceberg_update.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "duckdb/execution/operator/join/physical_asof_join.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_range_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "execution/operator/iceberg_distributed_write.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#endif

namespace duckdb {

void IcebergMergeInto::ProjectAndCastForCopy(ClientContext &context, DataChunk &input_chunk, PhysicalOperator &copy_op,
                                             ExpressionExecutor *expression_executor, DataChunk &projected_chunk,
                                             DataChunk &cast_chunk) {
	reference<DataChunk> chunk_ref = input_chunk;
	if (expression_executor) {
		projected_chunk.Reset();
		expression_executor->Execute(input_chunk, projected_chunk);
		chunk_ref = projected_chunk;
	}
	auto &copy_types = copy_op.Cast<PhysicalCopyToFile>().expected_types;
	for (idx_t i = 0; i < chunk_ref.get().ColumnCount(); i++) {
		if (chunk_ref.get().data[i].GetType() != copy_types[i]) {
			VectorOperations::Cast(context, chunk_ref.get().data[i], cast_chunk.data[i], chunk_ref.get().size());
		} else {
			cast_chunk.data[i].Reference(chunk_ref.get().data[i]);
		}
	}
	cast_chunk.SetCardinality(chunk_ref.get().size());
}

void IcebergMergeInto::FinalizeCopyToInsert(Pipeline &pipeline, Event &event, ClientContext &context,
                                            PhysicalOperator &copy_op, PhysicalOperator &insert_op,
                                            InterruptState &interrupt_state) {
	DataChunk chunk;
	chunk.Initialize(context, copy_op.types);

	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);

	auto copy_global = copy_op.GetGlobalSourceState(context);
	auto copy_local = copy_op.GetLocalSourceState(exec_context, *copy_global);
	OperatorSourceInput source_input {*copy_global, *copy_local, interrupt_state};

	auto insert_global = insert_op.GetGlobalSinkState(context);
	auto insert_local = insert_op.GetLocalSinkState(exec_context);
	OperatorSinkInput sink_input {*insert_global, *insert_local, interrupt_state};
	SourceResultType source_res = SourceResultType::HAVE_MORE_OUTPUT;
	while (source_res == SourceResultType::HAVE_MORE_OUTPUT) {
		chunk.Reset();
		source_res = copy_op.GetData(exec_context, chunk, source_input);
		if (chunk.size() == 0) {
			continue;
		}
		if (source_res == SourceResultType::BLOCKED) {
			throw InternalException("BLOCKED not supported in IcebergMerge");
		}

		auto sink_result = insert_op.Sink(exec_context, chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("BLOCKED not supported in IcebergMerge");
		}
	}
	OperatorSinkCombineInput combine_input {*insert_global, *insert_local, interrupt_state};
	auto combine_res = insert_op.Combine(exec_context, combine_input);
	if (combine_res == SinkCombineResultType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMerge");
	}
	OperatorSinkFinalizeInput finalize_input {*insert_global, interrupt_state};
	auto finalize_res = insert_op.Finalize(pipeline, event, context, finalize_input);
	if (finalize_res == SinkFinalizeType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMerge");
	}
}

#ifdef ICEBERG_VANE_DISTRIBUTED
static vector<unique_ptr<Expression>>
CopyDistributedMergeExpressions(const vector<unique_ptr<Expression>> &expressions) {
	vector<unique_ptr<Expression>> result;
	result.reserve(expressions.size());
	for (const auto &expression : expressions) {
		result.push_back(expression->Copy());
	}
	return result;
}

static void CollectDistributedMergeInputReferences(const unique_ptr<Expression> &expression,
                                                   const vector<LogicalType> &input_types,
                                                   unordered_set<idx_t> *referenced_input_indexes) {
	if (!expression) {
		return;
	}
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(
	    *expression, [&](const BoundReferenceExpression &reference) {
		    if (reference.index >= input_types.size() || reference.return_type != input_types[reference.index]) {
			    throw InternalException("Iceberg distributed MERGE action reference is invalid");
		    }
		    if (referenced_input_indexes) {
			    referenced_input_indexes->insert(reference.index);
		    }
	    });
}

static bool CollectDistributedMergeInsertPartitionIndexes(const PhysicalOperator &input,
                                                          const vector<IcebergDistributedMergePlanAction> &actions,
                                                          vector<idx_t> &null_target_partition_indexes) {
	if (input.type != PhysicalOperatorType::PROJECTION) {
		return false;
	}
	unordered_set<idx_t> insert_input_indexes;
	for (const auto &action : actions) {
		auto referenced_input_indexes =
		    action.action_type == MergeActionType::MERGE_INSERT ? &insert_input_indexes : nullptr;
		CollectDistributedMergeInputReferences(action.condition, input.types, referenced_input_indexes);
		for (const auto &expression : action.expressions) {
			CollectDistributedMergeInputReferences(expression, input.types, referenced_input_indexes);
		}
	}
	for (idx_t input_index = 0; input_index < input.types.size(); input_index++) {
		if (insert_input_indexes.find(input_index) != insert_input_indexes.end()) {
			null_target_partition_indexes.push_back(input_index);
		}
	}
	return true;
}

struct DistributedMergeTargetScanColumn {
	optional_ptr<PhysicalTableScan> scan;
	idx_t output_index = 0;
};

static DistributedMergeTargetScanColumn TraceDistributedMergeTargetRowPosition(PhysicalOperator &plan,
                                                                               idx_t output_index) {
	if (output_index >= plan.types.size()) {
		throw InternalException("Iceberg distributed MERGE target row-position reference is out of range");
	}
	switch (plan.type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return {&plan.Cast<PhysicalTableScan>(), output_index};
	case PhysicalOperatorType::EMPTY_RESULT:
		return {};
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = plan.Cast<PhysicalProjection>();
		if (projection.children.size() != 1 || output_index >= projection.select_list.size()) {
			throw InvalidInputException(
			    "Distributed Iceberg MERGE could not trace the target row position through its projection");
		}
		auto &expression = *projection.select_list[output_index];
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    expression.Cast<BoundConstantExpression>().value.IsNull()) {
			return {};
		}
		if (expression.GetExpressionType() != ExpressionType::BOUND_REF) {
			throw InvalidInputException(
			    "Distributed Iceberg MERGE could not trace the target row position through its projection");
		}
		auto &reference = expression.Cast<BoundReferenceExpression>();
		return TraceDistributedMergeTargetRowPosition(projection.children[0].get(), reference.index);
	}
	case PhysicalOperatorType::ORDER_BY: {
		auto &order = plan.Cast<PhysicalOrder>();
		if (order.children.size() != 1 || output_index >= order.projections.size()) {
			throw InvalidInputException(
			    "Distributed Iceberg MERGE could not trace the target row position through its ordering");
		}
		return TraceDistributedMergeTargetRowPosition(order.children[0].get(), order.projections[output_index]);
	}
	case PhysicalOperatorType::HASH_JOIN: {
		auto &join = plan.Cast<PhysicalHashJoin>();
		if (join.children.size() != 2) {
			throw InternalException("Iceberg distributed MERGE hash join has an invalid child count");
		}
		if (output_index < join.lhs_output_columns.col_idxs.size()) {
			return TraceDistributedMergeTargetRowPosition(join.children[0].get(),
			                                              join.lhs_output_columns.col_idxs[output_index]);
		}
		auto right_output_index = output_index - join.lhs_output_columns.col_idxs.size();
		if (right_output_index >= join.rhs_output_columns.col_idxs.size()) {
			throw InternalException("Iceberg distributed MERGE hash-join output mapping is invalid");
		}
		auto hash_table_index = join.rhs_output_columns.col_idxs[right_output_index];
		idx_t child_index;
		if (hash_table_index < join.conditions.size()) {
			auto &condition = join.conditions[hash_table_index];
			if (condition.right->GetExpressionType() != ExpressionType::BOUND_REF) {
				throw InternalException("Iceberg distributed MERGE hash-join key mapping is invalid");
			}
			child_index = condition.right->Cast<BoundReferenceExpression>().index;
		} else {
			auto payload_index = hash_table_index - join.conditions.size();
			if (payload_index >= join.payload_columns.col_idxs.size()) {
				throw InternalException("Iceberg distributed MERGE hash-join payload mapping is invalid");
			}
			child_index = join.payload_columns.col_idxs[payload_index];
		}
		return TraceDistributedMergeTargetRowPosition(join.children[1].get(), child_index);
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN: {
		auto &join = static_cast<PhysicalRangeJoin &>(plan);
		if (join.children.size() != 2) {
			throw InternalException("Iceberg distributed MERGE range join has an invalid child count");
		}
		if (output_index < join.left_projection_map.size()) {
			return TraceDistributedMergeTargetRowPosition(join.children[0].get(),
			                                              join.left_projection_map[output_index]);
		}
		auto right_output_index = output_index - join.left_projection_map.size();
		if (right_output_index >= join.right_projection_map.size()) {
			throw InternalException("Iceberg distributed MERGE range-join output mapping is invalid");
		}
		return TraceDistributedMergeTargetRowPosition(join.children[1].get(),
		                                              join.right_projection_map[right_output_index]);
	}
	case PhysicalOperatorType::ASOF_JOIN: {
		auto &join = plan.Cast<PhysicalAsOfJoin>();
		if (join.children.size() != 2) {
			throw InternalException("Iceberg distributed MERGE ASOF join has an invalid child count");
		}
		auto left_count = join.children[0].get().types.size();
		if (output_index < left_count) {
			return TraceDistributedMergeTargetRowPosition(join.children[0].get(), output_index);
		}
		auto right_output_index = output_index - left_count;
		if (right_output_index >= join.right_projection_map.size()) {
			throw InternalException("Iceberg distributed MERGE ASOF-join output mapping is invalid");
		}
		return TraceDistributedMergeTargetRowPosition(join.children[1].get(),
		                                              join.right_projection_map[right_output_index]);
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::POSITIONAL_JOIN: {
		if (plan.children.size() != 2) {
			throw InternalException("Iceberg distributed MERGE join has an invalid child count");
		}
		auto left_count = plan.children[0].get().types.size();
		if (output_index < left_count) {
			return TraceDistributedMergeTargetRowPosition(plan.children[0].get(), output_index);
		}
		return TraceDistributedMergeTargetRowPosition(plan.children[1].get(), output_index - left_count);
	}
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN: {
		auto &join = static_cast<PhysicalDelimJoin &>(plan);
		return TraceDistributedMergeTargetRowPosition(join.join, output_index);
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LOCAL_EXCHANGE:
	case PhysicalOperatorType::VERIFY_VECTOR:
		if (plan.children.size() != 1 || output_index >= plan.children[0].get().types.size() ||
		    plan.types[output_index] != plan.children[0].get().types[output_index]) {
			throw InvalidInputException(
			    "Distributed Iceberg MERGE could not trace the target row position through operator %s",
			    PhysicalOperatorToString(plan.type));
		}
		return TraceDistributedMergeTargetRowPosition(plan.children[0].get(), output_index);
	default:
		throw NotImplementedException(
		    "Distributed Iceberg MERGE cannot trace its target row position through physical operator %s",
		    PhysicalOperatorToString(plan.type));
	}
}

static optional_ptr<IcebergMultiFileList> ResolveDistributedMergeTargetFileList(PhysicalOperator &plan,
                                                                                idx_t row_position_index,
                                                                                const IcebergTableEntry &target) {
	auto scan = TraceDistributedMergeTargetRowPosition(plan, row_position_index);
	if (!scan.scan) {
		return nullptr;
	}
	auto scan_column_index = scan.output_index;
	if (!scan.scan->projection_ids.empty()) {
		if (scan.output_index >= scan.scan->projection_ids.size()) {
			throw InternalException("Iceberg distributed MERGE target scan projection is invalid");
		}
		scan_column_index = scan.scan->projection_ids[scan.output_index];
	}
	if (scan_column_index >= scan.scan->column_ids.size() ||
	    scan.scan->column_ids[scan_column_index].GetPrimaryIndex() !=
	        MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		throw InvalidInputException("Distributed Iceberg MERGE target row position did not resolve to file_row_number");
	}
	if (scan.scan->function.name != "iceberg_scan" ||
	    scan.scan->function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance || !scan.scan->bind_data) {
		throw InvalidInputException("Distributed Iceberg MERGE target row position did not resolve to an Iceberg scan");
	}
	auto &bind = scan.scan->bind_data->Cast<MultiFileBindData>();
	if (!bind.file_list) {
		throw InvalidInputException("Distributed Iceberg MERGE target scan has no file list");
	}
	auto &file_list = bind.file_list->Cast<IcebergMultiFileList>();
	if (!file_list.HasDistributedScanPlan() ||
	    file_list.GetDistributedScanTableUUID() != target.GetLogicalWriteTargetIdentity()) {
		throw InvalidInputException("Distributed Iceberg MERGE target scan identity does not match its write target");
	}
	return &file_list;
}

static void AddDistributedMergeCommitRequirements(IcebergTransactionData &transaction_data) {
	transaction_data.requirements.push_back(make_uniq<AssertCurrentSchemaIdRequirement>(transaction_data.table_info));
	transaction_data.TableAddAssertDefaultSpecId();
}

class PhysicalIcebergDistributedMergeInto final : public PhysicalMergeInto,
                                                  public distributed::ExtensionWriteTaskProvider {
public:
	PhysicalIcebergDistributedMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                                    map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions,
	                                    idx_t row_id_index, optional_idx source_marker, bool return_chunk,
	                                    ClientContext &context, IcebergTableEntry &table,
	                                    PhysicalOperator &distributed_worker_child_p,
	                                    vector<IcebergDistributedMergePlanAction> distributed_actions_p,
	                                    idx_t native_update_delete_count_p, bool has_update_p, bool has_delete_p,
	                                    bool worker_plan_is_statically_empty_p)
	    : PhysicalMergeInto(physical_plan, std::move(types), std::move(actions), row_id_index, source_marker, true,
	                        return_chunk),
	      planning_context(context), planned_table(table), distributed_worker_child(distributed_worker_child_p),
	      distributed_actions(std::move(distributed_actions_p)),
	      worker_plan_is_statically_empty(worker_plan_is_statically_empty_p),
	      native_update_delete_count(native_update_delete_count_p), has_update(has_update_p), has_delete(has_delete_p) {
		distributed_write_plan.extension_name = "iceberg";
		distributed_write_plan.operator_name = "merge";
		distributed_artifact_namespace = CreateIcebergDistributedArtifactNamespace();
		auto &metadata = table.table_info.table_metadata;
		distributed_catalog_name = table.catalog.GetName();
		distributed_schema_name = table.schema.name;
		distributed_table_name = table.name;
		distributed_table_uuid = metadata.table_uuid;
		distributed_data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
		distributed_schema_id = metadata.GetCurrentSchemaId();
		distributed_partition_spec_id = metadata.default_spec_id;
		distributed_iceberg_version = metadata.iceberg_version;
		distributed_sort_order_id = metadata.default_sort_order_id;
		auto snapshot = metadata.GetLatestSnapshot();
		distributed_has_snapshot = snapshot != nullptr;
		distributed_snapshot_id = snapshot ? snapshot->snapshot_id : 0;
	}

	optional_ptr<distributed::ExtensionWriteTaskProvider> GetExtensionWriteTaskProvider() override {
		SelectDistributedWorkerPlan();
		return this;
	}

	const distributed::DistributedExtensionWritePlan &WritePlan() const override {
		ValidateDistributedWriteShape();
		return distributed_write_plan;
	}

	void ValidateDistributedWrite(ClientContext &context) const override {
		auto &table = ResolveDistributedWriteTable(context);
		ValidateIcebergDistributedTargetPartitionSpec(table.table_info.table_metadata, "MERGE");
		if ((has_update || has_delete) && !target_is_statically_empty) {
			ValidateIcebergDistributedRowDeltaSourceBaseline(*target_file_list, table.table_info.table_metadata,
			                                                 "MERGE");
			ValidateIcebergDistributedRowDeltaSourceSpecs(*target_file_list,
			                                              table.table_info.table_metadata.default_spec_id, "MERGE");
		}
	}

	idx_t FinalizeDistributedWrite(ClientContext &context,
	                               const vector<DistributedWriteTaskResult> &results) const override {
		bool ownership_transferred = false;
		try {
			auto write_info = distributed::ResolveDistributedExtensionWriteInfo(context, WritePlan());
			auto decoded = DecodeIcebergDistributedMergeResults(
			    context, distributed_data_path, distributed_artifact_namespace, write_info, results,
			    distributed_iceberg_version, worker_plan_is_statically_empty);
			if (target_is_statically_empty &&
			    (decoded.updated_rows != 0 || decoded.deleted_rows != 0 || !decoded.delete_files.empty())) {
				throw InvalidInputException("Statically empty distributed Iceberg MERGE returned target row changes");
			}
			ValidateIcebergDistributedDataFileArtifacts(context, distributed_data_path, decoded.data_files);
			CleanupIcebergDistributedMerge(context, distributed_data_path, distributed_artifact_namespace,
			                               &decoded.selected_artifact_paths);
			auto &table = ResolveDistributedWriteTable(context);
			if ((has_update || has_delete) && !target_is_statically_empty) {
				ValidateIcebergDistributedRowDeltaSourceBaseline(*target_file_list, table.table_info.table_metadata,
				                                                 "MERGE");
				ValidateIcebergDistributedRowDeltaSourceSpecs(*target_file_list,
				                                              table.table_info.table_metadata.default_spec_id, "MERGE");
			}

			IcebergInsertGlobalState insert_state(context);
			IcebergInsert::AddDistributedDataFiles(context, insert_state, table, decoded.data_files);
			auto data_files = IcebergInsert::GetInsertManifestEntries(insert_state);
			IcebergDeleteGlobalState delete_state;
			if (!decoded.delete_files.empty()) {
				if (!target_file_list) {
					throw InvalidInputException("Distributed Iceberg MERGE returned deletes without a target scan");
				}
				IcebergDelete::AddDistributedDeleteArtifacts(context, *target_file_list, decoded.delete_files,
				                                             delete_state, "MERGE");
			}
			if (decoded.updated_rows > NumericLimits<idx_t>::Maximum() - decoded.deleted_rows ||
			    decoded.inserted_rows > NumericLimits<idx_t>::Maximum() - decoded.updated_rows) {
				throw InvalidInputException("Distributed Iceberg MERGE action count overflow");
			}
			auto expected_delete_rows = decoded.updated_rows + decoded.deleted_rows;
			auto expected_data_rows = decoded.inserted_rows + decoded.updated_rows;
			delete_state.total_deleted_count = expected_delete_rows;
			auto delete_files = IcebergDelete::GenerateDeleteManifestEntries(delete_state);
			if (insert_state.insert_count.load() != expected_data_rows ||
			    delete_state.total_deleted_count.load() != expected_delete_rows ||
			    (expected_data_rows != 0 && data_files.empty()) ||
			    (expected_delete_rows != 0 && delete_files.empty())) {
				throw InternalException("Distributed Iceberg MERGE coordinator produced inconsistent action artifacts");
			}
			if (decoded.affected_rows == 0) {
				return 0;
			}

			auto &transaction = IcebergTransaction::Get(context, table.catalog);
			ApplyTableUpdate(table.table_info, transaction, [&](IcebergTableInformation &table_info) {
				auto &transaction_data = table_info.GetOrCreateTransactionData(transaction);
				AddDistributedMergeCommitRequirements(transaction_data);
				transaction_data.RetainAddedSnapshotFilesOnRollback();
				if (!data_files.empty() && !delete_files.empty()) {
					transaction_data.AddUpdateSnapshot(std::move(delete_files), std::move(data_files),
					                                   std::move(delete_state.altered_manifests));
				} else if (!data_files.empty()) {
					IcebergManifestDeletes empty_deletes;
					transaction_data.AddSnapshot(IcebergSnapshotOperationType::APPEND, std::move(data_files),
					                             std::move(empty_deletes));
				} else {
					transaction_data.AddSnapshot(IcebergSnapshotOperationType::DELETE, std::move(delete_files),
					                             std::move(delete_state.altered_manifests));
				}
				if (distributed_iceberg_version >= 3) {
					for (const auto &entry : delete_state.written_files) {
						transaction_data.transactional_delete_files[entry.second.data_file_path] =
						    entry.second.file_name;
					}
				}
			});
			ownership_transferred = true;
			return decoded.affected_rows;
		} catch (...) {
			if (!ownership_transferred) {
				try {
					CleanupIcebergDistributedMerge(context, distributed_data_path, distributed_artifact_namespace);
				} catch (...) {
				}
			}
			throw;
		}
	}

	void AbortDistributedWrite(ClientContext &context, const vector<DistributedWriteTaskResult> &) const override {
		CleanupIcebergDistributedMerge(context, distributed_data_path, distributed_artifact_namespace);
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		if (distributed_worker_plan_selected) {
			throw InvalidInputException(
			    "A distributed Iceberg MERGE worker plan cannot be executed as a native coordinator operator");
		}
		if (native_update_delete_count > 1) {
			throw NotImplementedException(
			    "MERGE INTO with Iceberg only supports a single UPDATE/DELETE action currently");
		}
		PhysicalMergeInto::BuildPipelines(current, meta_pipeline);
	}

	string GetName() const override {
		return distributed_worker_plan_selected ? "ICEBERG_DISTRIBUTED_MERGE" : PhysicalMergeInto::GetName();
	}

private:
	void SelectDistributedWorkerPlan() {
		if (distributed_worker_plan_selected) {
			return;
		}
		if (children.size() != 1) {
			throw InvalidInputException("Distributed Iceberg MERGE requires exactly one native input");
		}
		if (has_update || has_delete) {
			auto row_position_index = row_id_index + (distributed_iceberg_version >= 3 ? 2 : 1);
			target_file_list =
			    ResolveDistributedMergeTargetFileList(children[0].get(), row_position_index, planned_table);
			target_is_statically_empty = !target_file_list || target_file_list->GetTotalFileCount() == 0;
		}
		distributed_write_plan.worker_bind_data = BuildIcebergDistributedMergeBind(
		    planning_context, planned_table, target_file_list.get(), distributed_actions, distributed_worker_child,
		    row_id_index, source_marker, target_is_statically_empty, worker_plan_is_statically_empty,
		    distributed_artifact_namespace);
		children[0] = distributed_worker_child;
		type = PhysicalOperatorType::EXTENSION;
		distributed_worker_plan_selected = true;
	}

	void ValidateDistributedWriteShape() const {
		if (distributed_write_plan.extension_name != "iceberg" || distributed_write_plan.operator_name != "merge" ||
		    !distributed_worker_plan_selected || type != PhysicalOperatorType::EXTENSION || children.size() != 1 ||
		    distributed_write_plan.worker_bind_data.empty() || distributed_actions.empty() ||
		    distributed_data_path.empty() || distributed_artifact_namespace.empty()) {
			throw InvalidInputException("Distributed Iceberg MERGE worker plan was not initialized");
		}
		if (distributed_iceberg_version != 2 && distributed_iceberg_version != 3) {
			throw NotImplementedException("Distributed Iceberg MERGE supports format-version 2 and 3 tables only");
		}
		if ((has_update || has_delete) && !target_is_statically_empty && !target_file_list) {
			throw InvalidInputException("Distributed Iceberg MERGE target scan was not initialized");
		}
	}

	IcebergTableEntry &ResolveDistributedWriteTable(ClientContext &context) const {
		ValidateDistributedWriteShape();
		auto &table = Catalog::GetEntry<TableCatalogEntry>(context, distributed_catalog_name, distributed_schema_name,
		                                                   distributed_table_name)
		                  .Cast<IcebergTableEntry>();
		table.PrepareIcebergScanFromEntry(context);
		auto &metadata = table.table_info.table_metadata;
		if (metadata.table_uuid != distributed_table_uuid) {
			throw TransactionException("Iceberg table %s.%s.%s was replaced after the distributed MERGE was planned",
			                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
		}
		bool sort_order_changed = metadata.default_sort_order_id.IsValid() != distributed_sort_order_id.IsValid();
		if (!sort_order_changed && metadata.default_sort_order_id.IsValid()) {
			sort_order_changed = metadata.default_sort_order_id.GetIndex() != distributed_sort_order_id.GetIndex();
		}
		if (metadata.GetCurrentSchemaId() != distributed_schema_id ||
		    metadata.default_spec_id != distributed_partition_spec_id ||
		    metadata.iceberg_version != distributed_iceberg_version || sort_order_changed) {
			throw TransactionException("Iceberg table %s.%s.%s layout changed after the distributed MERGE was planned",
			                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
		}
		auto snapshot = metadata.GetLatestSnapshot();
		if ((snapshot != nullptr) != distributed_has_snapshot ||
		    (snapshot && snapshot->snapshot_id != distributed_snapshot_id)) {
			throw TransactionException(
			    "Iceberg table %s.%s.%s snapshot changed after the distributed MERGE was planned",
			    distributed_catalog_name, distributed_schema_name, distributed_table_name);
		}
		auto current_data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
		if (current_data_path != distributed_data_path) {
			throw TransactionException(
			    "Iceberg table %s.%s.%s data path changed after the distributed MERGE was planned",
			    distributed_catalog_name, distributed_schema_name, distributed_table_name);
		}
		if (has_update && !metadata.PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType::OVERWRITE)) {
			throw NotImplementedException(IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(
			    distributed_table_name, WRITE_UPDATE_MODE, metadata.GetTableProperty(WRITE_UPDATE_MODE)));
		}
		if (has_delete && !metadata.PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType::DELETE)) {
			throw NotImplementedException(IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(
			    distributed_table_name, WRITE_DELETE_MODE, metadata.GetTableProperty(WRITE_DELETE_MODE)));
		}
		return table;
	}

private:
	ClientContext &planning_context;
	IcebergTableEntry &planned_table;
	optional_ptr<IcebergMultiFileList> target_file_list;
	PhysicalOperator &distributed_worker_child;
	vector<IcebergDistributedMergePlanAction> distributed_actions;
	distributed::DistributedExtensionWritePlan distributed_write_plan;
	string distributed_catalog_name;
	string distributed_schema_name;
	string distributed_table_name;
	string distributed_table_uuid;
	string distributed_data_path;
	string distributed_artifact_namespace;
	int32_t distributed_schema_id = -1;
	int32_t distributed_partition_spec_id = -1;
	int32_t distributed_iceberg_version = 0;
	optional_idx distributed_sort_order_id;
	bool distributed_has_snapshot = false;
	int64_t distributed_snapshot_id = 0;
	bool target_is_statically_empty = false;
	bool distributed_worker_plan_selected = false;
	bool worker_plan_is_statically_empty = false;
	idx_t native_update_delete_count;
	bool has_update;
	bool has_delete;
};
#endif

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
static unique_ptr<MergeIntoOperator> IcebergPlanMergeIntoAction(IcebergCatalog &catalog, ClientContext &context,
                                                                LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                                BoundMergeIntoAction &action,
                                                                PhysicalOperator &child_plan
#ifdef ICEBERG_VANE_DISTRIBUTED
                                                                ,
                                                                IcebergDistributedMergePlanAction &distributed_action
#endif
) {
	auto result = make_uniq<MergeIntoOperator>();

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}
	auto return_types = op.types;

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, catalog);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	auto iceberg_version = table_metadata.iceberg_version;

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
#ifdef ICEBERG_VANE_DISTRIBUTED
		LogicalUpdate update(context, op.table);
#else
		LogicalUpdate update(op.table);
#endif
		for (auto &def : op.bound_defaults) {
			update.bound_defaults.push_back(def->Copy());
		}
		update.bound_constraints = std::move(bound_constraints);
		update.expressions = std::move(action.expressions);
		update.columns = std::move(action.columns);
		update.update_is_del_and_insert = action.update_is_del_and_insert;

		IcebergCopyInput copy_input(context, table_metadata, schema);
		if (iceberg_version >= 3) {
			copy_input.virtual_columns = IcebergInsertVirtualColumns::WRITE_ROW_ID;
		}

		auto &update_op =
		    IcebergUpdate::PlanUpdateOperator(context, planner, updated_table_entry, update, child_plan, copy_input);

		// The row_id comes before the deletion information, that is always the 3 last column of the chunk.
		if (table_metadata.iceberg_version >= 3) {
			update_op.row_id_index = child_plan.types.size() - 4;
		}

		// plan copy and insert
		auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input);
		auto &copy_op = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert_op = IcebergInsert::PlanInsert(context, planner, updated_table_entry).Cast<IcebergInsert>();
		insert_op.children.push_back(copy_op);
		insert_op.update_delete_op = update_op.delete_op;

		// wrap in IcebergMergeUpdate
		auto &merge_update =
		    planner.Make<IcebergMergeUpdate>(return_types, update_op, copy_op, insert_op).Cast<IcebergMergeUpdate>();
		merge_update.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_update;
#ifdef ICEBERG_VANE_DISTRIBUTED
		distributed_action.expressions = CopyDistributedMergeExpressions(update_op.expressions);
		distributed_action.projections = CopyDistributedMergeExpressions(merge_update.extra_projections);
		distributed_action.copy = &copy_op.Cast<PhysicalCopyToFile>();
#endif
		break;
	}
	case MergeActionType::MERGE_DELETE: {
#ifdef ICEBERG_VANE_DISTRIBUTED
		LogicalDelete delete_op(context, op.table, 0);
#else
		LogicalDelete delete_op(op.table, 0);
#endif

		// we only push 2 columns for positional deletes
		idx_t column_offset = 0;
		if (iceberg_version >= 3) {
			delete_op.expressions.push_back(nullptr);
			//! The row ids of the table contain the _row_id column, which we're not interested in
			column_offset = 1;
		}
		vector<LogicalType> row_id_types {LogicalType::VARCHAR, LogicalType::BIGINT};
		for (idx_t i = 0; i < 2; i++) {
			auto ref = make_uniq<BoundReferenceExpression>(row_id_types[i], op.row_id_start + i + column_offset);
			delete_op.expressions.push_back(std::move(ref));
		}
		delete_op.bound_constraints = std::move(bound_constraints);
		result->op = catalog.PlanDelete(context, planner, delete_op, child_plan);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
#ifdef ICEBERG_VANE_DISTRIBUTED
		LogicalInsert insert_op(context, op.table, 0);
#else
		LogicalInsert insert_op(op.table, 0);
#endif
		insert_op.bound_constraints = std::move(bound_constraints);
		for (auto &def : op.bound_defaults) {
			insert_op.bound_defaults.push_back(def->Copy());
		}
		// transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);

		IcebergCopyInput copy_input(context, table_metadata, schema);
		auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input);
		auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert = IcebergInsert::PlanInsert(context, planner, table_entry);
		insert.children.push_back(physical_copy);

		auto &merge_insert =
		    planner.Make<IcebergMergeInsert>(insert.types, insert, physical_copy).Cast<IcebergMergeInsert>();
		merge_insert.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_insert;
#ifdef ICEBERG_VANE_DISTRIBUTED
		distributed_action.expressions = CopyDistributedMergeExpressions(result->expressions);
		distributed_action.projections = CopyDistributedMergeExpressions(merge_insert.extra_projections);
		distributed_action.copy = &physical_copy.Cast<PhysicalCopyToFile>();
#endif
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
#ifdef ICEBERG_VANE_DISTRIBUTED
		distributed_action.expressions = CopyDistributedMergeExpressions(result->expressions);
#endif
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
#ifdef ICEBERG_VANE_DISTRIBUTED
	distributed_action.condition = result->condition ? result->condition->Copy() : nullptr;
#endif
	return result;
}

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not implemented for Iceberg yet");
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	// plan the merge into clauses
	idx_t update_delete_count = 0;
#ifdef ICEBERG_VANE_DISTRIBUTED
	bool has_update = false;
	bool has_delete = false;
	vector<IcebergDistributedMergePlanAction> distributed_actions;
#endif
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
#ifndef ICEBERG_VANE_DISTRIBUTED
				if (update_delete_count > 1) {
					throw NotImplementedException(
					    "MERGE INTO with Iceberg only supports a single UPDATE/DELETE action currently");
				}
#endif
#ifdef ICEBERG_VANE_DISTRIBUTED
				has_update = has_update || action->action_type == MergeActionType::MERGE_UPDATE;
				has_delete = has_delete || action->action_type == MergeActionType::MERGE_DELETE;
#endif
			}
#ifdef ICEBERG_VANE_DISTRIBUTED
			IcebergDistributedMergePlanAction distributed_action;
			distributed_action.match_condition = entry.first;
			distributed_action.action_type = action->action_type;
			planned_actions.push_back(
			    IcebergPlanMergeIntoAction(*this, context, op, planner, *action, plan, distributed_action));
			distributed_actions.push_back(std::move(distributed_action));
#else
			planned_actions.push_back(IcebergPlanMergeIntoAction(*this, context, op, planner, *action, plan));
#endif
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

#ifdef ICEBERG_VANE_DISTRIBUTED
	auto worker_plan_is_statically_empty = plan.type == PhysicalOperatorType::EMPTY_RESULT;
	vector<idx_t> null_target_partition_indexes;
	if (!CollectDistributedMergeInsertPartitionIndexes(plan, distributed_actions, null_target_partition_indexes)) {
		for (idx_t index = 0; index < op.row_id_start; index++) {
			if (!op.source_marker.IsValid() || index != op.source_marker.GetIndex()) {
				null_target_partition_indexes.push_back(index);
			}
		}
	}
	auto iceberg_version = table_entry.table_info.table_metadata.iceberg_version;
	auto file_path_index = op.row_id_start + (iceberg_version >= 3 ? 1 : 0);
	auto &distributed_worker_child =
	    PlanIcebergDistributedMergeRepartition(planner, plan, file_path_index, null_target_partition_indexes);
	auto &result = planner.Make<PhysicalIcebergDistributedMergeInto>(
	    op.types, std::move(actions), op.row_id_start, op.source_marker, op.return_chunk, context, table_entry,
	    distributed_worker_child, std::move(distributed_actions), update_delete_count, has_update, has_delete,
	    worker_plan_is_statically_empty);
#else
	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               true, op.return_chunk);
#endif
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
