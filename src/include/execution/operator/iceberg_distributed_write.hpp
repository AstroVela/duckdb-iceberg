//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_distributed_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/distributed/copy_to_file.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/function/distributed_write.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
class IcebergMultiFileList;
struct IcebergTableMetadata;
class IcebergTableEntry;
class PhysicalCopyToFile;
class PhysicalOperator;
class PhysicalPlanGenerator;

enum class IcebergDistributedRowDeltaKind : uint8_t { DELETE = 0, UPDATE = 1 };

struct IcebergDistributedDeleteFileResult {
	string data_file_path;
	string delete_file_path;
	bool is_deletion_vector = false;
	idx_t new_delete_count = 0;
	idx_t delete_count = 0;
	idx_t file_size_bytes = 0;
	idx_t footer_size_bytes = 0;
	idx_t content_offset = 0;
	idx_t content_size_in_bytes = 0;
	idx_t pos_min_value = 0;
	idx_t pos_max_value = 0;
};

struct IcebergDistributedRowDeltaResult {
	vector<distributed::DistributedCopyFileInfo> data_files;
	vector<IcebergDistributedDeleteFileResult> delete_files;
	unordered_set<string> selected_artifact_paths;
	idx_t affected_rows = 0;
};

struct IcebergDistributedMergeResult {
	vector<distributed::DistributedCopyFileInfo> data_files;
	vector<IcebergDistributedDeleteFileResult> delete_files;
	unordered_set<string> selected_artifact_paths;
	idx_t inserted_rows = 0;
	idx_t updated_rows = 0;
	idx_t deleted_rows = 0;
	idx_t affected_rows = 0;
};

struct IcebergDistributedMergePlanAction {
	MergeActionCondition match_condition = MergeActionCondition::WHEN_MATCHED;
	MergeActionType action_type = MergeActionType::MERGE_DO_NOTHING;
	unique_ptr<Expression> condition;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> projections;
	optional_ptr<PhysicalCopyToFile> copy;
};

string CreateIcebergDistributedArtifactNamespace();
PhysicalOperator &PlanIcebergDistributedRowDeltaRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                            idx_t file_path_index);
PhysicalOperator &PlanIcebergDistributedMergeRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                         idx_t file_path_index,
                                                         const vector<idx_t> &null_target_partition_indexes);
string BuildIcebergDistributedDeleteBind(ClientContext &context, const IcebergTableEntry &table,
                                         const IcebergMultiFileList &file_list, const vector<idx_t> &row_id_indexes,
                                         const string &artifact_namespace);
string BuildIcebergDistributedUpdateBind(ClientContext &context, const IcebergTableEntry &table,
                                         const IcebergMultiFileList &file_list, const PhysicalCopyToFile &copy,
                                         idx_t copy_column_count, idx_t file_path_index, idx_t row_position_index,
                                         const string &artifact_namespace);
string BuildIcebergDistributedEmptyUpdateBind(ClientContext &context, const IcebergTableEntry &table,
                                              const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                              idx_t file_path_index, idx_t row_position_index,
                                              const string &artifact_namespace);
string BuildIcebergDistributedMergeBind(ClientContext &context, const IcebergTableEntry &table,
                                        optional_ptr<const IcebergMultiFileList> target_file_list,
                                        const vector<IcebergDistributedMergePlanAction> &actions,
                                        const PhysicalOperator &worker_plan, idx_t row_id_start,
                                        optional_idx source_marker, bool target_is_statically_empty,
                                        bool worker_plan_is_statically_empty, const string &artifact_namespace);

DistributedExtensionWriteCallbacks IcebergDistributedRowDeltaCallbacks();

IcebergDistributedRowDeltaResult
DecodeIcebergDistributedRowDeltaResults(ClientContext &context, const string &data_path,
                                        const string &artifact_namespace, const DistributedExtensionWriteInfo &info,
                                        const vector<DistributedWriteTaskResult> &results,
                                        IcebergDistributedRowDeltaKind expected_kind, int32_t expected_iceberg_version);
IcebergDistributedMergeResult DecodeIcebergDistributedMergeResults(ClientContext &context, const string &data_path,
                                                                   const string &artifact_namespace,
                                                                   const DistributedExtensionWriteInfo &info,
                                                                   const vector<DistributedWriteTaskResult> &results,
                                                                   int32_t expected_iceberg_version,
                                                                   bool worker_plan_is_statically_empty);
void ValidateIcebergDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                 const vector<distributed::DistributedCopyFileInfo> &files);
void ValidateIcebergDistributedTargetPartitionSpec(const IcebergTableMetadata &metadata, const string &operation_name);
void ValidateIcebergDistributedRowDeltaSourceSpecs(const IcebergMultiFileList &file_list,
                                                   int32_t expected_partition_spec_id, const string &operation_name);
void ValidateIcebergDistributedRowDeltaSourceBaseline(const IcebergMultiFileList &file_list,
                                                      const IcebergTableMetadata &target_metadata,
                                                      const string &operation_name);

void CleanupIcebergDistributedRowDelta(ClientContext &context, const string &data_path,
                                       const string &artifact_namespace,
                                       const unordered_set<string> *paths_to_keep = nullptr);
void CleanupIcebergDistributedMerge(ClientContext &context, const string &data_path, const string &artifact_namespace,
                                    const unordered_set<string> *paths_to_keep = nullptr);

void RegisterIcebergDistributedWrites(ExtensionLoader &loader);

} // namespace duckdb
