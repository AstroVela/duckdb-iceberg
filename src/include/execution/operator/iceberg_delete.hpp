//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#endif

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

class PhysicalTableScan;
struct IcebergMultiFileList;
#ifdef ICEBERG_VANE_DISTRIBUTED
class IcebergInsert;
class IcebergUpdate;
struct IcebergDistributedDeleteFileResult;
#endif

struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

//! A single `column = constant` predicate extracted from a DELETE's WHERE clause, used to
//! materialize an Iceberg equality-delete file.
struct IcebergEqualityDeletePredicate {
	//! The Iceberg field-id of the column
	int32_t field_id;
	//! The column name (used both for the parquet column name and its field-id metadata)
	string column_name;
	//! The column type
	LogicalType type;
	//! The constant value to delete (cast to `type`)
	Value value;
};

class IcebergDeleteLocalState : public LocalSinkState {
public:
	string current_file_name;
	vector<idx_t> file_row_numbers;
};

struct IcebergDeleteFileInfo {
	string data_file_path;
	string file_name;
	string file_format;
	optional_idx footer_size;
	idx_t delete_count;
	idx_t file_size_bytes = 0;
	idx_t pos_max_value;
	idx_t pos_min_value;
	optional_idx content_size_in_bytes;
	optional_idx content_offset;
	vector<IcebergPartitionInfo> partition_info;
	//! When non-empty, this is an equality-delete file; holds the field-ids it applies to
	vector<int32_t> equality_ids;
};

class IcebergDeleteGlobalState : public GlobalSinkState {
public:
	explicit IcebergDeleteGlobalState() {
		written_columns["file_path"] = WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::FILENAME_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::ORDINAL_FIELD_ID);
		total_deleted_count = 0;
	}

	mutex lock;
	unordered_map<string, IcebergDeleteFileInfo> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	atomic<idx_t> total_deleted_count;
	// data file name -> newly deleted rows.
	unordered_map<string, vector<idx_t>> deleted_rows;
	IcebergManifestDeletes altered_manifests;
	//! Guards the one-time write of the equality-delete file (Sink runs in parallel)
	bool equality_delete_written = false;

	void Flush(IcebergDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto &global_entry = deleted_rows[local_state.current_file_name];
		global_entry.insert(global_entry.end(), local_entry.begin(), local_entry.end());
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(IcebergDeleteLocalState &local_state) {
		Flush(local_state);
	}
};

class IcebergDelete : public PhysicalOperator
#ifdef ICEBERG_VANE_DISTRIBUTED
    ,
                      public distributed::ExtensionWriteTaskProvider
#endif
{
public:
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	IcebergDelete(PhysicalPlan &physical_plan, IcebergTableEntry &table,
	              optional_ptr<IcebergMultiFileList> multi_file_list, PhysicalOperator &child,
	              vector<idx_t> row_id_indexes, bool is_equality_delete,
	              vector<IcebergEqualityDeletePredicate> equality_predicates)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1),
	      is_equality_delete(is_equality_delete), table(table), multi_file_list(multi_file_list),
	      row_id_indexes(std::move(row_id_indexes)), equality_predicates(std::move(equality_predicates)) {
		children.push_back(child);
	}
	//! Whether this delete is written as an Iceberg equality-delete file. Hard-wired to a
	//! `static constexpr false` when the equality-delete write feature is compiled out, so the
	//! dead branches that reference it can never trigger at runtime in shipped builds.
	bool is_equality_delete;
#else
	static constexpr bool is_equality_delete = false;
#endif

	IcebergDelete(PhysicalPlan &physical_plan, IcebergTableEntry &table,
	              optional_ptr<IcebergMultiFileList> multi_file_list, PhysicalOperator &child,
	              vector<idx_t> row_id_indexes);

	//! The table to delete from
	IcebergTableEntry &table;
	//! MultiFile list of the Iceberg Scan of the table we are deleting from.
	//! May be null when the planner has optimized the
	//! source scan away (e.g. an always-false WHERE clause like `id = NULL`).
	optional_ptr<IcebergMultiFileList> multi_file_list;
	//! The column indexes for the relevant row-id columns
	vector<idx_t> row_id_indexes;

	vector<IcebergEqualityDeletePredicate> equality_predicates;

#ifdef ICEBERG_VANE_DISTRIBUTED
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
	bool distributed_has_snapshot = false;
	int64_t distributed_snapshot_id = 0;
	optional_ptr<PhysicalOperator> distributed_worker_child;
	bool distributed_worker_plan_selected = false;
#endif

public:
#ifdef ICEBERG_VANE_DISTRIBUTED
	//! Vane distributed-write runtime interface.
	optional_ptr<distributed::ExtensionWriteTaskProvider> GetExtensionWriteTaskProvider() override;
	const distributed::DistributedExtensionWritePlan &WritePlan() const override;
	void ValidateDistributedWrite(ClientContext &context) const override;
	idx_t FinalizeDistributedWrite(ClientContext &context,
	                               const vector<DistributedWriteTaskResult> &results) const override;
	void AbortDistributedWrite(ClientContext &context,
	                           const vector<DistributedWriteTaskResult> &selected_results) const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
#endif

private:
#ifdef ICEBERG_VANE_DISTRIBUTED
	friend class IcebergInsert;
	friend class IcebergUpdate;
	static void AddDistributedDeleteArtifacts(ClientContext &context, const IcebergMultiFileList &file_list,
	                                          const vector<IcebergDistributedDeleteFileResult> &files,
	                                          IcebergDeleteGlobalState &global_state, const string &operation_name);
	static PhysicalOperator &PlanDeleteInternal(ClientContext &context, PhysicalPlanGenerator &planner,
	                                            IcebergTableEntry &table, PhysicalOperator &child_plan,
	                                            vector<idx_t> row_id_indexes, bool initialize_distributed_write);
	void InitializeDistributedWritePlan(ClientContext &context);
	void SelectDistributedWorkerPlan();
	IcebergTableEntry &ResolveDistributedWriteTable(ClientContext &context) const;
#endif

public:
	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    IcebergTableEntry &table, PhysicalOperator &child_plan,
	                                    vector<idx_t> row_id_indexes);

	//! Detects whether `child_plan`'s pushed-down filters describe a pure conjunction of equality
	//! predicates, and if so extracts them into `equality_predicates`. Returns false otherwise.
	static bool TryGetEqualityDeletePredicates(ClientContext &context, IcebergTableEntry &table,
	                                           PhysicalOperator &child_plan,
	                                           vector<IcebergEqualityDeletePredicate> &equality_predicates);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	static vector<IcebergManifestEntry> GenerateDeleteManifestEntries(IcebergDeleteGlobalState &global_state);

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	void FlushDeletes(IcebergTransaction &transaction, ClientContext &context,
	                  IcebergDeleteGlobalState &global_state) const;

private:
	//! Walk `plan` for the PhysicalTableScan that emits the row-id virtual columns the delete needs.
	static optional_ptr<PhysicalTableScan> FindDeleteSource(PhysicalOperator &plan);
	void WritePositionalDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
	                               const string &filename, IcebergDeleteFileInfo delete_file,
	                               set<idx_t> sorted_deletes) const;
	void WriteDeletionVectorFile(ClientContext &context, IcebergDeleteGlobalState &global_state, const string &filename,
	                             IcebergDeleteFileInfo delete_file, const set<idx_t> &sorted_deletes) const;
	//! Writes the Iceberg equality-delete parquet file (one column per equality field, one row of
	//! constants) and records it in `global_state.written_files`.
	void WriteEqualityDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state) const;
};

} // namespace duckdb
