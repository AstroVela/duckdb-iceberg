//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#endif

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "execution/operator/physical_iceberg_create_table.hpp"

namespace duckdb {

enum class IcebergInsertVirtualColumns { NONE, WRITE_ROW_ID, WRITE_SEQUENCE_NUMBER, WRITE_ROW_ID_AND_SEQUENCE_NUMBER };

struct IcebergCopyInput {
	explicit IcebergCopyInput(ClientContext &context, const IcebergTableMetadata &table_metadata,
	                          const IcebergTableSchema &schema);

public:
	const IcebergTableMetadata &table_metadata;
	const IcebergTableSchema &schema;
	string data_path;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;
	//! Partition specification for the table (if partitioned)
	optional_ptr<const IcebergPartitionSpec> partition_spec;
	//! Table index for logical plan generation (used when generating partition expressions)
	optional_idx get_table_index;
	IcebergInsertVirtualColumns virtual_columns = IcebergInsertVirtualColumns::NONE;
};

struct IcebergCopyOptions {
public:
	IcebergCopyOptions(unique_ptr<CopyInfo> info, CopyFunction copy_function);

public:
	unique_ptr<CopyInfo> info;
	CopyFunction copy_function;
	unique_ptr<FunctionData> bind_data;

	string file_path;
	bool use_tmp_file;
	FilenamePattern filename_pattern;
	string file_extension;
	CopyOverwriteMode overwrite_mode;
	bool per_thread_output;
	optional_idx file_size_bytes;
	bool rotate;
	CopyFunctionReturnType return_type;
	bool hive_file_pattern;

	//! Partitioning
	bool partition_output;
	bool write_partition_columns;
	bool write_empty_file = true;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

	//! Set of projection columns to execute prior to inserting (if any)
	vector<unique_ptr<Expression>> projection_list;
};

class IcebergInsertGlobalState : public GlobalSinkState {
public:
	explicit IcebergInsertGlobalState(ClientContext &context);

public:
	void AddFiles(DataChunk &chunk, const string &table_name, const IcebergTableMetadata &table_metadata);

public:
	ClientContext &context;
	mutex lock;
	vector<IcebergManifestEntry> written_files;
	atomic<idx_t> insert_count;
};

class IcebergInsert : public PhysicalOperator
#ifdef ICEBERG_VANE_DISTRIBUTED
    ,
                      public distributed::ExtensionWriteTaskProvider
#endif
{
public:
	//! INSERT INTO
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	              physical_index_vector_t<idx_t> column_index_map);
	IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table);

	//! CREATE TABLE AS
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	              unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;
	//! The physical copy used internally by this insert
	unique_ptr<PhysicalOperator> physical_copy_to_file;
	//! When set, this insert is part of an UPDATE: points to the delete operator so Finalize
	//! can call AddUpdateSnapshot instead of AddSnapshot.
	optional_ptr<PhysicalOperator> update_delete_op;
	//! When set, this insert is a CTAS whose table is created lazily by an
	//! upstream PhysicalIcebergCreateTable. Sink/Finalize resolve the
	//! TableCatalogEntry through this shared state instead of `table`.
	shared_ptr<IcebergCTASCreateState> create_state;

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
	optional_idx distributed_sort_order_id;
	bool distributed_has_snapshot = false;
	int64_t distributed_snapshot_id = 0;
	bool has_distributed_target = false;
	bool distributed_ctas_has_void_partition_transform = false;
	bool distributed_update_source_is_statically_empty = false;
	optional_ptr<PhysicalOperator> distributed_worker_child;
	bool distributed_worker_plan_selected = false;
	optional_ptr<PhysicalIcebergCreateTable> distributed_ctas_create;
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

	//! Distributed-write planning entry points used by other physical planners.
	void InitializeDistributedWriteTarget(IcebergTableEntry &table_entry, ClientContext &context);
	void ConfigureDistributedCTAS(PhysicalIcebergCreateTable &create, PhysicalCopyToFile &native_copy,
	                              PhysicalCopyToFile &worker_copy, string data_path, bool has_void_partition_transform);
	void ConfigureDistributedUpdate(ClientContext &context, PhysicalCopyToFile &copy, PhysicalOperator &worker_input,
	                                PhysicalOperator &delete_op);
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
#endif

private:
#ifdef ICEBERG_VANE_DISTRIBUTED
	void InitializeDistributedWritePlan();
	void SelectDistributedWorkerPlan();
	void ValidateDistributedWriteShape() const;
	IcebergTableEntry &ResolveDistributedWriteTable(ClientContext &context) const;
#endif

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	static PhysicalOperator &PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           const IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan);
	static IcebergCopyOptions GetCopyOptions(ClientContext &context, const IcebergCopyInput &copy_input);

	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    IcebergTableEntry &table);
	static vector<IcebergManifestEntry> GetInsertManifestEntries(IcebergInsertGlobalState &global_state);
	static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
	                            optional_ptr<TableCatalogEntry> table);

	//! Resolve the catalog entry this insert is targeting. For INSERT INTO this
	//! is just `this->table`; for CTAS the table is created lazily by an
	//! upstream PhysicalIcebergCreateTable, so we read it from `create_state`.
	optional_ptr<TableCatalogEntry> GetEffectiveTable() const;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
