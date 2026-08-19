//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scan/iceberg_distributed_scan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/distributed_table_function.hpp"

#include "core/deletes/iceberg_delete_data.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

struct IcebergScanInfo;
struct IcebergSnapshotScanInfo;
struct IcebergTableMetadata;
class IcebergTableSchema;

struct IcebergDistributedWorkerScanInfo {
	int32_t schema_id = 0;
	string metadata_json;
	string scan_task_set_id;
	string table_uuid;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	idx_t output_column_count = 0;
	unordered_map<int32_t, idx_t> field_id_to_output_index;
	unordered_map<int32_t, LogicalType> field_id_to_type;
};

class IcebergDistributedScanState {
public:
	IcebergDistributedScanState();
	~IcebergDistributedScanState();

public:
	void InitializePlannedScan(vector<string> payloads, shared_ptr<IcebergScanInfo> scan_info, string scan_task_set_id,
	                           string table_uuid, bool has_snapshot, int64_t snapshot_id);
	void InitializeWorkerScan(IcebergDistributedWorkerScanInfo worker_scan_info);
	void AssignWorkerTasks(vector<string> payloads);
	bool HasPlannedTasks() const;
	bool HasPlannedScanInfo() const;
	bool HasWorkerScan() const;
	bool HasWorkerTasksAssigned() const;
	const IcebergDistributedWorkerScanInfo &GetWorkerScanInfo() const;
	const IcebergTableMetadata &GetPlannedMetadata() const;
	const IcebergSnapshotScanInfo &GetPlannedSnapshot() const;
	const IcebergTableSchema &GetPlannedSchema() const;
	const string &GetScanTaskSetId() const;
	const string &GetPlannedTableUUID() const;
	bool PlannedScanHasSnapshot() const;
	int64_t GetPlannedSnapshotId() const;
	string GetPlannedTaskPayload(idx_t file_id) const;
	vector<int32_t> GetEqualityDeleteFieldIds() const;
	vector<OpenFileInfo> GetAllFiles() const;
	FileExpandResult GetExpandResult() const;
	idx_t GetTotalFileCount() const;
	unique_ptr<NodeStatistics> GetCardinality() const;
	OpenFileInfo GetFile(idx_t file_id) const;
	BoundIcebergManifestEntry GetManifestEntry(idx_t file_id) const;
	const IcebergManifestFile &GetManifestFile(const BoundIcebergManifestEntry &entry) const;
	vector<IcebergPartitionInfo> GetPartitionInfo(const string &file_path) const;
	unique_ptr<DeleteFilter> GetPositionalDeletes(const string &file_path) const;
	shared_ptr<IcebergDeleteData> GetPositionalDeleteData(const string &file_path) const;
	vector<reference<const IcebergEqualityDeleteRow>> GetEqualityDeletes(const BoundIcebergManifestEntry &entry) const;

private:
	enum class Phase : uint8_t { EMPTY, PLANNED, WORKER_TEMPLATE, WORKER_ASSIGNED };

	struct FileState;
	void LoadTaskPayloads(vector<string> payloads, const string &expected_scan_task_set_id,
	                      optional_ptr<const unordered_map<int32_t, idx_t>> field_id_to_output_index,
	                      optional_ptr<const unordered_map<int32_t, LogicalType>> field_id_to_type);
	void RequireWorkerTasksAssigned() const;
	vector<unique_ptr<FileState>> files;
	shared_ptr<IcebergScanInfo> planned_scan_info;
	string planned_scan_task_set_id;
	string planned_table_uuid;
	bool planned_scan_has_snapshot = false;
	int64_t planned_snapshot_id = 0;
	Phase phase = Phase::EMPTY;
	IcebergDistributedWorkerScanInfo worker_scan_info;
};

void ConfigureIcebergDistributedScan(TableFunction &function);

} // namespace duckdb
