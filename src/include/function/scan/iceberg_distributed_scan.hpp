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

class IcebergDistributedScanState {
public:
	IcebergDistributedScanState();
	~IcebergDistributedScanState();

public:
	void InstallCoordinatorTasks(vector<string> payloads, shared_ptr<IcebergScanInfo> scan_info, string scan_set_id,
	                             string table_uuid, bool has_snapshot, int64_t snapshot_id);
	void ConfigureWorkerEqualityDeleteMapping(unordered_map<int32_t, idx_t> field_id_to_output_index,
	                                          unordered_map<int32_t, LogicalType> field_id_to_type, string scan_set_id);
	void InstallWorkerTasks(vector<string> payloads);
	bool IsCoordinatorTaskSet() const;
	bool HasCoordinatorScanInfo() const;
	bool IsWorkerTaskSet() const;
	const IcebergTableMetadata &GetCoordinatorMetadata() const;
	const IcebergSnapshotScanInfo &GetCoordinatorSnapshot() const;
	const IcebergTableSchema &GetCoordinatorSchema() const;
	const string &GetCoordinatorScanSetId() const;
	const string &GetCoordinatorTableUUID() const;
	bool CoordinatorHasSnapshot() const;
	int64_t GetCoordinatorSnapshotId() const;
	string GetTaskPayload(idx_t file_id) const;
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
	struct FileState;
	void InstallTasksInternal(vector<string> payloads, const string &expected_scan_set_id,
	                          optional_ptr<const unordered_map<int32_t, idx_t>> field_id_to_output_index,
	                          optional_ptr<const unordered_map<int32_t, LogicalType>> field_id_to_type);
	void RequireInstalledTasks() const;
	vector<unique_ptr<FileState>> files;
	shared_ptr<IcebergScanInfo> coordinator_scan_info;
	string coordinator_scan_set_id;
	string coordinator_table_uuid;
	bool coordinator_has_snapshot = false;
	int64_t coordinator_snapshot_id = 0;
	bool coordinator_task_set = false;
	bool worker_mapping_configured = false;
	bool worker_tasks_installed = false;
	string worker_scan_set_id;
	unordered_map<int32_t, idx_t> worker_field_id_to_output_index;
	unordered_map<int32_t, LogicalType> worker_field_id_to_type;
};

void ConfigureIcebergDistributedScan(TableFunction &function);

} // namespace duckdb
