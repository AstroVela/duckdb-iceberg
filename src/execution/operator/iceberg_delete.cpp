#include "execution/operator/iceberg_delete.hpp"

#include "iceberg_logging.hpp"
#ifdef ICEBERG_VANE_DISTRIBUTED
#include "duckdb/common/bswap.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "execution/operator/iceberg_distributed_write.hpp"
#endif
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

#include "core/deletes/iceberg_deletion_vector.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {
class IcebergDeleteLocalState;
class IcebergDeleteGlobalState;
class IcebergTableEntry;

IcebergDelete::IcebergDelete(PhysicalPlan &physical_plan, IcebergTableEntry &table,
                             optional_ptr<IcebergMultiFileList> multi_file_list, PhysicalOperator &child,
                             vector<idx_t> row_id_indexes)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      multi_file_list(multi_file_list), row_id_indexes(std::move(row_id_indexes)) {
	children.push_back(child);
}

#ifdef ICEBERG_VANE_DISTRIBUTED
static void AddDistributedDeleteCommitRequirements(IcebergTransactionData &transaction_data) {
	transaction_data.requirements.push_back(make_uniq<AssertCurrentSchemaIdRequirement>(transaction_data.table_info));
	transaction_data.TableAddAssertDefaultSpecId();
}

void IcebergDelete::InitializeDistributedWritePlan(ClientContext &context) {
	distributed_write_plan.extension_name = "iceberg";
	distributed_write_plan.operator_name = "delete";
	distributed_artifact_namespace = CreateIcebergDistributedArtifactNamespace();
	auto &metadata = table.table_info.table_metadata;
	// A Vane-enabled build still executes local-fast and ordinary direct SQL through
	// the native child. Only a logical plan transported to the Ray coordinator has
	// the planned scan state required to freeze v3 deletion-vector inputs. An
	// explicitly submitted native physical plan remains fail-closed in WritePlan().
	if (multi_file_list && (metadata.iceberg_version < 3 || multi_file_list->HasDistributedScanPlan())) {
		distributed_write_plan.worker_bind_data = BuildIcebergDistributedDeleteBind(
		    context, table, *multi_file_list, row_id_indexes, distributed_artifact_namespace);
	}
	distributed_catalog_name = table.catalog.GetName();
	distributed_schema_name = table.schema.name;
	distributed_table_name = table.name;
	distributed_table_uuid = metadata.table_uuid;
	distributed_data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	distributed_schema_id = metadata.GetCurrentSchemaId();
	distributed_partition_spec_id = metadata.default_spec_id;
	distributed_iceberg_version = metadata.iceberg_version;
	auto snapshot = metadata.GetLatestSnapshot();
	distributed_has_snapshot = snapshot != nullptr;
	distributed_snapshot_id = snapshot ? snapshot->snapshot_id : 0;
}

optional_ptr<distributed::ExtensionWriteTaskProvider> IcebergDelete::GetExtensionWriteTaskProvider() {
	SelectDistributedWorkerPlan();
	return this;
}

void IcebergDelete::SelectDistributedWorkerPlan() {
	if (distributed_worker_plan_selected) {
		return;
	}
	if (!distributed_worker_child || children.size() != 1) {
		throw InvalidInputException("Distributed Iceberg DELETE requires exactly one worker child");
	}
	children[0] = *distributed_worker_child;
	distributed_worker_plan_selected = true;
}

const distributed::DistributedExtensionWritePlan &IcebergDelete::WritePlan() const {
	if (distributed_write_plan.worker_bind_data.empty() || distributed_artifact_namespace.empty() ||
	    !distributed_worker_plan_selected || !multi_file_list) {
		throw InvalidInputException("Iceberg distributed DELETE was not initialized during planning");
	}
	if (is_equality_delete) {
		throw NotImplementedException("Distributed Iceberg equality DELETE is not supported");
	}
	if (distributed_iceberg_version != 2 && distributed_iceberg_version != 3) {
		throw NotImplementedException("Distributed Iceberg DELETE supports format-version 2 and 3 tables only");
	}
	if (children.size() != 1) {
		throw InvalidInputException("Distributed Iceberg DELETE requires exactly one worker input");
	}
	return distributed_write_plan;
}

IcebergTableEntry &IcebergDelete::ResolveDistributedWriteTable(ClientContext &context) const {
	WritePlan();
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, distributed_catalog_name, distributed_schema_name,
	                                                         distributed_table_name)
	                        .Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);
	auto &metadata = table_entry.table_info.table_metadata;
	if (metadata.table_uuid != distributed_table_uuid) {
		throw TransactionException("Iceberg table %s.%s.%s was replaced after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (metadata.GetCurrentSchemaId() != distributed_schema_id ||
	    metadata.default_spec_id != distributed_partition_spec_id ||
	    metadata.iceberg_version != distributed_iceberg_version) {
		throw TransactionException("Iceberg table %s.%s.%s layout changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	auto snapshot = metadata.GetLatestSnapshot();
	if ((snapshot != nullptr) != distributed_has_snapshot ||
	    (snapshot && snapshot->snapshot_id != distributed_snapshot_id)) {
		throw TransactionException("Iceberg table %s.%s.%s snapshot changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	auto current_data_path = metadata.GetDataPath(FileSystem::GetFileSystem(context));
	if (current_data_path != distributed_data_path) {
		throw TransactionException("Iceberg table %s.%s.%s data path changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (!metadata.PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType::DELETE)) {
		throw NotImplementedException(IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(
		    distributed_table_name, WRITE_DELETE_MODE, metadata.GetTableProperty(WRITE_DELETE_MODE)));
	}
	return table_entry;
}

void IcebergDelete::ValidateDistributedWrite(ClientContext &context) const {
	auto &table_entry = ResolveDistributedWriteTable(context);
	ValidateIcebergDistributedTargetPartitionSpec(table_entry.table_info.table_metadata, "DELETE");
	ValidateIcebergDistributedRowDeltaSourceBaseline(*multi_file_list, table_entry.table_info.table_metadata, "DELETE");
	ValidateIcebergDistributedRowDeltaSourceSpecs(*multi_file_list,
	                                              table_entry.table_info.table_metadata.default_spec_id, "DELETE");
}

static idx_t FindDistributedDeleteDataFile(const IcebergMultiFileList &file_list, const string &path) {
	file_list.GetTotalFileCount();
	auto files = file_list.GetAllFiles();
	for (idx_t index = 0; index < files.size(); index++) {
		if (files[index].path == path || file_list.GetManifestEntry(index).entry.data_file.file_path == path) {
			return index;
		}
	}
	throw InvalidInputException("Distributed Iceberg DELETE referenced unplanned data file '%s'", path);
}

static bool HasPuffinMagic(const_data_ptr_t data) {
	static constexpr data_t PUFFIN_MAGIC[] = {0x50, 0x46, 0x41, 0x31};
	return memcmp(data, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC)) == 0;
}

static void ValidateDistributedDeletionVectorBlob(const vector<data_t> &blob) {
	static constexpr data_t DELETION_VECTOR_MAGIC[] = {0xD1, 0xD3, 0x39, 0x64};
	static constexpr idx_t FIXED_PREFIX_SIZE = sizeof(uint32_t) + sizeof(DELETION_VECTOR_MAGIC) + sizeof(uint64_t);
	static constexpr idx_t CHECKSUM_SIZE = sizeof(uint32_t);
	if (blob.size() <= FIXED_PREFIX_SIZE + CHECKSUM_SIZE) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned a truncated deletion-vector blob");
	}

	auto cursor = blob.data();
	auto end = blob.data() + blob.size();
	auto vector_size = BSwap(Load<uint32_t>(cursor));
	if (NumericCast<idx_t>(vector_size) != blob.size() - sizeof(uint32_t) - CHECKSUM_SIZE) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid deletion-vector length");
	}
	cursor += sizeof(uint32_t);
	if (memcmp(cursor, DELETION_VECTOR_MAGIC, sizeof(DELETION_VECTOR_MAGIC)) != 0) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned invalid deletion-vector magic");
	}
	cursor += sizeof(DELETION_VECTOR_MAGIC);

	auto bitmap_count = Load<uint64_t>(cursor);
	cursor += sizeof(uint64_t);
	if (bitmap_count == 0 || bitmap_count > NumericCast<uint64_t>((end - CHECKSUM_SIZE - cursor) / sizeof(int32_t))) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid deletion-vector bitmap count");
	}

	int32_t previous_key = -1;
	for (uint64_t bitmap_index = 0; bitmap_index < bitmap_count; bitmap_index++) {
		if (end - CHECKSUM_SIZE - cursor <= NumericCast<int64_t>(sizeof(int32_t))) {
			throw InvalidInputException("Distributed Iceberg v3 DELETE returned a truncated deletion-vector bitmap");
		}
		auto key = Load<int32_t>(cursor);
		cursor += sizeof(int32_t);
		if (key < 0 || key <= previous_key) {
			throw InvalidInputException("Distributed Iceberg v3 DELETE returned unordered deletion-vector bitmap keys");
		}
		previous_key = key;

		auto available = NumericCast<idx_t>(end - CHECKSUM_SIZE - cursor);
		roaring::Roaring bitmap;
		try {
			bitmap = roaring::Roaring::readSafe(reinterpret_cast<const char *>(cursor), available);
		} catch (const std::exception &ex) {
			throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid Roaring bitmap: %s",
			                            ex.what());
		}
		auto bitmap_size = bitmap.getSizeInBytes(true);
		if (bitmap_size == 0 || bitmap_size > available) {
			throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid Roaring bitmap size");
		}
		vector<data_t> canonical_bitmap(bitmap_size);
		if (bitmap.write(reinterpret_cast<char *>(canonical_bitmap.data()), true) != bitmap_size ||
		    memcmp(cursor, canonical_bitmap.data(), bitmap_size) != 0) {
			throw InvalidInputException("Distributed Iceberg v3 DELETE returned a non-canonical Roaring bitmap");
		}
		cursor += bitmap_size;
	}
	if (cursor != end - CHECKSUM_SIZE) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned trailing deletion-vector data");
	}
}

static set<idx_t> ValidateDistributedDeletionVectorArtifact(ClientContext &context,
                                                            const IcebergDistributedDeleteFileResult &file,
                                                            const BoundIcebergManifestEntry &source_entry) {
	static constexpr idx_t PUFFIN_MAGIC_SIZE = 4;
	static constexpr idx_t FOOTER_TRAILER_SIZE = sizeof(int32_t) + sizeof(uint32_t) + PUFFIN_MAGIC_SIZE;
	if (!file.is_deletion_vector || file.content_offset != PUFFIN_MAGIC_SIZE || file.content_size_in_bytes < 12 ||
	    file.file_size_bytes <= 2 * PUFFIN_MAGIC_SIZE + FOOTER_TRAILER_SIZE ||
	    file.content_offset > file.file_size_bytes ||
	    file.content_size_in_bytes > file.file_size_bytes - file.content_offset) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned invalid Puffin artifact metadata");
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(file.delete_file_path, FileOpenFlags::FILE_FLAGS_READ);
	if (handle->GetFileSize() != file.file_size_bytes) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE Puffin artifact size changed during finalization");
	}

	data_t leading_magic[PUFFIN_MAGIC_SIZE];
	data_t trailer[FOOTER_TRAILER_SIZE];
	handle->Read(leading_magic, sizeof(leading_magic), 0);
	handle->Read(trailer, sizeof(trailer), file.file_size_bytes - FOOTER_TRAILER_SIZE);
	auto footer_payload_size = Load<int32_t>(trailer);
	auto footer_flags = Load<uint32_t>(trailer + sizeof(int32_t));
	if (!HasPuffinMagic(leading_magic) || !HasPuffinMagic(trailer + sizeof(int32_t) + sizeof(uint32_t)) ||
	    footer_payload_size <= 0 || footer_flags != 0) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid Puffin container");
	}

	auto footer_payload_bytes = NumericCast<idx_t>(footer_payload_size);
	if (footer_payload_bytes > file.file_size_bytes - FOOTER_TRAILER_SIZE - PUFFIN_MAGIC_SIZE) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned an invalid Puffin footer size");
	}
	auto footer_magic_offset = file.file_size_bytes - FOOTER_TRAILER_SIZE - footer_payload_bytes - PUFFIN_MAGIC_SIZE;
	if (file.content_size_in_bytes > file.file_size_bytes - file.content_offset ||
	    file.content_offset + file.content_size_in_bytes != footer_magic_offset) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned a non-canonical Puffin blob range");
	}

	data_t footer_magic[PUFFIN_MAGIC_SIZE];
	handle->Read(footer_magic, sizeof(footer_magic), footer_magic_offset);
	if (!HasPuffinMagic(footer_magic)) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned invalid Puffin footer magic");
	}
	vector<data_t> footer_payload(footer_payload_bytes);
	handle->Read(footer_payload.data(), footer_payload.size(), footer_magic_offset + PUFFIN_MAGIC_SIZE);
	auto footer_doc = unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(
	    reinterpret_cast<const char *>(footer_payload.data()), static_cast<size_t>(footer_payload.size()), 0));
	auto root = footer_doc ? yyjson_doc_get_root(footer_doc.get()) : nullptr;
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned invalid Puffin footer JSON");
	}
	auto blobs = root ? yyjson_obj_get(root, "blobs") : nullptr;
	auto blob = blobs && yyjson_is_arr(blobs) && yyjson_arr_size(blobs) == 1 ? yyjson_arr_get_first(blobs) : nullptr;
	if (!blob || !yyjson_is_obj(blob)) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned invalid Puffin blob metadata");
	}
	auto type = blob ? yyjson_obj_get(blob, "type") : nullptr;
	auto fields = blob ? yyjson_obj_get(blob, "fields") : nullptr;
	auto snapshot_id = blob ? yyjson_obj_get(blob, "snapshot-id") : nullptr;
	auto sequence_number = blob ? yyjson_obj_get(blob, "sequence-number") : nullptr;
	auto offset = blob ? yyjson_obj_get(blob, "offset") : nullptr;
	auto length = blob ? yyjson_obj_get(blob, "length") : nullptr;
	auto compression_codec = blob ? yyjson_obj_get(blob, "compression-codec") : nullptr;
	auto properties = blob ? yyjson_obj_get(blob, "properties") : nullptr;
	if (!properties || !yyjson_is_obj(properties) || compression_codec) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned compressed or invalid Puffin metadata");
	}
	auto referenced_data_file = properties ? yyjson_obj_get(properties, "referenced-data-file") : nullptr;
	auto cardinality = properties ? yyjson_obj_get(properties, "cardinality") : nullptr;
	if (!blob || !type || !yyjson_equals_str(type, "deletion-vector-v1") || !fields || !yyjson_is_arr(fields) ||
	    yyjson_arr_size(fields) != 0 || !snapshot_id || !yyjson_is_int(snapshot_id) ||
	    yyjson_get_sint(snapshot_id) != -1 || !sequence_number || !yyjson_is_int(sequence_number) ||
	    yyjson_get_sint(sequence_number) != -1 || !offset || !yyjson_is_int(offset) ||
	    yyjson_get_sint(offset) != NumericCast<int64_t>(file.content_offset) || !length || !yyjson_is_int(length) ||
	    yyjson_get_sint(length) != NumericCast<int64_t>(file.content_size_in_bytes) || !referenced_data_file ||
	    !yyjson_is_str(referenced_data_file) || string(yyjson_get_str(referenced_data_file)) != file.data_file_path ||
	    !cardinality || !yyjson_is_str(cardinality) ||
	    string(yyjson_get_str(cardinality)) !=
	        StringUtil::Format("%llu", static_cast<unsigned long long>(file.delete_count))) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned inconsistent Puffin footer metadata");
	}

	vector<data_t> deletion_vector_blob(file.content_size_in_bytes);
	handle->Read(deletion_vector_blob.data(), deletion_vector_blob.size(), file.content_offset);
	ValidateDistributedDeletionVectorBlob(deletion_vector_blob);
	auto deletion_vector =
	    IcebergDeletionVectorData::FromBlob(source_entry, deletion_vector_blob.data(), deletion_vector_blob.size());
	set<idx_t> deleted_rows;
	deletion_vector->ToSet(deleted_rows);
	if (deleted_rows.empty() || deleted_rows.size() != file.delete_count ||
	    *deleted_rows.begin() != file.pos_min_value || *deleted_rows.rbegin() != file.pos_max_value) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned inconsistent deletion-vector rows");
	}
	auto source_record_count = NumericCast<idx_t>(source_entry.entry.data_file.record_count);
	if (*deleted_rows.rbegin() >= source_record_count) {
		throw InvalidInputException("Distributed Iceberg v3 DELETE returned an out-of-range deletion-vector row");
	}
	return deleted_rows;
}

idx_t IcebergDelete::FinalizeDistributedWrite(ClientContext &context,
                                              const vector<DistributedWriteTaskResult> &results) const {
	auto write_info = distributed::ResolveDistributedExtensionWriteInfo(context, WritePlan());
	auto decoded = DecodeIcebergDistributedRowDeltaResults(
	    context, distributed_data_path, distributed_artifact_namespace, write_info, results,
	    IcebergDistributedRowDeltaKind::DELETE, distributed_iceberg_version);
	CleanupIcebergDistributedRowDelta(context, distributed_data_path, distributed_artifact_namespace,
	                                  &decoded.selected_artifact_paths);
	auto &iceberg_table = ResolveDistributedWriteTable(context);
	ValidateIcebergDistributedRowDeltaSourceBaseline(*multi_file_list, iceberg_table.table_info.table_metadata,
	                                                 "DELETE");
	ValidateIcebergDistributedRowDeltaSourceSpecs(*multi_file_list,
	                                              iceberg_table.table_info.table_metadata.default_spec_id, "DELETE");
	IcebergDeleteGlobalState global_state;
	for (auto &file : decoded.delete_files) {
		auto file_index = FindDistributedDeleteDataFile(*multi_file_list, file.data_file_path);
		auto source_entry = multi_file_list->GetManifestEntry(file_index);
		auto source_record_count = NumericCast<idx_t>(source_entry.entry.data_file.record_count);
		if (source_record_count == 0 || file.pos_max_value >= source_record_count) {
			throw InvalidInputException(
			    "Distributed Iceberg DELETE returned a row position outside data file '%s' (%llu records)",
			    file.data_file_path, static_cast<unsigned long long>(source_record_count));
		}
		IcebergDeleteFileInfo delete_file;
		delete_file.data_file_path = file.data_file_path;
		delete_file.file_name = file.delete_file_path;
		delete_file.file_format = file.is_deletion_vector ? "puffin" : "parquet";
		if (file.is_deletion_vector) {
			auto deleted_rows = ValidateDistributedDeletionVectorArtifact(context, file, source_entry);
			set<idx_t> existing_rows;
			auto existing_deletes = multi_file_list->GetExistingPositionalDeleteData(file.data_file_path);
			if (existing_deletes) {
				existing_deletes->ToSet(existing_rows);
			}
			if (deleted_rows.size() != existing_rows.size() + file.new_delete_count) {
				throw InvalidInputException(
				    "Distributed Iceberg v3 DELETE did not preserve exactly the planned existing delete rows");
			}
			for (auto row : existing_rows) {
				if (!deleted_rows.count(row)) {
					throw InvalidInputException(
					    "Distributed Iceberg v3 DELETE omitted a planned existing deletion-vector row");
				}
			}
			delete_file.content_offset = optional_idx(file.content_offset);
			delete_file.content_size_in_bytes = optional_idx(file.content_size_in_bytes);
			auto &existing_deletion_vector_path =
			    multi_file_list->GetDistributedExistingDeletionVectorPath(file.data_file_path);
			if (!existing_deletion_vector_path.empty()) {
				global_state.altered_manifests.InvalidateFile(existing_deletion_vector_path);
			}
		} else {
			delete_file.footer_size = file.footer_size_bytes;
		}
		delete_file.delete_count = file.delete_count;
		delete_file.file_size_bytes = file.file_size_bytes;
		delete_file.pos_min_value = file.pos_min_value;
		delete_file.pos_max_value = file.pos_max_value;
		delete_file.partition_info = multi_file_list->GetPartitionInfoForDataFile(file.data_file_path);
		if (!global_state.written_files.emplace(file.data_file_path, std::move(delete_file)).second) {
			throw InvalidInputException("Distributed Iceberg DELETE produced multiple delete files for data file '%s'",
			                            file.data_file_path);
		}
	}
	global_state.total_deleted_count = decoded.affected_rows;
	if (decoded.affected_rows != 0 && global_state.written_files.empty()) {
		throw InternalException("Distributed Iceberg DELETE did not produce delete files");
	}
	if (decoded.affected_rows != 0) {
		auto manifest_entries = GenerateDeleteManifestEntries(global_state);
		auto &transaction = IcebergTransaction::Get(context, iceberg_table.catalog);
		ApplyTableUpdate(iceberg_table.table_info, transaction, [&](IcebergTableInformation &table_info) {
			auto &transaction_data = table_info.GetOrCreateTransactionData(transaction);
			AddDistributedDeleteCommitRequirements(transaction_data);
			transaction_data.RetainAddedSnapshotFilesOnRollback();
			transaction_data.AddSnapshot(IcebergSnapshotOperationType::DELETE, std::move(manifest_entries),
			                             std::move(global_state.altered_manifests));
			if (distributed_iceberg_version >= 3) {
				for (const auto &entry : global_state.written_files) {
					transaction_data.transactional_delete_files[entry.second.data_file_path] = entry.second.file_name;
				}
			}
		});
	}
	return decoded.affected_rows;
}

void IcebergDelete::AbortDistributedWrite(ClientContext &context, const vector<DistributedWriteTaskResult> &) const {
	CleanupIcebergDistributedRowDelta(context, distributed_data_path, distributed_artifact_namespace);
}

void IcebergDelete::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (distributed_worker_plan_selected) {
		throw InvalidInputException(
		    "A distributed Iceberg DELETE worker plan cannot be executed as a native coordinator operator");
	}
	PhysicalOperator::BuildPipelines(current, meta_pipeline);
}
#endif

unique_ptr<GlobalSinkState> IcebergDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergDeleteGlobalState>();
}

unique_ptr<LocalSinkState> IcebergDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<IcebergDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType IcebergDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

	if (is_equality_delete) {
		//! The equality-delete file's contents come entirely from the planning-time predicates,
		//! not from the streamed rows - write it exactly once and stop consuming input.
		bool should_write = false;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (!global_state.equality_delete_written) {
				global_state.equality_delete_written = true;
				should_write = true;
			}
		}
		if (should_write) {
			WriteEqualityDeleteFile(context.client, global_state);
		}
		return SinkResultType::FINISHED;
	}

	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_row_number = chunk.data[row_id_indexes[1]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(chunk.size(), row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(chunk.size(), file_name_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto row_idx = row_data.sel->get_index(i);
		auto file_name_idx = file_name_vdata.sel->get_index(i);
		if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
		auto file_name = file_name_data[file_name_idx].GetString();

		if (local_state.current_file_name.empty() || local_state.current_file_name != file_name) {
			// local_state points to new file, flush to global state
			global_state.Flush(local_state);
			local_state.current_file_name = file_name;
		}
		auto row_number = file_row_data[row_idx];
		local_state.file_row_numbers.push_back(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType IcebergDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();
	global_state.FinalFlush(local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

void IcebergDelete::WriteDeletionVectorFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
                                            const string &filename, IcebergDeleteFileInfo delete_file,
                                            const set<idx_t> &sorted_deletes) const {
	auto delete_file_path = delete_file.file_name;

	// Build deletion vector data
	unordered_map<int32_t, roaring::Roaring> bitmaps;

	// Group row indices by high 32 bits
	for (auto row_idx : sorted_deletes) {
		int64_t row_id = static_cast<int64_t>(row_idx);
		int32_t high_bits = static_cast<int32_t>(row_id >> 32);
		uint32_t low_bits = static_cast<uint32_t>(row_id & 0xFFFFFFFF);

		auto &bitmap = bitmaps[high_bits];
		bitmap.add(low_bits);
	}

	// Serialize the deletion vector to a blob, then wrap it in a valid Puffin file
	// container (Magic + Blob + Footer) so the output is a spec-compliant Puffin file
	// rather than a bare blob. The blob is placed at offset 4, after the leading magic.
	auto blob_data = IcebergDeletionVectorData::ToBlob(bitmaps);
	auto puffin_file = IcebergDeletionVectorData::ToPuffinFile(blob_data, filename, sorted_deletes.size());

	// Write the Puffin file
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_handle =
	    fs.OpenFile(delete_file_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	file_handle->Write(puffin_file.data(), puffin_file.size());
	file_handle->Close();

	delete_file.file_name = delete_file_path;
	delete_file.file_format = "puffin";
	delete_file.delete_count = sorted_deletes.size();
	// The deletion-vector blob is written immediately after the 4-byte leading Puffin magic.
	delete_file.content_offset = 4;
	delete_file.content_size_in_bytes = blob_data.size();
	delete_file.file_size_bytes = puffin_file.size();
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg DELETE, wrote deletion_vector_file '%s' for data_file '%s', delete_count=%llu, "
	           "file_size=%llu bytes",
	           delete_file_path, filename, sorted_deletes.size(), puffin_file.size());
	global_state.written_files.emplace(filename, std::move(delete_file));
}

void IcebergDelete::WritePositionalDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
                                              const string &filename, IcebergDeleteFileInfo delete_file,
                                              set<idx_t> sorted_deletes) const {
	auto delete_file_path = delete_file.file_name;
	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs ids and names for the delete files
	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::DELETE_FILE_PATH_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::DELETE_POS_FIELD_ID));
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	// generate the physical copy to file
	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(context));

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// run the copy to file
	vector<LogicalType> write_types;
	write_types.push_back(LogicalType::VARCHAR);
	write_types.push_back(LogicalType::BIGINT);

	DataChunk write_chunk;
	write_chunk.Initialize(context, write_types);
	// the first vector is constant (the file name)
	Value filename_val(filename);
	write_chunk.data[0].Reference(filename_val);

	idx_t row_count = 0;
	auto row_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	for (auto &row_idx : sorted_deletes) {
		row_data[row_count++] = NumericCast<int64_t>(row_idx);
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetCardinality(row_count);
			copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
			                               write_chunk);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetCardinality(row_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = stats.row_count;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.footer_size = stats.footer_size_bytes.GetValue<idx_t>();
	auto pos_stats = stats.column_statistics.find("\"pos\"");
	auto pos_min = pos_stats->second.find("min");
	auto pos_min_value = pos_min->second.GetValue<idx_t>();
	auto pos_max = pos_stats->second.find("max");
	auto pos_max_value = pos_max->second.GetValue<idx_t>();
	delete_file.pos_min_value = pos_min_value;
	delete_file.pos_max_value = pos_max_value;
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg DELETE, wrote positional_delete_file '%s' for data_file '%s', delete_count=%llu, "
	           "file_size=%llu bytes",
	           delete_file_path, filename, stats.row_count, stats.file_size_bytes);
	global_state.written_files.emplace(filename, std::move(delete_file));
}

static void PopulateAlteredManifests(const IcebergMultiFileList &multi_file_list, IcebergManifestDeletes &out,
                                     IcebergDeleteData &delete_data) {
	if (delete_data.type != IcebergDeleteType::DELETION_VECTOR) {
		return;
	}
	for (auto &bound_entry : delete_data.entries) {
		auto &entry = bound_entry.entry;
		out.InvalidateFile(entry.data_file.file_path);
	}
}

void IcebergDelete::FlushDeletes(IcebergTransaction &transaction, ClientContext &context,
                                 IcebergDeleteGlobalState &global_state) const {
	bool write_deletion_vector = table.table_info.table_metadata.iceberg_version >= 3;

	if (!multi_file_list) {
		throw InternalException("IcebergDelete multi_file_list is NULL");
	}
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : global_state.deleted_rows) {
		auto &filename = entry.first;
		auto &deleted_rows = entry.second;

		// sort and duplicate eliminate the deletes
		set<idx_t> sorted_deletes;
		for (auto &row_idx : deleted_rows) {
			sorted_deletes.insert(row_idx);
		}
		if (sorted_deletes.size() != deleted_rows.size()) {
			throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
			                              "Iceberg. Eliminate duplicate matches prior to running the UPDATE");
		}
		if (write_deletion_vector) {
			//! Addd the existing delete we're replacing
			auto existing_delete = multi_file_list->GetExistingPositionalDeleteData(filename);
			if (existing_delete) {
				auto &delete_data = *existing_delete;
				PopulateAlteredManifests(*multi_file_list, global_state.altered_manifests, delete_data);
				delete_data.ToSet(sorted_deletes);
			}
		}

		IcebergDeleteFileInfo delete_file;
		delete_file.data_file_path = filename;
		delete_file.partition_info = multi_file_list->GetPartitionInfoForDataFile(filename);

		auto &fs = FileSystem::GetFileSystem(context);

		string file_format;
		if (write_deletion_vector) {
			file_format = "puffin";
		} else {
			file_format = "parquet";
		}

		string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes." + file_format;
		// Place the delete file in the same directory as the data file it references,
		// so that for partitioned tables it lands in the correct partition folder.
		auto sep = fs.PathSeparator(filename);
		auto last_sep = filename.rfind(sep);
		if (last_sep == string::npos) {
			throw InvalidConfigurationException("Cannot create valid file path for delete file");
		}
		string data_file_dir = filename.substr(0, last_sep);
		string delete_file_path = fs.JoinPath(data_file_dir, delete_filename);

		delete_file.file_name = delete_file_path;

		if (!write_deletion_vector) {
			WritePositionalDeleteFile(context, global_state, filename, delete_file, sorted_deletes);
		} else {
			WriteDeletionVectorFile(context, global_state, filename, delete_file, sorted_deletes);
		}
	}
}

vector<IcebergManifestEntry> IcebergDelete::GenerateDeleteManifestEntries(IcebergDeleteGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	auto &delete_files = global_state.written_files;
	vector<IcebergManifestEntry> iceberg_delete_files;
	for (auto &delete_entry : delete_files) {
		auto data_file_name = delete_entry.first;
		auto &delete_file = delete_entry.second;

		IcebergManifestEntry manifest_entry;
		manifest_entry.status = IcebergManifestEntryStatusType::ADDED;
		auto &data_file = manifest_entry.data_file;

		// This will only ever be triggered when duckdb-iceberg
		// is built with ICEBERG_ENABLE_EQUALITY_DELETE_WRITES=1
		if (!delete_file.equality_ids.empty()) {
			//! Equality delete: a global delete identified only by the equality field values.
			//! It has no referenced data file, no filename bounds and no partition info.
			data_file.content = IcebergManifestEntryContentType::EQUALITY_DELETES;
			data_file.file_path = delete_file.file_name;
			data_file.file_format = delete_file.file_format;
			data_file.record_count = delete_file.delete_count;
			data_file.file_size_in_bytes = delete_file.file_size_bytes;
			data_file.equality_ids = delete_file.equality_ids;
			iceberg_delete_files.push_back(manifest_entry);
			continue;
		}

		data_file.content = IcebergManifestEntryContentType::POSITION_DELETES;
		data_file.file_path = delete_file.file_name;
		data_file.file_format = delete_file.file_format;
		data_file.record_count = delete_file.delete_count;
		data_file.file_size_in_bytes = delete_file.file_size_bytes;
		if (delete_file.content_size_in_bytes.IsValid()) {
			data_file.content_size_in_bytes = Value::BIGINT(delete_file.content_size_in_bytes.GetIndex());
		}
		if (delete_file.content_offset.IsValid()) {
			data_file.content_offset = Value::BIGINT(delete_file.content_offset.GetIndex());
		}

		// set lower and upper bound for the filename column
		data_file.lower_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
		data_file.upper_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
		// set referenced_data_file
		data_file.referenced_data_file = data_file_name;
		// copy partition info from the data file being deleted
		data_file.partition_info = delete_file.partition_info;
		iceberg_delete_files.push_back(manifest_entry);
	}
	return iceberg_delete_files;
}

SinkFinalizeType IcebergDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

	if (is_equality_delete) {
		//! Ensure the equality-delete file is written even if Sink never ran (e.g. zero matching rows).
		bool should_write = false;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (!global_state.equality_delete_written) {
				global_state.equality_delete_written = true;
				should_write = true;
			}
		}
		if (should_write) {
			WriteEqualityDeleteFile(context, global_state);
		}
	} else if (global_state.deleted_rows.empty()) {
		// FIXME: replace with get deleted rows
		return SinkFinalizeType::READY;
	}

	auto &iceberg_transaction = IcebergTransaction::Get(context, table.catalog);
	if (!is_equality_delete) {
		// write out the delete rows
		FlushDeletes(iceberg_transaction, context, global_state);
	}

	// write out the new manifest file
	auto &irc_table = table.Cast<IcebergTableEntry>();

	auto &table_info = irc_table.table_info;
	auto iceberg_delete_files = GenerateDeleteManifestEntries(global_state);

	if (!global_state.written_files.empty()) {
		ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTableInformation &tbl) {
			auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
			transaction_data.AddSnapshot(IcebergSnapshotOperationType::DELETE, std::move(iceberg_delete_files),
			                             std::move(global_state.altered_manifests));

			//! Add or overwrite the currently active transaction-local delete files
			for (auto &entry : global_state.written_files) {
				auto &delete_file = entry.second;
				if (table_info.table_metadata.iceberg_version >= 3) {
					transaction_data.transactional_delete_files[delete_file.data_file_path] = delete_file.file_name;
				}
			}
		});
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count.load()));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergDelete::GetName() const {
	return "ICEBERG_DELETE";
}

InsertionOrderPreservingMap<string> IcebergDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
#ifdef ICEBERG_VANE_DISTRIBUTED
	if (!distributed_table_name.empty()) {
		result["Table Name"] = distributed_table_name;
		return result;
	}
#endif
	result["Table Name"] = table.name;
	return result;
}

optional_ptr<PhysicalTableScan> IcebergDelete::FindDeleteSource(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();
		bool found = false;
		for (auto &col : scan.column_ids) {
			if (col.GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
		return scan;
	}
	for (auto &children : plan.children) {
		auto result = FindDeleteSource(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

PhysicalOperator &IcebergDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                            IcebergTableEntry &table, PhysicalOperator &child_plan,
                                            vector<idx_t> row_id_indexes) {
	auto table_scan = FindDeleteSource(child_plan);
	optional_ptr<IcebergMultiFileList> file_list;
	if (table_scan) {
		auto &bind_data = table_scan->bind_data->Cast<MultiFileBindData>();
		file_list = &bind_data.file_list->Cast<IcebergMultiFileList>();
	} else {
		DUCKDB_LOG_DEBUG(context, "Could not find IcebergDelete source. Iceberg Multi File list is empty");
	}

#ifdef ICEBERG_VANE_DISTRIBUTED
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	vector<IcebergEqualityDeletePredicate> equality_predicates;
	bool is_equality_delete = TryGetEqualityDeletePredicates(context, table, child_plan, equality_predicates);
	auto &result = planner
	                   .Make<IcebergDelete>(table, file_list, child_plan, std::move(row_id_indexes), is_equality_delete,
	                                        std::move(equality_predicates))
	                   .Cast<IcebergDelete>();
#else
	auto &result =
	    planner.Make<IcebergDelete>(table, file_list, child_plan, std::move(row_id_indexes)).Cast<IcebergDelete>();
#endif
	result.InitializeDistributedWritePlan(context);
	result.distributed_worker_child =
	    &PlanIcebergDistributedRowDeltaRepartition(planner, child_plan, result.row_id_indexes[0]);
	return result;
#else
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	vector<IcebergEqualityDeletePredicate> equality_predicates;
	bool is_equality_delete = TryGetEqualityDeletePredicates(context, table, child_plan, equality_predicates);
	return planner.Make<IcebergDelete>(table, file_list, child_plan, std::move(row_id_indexes), is_equality_delete,
	                                   std::move(equality_predicates));
#else
	return planner.Make<IcebergDelete>(table, file_list, child_plan, std::move(row_id_indexes));
#endif
#endif
}

PhysicalOperator &IcebergCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from Iceberg table");
	}
	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	auto iceberg_version = updated_table_entry.table_info.table_metadata.iceberg_version;
	if (iceberg_version < 2) {
		throw NotImplementedException("Delete from Iceberg V%d tables",
		                              updated_table_entry.table_info.table_metadata.iceberg_version);
	}

	vector<idx_t> row_id_indexes;
	// we only push 2 columns for positional deletes
	idx_t column_offset = 0;
	if (iceberg_version >= 3) {
		//! The row ids of the table contain the _row_id column, which we're not interested in
		column_offset = 1;
	}
	for (idx_t i = 0; i < 2; i++) {
		auto &bound_ref = op.expressions[column_offset + i]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.index);
	}

	auto allows_positional_deletes = updated_table_entry.table_info.table_metadata.PropertiesAllowPositionalDeletes(
	    IcebergSnapshotOperationType::DELETE);
	if (!allows_positional_deletes) {
		auto delete_table_property = updated_table_entry.table_info.table_metadata.GetTableProperty(WRITE_DELETE_MODE);
		auto error_message = IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(
		    updated_table_entry.name, WRITE_DELETE_MODE, delete_table_property);
		throw NotImplementedException(error_message);
	}

	auto &iceberg_delete = IcebergDelete::PlanDelete(context, planner, updated_table_entry, plan, row_id_indexes);
	return iceberg_delete;
}

} // namespace duckdb
