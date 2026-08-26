#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "core/deletes/iceberg_delete_data.hpp"

namespace duckdb {

struct IcebergManifestDeletes {
public:
	void InvalidateFile(const string &file_path) {
		data_files.insert(file_path);
	}
#ifdef ICEBERG_VANE_DISTRIBUTED
	void InvalidateFileForReferencedDataFile(const string &file_path, const string &referenced_data_file) {
		referenced_data_files[file_path].insert(referenced_data_file);
	}
#endif
	bool IsInvalidated(const string &file_path) const {
		return data_files.count(file_path);
	}
#ifdef ICEBERG_VANE_DISTRIBUTED
	bool IsInvalidated(const string &file_path, const string &referenced_data_file) const {
		if (IsInvalidated(file_path)) {
			return true;
		}
		auto entry = referenced_data_files.find(file_path);
		return entry != referenced_data_files.end() && entry->second.count(referenced_data_file);
	}
#endif
	bool IsEmpty() const {
#ifdef ICEBERG_VANE_DISTRIBUTED
		return data_files.empty() && referenced_data_files.empty();
#else
		return data_files.empty();
#endif
	}

private:
	//! The 'data_file.file_path' of invalidated data files
	unordered_set<string> data_files;
#ifdef ICEBERG_VANE_DISTRIBUTED
	//! A Puffin file can contain deletion vectors for multiple data files. Distributed
	//! row-delta commits invalidate only the manifest entry for the affected data file.
	unordered_map<string, unordered_set<string>> referenced_data_files;
#endif
};

} // namespace duckdb
