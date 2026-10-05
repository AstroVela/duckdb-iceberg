#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class IcebergTableMetadata;
struct IcebergCopyInput;
struct IcebergCopyOptions;

struct IcebergVortex {
	static string WriteFormat(const IcebergTableMetadata &metadata);
	static void ValidateTable(const IcebergTableMetadata &metadata);
	static unique_ptr<FunctionData> BindScan(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &types, vector<string> &names);
	static IcebergCopyOptions CopyOptions(ClientContext &context, const IcebergCopyInput &input);
};

} // namespace duckdb
