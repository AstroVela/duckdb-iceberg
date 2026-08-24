if(ICEBERG_VANE_DISTRIBUTED)
  add_library(iceberg_distributed_scan_objects OBJECT
              iceberg_distributed_scan.cpp)
  target_include_directories(
    iceberg_distributed_scan_objects
    PRIVATE "${DUCKDB_SOURCE_PATH}/extension/parquet/include"
            "${DUCKDB_SOURCE_PATH}/third_party/parquet"
            "${DUCKDB_SOURCE_PATH}/third_party/thrift")
  set(ALL_OBJECT_FILES
      ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_distributed_scan_objects>
      PARENT_SCOPE)
endif()
