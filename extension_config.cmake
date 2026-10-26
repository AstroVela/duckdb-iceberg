# This file is included by DuckDB's build system. It specifies which extension to load
option(ICEBERG_ENABLE_VORTEX "Enable experimental Vortex data files" OFF)
if(ICEBERG_ENABLE_VORTEX)
  # Iceberg needs Parquet registered before it in a statically linked shell.
  duckdb_extension_load(parquet)
  if(ICEBERG_VANE_DISTRIBUTED)
    set(VORTEX_VANE_DISTRIBUTED ON CACHE BOOL "Build Vane distributed Vortex support" FORCE)
  endif()
  duckdb_extension_load(vortex
    GIT_URL https://github.com/AstroVela/duckdb-vortex
    GIT_TAG 348d44eebb39009c291c776104a6c6649d8460d5
  )
endif()

if (NOT EMSCRIPTEN)
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/duckdb/duckdb-avro
  GIT_TAG 7f423d69709045e38f8431b3470e0395fce1a595
)
endif()

# Extension from this repo
if (DONT_LINK OR "$ENV{DONT_LINK}")
  set(ICEBERG_DONT_LINK "DONT_LINK")
else()
  set(ICEBERG_DONT_LINK "")
endif()


duckdb_extension_load(json)
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    ${ICEBERG_DONT_LINK}
)

if (NOT EMSCRIPTEN)
  duckdb_extension_load(tpch)
  duckdb_extension_load(icu)
  if (NOT ICEBERG_VANE_DISTRIBUTED)
    duckdb_extension_load(ducklake
          LOAD_TESTS
          GIT_URL https://github.com/duckdb/ducklake
          GIT_TAG a92abf755a7b4e2f3e410f8b89c72b990a0698da
  )
  endif()

  if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG ebce8e46e9a02576cfd0296fe29c23aeeddaa937
    )
  endif()
endif()
