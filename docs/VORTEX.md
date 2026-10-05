# Experimental Vortex data files

The native Iceberg extension can read and append Vortex data files through
AstroVela's Vortex extension. Iceberg continues to own snapshots, manifests,
catalog commits, and schema metadata. A snapshot can contain both Parquet and
Vortex files.

This is an AstroVela extension to Iceberg: `vortex` is not an Apache Iceberg
standard file format. Other engines and catalog implementations need separate
qualification before using these tables.

## Build and load

Build both extensions against the same DuckDB source tree:

```sh
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make release EXT_FLAGS="-DICEBERG_ENABLE_VORTEX=ON -DRust_TOOLCHAIN=1.91.0"
```

`ICEBERG_ENABLE_VORTEX` defaults to `OFF`. With the option disabled, the adapter
is not compiled, Vortex is not fetched or linked, and the original Iceberg scan
and write paths are used. Enabling it fetches a pinned revision
of `AstroVela/duckdb-vortex`, which pins its `AstroVela/vortex` Rust dependency.

```sql
LOAD parquet;
LOAD avro;
LOAD vortex;
LOAD iceberg;

COPY (
    SELECT i::BIGINT AS id, 'row-' || i AS payload
    FROM range(10000) AS t(i)
) TO '/tmp/vortex-table' (FORMAT iceberg, DATA_FORMAT vortex);

SELECT count(*), sum(id) FROM iceberg_scan('/tmp/vortex-table');
SELECT DISTINCT file_format FROM iceberg_metadata('/tmp/vortex-table');
```

`COPY ... FORMAT iceberg` without `DATA_FORMAT` continues to write Parquet.
Use a fresh destination for COPY; it creates a table export rather than
appending to an existing table.

For a table registered in a REST catalog, set `write.format.default` to
`vortex` and use ordinary `INSERT INTO`. Each writer produces a new UUID-named
file, and the existing Iceberg commit path publishes the new snapshot. Existing
Parquet files remain readable when the write format changes to Vortex. Catalog
acceptance of the custom file format must be tested for that catalog.

## Initial scope

- Native execution, local data files, Iceberg v2, and unpartitioned tables.
- Primitive columns with one fixed schema; no nested columns or schema evolution.
- Table export, scans with projection and filters, and catalog-backed appends.
- Accurate file row counts and sizes; optional column statistics are omitted.
- Required Iceberg fields are checked for NULLs before writing each batch.

Delete files, UPDATE, DELETE, MERGE, virtual row columns, and distributed Vane
Vortex scans/writes are not supported by this first implementation. Remote
Vortex data paths are rejected until object-store credentials are integrated.
Vortex appends are rejected before creating files if the current snapshot
contains delete manifests, including deletes made while the table still used
Parquet. A format change through `set_iceberg_table_properties` takes effect at
commit; inserts in that transaction continue to use the previously committed format.

With Vortex enabled, all row filters are evaluated by DuckDB. The pinned Vortex
reader distinguishes positive and negative zero, so even simple comparisons
and dynamic JOIN filters must not be delegated to it. This also disables row
filter pushdown for Parquet-only Iceberg scans in an enabled build, with a
possible performance cost. Iceberg metadata pruning remains enabled; default
builds retain the original Parquet filter pushdown.

The pinned Vortex writer also has a narrower timestamp range than DuckDB. Each
batch is checked before entering the writer: infinite or out-of-range values
raise a SQL error instead of terminating the process. `TIMESTAMP` and
`TIMESTAMPTZ` accept epoch microseconds from `-377705023201000000` through
`253402207200999999` (inclusive), the Jiff timestamp limits. This corresponds to
`10000-01-02 (BC) 01:59:59` through `9999-12-30 22:00:00.999999` in UTC.
`TIMESTAMP_NS` accepts finite values from `-9223372036854775806` through
`9223372036854775806` epoch nanoseconds. NULLs remain supported. In particular,
year 30000 is rejected by the Vortex writer; the Parquet writer is unchanged.

The adapter stores field IDs in physical names of the form
`__iceberg_vortex_v1_field_<id>`. Iceberg supplies the logical column names.
Every Vortex file must contain exactly the declared fields with matching types;
ordinary Vortex files with arbitrary column names are not accepted as Iceberg
data files. This convention does not change the Vortex binary file format.

## Validation

```sh
ICEBERG_TEST_VORTEX=1 ./build/release/test/unittest '*test/sql/local/vortex/*'
python3 test/vortex/test_local_catalog.py --duckdb build/release/duckdb
```

Set `ICEBERG_TEST_VORTEX=1` only for builds with the feature enabled. Default
builds skip these tests even if a standalone Vortex extension is installed.

The SQL tests cover NULLs, primitive types, filters (including NaN, signed zero,
infinity and JOINs), timestamp limits, projections, empty tables, manifest row
counts, and the default Parquet writer.
The Python test starts an isolated loopback REST catalog stub and verifies
Vortex appends, mixed-format snapshots, actual file sizes, old snapshot
readability, required fields, and unsupported-operation rejection. It also
verifies that Vortex appends to tables
with delete manifests are rejected before any file or new snapshot is written,
including after a transaction that deletes rows and changes the write format.
Invalid timestamp appends leave the committed snapshot readable and unchanged.
It requires no Docker, Spark, or shared catalog resources. It tests the
extension's REST commit path, not compatibility with a production catalog.
