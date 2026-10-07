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
is not compiled, Vortex is not fetched or linked, and the Parquet scan and write
implementation is used. COPY option validation still applies in OFF builds.
Enabling it fetches a pinned revision
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
Both builds validate `DATA_FORMAT`. An OFF build rejects an explicit request
for `vortex` with `Vortex support is not enabled`, including empty exports;
it never silently substitutes Parquet. Explicit `parquet` remains supported.
Use a fresh destination for COPY; it creates a table export rather than
appending to an existing table.

For a table registered in a REST catalog, set `write.format.default` to
`vortex` and use ordinary `INSERT INTO`. Each writer produces a new UUID-named
file, and the existing Iceberg commit path publishes the new snapshot. Existing
Parquet files remain readable when the write format changes to Vortex. Catalog
acceptance of the custom file format must be tested for that catalog.

## Compatibility changes

- In both ON and OFF builds, unknown Iceberg COPY options now raise a
  `BinderException`. Older OFF builds silently ignored them. Remove unsupported
  options from existing COPY statements; `DATA_FORMAT parquet` remains valid.
- With Vortex enabled, catalog INSERT accepts `write.format.default=parquet`
  or `vortex`, and defaults to Parquet when the property is absent. Other
  values, such as `orc`, raise `Unsupported Iceberg data format` instead of
  silently producing Parquet files. To retain the previous Parquet output,
  set the property to `parquet` or remove it before inserting. This validation
  of catalog writes applies to enabled builds.

## Initial scope

- Native execution, local data files, Iceberg v2, and unpartitioned tables.
- Primitive columns with one fixed schema; no nested columns or schema evolution.
- Table export, scans with projection and filters, and catalog-backed appends.
- Accurate file row counts and sizes; optional column statistics are omitted.
- Required Iceberg fields are checked for NULLs before writing each batch.
- Query results are cast to the Iceberg schema before writing. For example,
  `SUM(BIGINT)` produces a `HUGEINT` that is stored as `DECIMAL(38,0)`; values
  outside the decimal range raise a SQL cast error.

Delete files, UPDATE, DELETE, MERGE, virtual row columns, and distributed Vane
Vortex scans/writes are not supported by this first implementation. Remote
Vortex data paths are rejected until object-store credentials are integrated.
The pinned reader also cannot read URL-escaped local paths. Spaces, `#`, `%`,
non-ASCII characters, and other characters requiring URL escaping are rejected
before writing Vortex data files or committing a snapshot. Relative data paths
are expanded to absolute paths, including validation of the working directory.
Parquet data paths retain their existing behavior.

Schema changes are rejected before sending a catalog commit when the table
uses `write.format.default=vortex`, a retained snapshot references Vortex files,
or the transaction has appended Vortex files. This includes adding, dropping,
renaming and changing the types or nullability of columns. Switching the write
format back to Parquet does not make existing Vortex files support schema
evolution. Parquet-only tables retain schema evolution; in enabled builds this
requires inspecting retained manifests and asserting the current snapshot at
commit to reject a schema update if that snapshot changed during validation.
Validation opens one data manifest at a time and checks only newly read entries
after each batch. Finding a live Vortex entry stops further manifest and snapshot
reads immediately. A table without Vortex files still requires checking all
retained snapshot lists and unique data manifests; this worst-case I/O remains
proportional to the retained metadata.

Before binding or executing a Vortex file scan, the adapter opens the actual
data file through DuckDB's client filesystem to enforce its access policy.
With `enable_external_access=false`, permitting the metadata directory alone
does not permit reading data files. Each data file must be covered by
`allowed_directories` or `allowed_paths`, including in mixed-format snapshots
and queries prepared before external access was disabled.

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

`TIME` is checked in the same way and must be between `00:00:00` and
`23:59:59.999999` (inclusive). DuckDB's `TIME '24:00:00'` raises a SQL error
before entering the Vortex writer in both COPY and catalog INSERT. NULLs
remain supported, and Parquet continues to accept `24:00:00`.

The adapter stores field IDs in physical names of the form
`__iceberg_vortex_v1_field_<id>`. Iceberg supplies the logical column names.
Every Vortex file must contain exactly the declared fields with matching types;
ordinary Vortex files with arbitrary column names are not accepted as Iceberg
data files. This convention does not change the Vortex binary file format.

## Validation

The `Vortex format CI` workflow builds Linux native shells and SQL test runners
with Vortex enabled and with the default OFF setting. Each build runs the same
Parquet COPY, column mapping and delete-read regressions, plus COPY option
validation. ON additionally runs all Vortex SQL tests and the three local Python
regressions below; OFF checks that requesting Vortex fails. Tests run serially
within each isolated runner. No Docker or external catalog is needed.

The workflow builds `httpfs` from the revision pinned by DuckDB for the Parquet
tests. It checks the CMake option and build graph, rejects fetched Vortex/Rust
dependencies and Vortex symbols in OFF builds, and verifies statically linked
extensions. Each SQL file must report at least one successful assertion;
skipped tests cannot pass this CI check. Compiler, vcpkg and Rust build caches
are separate from the build configuration. Build logs and per-test XML/logs are
uploaded for both modes.

To repeat the CI tests against one of these native Ninja builds:

```sh
python3 scripts/ci/run_vortex_tests.py --mode ON --build-dir build/vortex-ci
# Use --mode OFF for the default build, in a separate build directory.
```

Individual regressions can also be run directly:

```sh
ICEBERG_TEST_VORTEX=1 ./build/release/test/unittest '*test/sql/local/vortex/*'
python3 test/vortex/test_local_catalog.py --duckdb build/release/duckdb
python3 test/vortex/test_file_access.py --duckdb build/release/duckdb
python3 test/vortex/test_schema_changes.py --duckdb build/release/duckdb
```

Set `ICEBERG_TEST_VORTEX=1` only for builds with the feature enabled. Default
builds skip these tests even if a standalone Vortex extension is installed.
For an OFF build, run the disabled-feature regression with
`ICEBERG_TEST_VORTEX=0 ./build/release/test/unittest '*test/sql/copy/vortex_disabled.test'`.
`test/sql/copy/data_format.test` checks COPY option validation in both builds.

The SQL tests cover NULLs, primitive types, filters (including NaN, signed zero,
infinity and JOINs), timestamp and TIME limits, projections, empty tables,
manifest row counts, aggregate output casts, decimal limits, and the default
Parquet writer.
The Python test starts an isolated loopback REST catalog stub and verifies
Vortex appends, mixed-format snapshots, actual file sizes, old snapshot
readability, required fields, and unsupported-operation rejection. It also
verifies that Vortex appends to tables
with delete manifests are rejected before any file or new snapshot is written,
including after a transaction that deletes rows and changes the write format.
Invalid timestamp and TIME appends leave the committed snapshot readable and
unchanged, including when an invalid TIME appears after earlier batches.
Rejected data paths leave the snapshot and files unchanged; the tests cover
spaces, `#`, `%`, non-ASCII names, and a relative path from a working directory
containing spaces. Aggregate appends preserve decimal values and NULLs.
It requires no Docker, Spark, or shared catalog resources. It tests the
extension's REST commit path, not compatibility with a production catalog.

The access test compares Parquet and Vortex with external access disabled. It
checks metadata-only denial, directory and exact-file grants, cached and
prepared queries, and individual file permissions in mixed-format snapshots.

The schema test checks that rejected ALTER commits leave the metadata, data
files, current reads and reads of older snapshots intact, and that a later
valid append succeeds. It covers Vortex and mixed snapshots, changing the write
format back to Parquet, Vortex files only in retained snapshots, and an append
followed by ALTER in one transaction. Parquet schema evolution remains enabled.
It also makes a later manifest temporarily unavailable to verify that finding
Vortex in the first manifest rejects the change without opening later files.
