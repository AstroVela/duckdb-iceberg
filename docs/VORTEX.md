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

- Native execution, local or S3 data-file reads, Iceberg v2, and unpartitioned tables.
- Primitive columns with one fixed schema; no nested columns or schema evolution.
- Table export, scans with projection and filters, and catalog-backed appends.
- Accurate file row counts and sizes. INTEGER, BIGINT, FLOAT and DOUBLE columns
  also record lower/upper bounds, total value counts and NULL counts; floating
  columns record exact NaN counts, including zero. Total value counts include
  NULLs and NaNs. Other column types omit these optional metrics.
- Required Iceberg fields are checked for NULLs before writing each batch.
- Query results are cast to the Iceberg schema before writing. For example,
  `SUM(BIGINT)` produces a `HUGEINT` that is stored as `DECIMAL(38,0)`; values
  outside the decimal range raise a SQL cast error.

Delete files, UPDATE, DELETE, MERGE, and distributed Vane
Vortex scans/writes are not supported by this implementation. Vortex export and
append destinations still require local paths. Remote reads support `s3://`
paths in native DuckDB builds; other remote schemes remain unsupported.
The pinned reader also cannot read URL-escaped local paths. Spaces, `#`, `%`,
non-ASCII characters, and other characters requiring URL escaping are rejected
before writing Vortex data files or committing a snapshot. Relative data paths
are expanded to absolute paths, including validation of the working directory.
Parquet data paths retain their existing behavior.

## Physical row positions

Native scans expose `file_row_number` as a zero-based `BIGINT` position in the
original data file. Projection, filtering, scan batch boundaries and thread
count do not renumber rows. Each file starts at zero; use `filename` together
with the position to identify a physical row within a pinned Iceberg snapshot:

```sql
SELECT filename, file_row_number, id
FROM iceberg_scan('/tmp/vortex-table')
WHERE id BETWEEN 100 AND 200;
```

This also works for catalog tables, S3 reads, and snapshots containing both
Parquet and Vortex files. The adapter requests Vortex's original `row_idx()`
before filtering and converts its unsigned result to Iceberg's `BIGINT` with a
checked cast. Positions outside the signed range fail rather than wrap.

These are physical addresses, not primary keys or persistent logical row IDs.
Rewriting or compacting a file creates new addresses. `file_index` is a
query-local file-list ordinal and must not be persisted as a file identity.
A durable file-ID registry, index row maps and delete-file handling are separate
steps; exposing positions does not enable UPDATE or DELETE.

## Reading S3 data files

Load `httpfs` and configure an ordinary DuckDB S3 secret before querying a
snapshot containing Vortex files. The same secret resolution applies to its
metadata, Parquet data files and Vortex data files:

```sql
LOAD httpfs;
CREATE SECRET iceberg_storage (
    TYPE s3,
    KEY_ID 'your-key-id',
    SECRET 'your-secret',
    SESSION_TOKEN 'your-temporary-session-token',
    REGION 'us-east-1',
    SCOPE 's3://your-bucket/'
);
SELECT id FROM iceberg_scan('s3://your-bucket/table/metadata/v1.metadata.json')
WHERE id BETWEEN 100 AND 200;
```

Omit `SESSION_TOKEN` when using credentials that do not require one. S3-compatible
stores can use the usual DuckDB `ENDPOINT`, `URL_STYLE` and `USE_SSL` secret
options. No credentials are copied into Iceberg metadata or Vortex files.

The native reader opens exact manifest paths through DuckDB's client filesystem
and reads byte ranges as Vortex requests them. The adapter does not stage objects
to a local directory; httpfs retains its normal forced-download and fallback
settings. Each execution, including a prepared query, reopens with
the current secret and access policy. File handles are serialized per file;
separate files can read in parallel. In-flight blocking I/O follows httpfs
timeouts; interruption is checked before and after each range read. Short reads
and authentication errors fail the query. Files are expected to be immutable,
as required by Iceberg's data-file model.

The S3 regression uses an isolated loopback GET/HEAD server that verifies SigV4
signatures and session tokens. It checks scoped credentials, credential changes
after preparation, access policy, mixed snapshots, projection range reads,
interruption and truncated responses. It does not qualify a production catalog
or every S3-compatible service. Remote writes remain a separate phase.

## Schema and scan behavior

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

With Vortex enabled, binding inspects live data-file entries in the selected
snapshot, including transaction-local appends. Parquet-only scans retain
Parquet's row-filter, expression and dynamic JOIN filter pushdown. Each scan is
classified independently from its actual files, including reads of older
snapshots; the table's current write-format property does not determine scan
capabilities. Classification reuses the existing manifest read state and may
wait for all data manifests of a Parquet-only snapshot during binding.

For Vortex and mixed-format scans, DuckDB evaluates the complete row predicate.
The adapter additionally copies static INTEGER/BIGINT comparisons and scalar
`IS NULL` / `IS NOT NULL` checks from metadata pruning into the Vortex reader,
reducing the rows exported before DuckDB's final check. Existing conjunctions
and disjunctions are copied only when every child is supported, including
DuckDB's optional wrapper around a complete same-column OR clause. Column
mapping uses Iceberg field IDs and the actual projected scan positions.
Floating comparisons, arbitrary expressions and dynamic JOIN filters remain in
DuckDB: the pinned Vortex reader distinguishes positive and negative zero.
Iceberg metadata pruning remains enabled, and default builds retain the
original Parquet scan path.

Iceberg min/max bounds exclude NaNs. Metadata pruning preserves possible NaN
matches when NaN statistics are absent and uses known zero counts to retain
safe upper-bound pruning. This applies to both build modes and to data-file
and partition summaries, separately from reader-level filter pushdown.

Vortex metrics are collected from the converted writer input in COPY and INSERT
and stored by Iceberg field ID. Bounds exclude NULLs and NaNs, preserve infinities,
and order negative zero before positive zero. Files containing only NULLs or NaNs
have no bounds. Existing files without metrics remain readable and are retained
conservatively during pruning; newly collected metrics do not rewrite old files.
The existing comparison pruner also retains files with no bounds. Direct
`IS NULL` and `IS NOT NULL` checks on scalar columns can prune files using counts
even without bounds: `IS NULL` skips files with a known zero NULL count, and
`IS NOT NULL` skips files whose known total and NULL counts are equal. Missing
counts remain unknown. Vortex/mixed scans retain the original row filters in
DuckDB; derived expressions, nested fields and NULL checks inside OR conditions
are not extracted by this metadata-only NULL optimization.

The table properties `write.metadata.metrics.default` and
`write.metadata.metrics.column.<name>` control collection. `none` disables metrics
for the column; `counts` collects total/NULL/NaN counts without bounds; `full` and
`truncate(n)` collect numeric bounds. Collection is scoped to each output file,
including parallel writes. The common manifest writer preserves exact NaN counts
when present; it leaves unknown counts unspecified, including for Parquet writers
that report only whether a NaN exists.
The shared writer also persists total counts supplied for top-level scalar
Parquet columns, in both build modes. It does not newly collect counts for nested
or repeated Parquet leaves. Existing counts survive manifest rewrites.
Both builds include the default unsorted order in COPY exports, so a subsequent
catalog append can resolve the metadata's default sort-order ID.

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
validation. ON additionally runs all Vortex SQL tests and the local catalog,
file-access, schema-change and file-metrics Python regressions; OFF checks that
requesting Vortex fails. Tests run serially within each isolated runner. No Docker
or external catalog is needed.
Both modes also run the NaN metadata, partition-summary and NULL-pruning
regressions, verify Parquet FLOAT/DOUBLE predicates and JOIN results, and check
scan-level filtering using JSON execution profiles. ON extends those checks to mixed snapshots,
old snapshots, prepared queries and transaction-local appends. The SQL suite
also checks NULL-filter results and the files skipped by metadata pruning using
the committed fixtures, without requiring generated data.

The workflow loads the static Parquet extension before Iceberg in both modes
and builds `httpfs` from the revision pinned by DuckDB for the Parquet tests.
The binaries use an isolated extension directory so preinstalled extensions
cannot mask missing dependencies or an incorrect load order.
It checks the CMake option and build graph, rejects fetched Vortex/Rust
dependencies and Vortex symbols in OFF builds, and verifies statically linked
extensions. SQL files are selected by their exact absolute registered names,
avoiding duplicate relative registrations from `UNITTEST_ROOT_DIRECTORY`.
Each SQL file must finish with an all-passed console summary and at least one
successful assertion. DuckDB's XML reporter omits mid-test skips, so the runner
uses console totals and exact case names from duration output instead. Default
HTTP/connection error skips are disabled; an unmet requirement also fails CI,
even after successful assertions. Both builds exercise these checks with real
sqllogictests. Compiler, vcpkg and Rust build caches are separate from the build
configuration. Build logs and per-test reports/logs are uploaded for both modes.

To repeat the CI tests against one of these native Ninja builds:

```sh
python3 scripts/ci/run_vortex_tests.py --mode ON --build-dir build/vortex-ci
# Use --mode OFF for the default build, in a separate build directory.
```

To compare Parquet scan performance across native builds on identical data:

```sh
python3 scripts/benchmark_vortex_pushdown.py \
  --duckdb OFF=build/off/duckdb --duckdb ON=build/on/duckdb \
  --directory build/pushdown-benchmark --output build/pushdown-benchmark/results.json
```

The benchmark validates every result, warms the OS file cache, and records median
JSON-profile latency, binding time and scan output rows for a selective range,
dynamic JOIN and full scan. Detailed profiling includes binding and optimization
in the total latency, including file-format classification; shell startup is
excluded. It uses one thread, two million rows and seven measured runs per query
by default. Performance timings are informational; CI asserts filtering behavior
and results without a timing threshold.

To measure Vortex file pruning and the cost of collecting metrics, compare two
ON builds, one before and one after this change:

```sh
python3 scripts/benchmark_vortex_metrics.py \
  --duckdb BEFORE=build/before/duckdb --duckdb AFTER=build/after/duckdb \
  --directory build/metrics-benchmark --output build/metrics-benchmark/results.json
```

This benchmark writes identical two-million-row datasets with each binary,
validates their contents, and measures COPY latency. It then measures integer
and floating range queries and a full scan over eight files, checking results
and reporting skipped files. Defaults are one thread, a warm-up and seven
measured runs; latency comes from detailed JSON profiles and excludes shell
startup. Query measurements alternate the build order on each repetition and
include individual samples in the JSON report. Use a fresh output directory for
each comparison.

Add `--null-files 4` to use four all-NULL and four non-NULL files and measure
`IS NULL` / `IS NOT NULL` pruning instead of ranges. Total row counts, non-NULL
counts and query results are checked in both binaries.

Individual regressions can also be run directly:

```sh
ICEBERG_TEST_VORTEX=1 ./build/release/test/unittest '*test/sql/local/vortex/*'
python3 test/standalone/test_nan_pruning.py --duckdb build/release/duckdb
python3 test/vortex/test_parquet_pushdown.py --mode ON --duckdb build/release/duckdb
python3 test/vortex/test_null_pruning.py --mode ON --duckdb build/release/duckdb
python3 test/vortex/test_local_catalog.py --duckdb build/release/duckdb
python3 test/vortex/test_file_access.py --duckdb build/release/duckdb
python3 test/vortex/test_schema_changes.py --duckdb build/release/duckdb
python3 test/vortex/test_file_metrics.py --duckdb build/release/duckdb
python3 test/vortex/test_format_benchmark.py --duckdb build/release/duckdb
python3 test/vortex/test_s3_reads.py --duckdb build/release/duckdb
python3 test/vortex/test_static_filter_hints.py --duckdb build/release/duckdb
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

The file-metrics test checks serialized bounds and counts, including integer
limits, subnormal floats, infinities, signed zero, NaNs and all-NULL files.
It covers metrics modes and column overrides, nonconsecutive field IDs, quoted
column names, filtered/constant vectors, multiple batches and parallel output
files. Mixed snapshots retain correct results and old snapshots remain readable;
comparison queries also assert how many files metadata pruning skips.

The NULL-pruning test checks direct and catalog reads, mixed formats, old
snapshots, column aliases, conjunctions, retained row filtering and prepared
queries before and after transaction-local appends. It also checks `counts` and
`none` modes, all-NULL files without bounds, and legacy manifests missing either
total counts or NULL counts. Parquet nested fields and derived NULL expressions
retain their results. Use `--mode OFF` for its Parquet-only coverage.

## Comparing Parquet and Vortex

Use one ON build to compare both formats with identical data, schema, input
order and data-file count. This Linux benchmark requires GNU `time`:

```sh
python3 scripts/benchmark_vortex_formats.py \
  --duckdb build/on/duckdb --directory build/formats-sorted \
  --output build/formats-sorted/results.json
python3 scripts/benchmark_vortex_formats.py \
  --duckdb build/on/duckdb --directory build/formats-permuted --order permuted \
  --output build/formats-permuted/results.json
```

Defaults are eight files of 250,000 rows, one query thread, a 1 GB DuckDB memory
limit, one warmup and five measured repetitions. Writes use one thread to keep
file counts equal. Each format uses its default compression. `--order permuted`
uses the same deterministic permutation for both formats, reducing the benefit
of sorted file bounds. Use a fresh directory for each run; existing data is
never overwritten. The final tables and JSON report are retained.

Every measured write is validated against a query over the original data.
Projection, range, equality, NULL, grouped aggregate, JOIN and full-scan queries
are also checked against that reference. Format order alternates between
repetitions. Query profiles exclude shell startup; write latency sums the
COPY/INSERT profiles and includes source generation. The append catalog is the
local REST test fixture, so these are not production catalog or S3 measurements.

The report includes individual samples, medians, compressed data/metadata sizes,
query latency and complete-process peak RSS, including Rust allocations. The
DuckDB memory limit governs its buffer manager, not all Vortex memory. Queries
use fresh processes and a warm OS cache; the benchmark does not evict shared
caches. Linux physical read bytes can therefore be zero. DuckDB's reported
read-byte and buffer-memory counters are included separately and do not cover
all Vortex I/O or allocations; they must not be interpreted as comparable
totals. CI runs only a small correctness smoke test, without timing thresholds.

To compare a filter change against a saved binary using the same retained data:

```sh
python3 scripts/profile_vortex_filters.py \
  --before build/baseline/duckdb --after build/on/duckdb \
  --data-directory build/formats-sorted --output-directory build/filter-profile
```

This runs the seven query classes with one thread, one warmup and five measured
repetitions. Variant order rotates between the two Vortex binaries and Parquet
in the new binary. Every result must agree. The output preserves detailed JSON
profiles, individual samples, binding time, scan output rows, medians and binary
checksums. `--queries selective_range null_filter` narrows the workload; `--rows`
must match the retained benchmark data. DuckDB's `cpu_time` is an operator timing
metric rather than whole-process CPU usage, and it must not be added to binding
time to reconstruct total latency.
