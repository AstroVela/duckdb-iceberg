# Apache Iceberg for Vane

[Overview](README.md) | [DuckDB guide](DUCKDB_README.md)

Use Apache Iceberg tables from Vane's SQL and Relation APIs, with Ray executing
supported scans and writes. The Vane build adds distributed execution to this
extension; its artifacts must match the Vane runtime that loads them.

## Installation

### Install the provider wheels

Install a matching release of `vane-ai`, `vane-extension-avro`, and
`vane-extension-iceberg`. Iceberg declares an exact dependency on its Avro
provider, and both providers require the same exact Vane version. Install this
complete package set in the application's environment and on every Ray node.

The branch's provider release lane targets Linux x86-64, with CPython 3.10
through 3.14 wheels. Python 3.12 is a suitable starting point. The current
release configuration uses `vane-ai==0.2.0.dev657`; select the corresponding
Avro and Iceberg versions from the same qualified release. Provider version
numbers include an artifact identity and are different from the Vane version.
See [provider releases](docs/VANE_RELEASE.md) for the version and dependency
contract. The examples below describe this branch; older provider artifacts
may not include all of its write capabilities.

For a TestPyPI release, replace the two provider-version placeholders below.
Use a fresh wheel directory for each package set:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip

VANE_VERSION='0.2.0.dev657'
AVRO_VERSION='<matching-avro-provider-version>'
ICEBERG_VERSION='<matching-iceberg-provider-version>'

python -m pip download --no-deps --only-binary=:all: \
  --index-url https://test.pypi.org/simple/ --dest iceberg-wheels \
  "vane-ai==$VANE_VERSION" \
  "vane-extension-avro==$AVRO_VERSION" \
  "vane-extension-iceberg==$ICEBERG_VERSION"
python -m pip install --index-url https://pypi.org/simple/ \
  iceberg-wheels/*.whl grpcio
python -m pip check
```

This downloads the exact runtime and providers from TestPyPI, then resolves
ordinary Python dependencies from PyPI. Initialize the installed provider in
Python:

```python
import vane
from vane import col, lit

connection = vane.connect()
vane.load_installed_extension("iceberg", connection=connection)
```

The loader also loads the exact installed Avro dependency. It does not install
packages. The base Vane wheel supplies the Parquet and HTTP/S3 support used by
these examples.

### Ray execution

Leave the runner unset to use Vane's default Ray runner. The walkthrough uses
`.show()` to display results; use `.fetchall()` when application code needs
Python rows. A query or write needs enough independent work to occupy multiple
workers, so a small example can execute on fewer workers than the cluster has.

To connect to an existing Ray cluster, configure it before the first relation:

```python
vane.set_runner_ray(address="auto")
```

The connection setup below prepares the calling process's native connection.
`ATTACH`, `SET`, and schema DDL passed to `execute()` currently do not dispatch
through Ray. In steps 1–5, SELECT relations and the supported Relation write
methods use Ray. In the pinned runtime, `execute()` and `sql()` also dispatch
supported SELECT, CTAS, INSERT, UPDATE, DELETE, and MERGE statements through
Ray, including parameterized SQL. The walkthrough uses Relation methods so
queries and writes can be composed with the same API.

## Create, modify, and query a table

The following steps run in order on the same connection. They require an
Iceberg REST catalog that supports staged table creation and an S3 location
readable and writable by the application, Ray coordinator, and workers.

For a catalog-free read example using committed repository data, jump to
[Read an existing Iceberg table](#read-an-existing-iceberg-table).

### Prepare the catalog connection

Replace the catalog, credential, and bucket placeholders with your own values.
The following storage settings describe AWS S3. Temporary credentials also
require `s3_session_token`. For S3-compatible services, configure their endpoint,
URL style, and TLS settings for your deployment; the repository's
[Ray integration setup](test/vane/test_vane_wheel_ray_iceberg.py) shows the
corresponding local fixture configuration.

```python
connection.execute("""
    SET s3_region = 'us-east-1';
    SET s3_access_key_id = 'your-access-key';
    SET s3_secret_access_key = 'your-secret-key';
""")

connection.execute("""
    ATTACH '' AS lake (
        TYPE ICEBERG,
        ENDPOINT 'https://catalog.example.com',
        CLIENT_ID 'your-client-id',
        CLIENT_SECRET 'your-client-secret',
        stage_create_tables true
    )
""")
connection.execute("CREATE SCHEMA IF NOT EXISTS lake.demo")
```

`ATTACH` registers the REST catalog under the name `lake`, so
`lake.demo.events` identifies a table in its `demo` namespace. This is catalog
registration; it does not copy table data. Catalog authentication and S3 access
are separate. Vane carries supported explicit connection settings in its query
snapshot; a DuckDB `CREATE SECRET` object alone is not transported to workers.

For a development catalog, see [Local catalog setup](DUCKDB_README.md#local-catalog-setup)
and the existing [catalog configurations](test/configs/).
These service targets change the active local catalog and can replace test
data. The table walkthrough itself does not need Spark or generated test data.

### 1. Create a table with CTAS

CTAS means **CREATE TABLE AS SELECT**: the query supplies both the schema and
initial rows. Vane's `.create()` method expresses CTAS through the Relation API.
Replace the S3 path before running this block:

```python
connection.sql("""
    SELECT
        i::BIGINT AS id,
        ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000) AS source(i)
""").create(
    "lake.demo.events",
    properties={
        "format-version": 2,
        "write.data.path": "s3://your-bucket/vane-iceberg-demo/events/data",
    },
    partition_by=["bucket(8, id)"],
)
```

This creates an Iceberg v2 table containing IDs 0–999. Workers write its data
files, and the coordinator finalizes the staged table through the Iceberg
catalog transaction.

Distributed CTAS requires `stage_create_tables true` on the catalog and an
explicit `location` or `write.data.path` table property. The path must be
accessible to all workers. Use a new table name and data path when repeating
the walkthrough; distributed CTAS does not implement `IF NOT EXISTS` or
`OR REPLACE`.

| Operation | Purpose | Execution in this guide |
| --- | --- | --- |
| Schema-only `CREATE TABLE` | Define columns without an input query | Native statement path |
| `relation.create(...)` | Create a table with the query's schema and data | Ray CTAS |
| `relation.insert_into(...)` | Append query rows to an existing table | Ray INSERT |

For Iceberg v3 CTAS, use `"format-version": 3` with a new table name and path.
The distributed writer also assigns the initial v3 row-lineage metadata.

### 2. Insert rows

```python
connection.sql("""
    SELECT
        i::BIGINT AS id,
        ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000, 1010) AS source(i)
""").insert_into("lake.demo.events")
```

The table now contains 1,010 rows. `insert_into()` appends to the table created
in step 1 and uses its existing schema and partition specification.

### 3. Update and delete rows

```python
connection.table("lake.demo.events").update(
    {"payload": lit("updated")},
    condition=col("id") < 5,
)
connection.table("lake.demo.events").delete(
    condition=col("id") >= 1005,
)
```

The update changes IDs 0–4. The delete removes IDs 1005–1009, leaving 1,005
rows. Both operations use the distributed write provider and commit through
the Iceberg catalog.

### 4. Merge changes

`merge_into()` matches source rows against a target and applies ordered SQL
`WHEN` clauses. This example updates ID 0 and inserts ID 1010:

```python
connection.sql("""
    SELECT * FROM (
        VALUES
            (0::BIGINT, 'merged-0'),
            (1010::BIGINT, 'new-1010')
    ) AS changes(id, payload)
""").merge_into(
    "lake.demo.events",
    "target.id = source.id",
    [
        "WHEN MATCHED THEN UPDATE SET payload = source.payload",
        "WHEN NOT MATCHED THEN INSERT (id, payload) "
        "VALUES (source.id, source.payload)",
    ],
)
```

`target` and `source` are the method's default SQL aliases. The table now has
1,006 rows. Keep source matches unambiguous: multiple source rows modifying
the same target row cause the distributed merge to fail.

### 5. Query the results

Use `sql()` to construct a SELECT relation and `.show()` to execute it:

```python
connection.sql("""
    SELECT
        count(*)::BIGINT AS rows,
        sum(id)::BIGINT AS id_sum,
        max(id) AS max_id
    FROM lake.demo.events
""").show()
# rows = 1006, id_sum = 505520, max_id = 1010

connection.sql("""
    SELECT id, payload
    FROM lake.demo.events
    WHERE id < 5
""").show()
# ID 0 has payload 'merged-0'; IDs 1–4 have payload 'updated'.
```

Row order is unspecified without an explicit ordering. The aggregate casts its
sum to `BIGINT` so the result uses a standard Arrow integer type.

#### Use the Relation API

```python
events = connection.table("lake.demo.events")
filtered = events.filter(col("id") >= 100).select(col("id"), col("payload"))
filtered.limit(5).show()

filtered.aggregate(
    "count(*) AS rows, sum(id)::BIGINT AS id_sum"
).show()
# rows = 906, id_sum = 500570
```

The limited preview is a separate relation; `filtered` still represents all
906 matching rows. Grouping uses the `group_expr` argument:

```python
(
    events.select((col("id") % 2).alias("bucket"), col("id"))
    .aggregate(
        "bucket, count(*) AS rows, sum(id)::BIGINT AS id_sum",
        group_expr="bucket",
    )
    .show()
)
```

Filtered relations can also be written with `.create()` or `.insert_into()`,
using the same requirements as steps 1 and 2.

## Read an existing Iceberg table

An explicit metadata-file path can be scanned without a REST catalog or
`ATTACH`. After installing and loading the provider, run this example from the
repository root against a committed test fixture:

```python
connection.sql("""
    SELECT id, league, ats_qty
    FROM iceberg_scan(
        'data/persistent/iceberg_v1_repro/repro/merch_v1/metadata/'
        '00003-8d01e4aa-d143-49c9-898e-b5e477577b70.metadata.json'
    )
""").show()
```

It returns four rows with IDs 2, 3, 4, and 6, in unspecified order. The same
fixture is covered by the repository's
[native scan test](test/sql/local/iceberg_scans/iceberg_v1_existing_manifest_entry.test).
An explicit metadata filename avoids version guessing. On a Ray cluster with
multiple hosts, make the metadata, manifests, and referenced data files
available at the paths each node resolves; local files are not uploaded
implicitly.

The extension also executes `iceberg_snapshots()` and `iceberg_metadata()`
through a single Ray scan task. Binding resolves the catalog entry or version
hint and captures an immutable metadata-file path; `iceberg_metadata()` also
pins the selected snapshot and schema. Workers read that file and its manifests
with their own filesystem session. A later catalog commit or version-hint
change does not change an already bound query. See the
[metadata Ray tests](test/vane/test_vane_wheel_ray_metadata.py) and the
[DuckDB Iceberg documentation](https://duckdb.org/docs/extensions/iceberg)
for examples and SQL syntax.

## Distributed capabilities and limits

| Capability | Iceberg v2 | Iceberg v3 |
| --- | --- | --- |
| Data scans | Positional and equality deletes | Puffin deletion vectors |
| Snapshot and manifest inspection | One Ray scan task | One Ray scan task |
| INSERT | Append to an existing table | Append with row-ID and sequence-number assignment |
| CTAS | Explicit worker data path and staged catalog creation | Same requirements, with initial row lineage |
| DELETE | Positional-delete files | Consolidated Puffin deletion vectors |
| UPDATE | Delete files and replacement data files | Deletion vectors and row-lineage-preserving replacements |
| MERGE | Distributed insert, update, and delete actions | Version-aware artifacts and row lineage |
| VARIANT and TIMESTAMP_NS | Not Iceberg v2 types | Reads and appends supported |

Distributed writes require auto-commit mode. Unsupported plans fail instead of
falling back to native execution. In particular:

- CTAS requires the path and staging options described in step 1.
- Writes reject a current partition spec containing a `VOID` transform.
- Row-delta operations reject selected source files with a partition spec that
  differs from the current default.
- UPDATE and MERGE reject `VARIANT` values that would cross Vane's repartition
  transport. V3 reads and appends remain supported; legacy Parquet VARIANT
  decoding is rejected.
- V3 plans validate their frozen snapshot, schema, and partition state before
  finalization. A conflicting change fails the operation.
- Distributed scans reject Parquet encryption keys and explicit cardinality
  overrides that cannot be transported with their required semantics.

Workers consume the coordinator's selected files, delete state, and required
schema and partition metadata. They produce immutable data/delete artifacts;
the coordinator validates selected results and finalizes them in one
non-retried call through the ordinary Iceberg catalog transaction. Iceberg is
the transaction authority. If a catalog response is lost after finalization
starts, the commit outcome can be unknown and selected files are retained;
catalog garbage collection handles true orphans.

The [two-worker Ray integration lane](test/vane/test_vane_wheel_ray_iceberg.py)
covers scans, CTAS, INSERT, UPDATE, DELETE, MERGE, partitioning, schema evolution,
v3 row lineage, and failure cases. Geometry is not qualified by that lane.
The dynamic-wheel and indexed-provider lanes also cover parameterized
`execute()` and `sql()` SELECT and writes for v2/v3, checking committed rows,
snapshot changes, and preservation of the source snapshot.

## Build and test the Vane integration

The default Make targets build against the upstream `duckdb/` submodule.
Vane-specific targets use the exact Vane revision in
[vane-extension.toml](vane-extension.toml) and its `external/duckdb` tree.
[extension_config_vane.cmake](extension_config_vane.cmake) enables
`ICEBERG_VANE_DISTRIBUTED` for that build.

```bash
git clone --branch v1.5-variegata_vane --recurse-submodules \
  https://github.com/AstroVela/duckdb-iceberg.git
cd duckdb-iceberg
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make vane_validate
make vane_ci VANE_BUILD_JOBS=8
```

`make vane_ci` builds the extension against Vane's native engine and runs the
manifest's selected native test. It does not run the full Python/Ray integration
lane or install a provider wheel. Provider packaging, its Avro dependency, and
release validation are documented in [Vane provider releases](docs/VANE_RELEASE.md)
and implemented by the [Vane workflow](.github/workflows/VaneExtension.yml).
Use non-editable installations for Python/Ray testing.

The Ray integration test requires the packaged runtime/providers and the
configured REST catalog and object store. Run catalog-backed tests serially
unless their resources are known to be independent. Standard DuckDB build,
formatting, catalog lifecycle, and generator commands are in the
[DuckDB guide](DUCKDB_README.md).
