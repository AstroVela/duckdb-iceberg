[Overview](README.md) | [Vane guide](VANE_README.md)

> **Disclaimer:** This extension is currently in an experimental state. Feel free to try it out, but be aware that things may not work as expected

# Apache Iceberg for DuckDB

This repository contains a DuckDB extension for [Apache Iceberg](https://iceberg.apache.org/). It supports reading and writing Iceberg tables, inspecting snapshots and manifests, and attaching Iceberg REST catalogs.

## Documentation

See the [Iceberg page in the DuckDB documentation](https://duckdb.org/docs/extensions/iceberg).

## Developer guide

### Dependencies

Building requires a C++ toolchain, CMake, and
[vcpkg](https://vcpkg.io/en/getting-started.html). The repository's
[vcpkg manifest](vcpkg.json) pins the native dependencies, including `avro-c`,
and its custom ports. The separate Avro extension is pinned in
[extension_config.cmake](extension_config.cmake).

### Test data generation

The generators in [scripts/data_generators/](scripts/data_generators/README.md)
use PySpark and the pinned packages in [scripts/requirements.txt](scripts/requirements.txt).
The catalog data targets create `.venv-spark4` and install these requirements.

### Building the extension

Clone this branch with its submodules:

```shell
git clone --branch v1.5-variegata_vane --recurse-submodules \
  https://github.com/AstroVela/duckdb-iceberg.git
cd duckdb-iceberg
```

To build the extension with vcpkg:

```shell
VCPKG_TOOLCHAIN_PATH='<path_to_your_vcpkg_repo>/scripts/buildsystems/vcpkg.cmake' make
```

This produces a DuckDB shell with the extension linked in and a separate
loadable extension artifact:

```text
build/release/duckdb
build/release/extension/iceberg/iceberg.duckdb_extension
```

Start the shell with `./build/release/duckdb`. A loadable `.duckdb_extension`
file is loaded by a compatible DuckDB runtime; it is not a shell executable.

### Vane build

For Vane installation, distributed execution, and provider wheels, see
[VANE_README.md](VANE_README.md). The default build described here targets the
upstream `duckdb/` submodule.

### Running tests

#### Generating test data

Generate data for REST-catalog tests or local file scans with the corresponding
target:

```shell
make fixture-data
# Or, for local file-based tests:
make fixture-data-local
```

These targets start the fixture, install the pinned Python requirements, and
generate data. They require Docker Compose and a Java runtime compatible with
the pinned PySpark version. See the [data generator guide](scripts/data_generators/README.md)
for individual cases and catalog profiles.

#### Running unit tests

Build the matching configuration before testing:

```shell
make release
make test
```

For a focused test that uses a fixture already committed to the repository:

```shell
./build/release/test/unittest \
  test/sql/local/iceberg_scans/iceberg_v1_existing_manifest_entry.test
```

Run catalog-backed tests serially unless their catalogs, tables, storage paths,
and services have been verified to be independent.

#### Running the local S3 test server

Running the S3 test cases requires the minio test server to be running and populated with `scripts/upload_iceberg_to_s3_test_server.sh`.
Note that this requires the relevant test data to have been generated first and also to have the aws cli and docker compose installed.

### Local catalog setup

The Makefile provides targets to spin up local Iceberg catalogs for development and testing. Each target clones the catalog repo (if needed) and starts the service:

```shell
make fixture      # Apache Iceberg REST Fixture (Docker)
make nessie       # Nessie catalog (Docker)
make lakekeeper   # Lakekeeper catalog (Docker)
make polaris      # Apache Polaris MinIO quickstart (Docker)
```

For starting the service AND generating data (to run tests that need it):

```shell
make fixture-data
make nessie-data
make lakekeeper-data
make polaris-data
```

Should you need to generate data for only one test (a test found under *scripts/data_generators/tests*), you can pass the test name as an argument, like so: `TEST=all_types_table make fixture-data`. The script will now only generate the needed data for that single test, which is faster.

All four service targets require Docker and Docker Compose. Polaris uses the
`release/1.4.x` branch's MinIO quickstart. Data generation additionally requires
Python and a Java runtime compatible with the pinned PySpark version.

Starting a catalog stops the active catalog recorded in
`.catalogs/.active_catalog`. Start and data-generation targets can remove
existing fixture data; use them in a dedicated development environment.

Fixture also has a local variant that generates data for local file-based testing instead of REST:

```shell
make fixture-data-local
```

## Acknowledgements

This extension was initially developed as part of a customer project for [RelationalAI](https://relational.ai/),
who have agreed to open source the extension. We would like to thank RelationalAI for their support
and their commitment to open source enabling us to share this extension with the community.
