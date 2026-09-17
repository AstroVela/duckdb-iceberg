# Apache Iceberg for Vane and DuckDB

This repository integrates [Apache Iceberg](https://iceberg.apache.org/) with
**Vane** and **DuckDB**. It supports reading Iceberg tables, inspecting their
metadata, and using Iceberg REST catalogs for table creation and mutations.
The Vane build adds distributed scans and writes through Ray.

The extension is experimental; support depends on the runtime, Iceberg format
version, and catalog. Choose the guide for the runtime you are using:

| Runtime | Guide | Execution and installation |
| --- | --- | --- |
| Vane | [VANE_README.md](VANE_README.md) | SQL and Relation APIs with the default Ray runner; matching Vane, Avro, and Iceberg provider wheels |
| DuckDB | [DUCKDB_README.md](DUCKDB_README.md) | Native DuckDB extension, source builds, and catalog-backed development tests |

Vane and DuckDB use separate build paths and compatible artifacts must be
selected for each runtime. The default Make targets use the upstream DuckDB
submodule; Vane targets use the exact runtime pinned in
[vane-extension.toml](vane-extension.toml).

## Start here

- [Install Iceberg for Vane](VANE_README.md#installation), then follow the
  [CTAS, insert, update/delete, merge, and query walkthrough](VANE_README.md#create-modify-and-query-a-table).
- [Use the Vane Relation API](VANE_README.md#use-the-relation-api) for filtering,
  projection, aggregation, and supported distributed writes.
- [Read an existing table without a catalog](VANE_README.md#read-an-existing-iceberg-table)
  from an explicit Iceberg metadata file.
- [Build the DuckDB extension](DUCKDB_README.md#building-the-extension) and use
  the [DuckDB Iceberg documentation](https://duckdb.org/docs/extensions/iceberg)
  for its SQL interface.

## Capabilities

The extension reads Iceberg data and metadata, applies snapshot delete state,
and supports catalog-backed writes. Vane's distributed integration includes:

- File scans with projection and filter pruning, including Iceberg delete files.
- CTAS and INSERT with worker-written data files and catalog finalization.
- UPDATE, DELETE, and MERGE through the Relation API.
- Iceberg v3 deletion vectors and row lineage, with version-specific type support.

Distributed CTAS needs staged catalog creation and an explicit shared data
path. Connection initialization and catalog/schema setup still use the native
connection API. See [Vane capabilities and limits](VANE_README.md#distributed-capabilities-and-limits)
for execution boundaries and supported cases.

## Development

- [DuckDB development and tests](DUCKDB_README.md#developer-guide)
- [Vane build and test integration](VANE_README.md#build-and-test-the-vane-integration)
- [Local REST catalogs](DUCKDB_README.md#local-catalog-setup)
- [Test data generation](scripts/data_generators/README.md)
- [Vane provider releases](docs/VANE_RELEASE.md)

## Acknowledgements

This extension was initially developed as part of a customer project for
[RelationalAI](https://relational.ai/), who agreed to open source it. We thank
RelationalAI for supporting open source development and enabling the extension
to be shared with the community.
