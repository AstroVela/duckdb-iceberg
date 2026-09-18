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
