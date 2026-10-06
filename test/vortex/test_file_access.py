"""Check Iceberg data-file access restrictions with ICEBERG_ENABLE_VORTEX=ON.

Run with an enabled shell:
    python3 test/vortex/test_file_access.py --duckdb build/vortex/duckdb
"""

import argparse
from pathlib import Path
import tempfile

from test_local_catalog import local_catalog, quote, run_sql


def restrictions(metadata, directories=(), files=()):
    allowed_directories = [metadata.parent, *directories]
    return (
        f"SET allowed_directories=[{', '.join(quote(path) for path in allowed_directories)}]; "
        f"SET allowed_paths=[{', '.join(quote(path) for path in files)}]; "
        "SET enable_external_access=false; "
    )


def check_file_access(shell, root, fmt):
    table = root / fmt
    run_sql(
        shell,
        f"COPY (SELECT 42::BIGINT id, 'private' payload) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {fmt});",
    )
    metadata = next((table / "metadata").glob("*.metadata.json"))
    data_file = next((table / "data").glob(f"*.{fmt}"))
    query = f"SELECT id, payload FROM iceberg_scan({quote(metadata)});"
    expected = [{"id": 42, "payload": "private"}]
    denied = restrictions(metadata)

    # Metadata permission alone must never authorize a data-file read.
    run_sql(shell, denied + query, error="Permission Error")
    run_sql(shell, restrictions(metadata, files=[str(data_file) + ".other"]) + query, error="Permission Error")

    for allowed in (restrictions(metadata, directories=[table]), restrictions(metadata, files=[data_file])):
        assert run_sql(shell, allowed + query) == expected
        assert run_sql(shell, f"PREPARE permitted AS {query}" + allowed + "EXECUTE permitted;") == expected

    # Cached metadata and a previously bound reader must not retain access after revocation.
    run_sql(shell, query + denied + query, error="Permission Error")
    run_sql(shell, f"PREPARE restricted AS {query}" + denied + "EXECUTE restricted;", error="Permission Error")


def check_mixed_file_access(shell, root):
    table = root / "mixed"
    run_sql(shell, f"COPY (SELECT 1::BIGINT id) TO {quote(table)} (FORMAT iceberg);")
    with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (2);")
        metadata = server.metadata_path
        assert len(server.commits) == 1
        assert not server.errors, server.errors

    files = sorted((table / "data").iterdir())
    assert len(files) == 2
    assert {path.suffix for path in files} == {".parquet", ".vortex"}
    query = f"SELECT id FROM iceberg_scan({quote(metadata)}) ORDER BY id;"
    for permitted_file in files:
        # Check each file, even when another file in the snapshot is explicitly permitted.
        run_sql(shell, restrictions(metadata, files=[permitted_file]) + query, error="Permission Error")
    assert run_sql(shell, restrictions(metadata, files=files) + query) == [{"id": 1}, {"id": 2}]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    args = parser.parse_args()
    shell = args.duckdb.resolve()
    with tempfile.TemporaryDirectory(prefix="iceberg-vortex-access-") as directory:
        root = Path(directory)
        for fmt in ("parquet", "vortex"):
            check_file_access(shell, root, fmt)
        check_mixed_file_access(shell, root)
    print("Iceberg Parquet, Vortex and mixed-file access regressions passed.")


if __name__ == "__main__":
    main()
