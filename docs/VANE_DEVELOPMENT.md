# Develop Iceberg against Vane 0.2.0

The normal development loop is **edit Iceberg → open a PR → inspect build-only
CI**. It does not require a new Vane publication, production signing credentials,
or a PyPI/TestPyPI upload for every change.

Here `0.2.0` is the Vane runtime version. Avro and Iceberg provider versions are
generated from their artifacts and have exact dependencies on that runtime and
each other. For application usage, see the [Vane guide](../VANE_README.md).

## 1. Keep the runtime baseline fixed

`vane-extension.toml` pins Vane `v0.2.0` at
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`. Leave that pin unchanged when
working on an Iceberg-only PR. Updating Vane itself is a separate baseline change.

The existing `VaneExtension.yml` runs on pushes and PRs. Its build-only path
builds a test runtime from the pinned source with
`VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY=ON`, builds Avro and Iceberg, signs them
with Vane's public test fixture key, and verifies the three wheels. The local
and two-worker Ray jobs install that exact set and run the integration suites.
The workflow uploads the set as `vane-iceberg-dynamic-wheels`.

This test runtime can report `0.2.0` while differing from the PyPI wheel in its
trusted keys. Always install the runtime and providers from the same CI build
into a dedicated environment. Do not upload these test artifacts to a package
index or replace the runtime with the PyPI wheel.

## 2. Develop and check each PR

Create an Iceberg branch, change the code and relevant tests, and open a PR.
No signing secret or release dispatch is needed. Inspect the existing **Vane
extension** workflow, including its dynamic-wheel build and two-worker Ray
integration job. A green build-only run qualifies the test package set; it
does not constitute production release qualification.

The comprehensive Ray suite covers scans, worker topology, SQL entrypoints,
CTAS, INSERT, UPDATE, DELETE, MERGE, schema evolution and write conflicts.
The metadata suite separately exercises metadata/snapshot queries and catalog
history. Both use real Ray clusters and a local REST catalog/MinIO fixture.

## 3. Reproduce a CI result locally when needed

Check out the same Iceberg commit as the selected CI run. Use Linux x86-64,
CPython 3.12 and a runtime compatible with the downloaded wheel tags; the
build-only jobs currently use Ubuntu 24.04. Use a clean test account or
container if the host already has DuckDB extensions installed. A Python venv
does not isolate `~/.duckdb`: these tests intentionally require provider-backed
loading, and existing extension files or writable cache ancestors can fail
their checks. Do not remove another environment's installed extensions.

Download the artifact from the PR's run, not from an unrelated branch:

```bash
gh run list -R AstroVela/duckdb-iceberg --workflow VaneExtension.yml
# Replace the value with the run for your PR commit.
RUN_ID=123456789
WHEELS_DIR=$(mktemp -d /tmp/iceberg-pr-wheels-XXXXXX)
gh run download "$RUN_ID" -R AstroVela/duckdb-iceberg \
  --name vane-iceberg-dynamic-wheels --dir "$WHEELS_DIR"

python3.12 -m venv .venv-vane-ray
source .venv-vane-ray/bin/activate
python -m pip install "$WHEELS_DIR"/*.whl
python -m pip check
```

Start the repository fixture on an otherwise unused test host. Its services
use ports 8181, 9000 and 9001 and fixed container names; do not run competing
catalog tests in parallel:

```bash
mkdir -p data/generated/iceberg/spark-rest data/generated/intermediates
docker compose -f scripts/docker-compose.yml up -d
docker compose -f scripts/docker-compose.yml logs mc
```

Wait for `MinIO warehouse and default namespace initialized.` in the `mc`
logs, then run the suites serially:

```bash
export VANE_RUNNER=ray
export VANE_EXPECTED_EXTENSION_TRUST_IDENTITY=vane-ci-test-key
export VANE_UDF_TARGET_MAX_BATCH_BYTES=4096
export RAY_DEDUP_LOGS=0
export VANE_SHUFFLE_LOCAL_DIRS=$(mktemp -d /tmp/iceberg-ray-shuffle-XXXXXX)
python -I test/vane/test_vane_wheel_ray_iceberg.py
python -I test/vane/test_vane_wheel_ray_metadata.py
docker compose -f scripts/docker-compose.yml down
```

The scripts each create and shut down their own head node and two execution
nodes. If a test fails, retain its log and still stop the fixture. Reusing
these wheels is enough for test-script changes; changing Iceberg C++ requires
new extension artifacts from CI or a local rebuild.

## 4. Rebuild locally for a shorter edit/test loop

Local native development needs Vane's native dependencies and the extension's
pinned Avro/vcpkg sources. Follow the pinned Vane checkout's `DEVELOPMENT.md`
for its toolchain and dependency setup. The repository's
[`build_vane_dynamic_wheels.py`](../scripts/build_vane_dynamic_wheels.py)
is the same helper used by build-only CI. Supply a clean Vane `v0.2.0`
checkout and use these options:

```bash
--phase full \
--signing-profile ci-test \
--signing-private-key "$VANE_SOURCE/external/duckdb/test/mbedtls/private.pem" \
--package-local-runtime
```

Its required path arguments are documented by `--help` and the build-only job
in [`VaneExtension.yml`](../.github/workflows/VaneExtension.yml). Keep the same
`--build-directory` between changes to reuse compiled objects; use a new empty
`--output-directory` for each wheel set. The helper may repackage the runtime
wheel without recompiling all of Vane. Install all three local wheels in the
test environment and rerun step 3. Do not use an editable runtime installation.

Developers use the committed public test fixture key, never a production
private key. A prebuilt test runtime from CI can be reused for the same Vane
source and build configuration. Ordinary TestPyPI dev runtimes instead trust
the dedicated TestPyPI signer; they are not interchangeable with the build-only
runtime for locally test-signed extensions.

## 5. Publish only when a release is ready

After merging and selecting the release contents, maintainers use the existing
protected `release` operation and `vane-extension-release.toml`. It builds
against the exact PyPI Vane 0.2.0 wheels, signs through the official production
environment, qualifies on TestPyPI, and promotes the same provider wheels to
PyPI after the required approvals. See [the release guide](VANE_RELEASE.md).

Daily PRs stop at build-only testing. With the current stable pin, the
`testpypi-dev` operation rejects the non-development version; publishing a
future dev runtime is not a prerequisite for developing Iceberg against 0.2.0.
