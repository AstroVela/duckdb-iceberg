# Vane provider release integration

`VaneExtension.yml` publishes the Avro and Iceberg providers through the shared
release validator in the exact `vane-extension-ci-tools` submodule. The wheel
matrix and provider dependency graph live in `vane-provider-release.toml`:

- CPython 3.10 through 3.14, `none` ABI, `manylinux_2_28_x86_64`;
- Avro requires the exact `vane-ai` candidate;
- Iceberg requires that same Vane candidate and the exact Avro provider version;
- each uploaded wheel must fit the configured 100,000,000-byte TestPyPI budget,
  independently of Vane's native artifact safety limits.

The release CLI runs under Python 3.11 or newer, independently of the supported
wheel interpreters. It uses `packaging` through the shared requirements file.

```sh
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt
python -I test/vane/test_vane_provider_release.py
```

The native manifest uses the shared tools' explicit vcpkg contract. Its vcpkg
revision matches the existing `vcpkg.json` baseline; neither that dependency
revision nor the pinned `vane-ai==0.2.0.dev612` source is changed by the migration.
The workflow and submodule must reference the same complete CI-tools commit.

## Release boundaries

The existing native builder continues to build, sign, and qualify both
providers against the exact Vane runtime wheels. Vane's builder and verifier
remain responsible for descriptor, SourceID, platform, signature, native
dependency, license, and archive-safety checks. The shared release validator is
an additional matrix, exact-dependency, upload-size, and TestPyPI hash gate, not
a replacement for those native artifact checks.

Build and assembly validate all ten wheels together. Each index-verification
job downloads the same assembled provider artifact as its upload job and
compares that provider's five filenames and SHA256 hashes with TestPyPI. Avro
can therefore be checked before Iceberg exists on the index.

The release order remains:

1. Build and verify Avro and Iceberg together, then assemble release evidence.
2. Publish Avro and verify its indexed wheel hashes.
3. Publish Iceberg and verify its indexed wheel hashes.
4. Install the exact indexed package graph for local and two-worker Ray tests.

All uploads stay in the repository's top-level `VaneExtension.yml`, using the
existing `testpypi` environment and Trusted Publisher configuration. The
signing identity and key configuration are unchanged. Push and PR CI do not
publish packages; publication still requires a manual `testpypi-dev` dispatch
on `v1.5-variegata_vane`.

## Validate an already-built release

Use the exact clean Vane checkout selected by `vane-extension.toml`. Source
verification is read-only and also checks the shared tools checkout against
the complete commit recorded by the extension repository.

```sh
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory dist/providers --vane-version 0.2.0.dev612 \
  --require-testpypi-publishable
```

After publication, run `verify-index` with the same source/configuration flags,
`--directory` pointing to that provider's five assembled wheels, and
`--provider avro` or `--provider iceberg` plus its exact `--version`.
