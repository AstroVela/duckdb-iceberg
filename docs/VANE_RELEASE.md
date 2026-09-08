# Vane provider release integration

`VaneExtension.yml` uses the exact `vane-extension-ci-tools` submodule for
channel, matrix, exact dependency, upload-size and immutable index validation.
The provider graph lives in `vane-provider-release.toml`: CPython 3.10–3.14,
`none` ABI, `manylinux_2_28_x86_64`, five wheels per provider. Every wheel must
fit the configured 100,000,000-byte index budget independently of native safety
limits. Avro pins the exact Vane runtime; Iceberg pins that runtime and exact Avro.

## Channels

| Operation | Committed manifest | Runtime source | Native signer | Publication |
| --- | --- | --- | --- | --- |
| `build-only` (default), push, PR | `vane-extension.toml` | Local CI build | `vane-ci-test-key` | None |
| `testpypi-dev` | `vane-extension.toml` | TestPyPI only | `astrovela/vane-testpypi` | TestPyPI only |
| `release` | `vane-extension-release.toml` | PyPI only | `astrovela/vane` | TestPyPI qualification, then identical wheels to PyPI |

The development manifest remains pinned to `vane-ai==0.2.0.dev612`. Its
dependencies and signing key are unchanged. Neither schema version nor loading
behavior changes. Publishing requires a manual dispatch in
`AstroVela/duckdb-iceberg` on the protected default branch
`v1.5-variegata_vane`. No provider-repository tag is required or created.

The production manifest currently pins
`033b549afcb498633fd6669b26c054c00363004e`, which adds Vane's production public
key but **is not a released runtime**. The `release` preflight deliberately
fails for this development version before opening the signing environment or
building native code. This prepares a channel; it does not publish or claim
end-to-end production qualification.

Before the first production run, publish a canonical non-development Vane
release (an alpha, beta or RC is also allowed) to PyPI. Update only the release
manifest through review to its complete exact commit, which must contain the
production-key commit above. The workflow derives the version from clean, full
Git history with version overrides removed and validates it with the shared
channel gate. All five exact runtime wheels are downloaded before native
dependency preparation. Missing wheels fail; no alternate-index or development
runtime fallback is available.

## Production signing and approval setup

All official providers share the `astrovela/vane` RSA-2048 signer. Its public
DER SubjectPublicKeyInfo SHA-256 fingerprint is
`8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
The builder checks this fingerprint before native build subprocesses, requires
consumed ephemeral key input and clears its key memory on failure and success.
Both testing-key CMake flags are explicitly OFF for production. A locally
built runtime cannot substitute for the exact indexed release. Unsigned loading
and alternative trust roots stay disabled.

Configure these GitHub environments before production use, restrict them to
the protected default branch and require reviewer approval:

- `production-signing`: keep the private key only in the environment secret
  `VANE_EXTENSION_SIGNING_PRIVATE_KEY`. This job has no PyPI OIDC permission.
- `testpypi`: retain the existing Avro and Iceberg TestPyPI Trusted Publishers
  and development signing secret `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY`.
- `pypi-avro`: PyPI Trusted Publisher for `vane-extension-avro`, owner
  `AstroVela`, repository `duckdb-iceberg`, workflow `VaneExtension.yml`,
  environment `pypi-avro`.
- `pypi-iceberg`: the corresponding `vane-extension-iceberg` publisher using
  environment `pypi-iceberg`.

The two PyPI environments give new projects distinct Pending Publisher
configurations. Do not register both pending projects with the same
owner/repository/workflow/environment tuple. Uploads remain top-level jobs:
PyPI currently does not support reusable workflows as Trusted Publishers
([PyPI documentation](https://docs.pypi.org/trusted-publishers/troubleshooting/#reusable-workflows-on-github)).
Common validation stays in the pinned tools. Private keys must never enter
source, logs, artifacts or the base Vane build. This preparation does not create
environments, register publishers, upload secrets, create tags or publish packages.

## Immutable release order

1. Pass the read-only context/version gate and signing approval. Build and sign
   both native extensions once. Vane's existing builder and clean verifier
   qualify all ten wheels against the exact indexed runtimes: descriptors,
   SourceID, platform, signatures, native dependencies, licenses and archive safety.
2. Validate the complete graph and both index destinations, assemble checksums,
   provenance and SBOM evidence, publish Avro to TestPyPI and verify its five
   indexed hashes, then publish and verify Iceberg.
3. Install the exact indexed graph in CPython 3.12 for local and two-worker Ray
   tests. Both compare each downloaded provider wheel byte-for-byte with the
   candidate artifact. Production runtime downloads use PyPI; provider downloads
   use TestPyPI. Ordinary Python dependencies come from PyPI.
4. Only after **both** smoke jobs succeed, approve `pypi-avro`. Download the
   complete original candidate graph, re-run shared `verify-promotion` against
   TestPyPI and PyPI, upload the unchanged Avro subset and verify PyPI hashes.
5. Approve `pypi-iceberg` after Avro succeeds. Recheck the complete graph again,
   upload the unchanged Iceberg subset and verify its PyPI files.

Promotion never rebuilds, relabels or re-signs wheels. Every workflow artifact
download rejects digest mismatches. Identical indexed files are retryable;
conflicts, extra files, yanks or changed hashes fail validation. Rerun failed
jobs within the 30-day artifact retention period to continue with the same
candidate. A fresh workflow run is a new build, not promotion of an older run.

The shared CLI is an additional publication gate, not a replacement for native
artifact checks. A successful build-only/PR run does not prove that production
credentials, approvals or publishing are configured. The first real release
still requires end-to-end qualification.

## Focused tooling tests and manual validation

Release tooling runs under Python 3.11 or newer, independently of wheel targets.
Tests use `packaging` and PyYAML, without native builds or live catalogs:

```sh
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt 'PyYAML>=6.0.2'
python -I test/vane/test_vane_provider_release.py
python -I test/vane/test_vane_production_release.py
```

Validate a development candidate with the exact clean Vane checkout:

```sh
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory dist/providers --vane-version 0.2.0.dev612 \
  --channel testpypi-dev --require-publishable-on testpypi
```

For production select the release manifest, exact non-development version,
`--channel release`, and both `--require-publishable-on testpypi` and
`--require-publishable-on pypi`. After staging, `verify-promotion` uses the full
ten-wheel directory, the same source/configuration flags and `--vane-version`.
Per-provider `verify-index` uses `--index testpypi` or `--index pypi`, the
directory containing that provider's five wheels, `--provider avro` or
`--provider iceberg`, and its exact `--version`.
