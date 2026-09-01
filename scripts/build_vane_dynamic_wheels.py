#!/usr/bin/env python3
"""Build and qualify local-provider wheels for Vane's Avro and Iceberg extensions."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Iterable, Sequence
from pathlib import Path

from packaging.tags import sys_tags

AVRO_REVISION = "7f423d69709045e38f8431b3470e0395fce1a595"
EXTENSION_NAMES = ("avro", "iceberg")
SIGNING_PROFILES = {
    "ci-test": ("vane-ci-test-key", "VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY"),
    "testpypi": ("astrovela/vane-testpypi", "VANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY"),
}
LICENSE_EXPRESSION = (
    "0BSD AND Apache-2.0 AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND ISC AND MIT AND "
    "Unicode-DFS-2015 AND Zlib AND curl"
)
EXPECTED_DUCKDB_LICENSE_PATTERNS = (
    "external/duckdb/LICENSE",
    "external/duckdb/src/include/duckdb/storage/compression/alp/algorithm/LICENSE",
    "external/duckdb/src/include/duckdb/storage/compression/alprd/algorithm/LICENSE",
    "external/duckdb/third_party/*/LICENSE",
    "external/duckdb/third_party/tdigest/NOTICES",
    "external/duckdb/third_party/thrift/thrift/LICENSE",
    "external/duckdb/third_party/jemalloc/LICENSE",
)
EXPECTED_VCPKG_LICENSE_COMPONENTS = frozenset(
    {
        "avro-c",
        "aws-c-auth",
        "aws-c-cal",
        "aws-c-common",
        "aws-c-compression",
        "aws-c-event-stream",
        "aws-c-http",
        "aws-c-io",
        "aws-c-mqtt",
        "aws-c-s3",
        "aws-c-sdkutils",
        "aws-checksums",
        "aws-crt-cpp",
        "aws-sdk-cpp",
        "curl",
        "jansson",
        "liblzma",
        "openssl",
        "roaring",
        "s2n",
        "snappy",
        "zlib",
    }
)
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_PLATFORM_TAG_RE = re.compile(r"^manylinux_[0-9]+_[0-9]+_x86_64$")
_MAX_SIGNING_PRIVATE_KEY_BYTES = 64 * 1024


class QualificationError(RuntimeError):
    """Raised when a qualification input or artifact violates the fixed contract."""


def _run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _capture(command: Sequence[str], *, cwd: Path) -> str:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def _require_directory(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_dir():
        raise QualificationError(f"{description} is not a directory: {resolved}")
    return resolved


def _require_file(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise QualificationError(f"{description} is not a file: {resolved}")
    return resolved


def _destroy_file(path: Path) -> None:
    size = path.stat().st_size
    with path.open("r+b", buffering=0) as destination:
        destination.write(b"\0" * size)
        os.fsync(destination.fileno())
    path.unlink()


def _read_signing_private_key(path: Path, *, consume: bool) -> bytearray:
    unresolved = path.expanduser().absolute()
    if consume and unresolved.is_symlink():
        raise QualificationError("consumed extension signing private key must not be a symbolic link")
    resolved = _require_file(path, "extension signing private key")
    metadata = resolved.stat()
    size = metadata.st_size
    if size <= 0 or size > _MAX_SIGNING_PRIVATE_KEY_BYTES:
        raise QualificationError("extension signing private key has an invalid size")
    if consume and (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise QualificationError("consumed extension signing private key must be a private, owned regular file")
    contents = bytearray(resolved.read_bytes())
    if consume:
        _destroy_file(resolved)
    return contents


def _require_git_revision(source: Path, expected: str, description: str) -> None:
    if not _REVISION_RE.fullmatch(expected):
        raise QualificationError(f"{description} expected revision is not a complete commit SHA: {expected!r}")
    actual = _capture(("git", "rev-parse", "HEAD^{commit}"), cwd=source)
    if actual != expected:
        raise QualificationError(f"{description} checkout is {actual}, expected {expected}")
    status = _capture(("git", "status", "--porcelain", "--untracked-files=no"), cwd=source)
    if status:
        raise QualificationError(f"{description} checkout has tracked working-tree changes")


def _require_avro_pin(extension_root: Path) -> None:
    config = (extension_root / "extension_config.cmake").read_text(encoding="utf-8")
    revisions = re.findall(
        r"duckdb_extension_load\(avro\b.*?\bGIT_TAG\s+([0-9a-f]{40})\b.*?\)",
        config,
        flags=re.DOTALL,
    )
    if revisions != [AVRO_REVISION]:
        raise QualificationError(
            "extension_config.cmake must declare exactly the qualified Avro revision "
            f"{AVRO_REVISION}; found {revisions}"
        )


def _one_wheel(directory: Path, pattern: str, description: str) -> Path:
    wheels = sorted(directory.glob(pattern))
    if len(wheels) != 1:
        raise QualificationError(f"expected exactly one {description}, found {len(wheels)} below {directory}")
    return wheels[0]


def _require_no_undefined_duckdb_symbols(artifact: Path) -> None:
    symbols = _capture(
        ("nm", "--dynamic", "--demangle", "--undefined-only", str(artifact)),
        cwd=artifact.parent,
    )
    unresolved = tuple(line.strip() for line in symbols.splitlines() if "duckdb::" in line)
    if unresolved:
        details = "\n".join(f"  {symbol}" for symbol in unresolved)
        raise QualificationError(f"{artifact.name} has unresolved DuckDB C++ symbols:\n{details}")


def _platform_tag() -> str:
    if sys.platform != "linux" or platform.machine() != "x86_64":
        raise QualificationError("dynamic wheel qualification requires Linux x86_64")
    tag = next((candidate.platform for candidate in sys_tags() if candidate.platform.startswith("manylinux_")), "")
    if not _PLATFORM_TAG_RE.fullmatch(tag):
        raise QualificationError(f"could not resolve one exact manylinux x86_64 platform tag: {tag!r}")
    return tag


def _compiler_launcher_arguments() -> list[str]:
    launcher = os.environ.get("VANE_CMAKE_COMPILER_LAUNCHER", "")
    if not launcher:
        return []
    if launcher != "ccache":
        raise QualificationError("VANE_CMAKE_COMPILER_LAUNCHER must be ccache when set")
    return ["-DCMAKE_C_COMPILER_LAUNCHER=ccache", "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"]


def _vcpkg_baseline(extension_root: Path) -> str:
    manifest = json.loads(
        _require_file(extension_root / "vcpkg.json", "extension vcpkg manifest").read_text(encoding="utf-8")
    )
    baseline = manifest.get("builtin-baseline")
    if not isinstance(baseline, str) or not _REVISION_RE.fullmatch(baseline):
        raise QualificationError("vcpkg.json must contain one complete builtin-baseline")
    return baseline


def _require_vcpkg_toolchain(path: Path, baseline: str) -> Path:
    toolchain = _require_file(path, "vcpkg toolchain")
    try:
        vcpkg_root = toolchain.parents[2]
    except IndexError:
        raise QualificationError(f"vcpkg toolchain does not have the expected repository layout: {toolchain}") from None
    expected = (vcpkg_root / "scripts/buildsystems/vcpkg.cmake").resolve()
    if toolchain != expected:
        raise QualificationError(f"vcpkg toolchain does not have the expected repository layout: {toolchain}")
    _require_git_revision(vcpkg_root, baseline, "extension vcpkg")
    return toolchain


def _build_environment(
    *,
    extension_root: Path,
    avro_source: Path,
    build_directory: Path,
    vane_vcpkg_installed: Path,
    vcpkg_toolchain: Path,
    jobs: int,
    signing_cmake_option: str,
) -> dict[str, str]:
    target_triplet = "x64-linux"
    dependency_prefix = vane_vcpkg_installed / target_triplet
    for relative in ("share/arrow/ArrowConfig.cmake", "share/arrowflight/ArrowFlightConfig.cmake"):
        _require_file(dependency_prefix / relative, "Vane native dependency configuration")

    prefix_config = build_directory / "vane-dynamic-wheel-dependency-prefix.cmake"
    prefix_config.write_text(
        "# Generated by scripts/build_vane_dynamic_wheels.py.\n"
        f'list(PREPEND CMAKE_PREFIX_PATH "{dependency_prefix}")\n',
        encoding="utf-8",
    )
    staged_extensions = build_directory / "vane_extensions"
    cmake_arguments = [
        "--fresh",
        "-DBUILD_DISTRIBUTED_EXCHANGE=ON",
        "-DENABLE_EXTENSION_AUTOLOADING=OFF",
        "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
        "-DEXTENSION_STATIC_BUILD=ON",
        "-DICEBERG_VANE_DISTRIBUTED=ON",
        f"-D{signing_cmake_option}=ON",
        "-DVANE_LOADABLE_EXTENSIONS=avro;iceberg",
        f"-DVANE_LOADABLE_EXTENSION_OUTPUT_DIRECTORY={staged_extensions}",
        "-DVCPKG_BUILD=ON",
        f"-DCMAKE_TOOLCHAIN_FILE={vcpkg_toolchain}",
        f"-DVCPKG_MANIFEST_DIR={extension_root}",
        f"-DVCPKG_INSTALLED_DIR={build_directory / 'vcpkg_installed'}",
        f"-DVCPKG_TARGET_TRIPLET={target_triplet}",
        f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={prefix_config}",
        *_compiler_launcher_arguments(),
    ]

    environment = os.environ.copy()
    selection_variables = {
        "VCPKG_CHAINLOAD_TOOLCHAIN_FILE",
        "VCPKG_DEFAULT_HOST_TRIPLET",
        "VCPKG_DEFAULT_TRIPLET",
        "VCPKG_OVERLAY_PORTS",
        "VCPKG_OVERLAY_TRIPLETS",
    }
    for name in tuple(environment):
        if (
            name
            in {
                "CMAKE_ARGS",
                "CMAKE_PREFIX_PATH",
                "COVERAGE",
                "DONT_LINK",
                "GITHUB_BASE_REF",
                "GITHUB_REF_NAME",
                "VANE_CMAKE_PREFIX_PATH",
                "VANE_CMAKE_COMPILER_LAUNCHER",
                "VANE_VERSION_BRANCH",
            }
            or name in selection_variables
            or name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION")
            or name.startswith("SKBUILD_")
            or (name.startswith("DUCKDB_") and name.endswith("_DIRECTORY"))
        ):
            environment.pop(name)
    environment.update(
        {
            "CMAKE_ARGS": shlex.join(cmake_arguments),
            "CMAKE_BUILD_PARALLEL_LEVEL": str(jobs),
            "CMAKE_GENERATOR": "Ninja",
            "DUCKDB_AVRO_DIRECTORY": str(avro_source),
            "DUCKDB_ICEBERG_DIRECTORY": str(extension_root),
            "SKBUILD_BUILD_DIR": str(build_directory),
            "SKBUILD_CMAKE_BUILD_TYPE": "Release",
            "VCPKG_MAX_CONCURRENCY": str(jobs),
            "VCPKG_TARGET_TRIPLET": target_triplet,
            "VCPKG_TOOLCHAIN_PATH": str(vcpkg_toolchain),
        }
    )
    return environment


def _render_vcpkg_license_bundle(extension_root: Path, share_directory: Path) -> str:
    baseline = _vcpkg_baseline(extension_root)
    records = sorted(
        (path for path in share_directory.glob("*/copyright") if not path.parent.name.startswith("vcpkg-")),
        key=lambda path: path.parent.name,
    )
    if not records:
        raise QualificationError(f"no installed vcpkg copyright records found below {share_directory}")
    components = frozenset(record.parent.name for record in records)
    if components != EXPECTED_VCPKG_LICENSE_COMPONENTS:
        missing = sorted(EXPECTED_VCPKG_LICENSE_COMPONENTS - components)
        unexpected = sorted(components - EXPECTED_VCPKG_LICENSE_COMPONENTS)
        raise QualificationError(
            "extension vcpkg license closure differs from the reviewed Linux qualification set: "
            f"missing={missing}, unexpected={unexpected}"
        )
    lines = [
        "Vane dynamic-extension vcpkg binary dependency licenses",
        "========================================================",
        "",
        "Generated from the exact extension vcpkg installation used by this qualification build.",
        f"vcpkg builtin baseline: {baseline}",
        "",
    ]
    for record in records:
        content = record.read_text(encoding="utf-8", errors="replace").strip()
        lines.extend(("=" * 80, f"Component: {record.parent.name}", "=" * 80, content, ""))
    return "\n".join(lines).rstrip() + "\n"


def _duckdb_license_sources(vane_source: Path) -> tuple[tuple[str, Path], ...]:
    project = tomllib.loads((vane_source / "pyproject.toml").read_text(encoding="utf-8")).get("project")
    if not isinstance(project, dict):
        raise QualificationError("Vane pyproject.toml must contain a project table")
    license_patterns = project.get("license-files")
    if not isinstance(license_patterns, list) or any(not isinstance(value, str) for value in license_patterns):
        raise QualificationError("Vane project.license-files must be a list of strings")
    # EXTENSION_STATIC_BUILD links duckdb_static and, for distributed Iceberg,
    # the Parquet objects. DuckDB's license covers those sources and the
    # third-party patterns below cover their codec dependencies; unrelated
    # base-wheel extensions such as ICU remain excluded.
    duckdb_patterns = tuple(
        value
        for value in license_patterns
        if value.startswith("external/duckdb/") and not value.startswith("external/duckdb/extension/")
    )
    if duckdb_patterns != EXPECTED_DUCKDB_LICENSE_PATTERNS:
        raise QualificationError(
            "Vane's declared DuckDB license patterns differ from the reviewed qualification set: " f"{duckdb_patterns}"
        )

    sources: dict[str, Path] = {}
    for pattern in duckdb_patterns:
        matches = sorted(vane_source.glob(pattern))
        if not matches:
            raise QualificationError(f"Vane DuckDB license pattern has no matches: {pattern!r}")
        for match in matches:
            source = _require_file(match, "Vane DuckDB license source")
            try:
                relative = source.relative_to(vane_source).as_posix()
            except ValueError:
                raise QualificationError(f"Vane DuckDB license source escapes the checkout: {source}") from None
            sources[relative] = source
    return tuple(sorted(sources.items()))


def _render_duckdb_license_bundle(vane_source: Path) -> str:
    lines = [
        "DuckDB static-engine source and third-party licenses",
        "====================================================",
        "",
        "Generated from Vane's exact project.license-files declarations for the DuckDB static engine.",
        "",
    ]
    for relative, source in _duckdb_license_sources(vane_source):
        content = source.read_text(encoding="utf-8", errors="replace").strip()
        lines.extend(("=" * 80, f"Source: {relative}", "=" * 80, content, ""))
    return "\n".join(lines).rstrip() + "\n"


def _copy_license(source: Path, destination: Path) -> Path:
    _require_file(source, "license source")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return destination


def _stage_license_files(
    *,
    extension_root: Path,
    avro_source: Path,
    vane_source: Path,
    build_directory: Path,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    license_directory = build_directory / "dynamic-extension-licenses"
    license_directory.mkdir(parents=True, exist_ok=True)
    duckdb_bundle = license_directory / "DuckDB-static-engine-licenses.txt"
    duckdb_bundle.write_text(_render_duckdb_license_bundle(vane_source), encoding="utf-8")
    common = (
        _copy_license(vane_source / "LICENSE", license_directory / "Vane-Apache-2.0.txt"),
        _copy_license(vane_source / "NOTICE", license_directory / "Vane-NOTICE.txt"),
        duckdb_bundle,
    )
    dependency_bundle = license_directory / "vcpkg-binary-dependencies.txt"
    dependency_bundle.write_text(
        _render_vcpkg_license_bundle(
            extension_root,
            build_directory / "vcpkg_installed/x64-linux/share",
        ),
        encoding="utf-8",
    )
    avro_licenses = (
        *common,
        _copy_license(avro_source / "LICENSE", license_directory / "DuckDB-Avro-MIT.txt"),
        dependency_bundle,
    )
    iceberg_licenses = (
        *common,
        _copy_license(extension_root / "LICENSE", license_directory / "DuckDB-Iceberg-MIT.txt"),
        dependency_bundle,
    )
    return avro_licenses, iceberg_licenses


def _builder_python(
    interpreter: Path,
    base_wheel: Path,
    parent: Path,
) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    temporary = tempfile.TemporaryDirectory(prefix="vane-dynamic-wheel-builder-", dir=parent)
    environment_root = Path(temporary.name)
    _run((str(interpreter), "-I", "-m", "venv", "--copies", str(environment_root)))
    python = environment_root / "bin/python"
    _run(
        (
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "packaging>=24.2",
            "tomli>=1.1; python_version < '3.11'",
            str(base_wheel),
        )
    )
    return temporary, python


def _build_provider_wheel(
    *,
    python: Path,
    vane_source: Path,
    artifact: Path,
    extension_name: str,
    output_directory: Path,
    platform_tag: str,
    trust_identity: str,
    license_files: Iterable[Path],
    dependency_wheel: Path | None = None,
) -> Path:
    command = [
        str(python),
        "-I",
        str(vane_source / "scripts/build_extension_wheel.py"),
        "--artifact",
        str(artifact),
        "--extension-name",
        extension_name,
        "--output-directory",
        str(output_directory),
        "--platform-tag",
        platform_tag,
        "--trust-identity",
        trust_identity,
        "--license-expression",
        LICENSE_EXPRESSION,
    ]
    for license_file in license_files:
        command.extend(("--license-file", str(license_file)))
    if dependency_wheel is not None:
        command.extend(
            (
                "--dependency-wheel",
                str(dependency_wheel),
                "--dependency-trust-identity",
                trust_identity,
            )
        )
    _run(command)
    return _one_wheel(
        output_directory,
        f"vane_extension_{extension_name}-*.whl",
        f"vane-extension-{extension_name} wheel",
    )


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", required=True, type=Path)
    parser.add_argument("--avro-source", required=True, type=Path)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--vane-vcpkg-installed", required=True, type=Path)
    parser.add_argument("--vcpkg-toolchain", required=True, type=Path)
    parser.add_argument("--build-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--jobs", default=8, type=int)
    parser.add_argument("--signing-profile", required=True, choices=tuple(SIGNING_PROFILES))
    parser.add_argument("--signing-private-key", required=True, type=Path)
    parser.add_argument(
        "--consume-signing-private-key",
        action="store_true",
        help="Securely remove the ephemeral key input before starting any build subprocess",
    )
    runtime_group = parser.add_mutually_exclusive_group(required=True)
    runtime_group.add_argument(
        "--package-local-runtime",
        action="store_true",
        help="Package and emit the locally built Vane wheel for CI-only qualification",
    )
    runtime_group.add_argument(
        "--runtime-python",
        action="append",
        default=[],
        type=Path,
        help="Interpreter matching one indexed runtime wheel; repeat with --runtime-wheel",
    )
    parser.add_argument(
        "--runtime-wheel",
        action="append",
        default=[],
        type=Path,
        help="Exact indexed Vane wheel for the matching --runtime-python",
    )
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    if arguments.jobs <= 0:
        raise QualificationError("--jobs must be a positive integer")
    if arguments.package_local_runtime and arguments.runtime_wheel:
        raise QualificationError("--runtime-wheel cannot be combined with --package-local-runtime")
    if len(arguments.runtime_python) != len(arguments.runtime_wheel):
        raise QualificationError("--runtime-python and --runtime-wheel must be supplied the same number of times")
    if not arguments.package_local_runtime and not arguments.runtime_python:
        raise QualificationError("at least one indexed runtime pair is required")

    extension_root = _require_directory(arguments.extension_root, "extension root")
    avro_source = _require_directory(arguments.avro_source, "Avro source")
    vane_source = _require_directory(arguments.vane_source, "Vane source")
    vane_vcpkg_installed = _require_directory(arguments.vane_vcpkg_installed, "Vane vcpkg installation")
    trust_identity, signing_cmake_option = SIGNING_PROFILES[arguments.signing_profile]
    if arguments.signing_profile == "testpypi" and not arguments.consume_signing_private_key:
        raise QualificationError("the TestPyPI signing profile requires --consume-signing-private-key")
    signing_private_key_contents = _read_signing_private_key(
        arguments.signing_private_key,
        consume=arguments.consume_signing_private_key,
    )
    indexed_runtimes: tuple[tuple[Path, Path], ...] = tuple(
        (
            _require_file(interpreter, "runtime Python interpreter"),
            _require_file(wheel, "indexed Vane runtime wheel"),
        )
        for interpreter, wheel in zip(arguments.runtime_python, arguments.runtime_wheel, strict=True)
    )
    for interpreter, _wheel in indexed_runtimes:
        if not os.access(interpreter, os.X_OK):
            raise QualificationError(f"runtime Python interpreter is not executable: {interpreter}")
    vcpkg_toolchain = _require_vcpkg_toolchain(
        arguments.vcpkg_toolchain,
        _vcpkg_baseline(extension_root),
    )
    build_directory = arguments.build_directory.expanduser().resolve()
    output_directory = arguments.output_directory.expanduser().resolve()
    build_directory.mkdir(parents=True, exist_ok=True)
    output_directory.mkdir(parents=True, exist_ok=True)
    existing_wheels = sorted(output_directory.glob("*.whl"))
    if existing_wheels:
        raise QualificationError(f"output directory already contains a wheel: {existing_wheels[0]}")

    _require_avro_pin(extension_root)
    _require_git_revision(avro_source, AVRO_REVISION, "Avro")
    _require_git_revision(vane_source, arguments.vane_revision, "Vane")
    platform_tag = _platform_tag()
    environment = _build_environment(
        extension_root=extension_root,
        avro_source=avro_source,
        build_directory=build_directory,
        vane_vcpkg_installed=vane_vcpkg_installed,
        vcpkg_toolchain=vcpkg_toolchain,
        jobs=arguments.jobs,
        signing_cmake_option=signing_cmake_option,
    )

    with tempfile.TemporaryDirectory(prefix="vane-base-wheel-", dir=build_directory.parent) as base_output_value:
        base_output = Path(base_output_value)
        _run(
            (
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--no-isolation",
                "--outdir",
                str(base_output),
                str(vane_source),
            ),
            cwd=extension_root,
            environment=environment,
        )
        base_wheel = _one_wheel(base_output, "vane_ai-*.whl", "base Vane wheel")
        _run(
            (
                "cmake",
                "--build",
                str(build_directory),
                "--target",
                "vane_loadable_extensions",
                "--parallel",
                str(arguments.jobs),
            ),
            cwd=extension_root,
            environment=environment,
        )

        unsigned_directory = build_directory / "vane_extensions"
        signed_directory = build_directory / "signed-vane-extensions"
        signed_directory.mkdir(parents=True, exist_ok=True)
        signed_artifacts: dict[str, Path] = {}
        key_handle, ephemeral_key_name = tempfile.mkstemp(
            prefix=".vane-extension-signing-",
            suffix=".pem",
            dir=signed_directory,
        )
        ephemeral_key = Path(ephemeral_key_name)
        try:
            with os.fdopen(key_handle, "wb") as key_output:
                os.fchmod(key_output.fileno(), 0o600)
                key_output.write(signing_private_key_contents)
                key_output.flush()
                os.fsync(key_output.fileno())
            for extension_name in EXTENSION_NAMES:
                unsigned = _require_file(
                    unsigned_directory / f"{extension_name}.duckdb_extension",
                    f"unsigned {extension_name} artifact",
                )
                _require_no_undefined_duckdb_symbols(unsigned)
                signed = signed_directory / unsigned.name
                _run(
                    (
                        sys.executable,
                        str(vane_source / "scripts/sign_test_dynamic_extension.py"),
                        "--private-key",
                        str(ephemeral_key),
                        str(unsigned),
                        str(signed),
                    )
                )
                signed_artifacts[extension_name] = signed
        finally:
            signing_private_key_contents[:] = b"\0" * len(signing_private_key_contents)
            signing_private_key_contents.clear()
            if ephemeral_key.exists():
                _destroy_file(ephemeral_key)

        avro_licenses, iceberg_licenses = _stage_license_files(
            extension_root=extension_root,
            avro_source=avro_source,
            vane_source=vane_source,
            build_directory=build_directory,
        )
        with tempfile.TemporaryDirectory(prefix="vane-qualified-wheels-", dir=output_directory.parent) as staging_value:
            staging = Path(staging_value)
            emitted_base_wheel: Path | None = None
            if arguments.package_local_runtime:
                repaired_base_directory = staging / "base"
                repaired_base_directory.mkdir()
                _run(
                    (
                        sys.executable,
                        "-m",
                        "auditwheel",
                        "repair",
                        "--plat",
                        platform_tag,
                        "--wheel-dir",
                        str(repaired_base_directory),
                        str(base_wheel),
                    )
                )
                emitted_base_wheel = _one_wheel(
                    repaired_base_directory,
                    "vane_ai-*.whl",
                    "repaired base Vane wheel",
                )
                runtimes = ((Path(sys.executable).resolve(), emitted_base_wheel),)
            else:
                runtimes = indexed_runtimes

            built_provider_wheels: list[Path] = []
            for runtime_index, (runtime_python, runtime_wheel) in enumerate(runtimes):
                provider_directory = staging / f"extensions-{runtime_index}"
                provider_directory.mkdir()
                builder_environment, builder_python = _builder_python(
                    runtime_python,
                    runtime_wheel,
                    build_directory.parent,
                )
                try:
                    avro_wheel = _build_provider_wheel(
                        python=builder_python,
                        vane_source=vane_source,
                        artifact=signed_artifacts["avro"],
                        extension_name="avro",
                        output_directory=provider_directory,
                        platform_tag=platform_tag,
                        trust_identity=trust_identity,
                        license_files=avro_licenses,
                    )
                    iceberg_wheel = _build_provider_wheel(
                        python=builder_python,
                        vane_source=vane_source,
                        artifact=signed_artifacts["iceberg"],
                        extension_name="iceberg",
                        output_directory=provider_directory,
                        platform_tag=platform_tag,
                        trust_identity=trust_identity,
                        license_files=iceberg_licenses,
                        dependency_wheel=avro_wheel,
                    )
                    _run(
                        (
                            str(builder_python),
                            "-I",
                            str(vane_source / "scripts/verify_extension_wheel.py"),
                            "--base-wheel",
                            str(runtime_wheel),
                            "--dependency-wheel",
                            str(avro_wheel),
                            "--extension-wheel",
                            str(iceberg_wheel),
                            "--extension-name",
                            "iceberg",
                            "--trust-identity",
                            trust_identity,
                            "--dependency-trust-identity",
                            trust_identity,
                        )
                    )
                finally:
                    builder_environment.cleanup()
                built_provider_wheels.extend((avro_wheel, iceberg_wheel))

            wheels_to_emit = (
                *((emitted_base_wheel,) if emitted_base_wheel is not None else ()),
                *built_provider_wheels,
            )
            for wheel in wheels_to_emit:
                destination = output_directory / wheel.name
                if destination.exists():
                    raise QualificationError(f"multiple runtime targets produced the same wheel: {destination.name}")
                shutil.copyfile(wheel, destination)
                print(destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except QualificationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
