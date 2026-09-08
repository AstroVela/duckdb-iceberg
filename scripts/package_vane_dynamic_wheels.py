#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Package already signed data in a fresh job with no keys or publishing OIDC."""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import stat
import tempfile
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "_iceberg_wheel_builder", Path(__file__).with_name("build_vane_dynamic_wheels.py")
)
builder = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(builder)

LICENSE_NAMES = {
    name: (
        "Vane-Apache-2.0.txt",
        "Vane-NOTICE.txt",
        "DuckDB-static-engine-licenses.txt",
        f"DuckDB-{name.title()}-MIT.txt",
        "vcpkg-binary-dependencies.txt",
    )
    for name in ("avro", "iceberg")
}


def require_signed_payload(unsigned: Path, signed: Path) -> Path:
    sizes = []
    for path in (unsigned, signed):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or not 256 < info.st_size <= 384 * 1024 * 1024:
            raise builder.QualificationError("native artifacts must be bounded regular files, never symlinks")
        sizes.append(info.st_size)
    if sizes[0] != sizes[1]:
        raise builder.QualificationError("signing changed the native artifact size")
    remaining = sizes[0] - 256
    with unsigned.open("rb") as before, signed.open("rb") as after:
        while remaining:
            size = min(1024 * 1024, remaining)
            if before.read(size) != after.read(size):
                raise builder.QualificationError("signing changed the native artifact payload")
            remaining -= size
        if before.read() != b"\0" * 256 or after.read() == b"\0" * 256:
            raise builder.QualificationError("signing must replace only the empty native signature slot")
    return signed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--unsigned-directory", required=True, type=Path)
    parser.add_argument("--signed-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--signing-profile", required=True, choices=("testpypi", "production"))
    parser.add_argument("--runtime-python", required=True, action="append", type=Path)
    parser.add_argument("--runtime-wheel", required=True, action="append", type=Path)
    args = parser.parse_args()
    if len(args.runtime_python) != len(args.runtime_wheel):
        raise builder.QualificationError("one exact runtime wheel is required per interpreter")
    vane_source = args.vane_source.resolve()
    builder._require_git_revision(vane_source, args.vane_revision, "Vane")
    output = args.output_directory.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise builder.QualificationError("packaging requires an empty output directory")
    identity, _option = builder.SIGNING_PROFILES[args.signing_profile]
    platform = builder._platform_tag()
    licenses = {}
    artifacts = {}
    for name in builder.EXTENSION_NAMES:
        directory = args.unsigned_directory / "licenses" / name
        if set(path.name for path in directory.iterdir()) != set(LICENSE_NAMES[name]):
            raise builder.QualificationError(f"{name} license bundle differs from the reviewed set")
        licenses[name] = tuple(
            builder._require_file(directory / filename, "license") for filename in LICENSE_NAMES[name]
        )
        artifacts[name] = require_signed_payload(
            args.unsigned_directory / "artifacts" / f"{name}.duckdb_extension",
            args.signed_directory / f"{name}.duckdb_extension",
        )
    if len({builder.parse_wheel_filename(wheel.name)[1] for wheel in args.runtime_wheel}) != 1:
        raise builder.QualificationError("runtime matrix must use one exact Vane version")
    for wheel in args.runtime_wheel:
        if args.signing_profile == "production":
            builder._require_production_runtime(wheel)
    with tempfile.TemporaryDirectory(prefix="vane-provider-packaging-", dir=output.parent) as value:
        staging = Path(value)
        for index, (interpreter, runtime_wheel) in enumerate(zip(args.runtime_python, args.runtime_wheel, strict=True)):
            directory = staging / str(index)
            directory.mkdir()
            environment, python = builder._builder_python(interpreter.resolve(), runtime_wheel.resolve(), staging)
            try:
                avro = builder._build_provider_wheel(
                    python=python,
                    vane_source=vane_source,
                    artifact=artifacts["avro"],
                    extension_name="avro",
                    output_directory=directory,
                    platform_tag=platform,
                    trust_identity=identity,
                    license_files=licenses["avro"],
                )
                iceberg = builder._build_provider_wheel(
                    python=python,
                    vane_source=vane_source,
                    artifact=artifacts["iceberg"],
                    extension_name="iceberg",
                    output_directory=directory,
                    platform_tag=platform,
                    trust_identity=identity,
                    license_files=licenses["iceberg"],
                    dependency_wheel=avro,
                )
                builder._run(
                    (
                        str(python),
                        "-I",
                        str(vane_source / "scripts/verify_extension_wheel.py"),
                        "--base-wheel",
                        str(runtime_wheel.resolve()),
                        "--dependency-wheel",
                        str(avro),
                        "--extension-wheel",
                        str(iceberg),
                        "--extension-name",
                        "iceberg",
                        "--trust-identity",
                        identity,
                        "--dependency-trust-identity",
                        identity,
                    )
                )
                for wheel in (avro, iceberg):
                    destination = output / wheel.name
                    if destination.exists():
                        raise builder.QualificationError(f"duplicate provider wheel: {wheel.name}")
                    shutil.copyfile(wheel, destination)
            finally:
                environment.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
