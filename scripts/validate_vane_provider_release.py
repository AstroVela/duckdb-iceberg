#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Validate and verify one immutable Vane provider-wheel candidate set."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from email.policy import default
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version

EXPECTED_INTERPRETERS = frozenset({"cp310", "cp311", "cp312", "cp313", "cp314"})
EXPECTED_PLATFORM = "manylinux_2_28_x86_64"
PROVIDER_DISTRIBUTIONS = {
    "avro": "vane-extension-avro",
    "iceberg": "vane-extension-iceberg",
}
TESTPYPI_JSON_BASE = "https://test.pypi.org/pypi"
MAX_METADATA_BYTES = 1024 * 1024


class ReleaseValidationError(RuntimeError):
    """Raised when provider distributions violate the release contract."""


@dataclass(frozen=True)
class WheelRecord:
    """Validated filename and dependency metadata for one provider wheel."""

    path: Path
    distribution_name: str
    version: str
    interpreter: str
    requirements: tuple[Requirement, ...]


def _canonical_version(value: str, description: str) -> str:
    parsed = Version(value)
    if str(parsed) != value:
        raise ReleaseValidationError(f"{description} must use canonical PEP 440 spelling: {parsed}")
    return value


def _read_metadata(path: Path):
    try:
        with zipfile.ZipFile(path) as wheel:
            members = [
                member
                for member in wheel.infolist()
                if member.filename.count("/") == 1 and member.filename.endswith(".dist-info/METADATA")
            ]
            if len(members) != 1:
                raise ReleaseValidationError(f"{path.name} must contain exactly one top-level METADATA file")
            if members[0].file_size > MAX_METADATA_BYTES:
                raise ReleaseValidationError(f"{path.name} METADATA exceeds {MAX_METADATA_BYTES} bytes")
            contents = wheel.read(members[0])
    except zipfile.BadZipFile as error:
        raise ReleaseValidationError(f"{path.name} is not a valid wheel archive") from error
    return BytesParser(policy=default).parsebytes(contents)


def _read_wheel(path: Path) -> WheelRecord:
    filename_name, filename_version, _build, tags = parse_wheel_filename(path.name)
    if len(tags) != 1:
        raise ReleaseValidationError(f"{path.name} must contain exactly one wheel tag")
    tag = next(iter(tags))
    if tag.interpreter not in EXPECTED_INTERPRETERS or tag.abi != "none" or tag.platform != EXPECTED_PLATFORM:
        raise ReleaseValidationError(f"{path.name} must use cp310-cp314-none-{EXPECTED_PLATFORM}, found {tag}")

    metadata = _read_metadata(path)
    names = [str(value) for value in metadata.get_all("Name", [])]
    versions = [str(value) for value in metadata.get_all("Version", [])]
    if len(names) != 1 or len(versions) != 1:
        raise ReleaseValidationError(f"{path.name} must declare exactly one Name and Version")
    distribution_name = names[0]
    version = _canonical_version(versions[0], f"{path.name} version")
    if canonicalize_name(distribution_name) != filename_name or Version(version) != filename_version:
        raise ReleaseValidationError(f"{path.name} filename and METADATA identities differ")
    try:
        requirements = tuple(Requirement(value) for value in metadata.get_all("Requires-Dist", []))
    except ValueError as error:
        raise ReleaseValidationError(f"{path.name} contains an invalid Requires-Dist") from error
    return WheelRecord(
        path=path,
        distribution_name=distribution_name,
        version=version,
        interpreter=tag.interpreter,
        requirements=requirements,
    )


def _exact_requirements(record: WheelRecord) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for requirement in record.requirements:
        name = canonicalize_name(requirement.name)
        specifiers = tuple(requirement.specifier)
        if (
            name in resolved
            or requirement.extras
            or requirement.url is not None
            or requirement.marker is not None
            or len(specifiers) != 1
            or specifiers[0].operator != "==="
        ):
            raise ReleaseValidationError(f"{record.path.name} must contain only unique exact === requirements")
        resolved[name] = specifiers[0].version
    return resolved


def validate_release(directory: Path, vane_version: str) -> dict[str, str]:
    """Validate a complete CPython 3.10-3.14 provider release set."""
    canonical_vane_version = _canonical_version(vane_version, "Vane version")
    paths = sorted(directory.expanduser().resolve().glob("*.whl"))
    records = tuple(_read_wheel(path) for path in paths)
    expected_names = {canonicalize_name(value) for value in PROVIDER_DISTRIBUTIONS.values()}
    if len(records) != 10 or {canonicalize_name(record.distribution_name) for record in records} != expected_names:
        raise ReleaseValidationError(
            f"release directory must contain exactly five Avro and five Iceberg wheels, found {len(records)}"
        )

    by_extension: dict[str, tuple[WheelRecord, ...]] = {}
    versions: dict[str, str] = {}
    for extension_name, distribution_name in PROVIDER_DISTRIBUTIONS.items():
        normalized_name = canonicalize_name(distribution_name)
        selected = tuple(record for record in records if canonicalize_name(record.distribution_name) == normalized_name)
        if len(selected) != 5 or {record.interpreter for record in selected} != EXPECTED_INTERPRETERS:
            raise ReleaseValidationError(
                f"{distribution_name} must contain exactly one wheel for each CPython 3.10 through 3.14"
            )
        selected_versions = {record.version for record in selected}
        if len(selected_versions) != 1:
            raise ReleaseValidationError(f"{distribution_name} wheels do not share one immutable version")
        by_extension[extension_name] = selected
        versions[extension_name] = next(iter(selected_versions))

    expected_avro_requirements = {canonicalize_name("vane-ai"): canonical_vane_version}
    expected_iceberg_requirements = {
        canonicalize_name("vane-ai"): canonical_vane_version,
        canonicalize_name("vane-extension-avro"): versions["avro"],
    }
    for record in by_extension["avro"]:
        if _exact_requirements(record) != expected_avro_requirements:
            raise ReleaseValidationError(
                f"{record.path.name} does not exactly require vane-ai {canonical_vane_version}"
            )
    for record in by_extension["iceberg"]:
        if _exact_requirements(record) != expected_iceberg_requirements:
            raise ReleaseValidationError(f"{record.path.name} does not exactly require vane-ai and vane-extension-avro")
    return versions


def _request_json(url: str) -> tuple[int, object | None]:
    request = urllib.request.Request(url, headers={"User-Agent": "vane-provider-release-validator/1"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None
    except (OSError, ValueError) as error:
        raise ReleaseValidationError(f"TestPyPI query failed for {url}: {error}") from error


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_wheel_hashes(directory: Path, distribution_name: str, version: str) -> dict[str, str]:
    normalized_name = canonicalize_name(distribution_name)
    canonical_version = _canonical_version(version, "provider version")
    expected = {
        path.name: _sha256(path)
        for path in sorted(directory.expanduser().resolve().glob("*.whl"))
        if parse_wheel_filename(path.name)[0] == normalized_name
        and str(parse_wheel_filename(path.name)[1]) == canonical_version
    }
    if len(expected) != 5:
        raise ReleaseValidationError(
            f"expected five local {distribution_name}=={canonical_version} wheels, found {len(expected)}"
        )
    return expected


def _indexed_wheel_hashes(document: object, distribution_name: str, version: str) -> dict[str, str]:
    if not isinstance(document, dict) or not isinstance(document.get("urls"), list):
        raise ReleaseValidationError(f"TestPyPI returned malformed metadata for {distribution_name}=={version}")

    actual: dict[str, str] = {}
    for item in document["urls"]:
        if not isinstance(item, dict):
            raise ReleaseValidationError(f"TestPyPI returned malformed files for {distribution_name}=={version}")
        filename = item.get("filename")
        digest = item.get("digests", {}).get("sha256") if isinstance(item.get("digests"), dict) else None
        if item.get("packagetype") != "bdist_wheel" or not isinstance(filename, str) or not isinstance(digest, str):
            raise ReleaseValidationError(
                f"TestPyPI returned a non-wheel or malformed file for {distribution_name}=={version}"
            )
        if filename in actual:
            raise ReleaseValidationError(
                f"TestPyPI returned duplicate file {filename} for {distribution_name}=={version}"
            )
        actual[filename] = digest
    if not actual:
        raise ReleaseValidationError(
            f"TestPyPI returned an existing version without wheels: {distribution_name}=={version}"
        )
    return actual


def require_indexes_publishable(directory: Path, versions: dict[str, str]) -> None:
    """Allow a first publish or an exact, potentially partial, immutable rerun."""
    for extension_name, version in versions.items():
        distribution_name = PROVIDER_DISTRIBUTIONS[extension_name]
        expected = _expected_wheel_hashes(directory, distribution_name, version)
        encoded_name = urllib.parse.quote(distribution_name, safe="")
        encoded_version = urllib.parse.quote(version, safe="")
        status, document = _request_json(f"{TESTPYPI_JSON_BASE}/{encoded_name}/{encoded_version}/json")
        if status == 404:
            continue
        if status != 200:
            raise ReleaseValidationError(
                f"expected absent or reusable TestPyPI version {distribution_name}=={version}, received HTTP {status}"
            )
        actual = _indexed_wheel_hashes(document, distribution_name, version)
        conflicts = {filename: digest for filename, digest in actual.items() if expected.get(filename) != digest}
        if conflicts:
            raise ReleaseValidationError(
                f"indexed wheel identities conflict for {distribution_name}=={version}: {conflicts}"
            )


def require_index_match(
    directory: Path,
    distribution_name: str,
    version: str,
    *,
    attempts: int,
    delay_seconds: int,
) -> None:
    """Wait for TestPyPI to expose exactly the locally assembled wheels."""
    canonical_version = _canonical_version(version, "provider version")
    expected = _expected_wheel_hashes(directory, distribution_name, canonical_version)

    encoded_name = urllib.parse.quote(distribution_name, safe="")
    encoded_version = urllib.parse.quote(canonical_version, safe="")
    url = f"{TESTPYPI_JSON_BASE}/{encoded_name}/{encoded_version}/json"
    last_problem = "the release was not indexed"
    for attempt in range(1, attempts + 1):
        try:
            status, document = _request_json(url)
            if status == 200:
                actual = _indexed_wheel_hashes(document, distribution_name, canonical_version)
                if actual == expected:
                    return
                last_problem = f"indexed wheel identities differ: expected={expected}, actual={actual}"
            else:
                last_problem = f"TestPyPI returned HTTP {status}"
        except ReleaseValidationError as error:
            last_problem = str(error)
        if attempt != attempts:
            time.sleep(delay_seconds)
    raise ReleaseValidationError(last_problem)


def _write_github_output(path: Path, vane_version: str, versions: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"vane_version={vane_version}\n")
        output.write(f"avro_version={versions['avro']}\n")
        output.write(f"iceberg_version={versions['iceberg']}\n")


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="validate a complete immutable release set")
    validate.add_argument("--directory", required=True, type=Path)
    validate.add_argument("--vane-version", required=True)
    validate.add_argument("--github-output", type=Path)
    validate.add_argument("--require-testpypi-publishable", action="store_true")

    verify = subparsers.add_parser("verify-index", help="compare one indexed release with local wheels")
    verify.add_argument("--directory", required=True, type=Path)
    verify.add_argument("--distribution-name", required=True, choices=tuple(PROVIDER_DISTRIBUTIONS.values()))
    verify.add_argument("--version", required=True)
    verify.add_argument("--attempts", default=5, type=int)
    verify.add_argument("--delay-seconds", default=15, type=int)
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    try:
        if arguments.command == "validate":
            versions = validate_release(arguments.directory, arguments.vane_version)
            if arguments.require_testpypi_publishable:
                require_indexes_publishable(arguments.directory, versions)
            if arguments.github_output is not None:
                _write_github_output(arguments.github_output, arguments.vane_version, versions)
            print(json.dumps({"vane_version": arguments.vane_version, **versions}, sort_keys=True))
        else:
            if arguments.attempts <= 0 or arguments.delay_seconds < 0:
                raise ReleaseValidationError("index retry settings must be non-negative and include an attempt")
            require_index_match(
                arguments.directory,
                arguments.distribution_name,
                arguments.version,
                attempts=arguments.attempts,
                delay_seconds=arguments.delay_seconds,
            )
    except (ReleaseValidationError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
