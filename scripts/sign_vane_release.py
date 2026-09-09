#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Minimal data-only signer. Run with system Python -I -S, never pip tooling."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

NAMES = ("avro", "iceberg")
MAX_ARTIFACT_BYTES = 384 * 1024 * 1024
KEY_FINGERPRINTS = {
    "production": "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb",
    "testpypi": "53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32",
}


def read_manifest(path: Path) -> str:
    manifest = tomllib.loads(path.read_text(encoding="utf-8"))
    vane = manifest["vane"]
    revision = vane["revision"]
    if vane["repository"] != "AstroVela/vane" or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("signing requires the committed exact official Vane source")
    return revision


def require_key(contents: bytearray, profile: str) -> None:
    result = subprocess.run(
        ["/usr/bin/openssl", "pkey", "-pubout", "-outform", "DER", "-passin", "pass:"],
        input=contents,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    if result.returncode or hashlib.sha256(result.stdout).hexdigest() != KEY_FINGERPRINTS[profile]:
        raise ValueError("signing key does not match the selected reviewed public fingerprint")


def require_artifact(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or not 256 < info.st_size <= MAX_ARTIFACT_BYTES:
        raise ValueError("signing input must be a bounded regular native artifact, never a symlink")
    with path.open("rb") as artifact:
        artifact.seek(-256, os.SEEK_END)
        if artifact.read() != b"\0" * 256:
            raise ValueError("signing input must have an empty native signature slot")


def sign(args: argparse.Namespace) -> None:
    contents = bytearray(os.environ.pop("VANE_SIGNING_PRIVATE_KEY").encode())
    try:
        _sign(args, contents)
    finally:
        contents[:] = b"\0" * len(contents)
        contents.clear()


def _sign(args: argparse.Namespace, contents: bytearray) -> None:
    # The source pin is read again from trusted checkout code, not job outputs
    # or any file supplied by the native build artifact.
    revision = read_manifest(args.manifest)
    actual = subprocess.run(
        ["/usr/bin/git", "-C", str(args.vane_source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    ).stdout.strip()
    if actual != revision:
        raise ValueError("signing utility checkout differs from the committed Vane manifest")
    if set(path.name for path in args.input_directory.iterdir()) != {f"{name}.duckdb_extension" for name in NAMES}:
        raise ValueError("signer accepts only the fixed Avro and Iceberg native inputs")
    for name in NAMES:
        require_artifact(args.input_directory / f"{name}.duckdb_extension")
    args.output_directory.mkdir(parents=True, exist_ok=True)
    if any(args.output_directory.iterdir()):
        raise ValueError("signer output directory must be empty")
    if not 0 < len(contents) <= 64 * 1024:
        raise ValueError("signing key must be non-empty and bounded")
    require_key(contents, args.profile)
    with tempfile.TemporaryDirectory(prefix=".vane-release-key-", dir=os.environ["RUNNER_TEMP"]) as value:
        key = Path(value) / "private.pem"
        try:
            with os.fdopen(os.open(key, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as output:
                output.write(contents)
            for name in NAMES:
                subprocess.run(
                    [
                        sys.executable,
                        "-I",
                        "-S",
                        str(args.vane_source / "scripts/sign_test_dynamic_extension.py"),
                        "--private-key",
                        str(key),
                        str(args.input_directory / f"{name}.duckdb_extension"),
                        str(args.output_directory / f"{name}.duckdb_extension"),
                    ],
                    check=True,
                    env={"PATH": "/usr/bin:/bin", "TMPDIR": os.environ["RUNNER_TEMP"]},
                )
        finally:
            if key.exists():
                with key.open("r+b") as output:
                    output.write(b"\0" * len(contents))
                    output.flush()
                    os.fsync(output.fileno())
                key.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    manifest = commands.add_parser("manifest")
    manifest.add_argument("--github-output", required=True, type=Path)
    signing = commands.add_parser("sign")
    signing.add_argument("--vane-source", required=True, type=Path)
    signing.add_argument("--input-directory", required=True, type=Path)
    signing.add_argument("--output-directory", required=True, type=Path)
    signing.add_argument("--profile", required=True, choices=tuple(KEY_FINGERPRINTS))
    args = parser.parse_args()
    if args.command == "manifest":
        revision = read_manifest(args.manifest)
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"vane_revision={revision}\n")
    else:
        sign(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
