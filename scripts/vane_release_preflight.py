#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Read-only publication gate; run before entering a signing environment."""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_KEY_REVISION = "033b549afcb498633fd6669b26c054c00363004e"


def require_publishing_context(environment: dict[str, str]) -> None:
    expected = {
        "GITHUB_REPOSITORY": "AstroVela/duckdb-iceberg",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/v1.5-variegata_vane",
        "GITHUB_REF_PROTECTED": "true",
    }
    for name, value in expected.items():
        if environment.get(name) != value:
            raise ValueError(f"publication requires {name}={value}")


def source_version(vane_source: Path) -> str:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION")
        and name not in {"GITHUB_BASE_REF", "GITHUB_REF_NAME", "VANE_VERSION_BRANCH", "PYTHONPATH", "PYTHONHOME"}
    }
    return subprocess.run(
        [sys.executable, "-m", "setuptools_scm"],
        cwd=vane_source,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def load_validator():
    path = ROOT / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    specification = importlib.util.spec_from_file_location("_iceberg_release_preflight_validator", path)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load shared validator: {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True, choices=("testpypi-dev", "release"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--ci-tools-version", required=True)
    parser.add_argument("--github-output", required=True, type=Path)
    args = parser.parse_args(argv)
    require_publishing_context(dict(os.environ))
    validator = load_validator()
    validator.verify_sources(args.manifest, ROOT, args.vane_source, args.ci_tools_version)
    if args.channel == "release":
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", PRODUCTION_KEY_REVISION, "HEAD"],
            cwd=args.vane_source,
            check=True,
        )
    version = source_version(args.vane_source)
    validator.validate_vane_version(version, args.channel)
    with args.github_output.open("a", encoding="utf-8") as output:
        output.write(f"vane_version={version}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
