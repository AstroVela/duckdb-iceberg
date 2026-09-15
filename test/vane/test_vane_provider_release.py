#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise the shared release CLI with this repository's two-provider contract."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPOSITORY_ROOT / "vane-provider-release.toml"
VANE_VERSION = "0.2.0.dev612"
VERSIONS = {"avro": "0.2.0.0.612.1", "iceberg": "0.2.0.0.612.2"}
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")
PLATFORM = "manylinux_2_28_x86_64"


def load_validator():
    path = REPOSITORY_ROOT / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    specification = importlib.util.spec_from_file_location("vane_iceberg_release_validator", path)
    if specification is None or specification.loader is None:
        raise AssertionError(f"could not load {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def write_wheels(
    directory: Path, provider: str, *, avro_requirement: str | None = None, vane_version: str = VANE_VERSION
) -> list[Path]:
    distribution = f"vane_extension_{provider}"
    version = VERSIONS[provider]
    requirements = [f"vane-ai==={vane_version}"]
    if provider == "iceberg":
        requirements.append(avro_requirement or f"vane-extension-avro==={VERSIONS['avro']}")
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: vane-extension-{provider}\n"
        f"Version: {version}\n" + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements) + "\n"
    )
    paths = []
    for interpreter in INTERPRETERS:
        path = directory / f"{distribution}-{version}-{interpreter}-none-{PLATFORM}.whl"
        with zipfile.ZipFile(path, "w") as wheel:
            wheel.writestr(f"{distribution}-{version}.dist-info/METADATA", metadata)
        paths.append(path)
    return paths


def source_arguments(directory: Path) -> list[str]:
    return [
        "--manifest",
        str(REPOSITORY_ROOT / "vane-extension.toml"),
        "--extension-root",
        str(REPOSITORY_ROOT),
        "--vane-source",
        str(directory / "vane"),
        "--ci-tools-version",
        "a" * 40,
        "--config",
        str(CONFIG_PATH),
        "--directory",
        str(directory),
    ]


class ProviderReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validator = load_validator()
        cls.config = cls.validator.load_config(CONFIG_PATH)

    @classmethod
    def tearDownClass(cls) -> None:
        sys.modules.pop(cls.validator.__name__, None)

    def test_configured_matrix_and_dependency_graph(self) -> None:
        self.assertEqual(self.config.interpreters, INTERPRETERS)
        self.assertEqual(self.config.platforms, (PLATFORM,))
        self.assertEqual(self.config.max_wheel_bytes, 100000000)
        self.assertEqual(
            [(provider.name, provider.distribution, provider.dependencies) for provider in self.config.providers],
            [("avro", "vane-extension-avro", ()), ("iceberg", "vane-extension-iceberg", ("avro",))],
        )

    def test_complete_release_cli_outputs(self) -> None:
        # Generic matrix/index edge cases live in the shared tools repository.
        # This smoke test uses the actual consumer config and workflow outputs.
        with tempfile.TemporaryDirectory(prefix="vane-iceberg-release-") as value:
            directory = Path(value)
            for provider in VERSIONS:
                write_wheels(directory, provider)
            outputs = directory / "github-output"
            command = ["validate", *source_arguments(directory)]
            command += [
                "--vane-version",
                VANE_VERSION,
                "--github-output",
                str(outputs),
                "--channel",
                "testpypi-dev",
                "--require-publishable-on",
                "testpypi",
            ]
            output = io.StringIO()
            with (
                mock.patch.object(self.validator, "verify_sources") as verify,
                mock.patch.object(self.validator, "_request_json", return_value=(404, None)) as query,
                redirect_stdout(output),
            ):
                self.assertEqual(self.validator.main(command), 0)
            verify.assert_called_once_with(
                REPOSITORY_ROOT / "vane-extension.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
            )
            self.assertEqual(query.call_count, 2)
            expected = {
                "vane_version": VANE_VERSION,
                **{f"{name}_version": version for name, version in VERSIONS.items()},
            }
            self.assertEqual(json.loads(output.getvalue()), expected)
            self.assertEqual(dict(line.split("=", 1) for line in outputs.read_text().splitlines()), expected)

    def test_iceberg_requires_the_assembled_avro_version_exactly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="vane-iceberg-dependency-") as value:
            directory = Path(value)
            write_wheels(directory, "avro")
            for requirement in ("vane-extension-avro>=0.2", "vane-extension-avro===0.2.0.0.612.9"):
                with self.subTest(requirement=requirement):
                    write_wheels(directory, "iceberg", avro_requirement=requirement)
                    command = [
                        "validate",
                        *source_arguments(directory),
                        "--channel",
                        "testpypi-dev",
                        "--vane-version",
                        VANE_VERSION,
                    ]
                    with mock.patch.object(self.validator, "verify_sources"), redirect_stderr(io.StringIO()):
                        self.assertEqual(self.validator.main(command), 2)

    def test_index_cli_accepts_each_assembled_provider_directory(self) -> None:
        # The Avro index gate runs before Iceberg is published. Each gate must
        # accept exactly the provider artifact downloaded by its upload job.
        for provider, version in VERSIONS.items():
            with self.subTest(provider=provider), tempfile.TemporaryDirectory(prefix="vane-iceberg-index-") as value:
                directory = Path(value)
                paths = write_wheels(directory, provider)
                document = {
                    "urls": [
                        {
                            "filename": path.name,
                            "packagetype": "bdist_wheel",
                            "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                            "yanked": False,
                        }
                        for path in paths
                    ]
                }
                command = ["verify-index", *source_arguments(directory), "--index", "testpypi"]
                command += ["--provider", provider, "--version", version, "--attempts", "1", "--delay-seconds", "0"]
                with (
                    mock.patch.object(self.validator, "verify_sources") as verify,
                    mock.patch.object(self.validator, "_request_json", return_value=(200, document)) as query,
                ):
                    self.assertEqual(self.validator.main(command), 0)
                verify.assert_called_once_with(
                    REPOSITORY_ROOT / "vane-extension.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
                )
                query.assert_called_once_with(f"https://test.pypi.org/pypi/vane-extension-{provider}/{version}/json")

    def test_release_channel_rejects_development_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            for provider in VERSIONS:
                write_wheels(directory, provider)
            command = ["validate", *source_arguments(directory), "--channel", "release", "--vane-version", VANE_VERSION]
            with mock.patch.object(self.validator, "verify_sources"), redirect_stderr(io.StringIO()):
                self.assertEqual(self.validator.main(command), 2)

    def test_promote_requires_the_complete_identical_avro_iceberg_graph(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            indexed = {}
            for provider in VERSIONS:
                paths = write_wheels(directory, provider, vane_version="0.2.0")
                indexed[provider] = {
                    "urls": [
                        {
                            "filename": path.name,
                            "packagetype": "bdist_wheel",
                            "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                            "yanked": False,
                        }
                        for path in paths
                    ]
                }

            def query(url):
                if url.startswith("https://pypi.org/"):
                    return 404, None
                for provider, document in indexed.items():
                    if f"/vane-extension-{provider}/" in url:
                        return 200, document
                self.fail(f"unexpected index URL: {url}")

            command = ["verify-promotion", *source_arguments(directory), "--vane-version", "0.2.0", "--attempts", "1"]
            with (
                mock.patch.object(self.validator, "verify_sources"),
                mock.patch.object(self.validator, "_request_json", side_effect=query) as request,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(self.validator.main(command), 0)
                self.assertEqual(request.call_count, 4)
                indexed["avro"]["urls"][0]["digests"]["sha256"] = "0" * 64
                with redirect_stderr(io.StringIO()):
                    self.assertEqual(self.validator.main(command), 2)

    def test_integration_source_pins(self) -> None:
        with (REPOSITORY_ROOT / "vane-extension.toml").open("rb") as source:
            manifest = tomllib.load(source)
        vcpkg = json.loads((REPOSITORY_ROOT / "vcpkg.json").read_text())
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(manifest["vane"]["revision"], "88b5b75a6bfd51b03998ca457083db6dbbe51bb8")
        release = tomllib.loads((REPOSITORY_ROOT / "vane-extension-release.toml").read_text())
        self.assertEqual(release["schema_version"], 2)
        self.assertEqual(release["vane"], manifest["vane"])
        self.assertEqual(release["vcpkg"], manifest["vcpkg"])
        self.assertEqual(manifest["vcpkg"]["repository"], "microsoft/vcpkg")
        self.assertEqual(manifest["vcpkg"]["revision"], vcpkg["builtin-baseline"])
        tools = REPOSITORY_ROOT / "vane-extension-ci-tools"
        actual = subprocess.check_output(["git", "-C", str(tools), "rev-parse", "HEAD"], text=True).strip()
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        self.assertEqual(workflow.count(actual), 4)
        self.assertNotIn("scripts/validate_vane_provider_release.py", workflow)

    def test_index_jobs_verify_the_published_artifacts_in_dependency_order(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()

        def job(name: str) -> str:
            match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [a-z][\w-]*:|\Z)", workflow)
            self.assertIsNotNone(match, name)
            return match.group(1)

        for provider in VERSIONS:
            with self.subTest(provider=provider):
                publish = job(f"publish-testpypi-{provider}")
                verify = job(f"verify-testpypi-{provider}")
                for fragment in (publish, verify):
                    self.assertIn(f"needs.assemble-testpypi-providers.outputs.{provider}_artifact_id", fragment)
                    self.assertIn("path: dist", fragment)
                    self.assertNotIn("name: vane-testpypi-", fragment)
                self.assertIn(f"--provider {provider} \\\n", verify)
                self.assertIn("--directory dist \\\n", verify)
                self.assertIn("--vane-source vane \\\n", verify)
                self.assertIn('--ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)"', verify)
                self.assertIn(f"needs: [assemble-testpypi-providers, publish-testpypi-{provider}]", verify)
        self.assertIn("needs: [assemble-testpypi-providers, verify-testpypi-avro]", job("publish-testpypi-iceberg"))


if __name__ == "__main__":
    unittest.main()
