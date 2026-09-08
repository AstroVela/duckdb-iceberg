#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Focused production signing, preflight and publication-graph regressions."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

ROOT = Path(__file__).resolve().parents[2]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


builder = load_script("build_vane_dynamic_wheels")
preflight = load_script("vane_release_preflight")


class SigningTests(unittest.TestCase):
    def test_production_profile_uses_only_the_production_identity(self):
        self.assertEqual(builder.SIGNING_PROFILES["production"], ("astrovela/vane", None))
        self.assertEqual(
            builder.PRODUCTION_PUBLIC_KEY_SHA256, "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb"
        )

    def test_wrong_or_unparseable_key_is_rejected(self):
        for result in (
            subprocess.CompletedProcess([], 1, b"", b"parser error"),
            subprocess.CompletedProcess([], 0, b"a different public key", b""),
        ):
            with self.subTest(result=result), mock.patch.object(builder.subprocess, "run", return_value=result):
                with self.assertRaises(builder.QualificationError):
                    builder._require_production_key(bytearray(b"private test input"))

    def test_fingerprint_uses_public_der_without_logging_private_material(self):
        public = b"public DER fixture"
        secret = bytearray(b"private fixture, never a real production key")
        with (
            mock.patch.object(builder, "PRODUCTION_PUBLIC_KEY_SHA256", hashlib.sha256(public).hexdigest()),
            mock.patch.object(
                builder.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, public, b"")
            ) as run,
        ):
            builder._require_production_key(secret)
        self.assertEqual(run.call_args.args[0], ["openssl", "pkey", "-pubout", "-outform", "DER", "-passin", "pass:"])
        self.assertIs(run.call_args.kwargs["input"], secret)
        self.assertNotIn(secret.decode(), repr(run.call_args.args))

    def test_production_runtime_version_gate(self):
        for version in ("0.2.0", "0.2.0rc1", "0.2.0.post1"):
            builder._require_production_runtime(Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl"))
        for version in ("0.2.0.dev612", "0.2.0+local", "1!0.2.0", "0.2", "00.2.0"):
            with self.subTest(version=version), self.assertRaises(builder.QualificationError):
                builder._require_production_runtime(Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl"))

    def test_production_rejects_unconsumed_keys_and_local_runtime_before_key_access(self):
        for consume, local in ((False, False), (True, True)):
            args = argparse.Namespace(
                signing_profile="production", consume_signing_private_key=consume, package_local_runtime=local
            )
            with (
                self.subTest(consume=consume, local=local),
                mock.patch.object(builder, "_parse_arguments", return_value=args),
                mock.patch.object(builder, "_read_signing_private_key") as read,
                self.assertRaises(builder.QualificationError),
            ):
                builder.main()
            read.assert_not_called()

    def test_key_is_consumed_before_build_and_cleared_on_build_failure(self):
        with tempfile.TemporaryDirectory() as value:
            key = Path(value) / "key.pem"
            key.write_bytes(b"temporary test key")
            key.chmod(0o600)
            args = argparse.Namespace(
                signing_profile="production",
                consume_signing_private_key=True,
                package_local_runtime=False,
                signing_private_key=key,
            )
            observed = []

            def fail(_args, contents):
                self.assertFalse(key.exists())
                observed.append(contents)
                raise RuntimeError("build failed")

            with (
                mock.patch.object(builder, "_parse_arguments", return_value=args),
                mock.patch.object(builder, "_require_production_key"),
                mock.patch.object(builder, "_build", side_effect=fail),
                self.assertRaisesRegex(RuntimeError, "build failed"),
            ):
                builder.main()
            self.assertEqual(observed, [bytearray()])

    def test_cmake_enables_only_the_selected_testing_key(self):
        for profile, (_identity, selected) in builder.SIGNING_PROFILES.items():
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as value:
                root = Path(value)
                with mock.patch.object(builder, "_require_file"), mock.patch.dict(os.environ, {}, clear=True):
                    environment = builder._build_environment(
                        extension_root=root,
                        avro_source=root,
                        build_directory=root,
                        vane_vcpkg_installed=root,
                        vcpkg_toolchain=root / "toolchain.cmake",
                        jobs=2,
                        signing_cmake_option=selected,
                    )
                flags = shlex.split(environment["CMAKE_ARGS"])
                for option in ("VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY", "VANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY"):
                    self.assertIn(f"-D{option}={'ON' if option == selected else 'OFF'}", flags)
                self.assertNotIn("-DNone=ON", flags)


class PreflightTests(unittest.TestCase):
    CONTEXT = {
        "GITHUB_REPOSITORY": "AstroVela/duckdb-iceberg",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/v1.5-variegata_vane",
        "GITHUB_REF_PROTECTED": "true",
    }

    def test_only_manual_protected_default_branch_is_publishable(self):
        preflight.require_publishing_context(self.CONTEXT)
        for name in self.CONTEXT:
            with self.subTest(name=name), self.assertRaises(ValueError):
                preflight.require_publishing_context({**self.CONTEXT, name: "wrong"})

    def test_version_derivation_scrubs_overrides(self):
        environment = {
            "KEEP": "present",
            "GITHUB_BASE_REF": "release/0.2",
            "GITHUB_REF_NAME": "release/0.2",
            "VANE_VERSION_BRANCH": "release/0.2",
            "SETUPTOOLS_SCM_PRETEND_VERSION": "0.2.0",
            "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_VANE_AI": "0.2.0",
            "PYTHONPATH": "/untrusted",
            "PYTHONHOME": "/untrusted",
        }
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(
                preflight.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "0.2.0\n")
            ) as run,
        ):
            self.assertEqual(preflight.source_version(Path("vane")), "0.2.0")
        self.assertEqual(run.call_args.kwargs["env"], {"KEEP": "present"})

    def test_release_preflight_uses_shared_version_gate_and_emits_nothing_for_dev(self):
        validator = preflight.load_validator()
        for version in ("0.2.0.dev612", "0.2.0"):
            with tempfile.TemporaryDirectory() as value, self.subTest(version=version):
                output = Path(value) / "output"
                args = [
                    "--channel",
                    "release",
                    "--manifest",
                    str(ROOT / "vane-extension-release.toml"),
                    "--vane-source",
                    "vane",
                    "--ci-tools-version",
                    "a" * 40,
                    "--github-output",
                    str(output),
                ]
                with (
                    mock.patch.dict(os.environ, self.CONTEXT, clear=True),
                    mock.patch.object(preflight, "load_validator", return_value=validator),
                    mock.patch.object(validator, "verify_sources") as verify,
                    mock.patch.object(preflight, "source_version", return_value=version),
                    mock.patch.object(preflight.subprocess, "run") as ancestry,
                ):
                    if "dev" in version:
                        with self.assertRaises(validator.ReleaseValidationError):
                            preflight.main(args)
                        self.assertFalse(output.exists())
                    else:
                        self.assertEqual(preflight.main(args), 0)
                        self.assertEqual(output.read_text(), "vane_version=0.2.0\n")
                verify.assert_called_once()
                self.assertIn(preflight.PRODUCTION_KEY_REVISION, ancestry.call_args.args[0])


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.load((ROOT / ".github/workflows/VaneExtension.yml").read_text(), Loader=yaml.BaseLoader)
        cls.jobs = cls.workflow["jobs"]

    def test_default_build_only_and_signing_requires_preflight(self):
        self.assertEqual(self.workflow["on"]["workflow_dispatch"]["inputs"]["operation"]["default"], "build-only")
        self.assertNotIn("environment", self.jobs["release-preflight"])
        self.assertNotIn("secrets.", repr(self.jobs["release-preflight"]))
        self.assertEqual(self.jobs["vane-testpypi-wheels"]["needs"], "release-preflight")
        for job in (
            "vane-native",
            "vane-dynamic-wheels",
            "vane-wheel-iceberg-integration",
            "vane-wheel-ray-iceberg-integration",
        ):
            self.assertIn("inputs.operation != 'release'", self.jobs[job]["if"])

    def test_pypi_requires_both_smokes_and_immutable_full_graph_gate(self):
        for provider in ("avro", "iceberg"):
            job = self.jobs[f"publish-pypi-{provider}"]
            self.assertEqual(job["if"], "inputs.operation == 'release'")
            self.assertEqual(job["environment"]["name"], f"pypi-{provider}")
            for dependency in ("testpypi-local-iceberg-integration", "testpypi-ray-iceberg-integration"):
                self.assertIn(dependency, job["needs"])
            steps = job["steps"]
            promotion = next(i for i, step in enumerate(steps) if "verify-promotion" in step.get("run", ""))
            upload = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("pypa/"))
            self.assertLess(promotion, upload)
            self.assertIn("--directory packages", steps[promotion]["run"])
            self.assertIn("--index pypi", steps[-1]["run"])
            self.assertIn("vane-testpypi-provider-wheels", repr(steps))
            self.assertNotIn("build_vane_dynamic_wheels.py", repr(steps))
        self.assertIn("publish-pypi-avro", self.jobs["publish-pypi-iceberg"]["needs"])

    def test_smokes_compare_both_providers_and_select_runtime_index(self):
        for name in ("testpypi-local-iceberg-integration", "testpypi-ray-iceberg-integration"):
            script = "\n".join(step.get("run", "") for step in self.jobs[name]["steps"])
            self.assertIn('cmp "${expected_avro[0]}" "${avro_wheels[0]}"', script)
            self.assertIn('cmp "${expected_iceberg[0]}" "${iceberg_wheels[0]}"', script)
            self.assertIn('download_exact "vane-ai==$VANE_VERSION" "$VANE_RUNTIME_INDEX_URL"', script)

    def test_every_artifact_download_fails_on_digest_mismatch(self):
        for job in self.jobs.values():
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/download-artifact@"):
                    self.assertEqual(step["with"]["digest-mismatch"], "error")
                    self.assertIn("@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", step["uses"])


if __name__ == "__main__":
    unittest.main()
