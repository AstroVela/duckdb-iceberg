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
signer = load_script("sign_vane_release")
packager = load_script("package_vane_dynamic_wheels")


class SigningTests(unittest.TestCase):
    def test_prepare_rejects_keys_and_never_reads_them(self):
        for key, consume, local in ((Path("key.pem"), False, False), (None, True, False), (None, False, True)):
            args = argparse.Namespace(
                phase="prepare",
                signing_profile="production",
                signing_private_key=key,
                consume_signing_private_key=consume,
                package_local_runtime=local,
            )
            with (
                self.subTest(key=key, consume=consume, local=local),
                mock.patch.object(builder, "_parse_arguments", return_value=args),
                mock.patch.object(builder, "_read_signing_private_key") as read,
                self.assertRaises(builder.QualificationError),
            ):
                builder.main()
            read.assert_not_called()
        args.signing_private_key = None
        args.consume_signing_private_key = args.package_local_runtime = False
        with (
            mock.patch.object(builder, "_parse_arguments", return_value=args),
            mock.patch.object(builder, "_build", return_value=0) as build,
        ):
            self.assertEqual(builder.main(), 0)
        build.assert_called_once_with(args, None)

    def test_no_publishing_profile_can_sign_inside_build(self):
        for profile in ("production", "testpypi"):
            with (
                self.subTest(profile=profile),
                mock.patch.object(
                    builder, "_parse_arguments", return_value=argparse.Namespace(phase="full", signing_profile=profile)
                ),
                mock.patch.object(builder, "_read_signing_private_key") as read,
                self.assertRaisesRegex(builder.QualificationError, "isolated signer"),
            ):
                builder.main()
            read.assert_not_called()

    def test_system_signer_has_no_site_package_requirement(self):
        subprocess.run(
            ["/usr/bin/python3", "-I", "-S", str(ROOT / "scripts/sign_vane_release.py"), "--help"],
            check=True,
            capture_output=True,
            timeout=30,
        )

    def test_openssl_fingerprint_round_trip_with_throwaway_key(self):
        # Generated in memory for this test; never access a configured signing key.
        generated = subprocess.run(
            ["/usr/bin/openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
            check=True,
            capture_output=True,
            timeout=30,
        ).stdout
        public = subprocess.run(
            ["/usr/bin/openssl", "pkey", "-pubout", "-outform", "DER"],
            input=generated,
            check=True,
            capture_output=True,
            timeout=30,
        ).stdout
        with mock.patch.dict(signer.KEY_FINGERPRINTS, {"production": hashlib.sha256(public).hexdigest()}):
            signer.require_key(bytearray(generated), "production")
        with self.assertRaises(ValueError):
            signer.require_key(bytearray(generated), "testpypi")

    def test_unsigned_input_requires_bounded_regular_file_and_empty_signature(self):
        with tempfile.TemporaryDirectory() as value:
            artifact = Path(value) / "artifact"
            artifact.write_bytes(b"payload" + b"\0" * 256)
            signer.require_artifact(artifact)
            link = Path(value) / "link"
            link.symlink_to(artifact)
            with self.assertRaises(ValueError):
                signer.require_artifact(link)
            with mock.patch.object(signer, "MAX_ARTIFACT_BYTES", 256), self.assertRaises(ValueError):
                signer.require_artifact(artifact)
            for payload in (b"\0" * 256, b"payload" + b"s" * 256):
                artifact.write_bytes(payload)
                with self.assertRaises(ValueError):
                    signer.require_artifact(artifact)

    def test_isolated_signer_unsets_key_and_cleans_private_file_on_success_or_failure(self):
        for fail in (False, True):
            with tempfile.TemporaryDirectory() as value, self.subTest(fail=fail):
                root = Path(value)
                inputs = root / "inputs"
                inputs.mkdir()
                for name in signer.NAMES:
                    (inputs / f"{name}.duckdb_extension").write_bytes(b"payload" + b"\0" * 256)
                args = argparse.Namespace(
                    manifest=ROOT / "vane-extension-release.toml",
                    vane_source=root / "vane",
                    input_directory=inputs,
                    output_directory=root / "signed",
                    profile="production",
                )
                revision = signer.read_manifest(args.manifest)
                observed = []
                key_paths = []

                def run(command, **kwargs):
                    self.assertNotIn("VANE_SIGNING_PRIVATE_KEY", os.environ)
                    self.assertNotIn("VANE_SIGNING_PRIVATE_KEY", kwargs["env"])
                    if command[0] == "/usr/bin/git":
                        return subprocess.CompletedProcess(command, 0, revision + "\n")
                    self.assertEqual(command[1:3], ["-I", "-S"])
                    key = Path(command[command.index("--private-key") + 1])
                    key_paths.append(key)
                    self.assertEqual(key.read_bytes(), b"fixture key")
                    self.assertEqual(key.stat().st_mode & 0o777, 0o600)
                    self.assertEqual(key.parent.stat().st_mode & 0o777, 0o700)
                    if fail:
                        raise RuntimeError("fixture signer failure")
                    return subprocess.CompletedProcess(command, 0)

                with (
                    mock.patch.dict(os.environ, {"VANE_SIGNING_PRIVATE_KEY": "fixture key", "RUNNER_TEMP": value}),
                    mock.patch.object(
                        signer, "require_key", side_effect=lambda contents, _profile: observed.append(contents)
                    ),
                    mock.patch.object(signer.subprocess, "run", side_effect=run),
                ):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, "fixture signer failure"):
                            signer.sign(args)
                    else:
                        signer.sign(args)
                self.assertEqual(observed, [bytearray()])
                self.assertTrue(key_paths)
                self.assertTrue(all(not path.exists() and not path.parent.exists() for path in key_paths))

    def test_signer_unsets_key_even_when_manifest_validation_fails(self):
        with (
            mock.patch.dict(os.environ, {"VANE_SIGNING_PRIVATE_KEY": "fixture key"}),
            mock.patch.object(signer, "read_manifest", side_effect=ValueError("manifest failure")),
            mock.patch.object(signer.subprocess, "run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "manifest failure"):
                signer.sign(argparse.Namespace(manifest=Path("manifest")))
            self.assertNotIn("VANE_SIGNING_PRIVATE_KEY", os.environ)
        run.assert_not_called()

    def test_production_profile_uses_only_the_production_identity(self):
        self.assertEqual(builder.SIGNING_PROFILES["production"], ("astrovela/vane", None))
        self.assertEqual(
            signer.KEY_FINGERPRINTS["production"], "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb"
        )

    def test_wrong_or_unparseable_key_is_rejected(self):
        for result in (
            subprocess.CompletedProcess([], 1, b"", b"parser error"),
            subprocess.CompletedProcess([], 0, b"a different public key", b""),
        ):
            with self.subTest(result=result), mock.patch.object(signer.subprocess, "run", return_value=result):
                with self.assertRaises(ValueError):
                    signer.require_key(bytearray(b"private test input"), "production")

    def test_fingerprint_uses_public_der_without_logging_private_material(self):
        public = b"public DER fixture"
        secret = bytearray(b"private fixture, never a real production key")
        with (
            mock.patch.dict(signer.KEY_FINGERPRINTS, {"production": hashlib.sha256(public).hexdigest()}),
            mock.patch.object(
                signer.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, public, b"")
            ) as run,
        ):
            signer.require_key(secret, "production")
        self.assertEqual(
            run.call_args.args[0], ["/usr/bin/openssl", "pkey", "-pubout", "-outform", "DER", "-passin", "pass:"]
        )
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
                phase="full",
                signing_profile="production",
                consume_signing_private_key=consume,
                package_local_runtime=local,
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
                phase="full",
                signing_profile="ci-test",
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


class PackagingTests(unittest.TestCase):
    def test_packaging_accepts_only_signature_slot_changes(self):
        with tempfile.TemporaryDirectory() as value:
            unsigned = Path(value) / "unsigned"
            signed = Path(value) / "signed"
            unsigned.write_bytes(b"original" + b"\0" * 256)
            signed.write_bytes(b"original" + b"s" * 256)
            self.assertEqual(packager.require_signed_payload(unsigned, signed), signed)
            for contents in (b"changed!" + b"s" * 256, b"original" + b"\0" * 256, b"extra" + b"original" + b"s" * 256):
                signed.write_bytes(contents)
                with self.subTest(contents=contents[:8]), self.assertRaises(packager.builder.QualificationError):
                    packager.require_signed_payload(unsigned, signed)
            link = Path(value) / "link"
            link.symlink_to(signed)
            with self.assertRaises(packager.builder.QualificationError):
                packager.require_signed_payload(unsigned, link)

    def test_packaging_reuses_native_data_and_exact_dependency_without_building(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            unsigned, signed, output = (root / name for name in ("unsigned", "signed", "dist"))
            (unsigned / "artifacts").mkdir(parents=True)
            signed.mkdir()
            for name in signer.NAMES:
                (unsigned / "artifacts" / f"{name}.duckdb_extension").write_bytes(b"original" + b"\0" * 256)
                (signed / f"{name}.duckdb_extension").write_bytes(b"original" + b"s" * 256)
                licenses = unsigned / "licenses" / name
                licenses.mkdir(parents=True)
                for filename in packager.LICENSE_NAMES[name]:
                    (licenses / filename).write_text("license fixture")
            argv = [
                "package",
                "--vane-source",
                str(root / "vane"),
                "--vane-revision",
                "a" * 40,
                "--unsigned-directory",
                str(unsigned),
                "--signed-directory",
                str(signed),
                "--output-directory",
                str(output),
                "--signing-profile",
                "production",
            ]
            for tag in ("cp311", "cp312"):
                argv.extend(
                    [
                        "--runtime-python",
                        str(root / tag),
                        "--runtime-wheel",
                        str(root / f"vane_ai-0.2.0-{tag}-{tag}-manylinux_2_28_x86_64.whl"),
                    ]
                )

            def wheel(**kwargs):
                path = (
                    kwargs["output_directory"]
                    / f"vane_extension_{kwargs['extension_name']}-0.2.0-{kwargs['python'].name}-none-manylinux_2_28_x86_64.whl"
                )
                path.write_bytes(b"wheel fixture")
                return path

            isolated = mock.Mock()
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(packager.builder, "_require_git_revision"),
                mock.patch.object(packager.builder, "_platform_tag", return_value="manylinux_2_28_x86_64"),
                mock.patch.object(
                    packager.builder, "_builder_python", side_effect=lambda python, _wheel, _stage: (isolated, python)
                ),
                mock.patch.object(packager.builder, "_build_provider_wheel", side_effect=wheel) as build,
                mock.patch.object(packager.builder, "_run") as run,
            ):
                self.assertEqual(packager.main(), 0)
            self.assertEqual(len(list(output.glob("*.whl"))), 4)
            self.assertEqual(isolated.cleanup.call_count, 2)
            for call in build.call_args_list:
                options = call.kwargs
                self.assertEqual(options["trust_identity"], "astrovela/vane")
                self.assertEqual(options["artifact"].parent, signed)
                if options["extension_name"] == "iceberg":
                    self.assertTrue(options["dependency_wheel"].name.startswith("vane_extension_avro-"))
            for call in run.call_args_list:
                self.assertIn(str(root / "vane/scripts/verify_extension_wheel.py"), call.args[0])
                self.assertIn("--base-wheel", call.args[0])
                self.assertIn("--dependency-wheel", call.args[0])


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
        self.assertEqual(self.jobs["vane-provider-prepare"]["needs"], "release-preflight")
        self.assertEqual(self.jobs["vane-provider-sign"]["needs"], "vane-provider-prepare")
        self.assertEqual(self.jobs["vane-testpypi-wheels"]["needs"], ["vane-provider-prepare", "vane-provider-sign"])
        for job in (
            "vane-native",
            "vane-dynamic-wheels",
            "vane-wheel-iceberg-integration",
            "vane-wheel-ray-iceberg-integration",
        ):
            self.assertIn("inputs.operation != 'release'", self.jobs[job]["if"])

    def test_pypi_requires_both_smokes_and_immutable_full_graph_gate(self):
        for provider in ("avro", "iceberg"):
            gate = self.jobs[f"verify-pypi-{provider}-promotion"]
            self.assertEqual(gate["permissions"], {"contents": "read"})
            self.assertEqual(gate["environment"]["name"], f"pypi-{provider}")
            for dependency in ("testpypi-local-iceberg-integration", "testpypi-ray-iceberg-integration"):
                self.assertIn(dependency, gate["needs"])
            self.assertIn("verify-promotion", repr(gate["steps"]))
            self.assertIn("candidate_artifact_id", repr(gate["steps"]))
            self.assertIn("--directory packages", repr(gate["steps"]))
            publisher = self.jobs[f"publish-pypi-{provider}"]
            self.assertIn(f"verify-pypi-{provider}-promotion", publisher["needs"])
            self.assertEqual(publisher["if"], "inputs.operation == 'release'")
            self.assertEqual(publisher["permissions"]["id-token"], "write")
            self.assertEqual(len(publisher["steps"]), 2)
            self.assertTrue(all("run" not in step for step in publisher["steps"]))
            self.assertIn(f"needs.assemble-testpypi-providers.outputs.{provider}_artifact_id", repr(publisher))
            indexed = self.jobs[f"verify-pypi-{provider}"]
            self.assertEqual(indexed["permissions"], {"contents": "read"})
            self.assertNotIn("environment", indexed)
            self.assertIn("--index pypi", repr(indexed))
        self.assertIn("verify-pypi-avro", self.jobs["verify-pypi-iceberg-promotion"]["needs"])

    def test_private_key_job_cannot_execute_mutable_build_or_package_tools(self):
        for name in ("vane-provider-prepare", "vane-testpypi-wheels"):
            job = self.jobs[name]
            self.assertNotIn("environment", job)
            self.assertEqual(job["permissions"], {"contents": "read"})
            self.assertNotIn("secrets", repr(job))
            self.assertNotIn("--signing-private-key", repr(job))
        signing = self.jobs["vane-provider-sign"]
        self.assertEqual(signing["permissions"], {"contents": "read"})
        script = "\n".join(step.get("run", "") for step in signing["steps"])
        self.assertNotIn("pip", script)
        self.assertNotIn("build_vane_dynamic_wheels", script)
        self.assertIn("/usr/bin/python3 -I -S scripts/sign_vane_release.py", script)
        self.assertIn("needs.vane-provider-prepare.outputs.artifact_id", repr(signing))
        self.assertNotIn("needs.vane-provider-prepare.outputs.vane_revision", repr(signing))
        self.assertEqual(signing["steps"][-1]["with"]["path"], "signed/*.duckdb_extension")

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
