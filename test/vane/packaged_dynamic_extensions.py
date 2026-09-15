"""Helpers for exercising the installed Avro and Iceberg provider wheels."""

from __future__ import annotations

import os
from importlib import import_module
from importlib.metadata import entry_points


def load_packaged_dynamic_iceberg(connection: object) -> None:
    """Load the exact installed Avro -> Iceberg descriptor graph."""
    from vane.extensions import DynamicExtensionDescriptor, DynamicExtensionResolver, LocalExtensionProvider

    trust_identity = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if not trust_identity:
        raise AssertionError("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY must name the explicit test trust root")

    installed = tuple(entry_points(group="vane.dynamic_extension_providers"))
    descriptors: dict[str, DynamicExtensionDescriptor] = {}
    providers: list[LocalExtensionProvider] = []
    for extension_name in ("avro", "iceberg"):
        matches = [candidate for candidate in installed if candidate.name == extension_name]
        if len(matches) != 1:
            raise AssertionError(
                f"expected exactly one installed {extension_name!r} provider entry point, found {len(matches)}"
            )
        entry_point = matches[0]
        provider = entry_point.load()()
        if not isinstance(provider, LocalExtensionProvider):
            raise AssertionError(f"{extension_name!r} entry point did not return LocalExtensionProvider")
        descriptor = import_module(entry_point.module).descriptor()
        if descriptor.name != extension_name:
            raise AssertionError(f"{extension_name!r} provider returned descriptor for {descriptor.name!r}")
        if descriptor.trust_identity != trust_identity:
            raise AssertionError(
                f"{extension_name!r} descriptor uses unexpected trust identity {descriptor.trust_identity!r}"
            )
        artifact = provider.find(descriptor.identity)
        if artifact is None or artifact.descriptor != descriptor:
            raise AssertionError(f"{extension_name!r} provider does not own its exact descriptor identity")
        descriptors[extension_name] = descriptor
        providers.append(provider)

    avro = descriptors["avro"]
    iceberg = descriptors["iceberg"]
    if avro.dependencies:
        raise AssertionError("the Avro wheel must be the leaf of the dynamic extension graph")
    if tuple(dependency.identity for dependency in iceberg.dependencies) != (avro.identity,):
        raise AssertionError("the Iceberg wheel must declare the exact Avro wheel as its sole dynamic dependency")

    security = connection.execute(
        """
        SELECT
            current_setting('allow_unsigned_extensions'),
            current_setting('autoinstall_known_extensions'),
            current_setting('autoload_known_extensions')
        """
    ).fetchone()
    if security != (False, False, False):
        raise AssertionError(f"dynamic extension security settings are not fail-closed: {security!r}")

    def extension_state(extension_name: str) -> tuple:
        # Direct system-table reads are connection metadata operations. Apply
        # filtering in Python to stay inside Vane's native read allowlist.
        rows = connection.execute(
            "SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions()"
        ).fetchall()
        matches = [row[1:] for row in rows if row[0] == extension_name]
        if len(matches) != 1:
            raise AssertionError(f"expected one extension state for {extension_name!r}, got {matches!r}")
        return matches[0]

    for extension_name in ("avro", "iceberg"):
        state = extension_state(extension_name)
        if state != (False, False, "NOT_INSTALLED"):
            raise AssertionError(
                f"{extension_name!r} was already installed or linked before resolver loading: {state!r}"
            )

    resolved = DynamicExtensionResolver(
        trusted_identities={trust_identity},
        providers=providers,
    ).load(connection, iceberg)
    if resolved.descriptor != iceberg:
        raise AssertionError("resolver did not return the exact Iceberg descriptor")

    for extension_name in ("avro", "iceberg"):
        state = extension_state(extension_name)
        if state != (True, False, "NOT_INSTALLED"):
            raise AssertionError(f"{extension_name!r} did not load dynamically from its provider wheel: {state!r}")


__all__ = ["load_packaged_dynamic_iceberg"]
