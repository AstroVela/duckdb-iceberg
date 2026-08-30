"""Helpers for exercising the installed Avro and Iceberg provider wheels."""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import entry_points

_TRUST_IDENTITY = "vane-ci-test-key"


def load_packaged_dynamic_iceberg(connection: object) -> None:
    """Load the exact installed Avro -> Iceberg descriptor graph."""
    from vane.extensions import DynamicExtensionDescriptor, DynamicExtensionResolver, LocalExtensionProvider

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
        if descriptor.trust_identity != _TRUST_IDENTITY:
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
            CAST(current_setting('allow_unsigned_extensions') AS BOOLEAN),
            CAST(current_setting('autoinstall_known_extensions') AS BOOLEAN),
            CAST(current_setting('autoload_known_extensions') AS BOOLEAN)
        """
    ).fetchone()
    if security != (False, False, False):
        raise AssertionError(f"dynamic extension security settings are not fail-closed: {security!r}")

    for extension_name in ("avro", "iceberg"):
        state = connection.execute(
            "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = ?",
            [extension_name],
        ).fetchone()
        if state is None:
            raise AssertionError(f"DuckDB does not expose extension state for {extension_name!r}")
        if state != (False, False, "NOT_INSTALLED"):
            raise AssertionError(
                f"{extension_name!r} was already installed or linked before resolver loading: {state!r}"
            )

    resolved = DynamicExtensionResolver(
        trusted_identities={_TRUST_IDENTITY},
        providers=providers,
    ).load(connection, iceberg)
    if resolved.descriptor != iceberg:
        raise AssertionError("resolver did not return the exact Iceberg descriptor")

    for extension_name in ("avro", "iceberg"):
        state = connection.execute(
            "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = ?",
            [extension_name],
        ).fetchone()
        if state != (True, False, "NOT_INSTALLED"):
            raise AssertionError(f"{extension_name!r} did not load dynamically from its provider wheel: {state!r}")


__all__ = ["load_packaged_dynamic_iceberg"]
