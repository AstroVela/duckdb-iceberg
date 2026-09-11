"""Helpers for exercising the installed Avro and Iceberg provider wheels."""

from __future__ import annotations

import os
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path


def load_packaged_dynamic_iceberg(connection: object) -> None:
    """Load the exact installed Avro -> Iceberg descriptor graph."""
    import vane
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

    # The caller supplies a fresh connection with explicit security options.
    # Inspect its captured bootstrap without dispatching client-state SQL to Ray.
    plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(connection.sql("SELECT 1"), None)
    bootstrap = plan.__getstate__()[3]["bootstrap"]["config"]
    for setting in ("allow_unsigned_extensions", "autoinstall_known_extensions", "autoload_known_extensions"):
        if str(bootstrap[setting]).lower() != "false":
            raise AssertionError(f"dynamic extension bootstrap must disable {setting}")

    def require_no_native_install() -> None:
        directory = Path(vane._native._dynamic_extension_directory(connection=connection))
        for name in ("avro", "iceberg"):
            if os.path.lexists(directory / f"{name}.duckdb_extension"):
                raise AssertionError(f"{name!r} has a native installed artifact outside the provider graph")

    require_no_native_install()
    for extension_name in ("avro", "iceberg"):
        state = vane._native._loaded_dynamic_extension(extension_name, connection=connection)
        if state is not None:
            raise AssertionError(f"{extension_name!r} was already loaded before resolver loading: {state!r}")

    resolved = DynamicExtensionResolver(
        trusted_identities={trust_identity},
        providers=providers,
    ).load(connection, iceberg)
    if resolved.descriptor != iceberg:
        raise AssertionError("resolver did not return the exact Iceberg descriptor")

    require_no_native_install()
    for extension_name in ("avro", "iceberg"):
        state = vane._native._loaded_dynamic_extension(extension_name, connection=connection)
        if (
            state is None
            or state["canonical_name"] != extension_name
            or state["install_mode"] != "NOT_INSTALLED"
            or state["extension_version"] != descriptors[extension_name].extension_version
            or not state["full_path"]
        ):
            raise AssertionError(f"{extension_name!r} did not load dynamically from its provider wheel: {state!r}")


__all__ = ["load_packaged_dynamic_iceberg"]
