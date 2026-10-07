"""Dependency injection framework and the global module registry."""

import importlib
import inspect
import logging
from collections.abc import Callable
from typing import Any, Protocol

# A global module registry mapping module names to their constructors.
global_registry: dict[str, Callable[..., object]] = {}


class Module(Protocol):
    """Protocol for an import module that can be registered."""

    def register() -> None:
        """Register the module's constructor by its name."""


# Top-level packages of the optional dependency groups that every image ships
# since 2.4.2, mapped to the group that installs them outside Docker.
OPTIONAL_PACKAGE_GROUPS = {
    "asammdf": "automotive",
    "can": "automotive",
    "cantools": "automotive",
    "azure": "upload",
    "google": "upload",
    "wasmtime": "cloudini",
    "rerun": "viz",
}


def missing_group_hint(group: str) -> str:
    """Return how to install an optional dependency group, in Docker or out of it."""
    return (
        f"In Docker, pull a Bagel image from 2.4.2 or later (`docker compose pull`); "
        f"outside Docker, run: uv sync --group {group}"
    )


def import_module(import_path: str) -> Module:
    """Import a module, naming the fix when an optional dependency is missing.

    Raises:
        ModuleNotFoundError: With the dependency group to install, when the
            missing package belongs to one.

    """
    try:
        return importlib.import_module(import_path)
    except ModuleNotFoundError as error:
        package = (error.name or "").split(".")[0]
        group = OPTIONAL_PACKAGE_GROUPS.get(package)
        if group is None:
            raise
        raise ModuleNotFoundError(
            f"{import_path} needs the optional '{error.name}' package. {missing_group_hint(group)}",
            name=error.name,
        ) from error


def provide(import_path: str, args: dict[str, Any]) -> object:
    """Provide an instance of a module based on the base module and data source.

    Args:
        import_path (str): The import path of the module.
        args (dict[str, Any]): Arguments to pass to the module constructor.

    Returns:
        object: An instance of the module.

    """
    module = import_module(import_path)
    module.register()
    return construct(global_registry[import_path], args)


def construct(constructor: Callable[..., object], args: dict[str, Any]) -> object:
    """Construct an instance of a module.

    Args:
        constructor (Callable[..., object]): The constructor of the module.
        args (dict[str, Any]): Arguments to pass to the constructor.

    Raises:
        ValueError: If there are missing required constructor arguments.

    Returns:
        object: An instance of the module.

    """
    signature = inspect.signature(constructor)
    unexpected_args = list(set(args) - set(signature.parameters))
    if unexpected_args:
        logging.debug(
            "Ignoring unexpected constructor arguments: %s",
            ", ".join(unexpected_args),
        )
    missing_args = [
        param.name
        for param in signature.parameters.values()
        if param.default is param.empty and param.name not in args
    ]
    if missing_args:
        raise ValueError(f"Missing required constructor arguments: {', '.join(missing_args)}")
    return constructor(**{k: v for k, v in args.items() if k in signature.parameters})
