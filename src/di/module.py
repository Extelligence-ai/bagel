"""Dependency injection framework and the global module registry."""

import importlib
import inspect
import logging
from collections.abc import Callable
from typing import Any, Protocol

from src.di.types.base_module import BaseModule
from src.di.types.data_source import DataSource

# A global module registry mapping module names to their constructors.
global_registry: dict[str, Callable[..., object]] = {}


class Module(Protocol):
    """Protocol for an import module that can be registered."""

    def register() -> None:
        """Register the module's constructor by its name."""


def provide(base_module: BaseModule, data_source: DataSource, args: dict[str, Any]) -> object:
    """Provide an instance of a module based on the base module and data source.

    Args:
        base_module (BaseModule): The base module.
        data_source (DataSource): The data source.
        args (dict[str, Any]): Arguments to pass to the module constructor.

    Returns:
        object: An instance of the module.

    """
    name = f"{base_module.value}.{data_source.value}"
    module: Module = importlib.import_module(name)
    module.register()
    constructor = global_registry[name]

    signature = inspect.signature(constructor)
    unexpected_args = list(set(args) - set(signature.parameters))
    if unexpected_args:
        logging.debug(
            "Ignoring unexpected constructor arguments provided for module '%s': %s",
            name,
            ", ".join(unexpected_args),
        )
    missing_args = [
        param.name
        for param in signature.parameters.values()
        if param.default is param.empty and param.name not in args
    ]
    if missing_args:
        raise ValueError(
            f"Missing required constructor arguments for module '{name}': "
            + ", ".join(missing_args)
        )

    return constructor(**{k: v for k, v in args.items() if k in signature.parameters})
