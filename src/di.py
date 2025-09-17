"""Dependency injection framework."""

import importlib
from collections.abc import Callable
from typing import Any, Protocol

# Registry mapping module names to their constructors.
module_registry: dict[str, Callable[..., object]] = {}


class Module(Protocol):
    """Protocol for a module that can register itself."""

    def register() -> None:
        """Register the module's constructor by module name."""


def provide(module_name: str, constructor_arguments: dict[str, Any]) -> object:
    """Provide an object for a given module name.

    Args:
        module_name (str): The name of the module to provide, e.g., 'src.source.ros1.bag'.
        constructor_arguments (dict[str, Any]): Arguments to pass to the constructor.

    Returns:
        object: The constructed object.

    """
    module: Module = importlib.import_module(module_name)
    module.register()
    return module_registry[module_name](**constructor_arguments)
