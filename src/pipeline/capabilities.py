"""Introspect the available pipeline tasks and gates.

Backs the ``list_pipeline_capabilities`` MCP tool: it discovers every task and
gate module under ``src/pipeline/tasks`` and ``src/pipeline/gates``, and reports
its module path, kind, constructor parameters, and docstring summary so an LLM can
compose a pipeline without reading the source. Modules that fail to import (for
example, a ROS task on a host without ROS installed) are reported as unavailable
rather than crashing the listing.
"""

import importlib
import inspect
import pathlib
from typing import Any

from src.di import module
from src.pipeline import base, gates, tasks

_OPERATOR_PACKAGES = (tasks, gates)


def _iter_module_names() -> list[str]:
    """Return the dotted names of every module under the task and gate packages.

    Walks the filesystem rather than using ``pkgutil.walk_packages`` so that PEP 420
    namespace subpackages (which have no ``__init__.py``, e.g. ``tasks.snippet.ros2``)
    are discovered reliably.
    """
    names: set[str] = set()
    for package in _OPERATOR_PACKAGES:
        for path_entry in package.__path__:
            root = pathlib.Path(path_entry)
            for python_file in root.rglob("*.py"):
                if python_file.name == "__init__.py":
                    continue
                relative = python_file.relative_to(root).with_suffix("")
                names.add(".".join((package.__name__, *relative.parts)))
    return sorted(names)


def _annotation(annotation: object) -> str | None:
    """Return a readable string for a parameter annotation, or None if absent."""
    if annotation is inspect.Parameter.empty:
        return None
    return getattr(annotation, "__name__", None) or str(annotation)


def _describe_operator(module_name: str, cls: type) -> dict[str, Any]:
    """Build a capability description for a Task or Gate class."""
    if issubclass(cls, base.Gate):
        kind = "gate"
    elif issubclass(cls, base.Task):
        kind = "task"
    else:
        kind = "operator"

    parameters = []
    for name, parameter in inspect.signature(cls.__init__).parameters.items():
        if name == "self":
            continue
        required = parameter.default is inspect.Parameter.empty
        parameters.append(
            {
                "name": name,
                "required": required,
                "default": None if required else parameter.default,
                "type": _annotation(parameter.annotation),
            }
        )

    summary = (inspect.getdoc(cls) or "").split("\n", 1)[0]
    return {
        "module": module_name,
        "kind": kind,
        "class": cls.__name__,
        "summary": summary,
        "parameters": parameters,
        "available": True,
    }


def list_capabilities(include_unavailable: bool = True) -> list[dict[str, Any]]:
    """Return descriptions of every discoverable pipeline task and gate.

    Args:
        include_unavailable: If True, modules that cannot be imported (e.g. missing
            optional dependencies) are included with ``available: False`` and the
            import error. If False, they are omitted.

    Returns:
        A list of capability dicts, sorted by module path. Each available entry has
        ``module``, ``kind`` (``task``/``gate``), ``class``, ``summary``,
        ``parameters`` (name/required/default/type), and ``available: True``.

    """
    capabilities: list[dict[str, Any]] = []
    for module_name in _iter_module_names():
        try:
            discovered = importlib.import_module(module_name)
        except Exception as error:  # optional deps (ROS, torch, ...) may be absent
            if include_unavailable:
                capabilities.append(
                    {"module": module_name, "available": False, "reason": str(error)}
                )
            continue

        register = getattr(discovered, "register", None)
        if not callable(register):
            continue
        register()
        cls = module.global_registry.get(module_name)
        if cls is None or not isinstance(cls, type):
            continue
        capabilities.append(_describe_operator(module_name, cls))

    capabilities.sort(key=lambda capability: capability["module"])
    return capabilities
