"""Discover and describe available pipeline gates and tasks."""

import inspect
from typing import Any


def _describe_params(cls: type) -> list[dict[str, Any]]:
    """Extract constructor parameters with types and defaults."""
    sig = inspect.signature(cls.__init__)
    params = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        info: dict[str, Any] = {"name": name}
        if param.annotation != inspect.Parameter.empty:
            info["type"] = inspect.formatannotation(param.annotation)
        if param.default != inspect.Parameter.empty:
            info["default"] = param.default
        else:
            info["required"] = True
        params.append(info)
    return params


_GATES = [
    {
        "module": "src.pipeline.gates.sql",
        "class": "SqlQueryGate",
        "description": (
            "Evaluates a SQL query on topic messages. "
            "The query must return a single boolean value."
        ),
        "mixin": "TopicMessageMixin",
    },
    {
        "module": "src.pipeline.gates.cv.object_too_close",
        "class": "ObjectTooCloseGate",
        "description": (
            "Detects if any object in a camera image is too close "
            "using YOLO object detection and depth estimation. Requires the cv dependency group."
        ),
        "mixin": "TopicImageMixin",
    },
]

_TASKS = [
    {
        "module": "src.pipeline.tasks.write_topics_to_file",
        "class": "WriteTopicsToFileTask",
        "description": "Writes topic messages to CSV, Parquet, or Arrow files.",
        "mixin": "TopicMessageMixin",
    },
    {
        "module": "src.pipeline.tasks.send_email",
        "class": "SendEmailTask",
        "description": "Sends an email via SMTP.",
        "mixin": None,
    },
    {
        "module": "src.pipeline.tasks.generate_gif",
        "class": "GenerateGifTask",
        "description": "Generates an animated GIF from images in a topic.",
        "mixin": "TopicImageMixin",
    },
    {
        "module": "src.pipeline.tasks.cloudini.decode_pointcloud",
        "class": "DecodePointCloudTask",
        "description": (
            "Decodes cloudini-compressed pointcloud messages "
            "and writes them to NPZ or CSV files."
        ),
        "mixin": None,
    },
    {
        "module": "src.pipeline.tasks.snippet.ros2.db3",
        "class": "SnipRosbagTask",
        "description": "Creates a ROS2 DB3 bag snippet from selected topics.",
        "mixin": "TopicMessageMixin",
    },
    {
        "module": "src.pipeline.tasks.snippet.ros1.bag",
        "class": "SnipRosbagTask",
        "description": "Creates a ROS1 bag snippet from selected topics.",
        "mixin": "TopicMessageMixin",
    },
]


def _enrich(entry: dict[str, Any]) -> dict[str, Any]:
    """Add constructor parameter info to a catalog entry by importing the class."""
    import importlib

    try:
        mod = importlib.import_module(entry["module"])
        cls = getattr(mod, entry["class"])
        entry["params"] = _describe_params(cls)
    except Exception:
        entry["params"] = []
    return entry


def catalog() -> dict[str, Any]:
    """Return the full pipeline module catalog.

    Returns:
        A dictionary with 'gates' and 'tasks' keys, each containing a list of
        module descriptors with module path, description, and constructor parameters.

    """
    return {
        "gates": [_enrich({**g}) for g in _GATES],
        "tasks": [_enrich({**t}) for t in _TASKS],
    }


def catalog_as_text() -> str:
    """Return the pipeline module catalog as a human-readable string.

    Suitable for embedding in prompts or POML context.

    """
    cat = catalog()
    lines = []

    lines.append("# Available Pipeline Gates")
    lines.append("")
    for gate in cat["gates"]:
        lines.append(f"## {gate['class']}")
        lines.append(f"  module: {gate['module']}")
        lines.append(f"  description: {gate['description']}")
        lines.append("  args:")
        for p in gate.get("params", []):
            default = f" (default: {p['default']!r})" if "default" in p else " (required)"
            ptype = p.get("type", "Any")
            lines.append(f"    {p['name']}: {ptype}{default}")
        lines.append("")

    lines.append("# Available Pipeline Tasks")
    lines.append("")
    for task in cat["tasks"]:
        lines.append(f"## {task['class']}")
        lines.append(f"  module: {task['module']}")
        lines.append(f"  description: {task['description']}")
        lines.append("  args:")
        for p in task.get("params", []):
            default = f" (default: {p['default']!r})" if "default" in p else " (required)"
            ptype = p.get("type", "Any")
            lines.append(f"    {p['name']}: {ptype}{default}")
        lines.append("")

    return "\n".join(lines)
