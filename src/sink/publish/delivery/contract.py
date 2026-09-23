"""Restricted, versioned translation to Bagel standing pipelines.

No caller-controlled module paths, SQL, network addresses, credentials or output
paths. Source aliases resolve only in the robot's administrator-owned config.
"""

from __future__ import annotations

import hashlib
import json
import math
import re

SCHEMA = "bagel.capture.v1"
RUNTIME = "bagel-standing-v1"
NAME = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
MAX_NAME = 64
MAX_TOPICS = 8
MAX_FIELD_DEPTH = 8
MAX_THRESHOLD = 1e308
TOPIC = re.compile(r"^[A-Za-z0-9_/.:-]{1,200}$")


def canonical(value: object) -> str:
    """Serialize the finite JSON contract deterministically."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def digest(value: object) -> str:
    """Return the SHA-256 content identity of a JSON value."""
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _keys(value: object, keys: tuple[str, ...]) -> None:
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("Unsupported or missing fields in capture definition.")


def _integer(value: object, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"Expected integer between {minimum} and {maximum}.")


def validate(spec: dict) -> dict:  # noqa: C901, PLR0912 -- closed wire-schema validation
    """Validate the closed capture schema before any runtime effects."""
    _keys(
        spec, ("schema", "name", "source", "topics", "trigger", "lookback_seconds", "output_format")
    )
    if spec["schema"] != SCHEMA:
        raise ValueError("Unsupported pipeline schema.")
    for key in ("name", "source"):
        if (
            not isinstance(spec[key], str)
            or len(spec[key]) > MAX_NAME
            or not NAME.fullmatch(spec[key])
        ):
            raise ValueError(f"{key} must be a short lower_snake_case identifier.")
    topics = spec["topics"]
    if (
        not isinstance(topics, list)
        or not 1 <= len(topics) <= MAX_TOPICS
        or any(not isinstance(t, str) or not TOPIC.fullmatch(t) for t in topics)
    ):
        raise ValueError("Supply 1-8 explicit topics; wildcards are not supported.")
    if len(topics) != len(set(topics)):
        raise ValueError("Duplicate topics are not supported.")
    _integer(spec["lookback_seconds"], 1, 3600)
    if spec["output_format"] not in ("csv", "parquet"):
        raise ValueError("Only CSV and Parquet capture are supported.")
    trigger = spec["trigger"]
    if not isinstance(trigger, dict) or trigger.get("topic") not in topics:
        raise ValueError("Trigger topic must be included in capture topics.")
    if trigger.get("kind") == "periodic":
        _keys(trigger, ("kind", "topic", "every_seconds"))
        _integer(trigger["every_seconds"], 5, 86400)
    elif trigger.get("kind") == "event":
        _keys(trigger, ("kind", "topic", "field", "operator", "value", "debounce_seconds"))
        if (
            not isinstance(trigger["field"], list)
            or not 1 <= len(trigger["field"]) <= MAX_FIELD_DEPTH
            or any(
                not isinstance(f, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", f)
                for f in trigger["field"]
            )
        ):
            raise ValueError("Event field must be a path of simple field names.")
        if trigger["operator"] not in ("gt", "gte", "lt", "lte", "eq", "ne"):
            raise ValueError("Unsupported event comparison.")
        if (
            type(trigger["value"]) not in (int, float)
            or not -MAX_THRESHOLD <= trigger["value"] <= MAX_THRESHOLD
            or not math.isfinite(trigger["value"])
        ):
            raise ValueError("Event threshold must be a finite number.")
        _integer(trigger["debounce_seconds"], 5, 3600)
    else:
        raise ValueError("Choose a periodic or event trigger.")
    return json.loads(canonical(spec))


def native(spec: dict, source_path: str, asset_id: str) -> dict:
    """Compile known constructor arguments from the fleet branch capture runtime.

    The single task and bounded lookback are intentional. Arbitrary pipeline
    YAML, custom code, uploads and software installation are not admitted here.
    """
    spec = validate(spec)
    trigger = spec["trigger"]
    if trigger["kind"] == "periodic":
        when = {"every": trigger["every_seconds"], "unit": "second"}
    else:
        field = '"' + trigger["topic"] + '"' + "".join("['" + f + "']" for f in trigger["field"])
        op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "eq": "=", "ne": "<>"}[
            trigger["operator"]
        ]
        when = {
            "on_event": {
                "predicate": f"{field} {op} {trigger['value']}",
                "debounce": {"last": trigger["debounce_seconds"], "unit": "second"},
            }
        }
    return {
        "name": spec["name"],
        "site": "fleet",
        "asset": "asset_" + hashlib.sha256(asset_id.encode()).hexdigest()[:16],
        "path": source_path,
        "allow_failure": False,
        "cadence": {"topic": trigger["topic"], "when": when},
        "tasks": [
            {
                "module": "src.pipeline.tasks.write_topics_to_file",
                "args": {
                    "topics": spec["topics"],
                    "output_format": spec["output_format"],
                    "flatten": True,
                },
                "lookback": {"last": spec["lookback_seconds"], "unit": "second"},
                "upload": False,
            }
        ],
    }
