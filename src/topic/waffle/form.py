"""A topic registry for WaffleForm files: component categories are topics. EXPERIMENTAL BETA."""

import pyarrow as pa

from src.di import module
from src.source.waffle.form import WaffleForm
from src.topic import base

NATIVE_TYPE_NAME = "waffle/component"

_DESCRIPTIONS = {
    "compute": "Compute units declared in the WaffleForm (hardware, OS, SDK versions).",
    "actuators": "Actuators declared in the WaffleForm (model and firmware per unit).",
    "sensors": "Sensors declared in the WaffleForm (model, firmware, mount pose).",
    "software": "Software inventory: ROS distro and package versions.",
    "calibration": "Calibration references declared in the WaffleForm.",
    "robot": "Robot-level identity: name, platform, URDF reference.",
}


class TopicRegistry(base.TopicRegistry):
    """A topic registry for WaffleForm files."""

    def available_topics(self, data_source: WaffleForm) -> list[str]:
        """Return the component categories present in the form."""
        return sorted(data_source.rows)

    def native_type_name(self, topic: str, data_source: WaffleForm) -> str:
        """Return the native type name for the given topic."""
        self._check(topic, data_source)
        return NATIVE_TYPE_NAME

    def message_count(self, topic: str, data_source: WaffleForm) -> int | None:
        """Return the number of components in the category."""
        self._check(topic, data_source)
        return len(data_source.rows[topic])

    def struct(self, topic: str, data_source: WaffleForm) -> pa.StructType:
        """Return the union schema of the category's components."""
        self._check(topic, data_source)
        return pa.struct(
            [
                pa.field(
                    name,
                    pa.float64() if kind == "float" else pa.string(),
                    metadata={"description": f"WaffleForm '{topic}' field '{name}'"},
                )
                for name, kind in data_source.fields[topic].items()
            ]
        )

    def describe(self, topic: str, data_source: WaffleForm) -> str | None:
        """Return a human-readable description of the category."""
        self._check(topic, data_source)
        names = ", ".join(str(row.get("name", "?")) for row in data_source.rows[topic])
        base_text = _DESCRIPTIONS.get(topic, f"WaffleForm category '{topic}'.")
        return (
            f"{base_text} Components: {names}. Note: version fields are strings; "
            "compare exact versions, not lexicographic ranges."
        )

    def _check(self, topic: str, data_source: WaffleForm) -> None:
        if topic not in data_source.rows:
            raise base.TopicNotFoundError(topic)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
