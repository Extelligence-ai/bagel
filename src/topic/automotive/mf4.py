"""A topic registry for ASAM MDF (.mf4) files: channel groups are topics."""

import numpy as np
import pyarrow as pa
from asammdf import MDF

from src.di import module
from src.topic import base

NATIVE_TYPE_NAME = "mdf/channel_group"


def topic_names(mdf: MDF) -> dict[str, int]:
    """Map each topic name to its channel group index.

    Uses the group's acquisition name when present (CANape/INCA set these to the
    message or measurement name); unnamed groups get a positional name. Duplicate
    names are disambiguated with their group index.
    """
    names: dict[str, int] = {}
    for index, group in enumerate(mdf.groups):
        name = group.channel_group.acq_name or f"ChannelGroup_{index}"
        if name in names:
            name = f"{name}_{index}"
        names[name] = index
    return names


def _channels(mdf: MDF, index: int) -> list[str]:
    """Non-master channel names for a group (the master/time channel is implicit)."""
    master_index = mdf.masters_db.get(index)
    return [
        channel.name
        for position, channel in enumerate(mdf.groups[index].channels)
        if position != master_index
    ]


def _arrow_type(dtype: np.dtype) -> pa.DataType:
    if dtype.kind == "b":
        return pa.bool_()
    if dtype.kind in "iu":
        return pa.int64()
    if dtype.kind == "f":
        return pa.float64()
    return pa.string()


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ASAM MDF files."""

    def available_topics(self, data_source: MDF) -> list[str]:
        """Return channel group names as topics."""
        return sorted(topic_names(data_source))

    def native_type_name(self, topic: str, data_source: MDF) -> str:
        """Return the native type name for the given topic."""
        self._group_index(topic, data_source)
        return NATIVE_TYPE_NAME

    def message_count(self, topic: str, data_source: MDF) -> int | None:
        """Return the sample count (cycles) of the channel group."""
        index = self._group_index(topic, data_source)
        return data_source.groups[index].channel_group.cycles_nr

    def struct(self, topic: str, data_source: MDF) -> pa.StructType:
        """Return one field per channel, with units attached as metadata."""
        index = self._group_index(topic, data_source)
        fields = []
        for name in _channels(data_source, index):
            # One sample is enough for dtype/unit/comment; loading the full
            # channel here made schema inference O(file) in memory (#134).
            signal = data_source.get(name, group=index, raw=False, record_offset=0, record_count=1)
            fields.append(
                pa.field(
                    name,
                    _arrow_type(signal.samples.dtype),
                    metadata={
                        "description": signal.comment or f"MDF channel '{name}'",
                        "units": signal.unit or "",
                    },
                )
            )
        return pa.struct(fields)

    def describe(self, topic: str, data_source: MDF) -> str | None:
        """Return a human-readable description of the channel group."""
        index = self._group_index(topic, data_source)
        group = data_source.groups[index]
        channels = ", ".join(_channels(data_source, index))
        comment = group.channel_group.comment or "ASAM MDF channel group"
        return f"{comment}. Channels: {channels}."

    def _group_index(self, topic: str, data_source: MDF) -> int:
        names = topic_names(data_source)
        if topic not in names:
            raise base.TopicNotFoundError(topic)
        return names[topic]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
