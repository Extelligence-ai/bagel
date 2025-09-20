"""A topic registry for Betaflight Blackbox logs."""

import pathlib
from collections import Counter

import orangebox
import pyarrow as pa
import yaml

from src.di import module
from src.topic import base
from src.topic.betaflight import schema

# These frame types do not contain timestamps, hence they are excluded. See:
# https://github.com/betaflight/betaflight/blob/master/src/main/blackbox/blackbox.c
NON_TIMESERIES_FRAME_TYPES = {
    orangebox.types.FrameType.GPS_HOME,  # blackboxGpsHFields
    orangebox.types.FrameType.SLOW,  # blackboxSlowFields
    orangebox.types.FrameType.EVENT,
}


class TopicRegistry(base.TopicRegistry):
    """A topic registry for Betaflight Blackbox logs."""

    # (orangebox.Parser, topic) -> message count
    _message_counts: Counter[tuple[orangebox.Parser, str]] | None = None

    def available_topics(self, data_source: orangebox.Parser) -> list[str]:
        """Return a list of available log frame types."""
        return sorted(
            [
                t.value
                for t in data_source.reader.field_defs.keys()
                if t not in NON_TIMESERIES_FRAME_TYPES
            ]
        )

    def native_type_name(self, topic: str, data_source: orangebox.Parser) -> str:
        """Return the native type name for the given log frame type."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        return orangebox.types.FrameType(topic).name

    def message_count(self, topic: str, data_source: orangebox.Parser) -> int:
        """Return the number of messages for the given log frame type."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        if self._message_counts is not None:
            return self._message_counts[(data_source, topic)]

        self._message_counts = Counter()
        for frame in data_source.frames():
            if frame.type not in NON_TIMESERIES_FRAME_TYPES:
                self._message_counts[(data_source, frame.type.value)] += 1
        return self._message_counts[(data_source, topic)]

    def struct(self, topic: str, data_source: orangebox.Parser) -> pa.StructType:
        """Return the PyArrow StructType for the given log frame type."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        frame_type = orangebox.types.FrameType(topic)
        field_defs = data_source.reader.field_defs[frame_type]
        pa_fields = [schema.to_pa_field(field_def) for field_def in field_defs]
        return pa.struct(pa_fields)

    def describe(self, topic: str, data_source: orangebox.Parser) -> str | None:
        """Return a human-readable description of the given log frame type."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        with open(pathlib.Path(__file__).parent / "frames.yaml") as f:
            description_lines = yaml.safe_load(f)[topic]

        lines = [f"# {line}" for line in description_lines] + [""]

        for field in self.struct(topic, data_source):
            line = [str(field.type), field.name]
            if field.metadata:
                description = field.metadata[base.DESCRIPTION_KEY.encode()].decode("utf-8")
                line.append(f"# {description}")
            lines.append(" ".join(line))

        return "\n".join(lines)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
