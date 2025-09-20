"""A topic registry for ArduPilot Dataflash logs."""

import pathlib

import lxml
import pyarrow as pa
from pymavlink import DFReader

from src.di import module
from src.topic import base
from src.topic.ardupilot import schema


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ArduPilot Dataflash logs."""

    def available_topics(self, data_source: DFReader.DFReader) -> list[str]:
        """Return a list of available format names."""
        fmt_names = []
        for fmt_id, cnt in enumerate(data_source.counts):
            if cnt > 0 and fmt_id in data_source.id_to_name:
                fmt_names.append(data_source.id_to_name[fmt_id])
        return sorted(fmt_names)

    def native_type_name(self, topic: str, data_source: DFReader.DFReader) -> str:
        """Return the native type name for the given format name."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        return str(data_source.name_to_id[topic])

    def message_count(self, topic: str, data_source: DFReader.DFReader) -> int:
        """Return the number of messages for the given format name."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        fmt_id = data_source.name_to_id[topic]
        return data_source.counts[fmt_id]

    def struct(self, topic: str, data_source: DFReader.DFReader) -> pa.StructType:
        """Return the PyArrow StructType for the given format name."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        self._download_metadata(data_source.metadata)

        fmt_id = data_source.name_to_id[topic]
        fmt = data_source.formats[fmt_id]
        return schema.to_pa_struct(fmt, data_source.metadata)

    def describe(self, topic: str, data_source: DFReader.DFReader) -> str | None:
        """Return a human-readable description of the given format name."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        self._download_metadata(data_source.metadata)

        fmt_id = data_source.name_to_id[topic]
        fmt = data_source.formats[fmt_id]
        node = data_source.metadata.metadata_tree().get(fmt.name)
        return (
            lxml.etree.tostring(node, pretty_print=True, encoding="unicode")
            if node is not None
            else None
        )

    def _download_metadata(self, metadata: DFReader.DFMetaData) -> None:
        dot_pymavlink_path = pathlib.Path(metadata.dot_pymavlink())
        if not dot_pymavlink_path.exists() or not any(dot_pymavlink_path.iterdir()):
            metadata.download()


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
