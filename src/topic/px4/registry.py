"""A topic registry for PX4 ULogs."""

import logging

import pyarrow as pa
from pyulog import core

from src.topic import base
from src.topic.px4 import parse, schema


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PX4 ULogs."""

    def available_topics(self, data_source: core.ULog) -> list[str]:
        """Return a list of available topic names.

        A topic name consists of the message type name + multi_id, e.g., action_request_0.
        """
        return sorted([f"{data.name}_{data.multi_id}" for data in data_source.data_list])

    def native_type_name(self, topic: str, data_source: core.ULog) -> str:
        """Return the native type name for the given topic."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        return topic.rsplit("_", 1)[0]

    def message_count(self, topic: str, data_source: core.ULog) -> int:
        """Return the number of messages for the given topic."""
        data = self._topic_data(topic, data_source)
        first_field = next(iter(data.data))
        return len(data.data[first_field])

    def struct(self, topic: str, data_source: core.ULog) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        data = self._topic_data(topic, data_source)
        ver_sw = data_source.msg_info_dict.get("ver_sw")
        return schema.to_pa_struct(data, ver_sw)

    def describe(self, topic: str, data_source: core.ULog) -> str | None:
        """Return a human-readable description of the given topic."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        type_name, _ = topic.rsplit("_", 1)
        ver_sw = data_source.msg_info_dict.get("ver_sw")

        try:
            return parse.msg_definition(type_name, ver_sw, overwrite=False)
        except Exception as e:
            logging.warning(
                f"Failed to fetch .msg for {type_name} at {ver_sw or parse.DEFAULT_BRANCH}: {e}"
            )
            return None

    def _topic_data(self, topic: str, data_source: core.ULog) -> core.ULog.Data:
        """Return the ULog.Data for the given topic."""
        type_name, multi_id = topic.rsplit("_", 1)
        for data in data_source.data_list:
            if data.name == type_name and str(data.multi_id) == multi_id:
                return data
        raise base.TopicNotFoundError(topic)
