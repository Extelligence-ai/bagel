"""Provide a topic sink that connects to ROS1 through a rosbridge server."""

from typing import Any

import pyarrow as pa
import roslibpy
from roslibpy import ros

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.sink import base, buffer
from src.topic.ros1 import parse, schema


class TopicSink(base.TopicSink):
    """A topic sink that connects to ROS1 through a rosbridge server.

    Make sure the rosbridge server is running before using this sink. For example:

    ```bash
    roslaunch rosbridge_server rosbridge_websocket.launch
    ```

    """

    def __init__(  # noqa: PLR0913
        self,
        host: str,
        port: int,
        is_secure: bool = False,
        headers: dict[str, str] | None = None,
        queue_length: int = settings.ROSBRIDGE_QUEUE_LENGTH,
        throttle_rate: int = 0,
    ) -> None:
        """Initialize the ROS1 bridge topic sink.

        Args:
            host (str): The hostname of the rosbridge server.
            port (int): The port number of the rosbridge server.
            is_secure (bool, optional): If True, use a secure WebSocket connection.
            headers (dict[str, str] | None, optional): Additional headers to include in
                the WebSocket connection.
            queue_length (int, optional): Number of incoming messages to buffer at the rosbridge
                server when subscribing. If the buffer is full, older messages are dropped as
                new ones arrive.
            throttle_rate (int, optional): Minimum time (in milliseconds) between messages
                received by the subscriber. If messages are arriving faster than this
                rate, some will be dropped.

        """
        self._client = roslibpy.Ros(host=host, port=port, is_secure=is_secure, headers=headers)
        self._queue_length = queue_length
        self._throttle_rate = throttle_rate

        super().__init__(host, port)  # establish connection

        service = ros.Service(
            self._client, "/rosapi/topics_and_raw_types", "rosapi_msgs/srv/TopicsAndRawTypes"
        )
        response = service.call({})
        self._type_names = {
            topic: type_name
            for topic, type_name in zip(response["topics"], response["types"], strict=True)
        }
        self._definitions = {
            topic: full_text
            for topic, full_text in zip(
                response["topics"], response["typedefs_full_text"], strict=True
            )
        }
        self._subscribers: dict[str, roslibpy.Topic] = {}

    @property
    def metadata(self) -> dict[str, Any]:
        """Metadata about the topic sink."""
        return {
            **super().metadata,
            "image_dataset_module": f"{BaseModule.IMAGE_DATASET.value}.ros1.sink",
        }

    def _connect(self) -> None:
        self._client.run()

    def _disconnect(self) -> None:
        self._client.terminate()

    def _available_topics(self) -> list[str]:
        return self._client.get_topics()

    def _type_name(self, topic: str) -> str:
        return self._type_names[topic]

    def _definition(self, topic: str) -> str:
        return self._definitions[topic]

    def _struct(self, topic: str) -> pa.StructType:
        main, deps = parse.parse(self._definition(topic))
        return schema.to_pa_struct(main, deps)

    def _subscribe(self, writer: buffer.TopicBufferWriter) -> None:
        if writer.topic not in self._subscribers:
            self._subscribers[writer.topic] = roslibpy.Topic(
                ros=self._client,
                name=writer.topic,
                message_type=self._type_name(writer.topic),
                queue_length=self._queue_length,
                throttle_rate=self._throttle_rate,
            )
        self._subscribers[writer.topic].subscribe(callback=writer.append)

    def _unsubscribe(self, writer: buffer.TopicBufferWriter) -> None:
        if writer.topic in self._subscribers:
            self._subscribers[writer.topic].unsubscribe()
            del self._subscribers[writer.topic]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicSink
