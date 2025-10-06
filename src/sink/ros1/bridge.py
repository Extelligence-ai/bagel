"""Provide a topic sink that connects to ROS1 through a rosbridge server."""

import pyarrow as pa
import roslibpy
from roslibpy import ros

from src.di import module
from src.sink import base, buffer
from src.topic.ros1 import parse, schema


class TopicSink(base.TopicSink):
    """A topic sink that connects to ROS1 through a rosbridge server.

    Make sure the rosbridge server is running before using this sink. For example:

    ```bash
    roslaunch rosbridge_server rosbridge_websocket.launch
    ```

    """

    def __init__(
        self,
        host: str,
        port: int,
        is_secure: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize the ROS1 bridge topic sink.

        Args:
            host (str): The hostname of the rosbridge server.
            port (int): The port number of the rosbridge server.
            is_secure (bool, optional): If True, use a secure WebSocket connection.
            headers (dict[str, str] | None, optional): Additional headers to include in
                the WebSocket connection.

        """
        self._client = roslibpy.Ros(host=host, port=port, is_secure=is_secure, headers=headers)

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
                self._client, writer.topic, self._type_name(writer.topic)
            )
        self._subscribers[writer.topic].subscribe(callback=writer.append)

    def _unsubscribe(self, writer: buffer.TopicBufferWriter) -> None:
        self._subscribers[writer.topic].unsubscribe()


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicSink
