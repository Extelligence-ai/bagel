"""Provide a topic sink that connects to ROS2 through a rosbridge server."""

import pyarrow as pa
import rosapi.objectutils
import roslibpy

from src.di import module
from src.sink import base, buffer
from src.topic.ros2.ros2msg import parse, schema


class TopicSink(base.TopicSink):
    """A topic sink that connects to ROS2 through a rosbridge server.

    Make sure the rosbridge server is running before using this sink. For example:

    ```bash
    ros2 launch rosbridge_server rosbridge_websocket_launch.xml
    ```

    """

    def __init__(
        self,
        host: str,
        port: int,
        overwrite: bool,
        is_secure: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize the ROS2 bridge topic sink.

        Args:
            host (str): The hostname of the rosbridge server.
            port (int): The port number of the rosbridge server.
            overwrite (bool): If True, overwrite any existing sink directory.
            is_secure (bool, optional): If True, use a secure WebSocket connection.
            headers (dict[str, str] | None, optional): Additional headers to include in
                the WebSocket connection.

        """
        self._client = roslibpy.Ros(host=host, port=port, is_secure=is_secure, headers=headers)
        self._type_names: dict[str, str] = {}
        self._definitions: dict[str, str] = {}
        self._subscribers: dict[str, roslibpy.Topic] = {}
        super().__init__(host, port, overwrite)

    def _connect(self) -> None:
        self._client.run()

    def _disconnect(self) -> None:
        self._client.terminate()

    def _available_topics(self) -> list[str]:
        return self._client.get_topics()

    def _type_name(self, topic: str) -> str:
        if topic not in self._type_names:
            self._type_names[topic] = self._client.get_topic_type(topic)
        return self._type_names[topic]

    def _definition(self, topic: str) -> str:
        if topic not in self._definitions:
            self._definitions[topic] = rosapi.objectutils.get_typedef_full_text(
                self._type_name(topic)
            )
        return self._definitions[topic]

    def _struct(self, topic: str) -> pa.Schema:
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
