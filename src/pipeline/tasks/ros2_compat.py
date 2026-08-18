"""Compatibility helpers across ROS2 distro API differences.

``rosbag2_py.TopicMetadata`` grew a required ``id`` argument after Humble;
Humble's constructor rejects the kwarg outright. Mirroring the approach of
``src/mcp_compat.py``, the difference is isolated here so the write-path
tasks run unmodified on every supported distro (Humble through Kilted).
"""

import rosbag2_py


def topic_metadata(
    topic_id: int, name: str, type_name: str, serialization_format: str
) -> rosbag2_py.TopicMetadata:
    """Build a ``TopicMetadata`` on any supported ROS2 distro."""
    try:  # Iron and newer require an id
        return rosbag2_py.TopicMetadata(
            id=topic_id,
            name=name,
            type=type_name,
            serialization_format=serialization_format,
        )
    except TypeError:  # Humble predates the id field
        return rosbag2_py.TopicMetadata(
            name=name, type=type_name, serialization_format=serialization_format
        )
