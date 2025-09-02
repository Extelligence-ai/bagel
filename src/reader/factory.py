"""Factory functions to create readers for different reading patterns and robolog types."""

import pathlib
from typing import Any

from settings import settings
from src import robolog
from src.reader.frequency import TopicFrequencyReader
from src.reader.topic import TopicMessageReader
from src.reader.type import TypeMessageReader


def make_topic_message_reader(  # noqa: PLR0911
    robolog_path: str | pathlib.Path, use_cache: bool | None = None, **kwargs: dict[str, Any]
) -> TopicMessageReader:
    """Create a TopicMessageReader based on the robolog type."""
    use_cache = use_cache if use_cache is not None else settings.USE_CACHE

    match robolog.detect_robolog_type(robolog_path):
        case robolog.RobologType.ROS1_BAG_FILE:
            from src.reader.ros1.bag import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_DB3_FILE | robolog.RobologType.ROS2_DB3_DIR:
            from src.reader.ros2.db3 import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_MCAP_FILE | robolog.RobologType.ROS2_MCAP_DIR:
            from src.reader.ros2.mcap import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.PX4_ULG_FILE:
            from src.reader.px4.ulg import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ARDUPILOT_BIN_FILE:
            from src.reader.ardupilot.bin import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.BETAFLIGHT_BBL_FILE:
            from src.reader.betaflight.bbl import topic

            return topic.TopicMessageReader(
                robolog_path, log_index=kwargs["log_index"], use_cache=use_cache
            )

        case robolog.RobologType.BETAFLIGHT_BFL_FILE:
            from src.reader.betaflight.bfl import topic

            return topic.TopicMessageReader(robolog_path, use_cache=use_cache)

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)


def make_type_message_reader(
    robolog_path: str | pathlib.Path, use_cache: bool | None = None
) -> TypeMessageReader:
    """Create a TypeMessageReader based on the robolog type."""
    use_cache = use_cache if use_cache is not None else settings.USE_CACHE

    match robolog.detect_robolog_type(robolog_path):
        case robolog.RobologType.ROS1_BAG_FILE:
            from src.reader.ros1.bag import type as type_

            return type_.TypeMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_DB3_FILE | robolog.RobologType.ROS2_DB3_DIR:
            from src.reader.ros2.db3 import type as type_

            return type_.TypeMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_MCAP_FILE | robolog.RobologType.ROS2_MCAP_DIR:
            from src.reader.ros2.mcap import type as type_

            return type_.TypeMessageReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.PX4_ULG_FILE:
            from src.reader.px4.ulg import type as type_

            return type_.TypeMessageReader(robolog_path, use_cache=use_cache)

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)


def make_topic_frequency_reader(
    robolog_path: str | pathlib.Path, use_cache: bool | None = None
) -> TopicFrequencyReader:
    """Create a TopicFrequencyReader based on the robolog type."""
    use_cache = use_cache if use_cache is not None else settings.USE_CACHE

    match robolog.detect_robolog_type(robolog_path):
        case robolog.RobologType.ROS1_BAG_FILE:
            from src.reader.ros1.bag import frequency

            return frequency.TopicFrequencyReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_DB3_FILE | robolog.RobologType.ROS2_DB3_DIR:
            from src.reader.ros2.db3 import frequency

            return frequency.TopicFrequencyReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.ROS2_MCAP_FILE | robolog.RobologType.ROS2_MCAP_DIR:
            from src.reader.ros2.mcap import frequency

            return frequency.TopicFrequencyReader(robolog_path, use_cache=use_cache)

        case robolog.RobologType.PX4_ULG_FILE:
            from src.reader.px4.ulg import frequency

            return frequency.TopicFrequencyReader(robolog_path, use_cache=use_cache)

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)
