import pathlib

import pytest

pytest.importorskip("genpy")

from settings import settings
from src.reader.ros1.bag.frequency import TopicFrequencyReader


def test_has_correct_properties() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "turtle.bag"

    # WHEN
    reader = TopicFrequencyReader(robolog_path)

    # THEN
    assert reader.robolog_id == "ab827c6b-e15a-5c8d-875b-593104b7f29b"
    assert reader.start_seconds == 1660676075.8220897
    assert reader.end_seconds == 1660676084.8992772
    assert reader.duration_seconds == 9.077187538146973
    assert reader.path == robolog_path.absolute()
    assert reader.size_bytes == 12464
    assert reader.total_message_count == 34
    assert reader.topics == ["/rosout", "/turtle1/cmd_vel"]
    assert reader.type_names == {
        "/rosout": "rosgraph_msgs/Log",
        "/turtle1/cmd_vel": "geometry_msgs/Twist",
    }


def test_can_read_frequencies() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "turtle.bag"
    reader = TopicFrequencyReader(robolog_path)

    # WHEN
    table = reader.read().to_table()

    # THEN
    assert table.num_rows == 34
    assert table.column_names == [
        settings.ROBOLOG_ID_COLUMN_NAME,
        settings.TIMESTAMP_SECONDS_COLUMN_NAME,
        "/rosout",
        "/turtle1/cmd_vel",
    ]


def test_can_read_time_range() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "turtle.bag"
    reader = TopicFrequencyReader(robolog_path)

    # WHEN
    table = reader.read(start_seconds=1660676078.0).to_table()

    # THEN
    assert table.num_rows == 33

    # WHEN
    table = reader.read(start_seconds=1660676078.0, end_seconds=1660676080.0).to_table()

    # THEN
    assert table.num_rows == 6

    # WHEN
    table = reader.read(end_seconds=1660676080.0).to_table()

    # THEN
    assert table.num_rows == 7
