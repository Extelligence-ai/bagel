import pathlib

import pytest

pytest.importorskip("rosbag2_py")

from settings import settings
from src.reader.ros2.db3.topic import TopicMessageReader


def test_has_correct_properties() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag"

    # WHEN
    reader = TopicMessageReader(robolog_path)

    # THEN
    assert reader.robolog_id == "079769d2-7089-5191-9ac3-c8a7261e065b"
    assert reader.start_seconds == 2.623e-06
    assert reader.end_seconds == 3.034e-06
    assert reader.duration_seconds == 4.1099999999999996e-07
    assert reader.path == robolog_path.absolute()
    assert reader.size_bytes == 439962
    assert reader.total_message_count == 6074
    assert sorted(reader.topics) == ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
    assert reader.type_names == {
        "AAA": "std_msgs/msg/String",
        "BBB": "std_msgs/msg/String",
        "CCC": "std_msgs/msg/String",
        "DDD": "std_msgs/msg/String",
        "EEE": "std_msgs/msg/String",
        "FFF": "std_msgs/msg/String",
        "GGG": "std_msgs/msg/String",
        "HHH": "std_msgs/msg/String",
    }

    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag" / "wbag_0.db3"

    # WHEN
    reader = TopicMessageReader(robolog_path)

    # THEN
    assert reader.robolog_id == "c8b3437c-937b-5670-b40d-b6448238d8e2"
    assert reader.start_seconds == 1e-06
    assert reader.end_seconds == 1.408e-06
    assert reader.duration_seconds == 4.0799999999999995e-07
    assert reader.path == robolog_path.absolute()
    assert reader.size_bytes == 90112
    assert reader.total_message_count == 1246
    assert sorted(reader.topics) == ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
    assert reader.type_names == {
        "AAA": "std_msgs/msg/String",
        "BBB": "std_msgs/msg/String",
        "CCC": "std_msgs/msg/String",
        "DDD": "std_msgs/msg/String",
        "EEE": "std_msgs/msg/String",
        "FFF": "std_msgs/msg/String",
        "GGG": "std_msgs/msg/String",
        "HHH": "std_msgs/msg/String",
    }


def test_can_all_read_messages() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag"
    reader = TopicMessageReader(robolog_path)

    # WHEN
    table = reader.read().to_table()

    # THEN
    assert table.num_rows == 6074
    assert set(table.column_names) == set(
        [
            settings.ROBOLOG_ID_COLUMN_NAME,
            settings.TIMESTAMP_SECONDS_COLUMN_NAME,
            "AAA",
            "BBB",
            "CCC",
            "DDD",
            "EEE",
            "FFF",
            "GGG",
            "HHH",
        ]
    )


def test_can_read_topics() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag"
    reader = TopicMessageReader(robolog_path)

    # WHEN
    table = reader.read(["AAA"]).to_table()

    # THEN
    assert table.num_rows == 804
    assert table.column_names == [
        settings.ROBOLOG_ID_COLUMN_NAME,
        settings.TIMESTAMP_SECONDS_COLUMN_NAME,
        "AAA",
    ]


def test_can_read_time_range() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag"
    reader = TopicMessageReader(robolog_path)

    # WHEN
    table = reader.read(start_seconds=2000 / 1e9).to_table()

    # THEN
    assert table.num_rows == 3064

    # WHEN
    table = reader.read(start_seconds=2000 / 1e9, end_seconds=2500 / 1e9).to_table()

    # THEN
    assert table.num_rows == 1582

    # WHEN
    table = reader.read(end_seconds=2000 / 1e9).to_table()

    # THEN
    assert table.num_rows == 3016


def test_can_apply_ffill() -> None:
    # GIVEN
    robolog_path = pathlib.Path(__file__).parent / "data" / "wbag"
    reader = TopicMessageReader(robolog_path)

    # WHEN
    table = reader.read(ffill=True).to_table()

    # THEN
    assert table.to_pandas()["EEE"].notnull().all()
