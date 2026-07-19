import duckdb
import pytest

from bagel.di import module
from bagel.di.types.base_module import BaseModule
from bagel.di.types.data_source import DataSource, resolve
from bagel.message.ros.log import MessageDataset
from bagel.source.ros.log import SourceFactory
from bagel.topic import base
from bagel.topic.ros.log import TopicRegistry


def test_resolves_ros_log_paths() -> None:
    assert resolve("data/sample/ros/log") is DataSource.ROS_LOG
    assert resolve("data/sample/ros/log/talker.log") is DataSource.ROS_LOG


def test_di_provides_the_full_adapter_family() -> None:
    # GIVEN the same provide chain server.py's read_loggings uses
    ds_type = resolve("data/sample/ros/log")

    # WHEN
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": "data/sample/ros/log"}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.LOGGING_DATASET.value}.{ds_type.value}", {})

    # THEN
    relation = dataset.to_duckdb(factory, registry)
    assert relation.shape == (12, 3)


def test_topic_registry_lists_nodes_as_topics() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    registry = TopicRegistry()
    data_source = factory.build()

    # WHEN
    topics = registry.available_topics(data_source)

    # THEN
    assert topics == ["/move_base", "/rosout", "launch", "talker", "talker-1"]
    assert registry.native_type_name("talker", data_source) == "ros/log"
    assert registry.message_count("/move_base", data_source) == 2
    assert "1 ERROR" in registry.describe("/move_base", data_source)
    with pytest.raises(base.TopicNotFoundError):
        registry.struct("no_such_node", data_source)


def test_message_dataset_supports_sql_over_logs() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=False)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["talker"])
    duckdb.register("talker_logs", relation)
    result = duckdb.sql(
        "SELECT COUNT(*) FROM talker_logs WHERE \"talker\"['level'] = 'ERROR'"
    ).fetchone()

    # THEN
    assert result[0] == 1


def test_message_dataset_respects_time_window() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    dataset = MessageDataset(use_cache=False)

    # WHEN
    relation = dataset.to_duckdb(
        factory,
        TopicRegistry(),
        topics=["talker"],
        start_seconds=1662400005.0,
        end_seconds=1662400010.5,
    )

    # THEN INFO at 5.2s and WARN at 10.5s (end is inclusive)
    assert relation.shape == (2, 2)
