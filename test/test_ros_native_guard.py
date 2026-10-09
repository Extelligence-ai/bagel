"""Bag support is a backend (`ros` extra or native ROS); without one, ROS modules stay out.

`rosbag`, `rosbag2_py` and `rclpy` are apt packages, not wheels, so they exist only in
the ROS service images. A pip install reads and writes bags through the ``ros`` extra's
pure-Python backend instead. With neither, the modules that need a backend must still
import, must refuse registration with an error that names both ways out, and must not
hide the fact that ROS 2 MCAP bags work without any of it.
"""

import importlib

import pytest

from bagel_mcp import bags, ros_native, server
from bagel_mcp.di import module
from bagel_mcp.di.types.data_source import DataSource, resolve
from bagel_mcp.pipeline import capabilities
from bagel_mcp.settings import settings

no_native_ros = pytest.mark.skipif(
    ros_native.available("rosbag2_py") or ros_native.available("rosbag"),
    reason="native ROS present: this is the Docker image",
)

ROS_BOUND_MODULES = [
    "bagel_mcp.bags.rosbags_backend",
    "bagel_mcp.bags.native_backend",
    "bagel_mcp.source.ros1.bag",
    "bagel_mcp.topic.ros1.bag",
    "bagel_mcp.message.ros1.bag",
    "bagel_mcp.logging.ros1.bag",
    "bagel_mcp.source.ros2.base",
    "bagel_mcp.source.ros2.db3",
    "bagel_mcp.source.ros2.mcap",
    "bagel_mcp.source.ros2.decompress",
    "bagel_mcp.topic.ros2.base",
    "bagel_mcp.topic.ros2.db3",
    "bagel_mcp.topic.ros2.mcap",
    "bagel_mcp.message.ros2.db3",
    "bagel_mcp.message.ros2.mcap",
    "bagel_mcp.logging.ros2.db3",
    "bagel_mcp.logging.ros2.mcap",
    "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
    "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
    "bagel_mcp.pipeline.tasks.snippet.ros1.bag",
]


@pytest.fixture
def without_bag_support(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the backend to native ROS, which the host does not have: no backend at all."""
    if ros_native.available("rosbag2_py") or ros_native.available("rosbag"):
        pytest.skip("native ROS present: this is the Docker image")
    monkeypatch.setattr(settings, "BAG_BACKEND", "native")
    assert bags.backend_name(1) is None
    assert bags.backend_name(2) is None


@pytest.mark.parametrize("name", ROS_BOUND_MODULES)
def test_ros_bound_modules_import_everywhere(name: str) -> None:
    importlib.import_module(name)


@no_native_ros
def test_missing_native_module_names_the_ways_out_on_use() -> None:
    stand_in = ros_native.optional("rosbag2_py", feature="The native ROS bag backend")
    with pytest.raises(ros_native.NativeRosUnavailableError) as raised:
        stand_in.Info()
    message = str(raised.value)
    assert "rosbag2_py" in message
    assert "bagel-mcp[ros]" in message  # the pip way
    assert "ros2-kilted" in message  # the Docker way
    assert isinstance(raised.value, ImportError)


@pytest.mark.usefixtures("without_bag_support")
@pytest.mark.parametrize(
    ("path", "image"),
    [
        ("./data/sample/ros2/db3", "ros2-kilted"),
        ("./data/sample/ros1/sample.bag", "ros1-noetic"),
    ],
)
def test_bag_sources_refuse_registration_naming_the_extra_and_the_image(
    path: str, image: str
) -> None:
    ds_type = resolve(path)
    assert ds_type in (DataSource.ROS2_DB3, DataSource.ROS1_BAG)
    with pytest.raises(bags.BagSupportUnavailableError) as raised:
        module.provide(f"bagel_mcp.source.{ds_type.value}", {"path": path})
    assert "bagel-mcp[ros]" in str(raised.value)
    assert image in str(raised.value)
    # The MCP tool surfaces the same error, not a bare ModuleNotFoundError.
    with pytest.raises(bags.BagSupportUnavailableError, match=image):
        server.describe_data_source(path)


@pytest.mark.usefixtures("without_bag_support")
def test_bag_pipeline_tasks_are_listed_as_unavailable() -> None:
    listed = {entry["module"]: entry for entry in capabilities.list_capabilities()}
    for name in (
        "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
        "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
        "bagel_mcp.pipeline.tasks.snippet.ros1.bag",
    ):
        assert listed[name]["available"] is False
        assert "bagel-mcp[ros]" in listed[name]["reason"]
    assert listed["bagel_mcp.pipeline.tasks.reduce.mcap"]["available"] is True
    available_only = {entry["module"] for entry in capabilities.list_capabilities(False)}
    assert "bagel_mcp.pipeline.tasks.reduce.ros2.db3" not in available_only


@pytest.mark.skipif(not bags.available(2), reason="needs a bag backend (the `ros` extra)")
def test_bag_pipeline_tasks_are_available_with_a_backend() -> None:
    listed = {entry["module"]: entry for entry in capabilities.list_capabilities()}
    for name in (
        "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
        "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
        "bagel_mcp.pipeline.tasks.snippet.ros1.bag",
    ):
        assert listed[name]["available"] is True, listed[name]


def test_an_unknown_backend_setting_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "BAG_BACKEND", "carrier-pigeon")
    with pytest.raises(ValueError, match="BAG_BACKEND"):
        bags.backend_name(2)


def test_ros2_mcap_bags_work_through_the_generic_reader() -> None:
    """A rosbag2-produced MCAP directory needs no bag backend at all."""
    path = "./data/sample/ros2/mcap"
    assert resolve(path) is DataSource.MCAP
    assert server.describe_data_source(path)
    assert server.describe_topic(path, "/rosout")
    rows = server.query_messages(path, 'SELECT COUNT(*) AS n FROM "/rosout"', "/rosout")
    assert rows[0]["n"] > 0
