"""Without native ROS (a pip install), ROS-bound modules stay importable but unregistered.

`rosbag`, `rosbag2_py` and `rclpy` are apt packages, not wheels, so they exist only in
the ROS service images. On the host (and for every pip user) the modules that need them
must still import, must refuse registration with an error that names the Docker image,
and must not hide the fact that ROS 2 MCAP bags work without any of it.
"""

import importlib

import pytest

from bagel_mcp import ros_native, server
from bagel_mcp.di import module
from bagel_mcp.di.types.data_source import DataSource, resolve
from bagel_mcp.pipeline import capabilities

no_native_ros = pytest.mark.skipif(
    ros_native.available("rosbag2_py") or ros_native.available("rosbag"),
    reason="native ROS present: this is the Docker image, where these modules register",
)

ROS_BOUND_MODULES = [
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
    "bagel_mcp.pipeline.tasks.ros2_compat",
    "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
    "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
]


@pytest.mark.parametrize("name", ROS_BOUND_MODULES)
def test_ros_bound_modules_import_everywhere(name: str) -> None:
    importlib.import_module(name)


@no_native_ros
def test_missing_native_module_raises_a_pointer_to_docker_on_use() -> None:
    stand_in = ros_native.optional("rosbag2_py", feature="Reading ROS 2 .db3 bags")
    with pytest.raises(ros_native.NativeRosUnavailableError) as raised:
        stand_in.Info()
    message = str(raised.value)
    assert "rosbag2_py" in message
    assert "ros2-kilted" in message
    assert "MCAP" in message  # the pip-friendly alternative is named, not just the image
    assert isinstance(raised.value, ImportError)


@no_native_ros
@pytest.mark.parametrize(
    ("path", "image"),
    [
        ("./data/sample/ros2/db3", "ros2-kilted"),
        ("./data/sample/ros1/sample.bag", "ros1-noetic"),
    ],
)
def test_native_ros_sources_refuse_registration_with_the_image_to_use(
    path: str, image: str
) -> None:
    ds_type = resolve(path)
    assert ds_type in (DataSource.ROS2_DB3, DataSource.ROS1_BAG)
    with pytest.raises(ros_native.NativeRosUnavailableError, match=image):
        module.provide(f"bagel_mcp.source.{ds_type.value}", {"path": path})
    # The MCP tool surfaces the same error, not a bare ModuleNotFoundError.
    with pytest.raises(ros_native.NativeRosUnavailableError, match=image):
        server.describe_data_source(path)


@no_native_ros
def test_native_ros_pipeline_tasks_are_listed_as_unavailable() -> None:
    listed = {entry["module"]: entry for entry in capabilities.list_capabilities()}
    for name in (
        "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
        "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
    ):
        assert listed[name]["available"] is False
        assert "ros2-kilted" in listed[name]["reason"]
    assert listed["bagel_mcp.pipeline.tasks.reduce.mcap"]["available"] is True
    available_only = {entry["module"] for entry in capabilities.list_capabilities(False)}
    assert "bagel_mcp.pipeline.tasks.reduce.ros2.db3" not in available_only


@no_native_ros
def test_ros2_mcap_bags_work_through_the_generic_reader() -> None:
    """A rosbag2-produced MCAP directory needs no native ROS at all."""
    path = "./data/sample/ros2/mcap"
    assert resolve(path) is DataSource.MCAP
    assert server.describe_data_source(path)
    assert server.describe_topic(path, "/rosout")
    rows = server.query_messages(path, 'SELECT COUNT(*) AS n FROM "/rosout"', "/rosout")
    assert rows[0]["n"] > 0
