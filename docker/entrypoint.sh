#!/bin/sh
# Dispatches `docker run <image> demo [path]` to the headless hello-world demo
# (demo.py) instead of the MCP server; every other invocation (no args, or
# any other command) is handed through unchanged.
#
# On ROS-based images (ros:${ROS_DISTRO}-ros-core), this chains into the base
# image's own /ros_entrypoint.sh (which sources /opt/ros/$ROS_DISTRO/setup.bash
# before exec'ing its arguments) instead of replacing it -- replacing it
# outright drops PYTHONPATH/AMENT_PREFIX_PATH and breaks rosbag2_py imports.
# Non-ROS images (px4, ...) have no such script, so fall through to a plain exec.
set -e

if [ "$1" = "demo" ]; then
    shift
    set -- uv run python demo.py "$@"
fi

if [ -x /ros_entrypoint.sh ]; then
    exec /ros_entrypoint.sh "$@"
fi
exec "$@"
