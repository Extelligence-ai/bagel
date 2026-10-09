r"""Smoke-test a pip-installed Bagel: no checkout on sys.path, no ROS, every non-ROS source.

Run from a directory OUTSIDE the repository with the interpreter of a clean virtualenv
that has the built wheel installed with every non-ROS extra::

    cd "$(mktemp -d)" && /path/to/venv/bin/python /path/to/bagel/scripts/smoke_pip_install.py \\
        --repo /path/to/bagel

It proves four things a pip user depends on and the Docker-based test suite never
exercises:

1. ``bagel_mcp`` resolves to the installed wheel, not the source tree.
2. The ``bagel-mcp`` console script serves MCP over stdio (what ``uvx bagel-mcp
   --transport stdio`` gives an MCP client) and the builtin capabilities load from the
   wheel.
3. Every non-ROS source in ``data/sample`` and the generated ``test/_fixtures`` inputs
   describe, drill into a topic and answer a SQL count over MCP -- including
   rosbag2-produced MCAP bags, which go through Bagel's own MCAP reader.
4. ROS 1 ``.bag`` and ROS 2 ``.db3`` bags describe and query through the ``ros``
   extra's pure-Python backend, and a reduce pipeline writes a ``.db3`` bag that the
   same backend reopens. Without the extra, those sources fail with the error that
   names it and the Docker image, never with a traceback about a missing module.
"""

from __future__ import annotations

# ruff: noqa: S101 -- a smoke test asserts on purpose; every message names the check
import argparse
import asyncio
import importlib.util
import json
import os
import pathlib
import re
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from http import HTTPStatus
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

TOPICS_BLOCK = re.compile(
    r"# Available Topics in the Data Source\s*```json\s*(\[.*?\])\s*```", re.DOTALL
)


def _load_fixture_module(repo: pathlib.Path, name: str) -> Any:  # noqa: ANN401
    """Import ``test/_fixtures/<name>.py`` by file path, without putting the repo on sys.path."""
    file = repo / "test" / "_fixtures" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"bagel_smoke_fixture_{name}", file)
    assert spec and spec.loader, file
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def _generated_inputs(repo: pathlib.Path, scratch: pathlib.Path) -> list[tuple[str, dict]]:
    """Build the inputs the test suite generates on the fly rather than checking in."""
    inputs: list[tuple[str, dict]] = []

    fault_log = _load_fixture_module(repo, "fault_log")
    inputs.append((str(fault_log.write_fault_log(scratch / "fault_log.mcap")), {}))

    gantry = _load_fixture_module(repo, "gantry_evidence")
    inputs.append((str(gantry.write_bundle(scratch / "gantry_evidence")), {}))

    # A CAN capture with its DBC and an MDF4 measurement, built the way
    # test/adversarial builds them (the `automotive` extra provides the writers).
    import can
    import numpy as np
    from asammdf import MDF, Signal

    dbc = scratch / "bus.dbc"
    dbc.write_text(
        'VERSION ""\n\nNS_ :\n\nBS_:\n\nBU_: ECU\n\nBO_ 100 EngineData: 8 ECU\n'
        ' SG_ Speed : 0|16@1+ (1,0) [0|65535] "" ECU\n',
        encoding="utf-8",
    )
    blf = scratch / "capture.blf"
    with can.BLFWriter(str(blf)) as writer:
        for index in range(10):
            writer.on_message_received(
                can.Message(
                    arbitration_id=0x64,
                    data=bytes([index, 0, 0, 0, 0, 0, 0, 0]),
                    is_extended_id=False,
                    timestamp=1.0 + index * 0.1,
                )
            )
    inputs.append((str(blf), {"dbc": str(dbc)}))

    mf4 = scratch / "measurement.mf4"
    measurement = MDF(version="4.10")
    measurement.append(
        [Signal(samples=np.arange(10, dtype=float), timestamps=np.arange(10) * 0.1, name="rpm")]
    )
    measurement.save(str(mf4), overwrite=True)
    measurement.close()
    inputs.append((str(mf4), {}))
    return inputs


def _text(result: Any) -> str:  # noqa: ANN401
    return "\n".join(block.text for block in result.content if getattr(block, "text", None))


def _parsed(result: Any) -> Any:  # noqa: ANN401
    """Tool results arrive as one JSON text block per returned item."""
    return [json.loads(block.text) for block in result.content if getattr(block, "text", None)]


def _ardupilot_metadata_reachable() -> bool:
    """ArduPilot field schemas come from autotest.ardupilot.org (as in the Docker image).

    pymavlink downloads them on first use; without that host, `describe_topic` on a
    `.bin` cannot build a schema, so the topic drill is skipped (not failed) when it
    is unreachable from the runner.
    """
    try:
        with urllib.request.urlopen(
            "http://autotest.ardupilot.org/LogMessages/Copter/LogMessages.xml.gz", timeout=15
        ) as response:
            return response.status == HTTPStatus.OK
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


async def _check_source(
    session: ClientSession, path: str, args: dict, *, describe_only_reason: str | None = None
) -> str:
    described = await session.call_tool("describe_data_source", {"path": path, "args": args})
    assert not described.isError, f"describe_data_source({path}): {_text(described)}"
    if describe_only_reason:
        return f"{path}: described; topic drill skipped ({describe_only_reason})"
    human = next(
        message["content"] for message in _parsed(described) if message.get("speaker") == "human"
    )
    match = TOPICS_BLOCK.search(human)
    assert match, f"no topic list for {path}: {human[:300]}"
    topics = json.loads(match.group(1))
    assert topics, f"no topics in {path}"

    # A bag can carry empty topics (rosbag2's /events/write_split); the check is
    # that at least one topic describes and answers a count.
    for topic in topics:
        detail = await session.call_tool(
            "describe_topic", {"path": path, "topic": topic, "args": args}
        )
        assert not detail.isError, f"describe_topic({path}, {topic}): {_text(detail)}"
        rows = await session.call_tool(
            "query_messages",
            {
                "path": path,
                "sql_statement": f'SELECT COUNT(*) AS n FROM "{topic}"',  # noqa: S608 -- topic from the server
                "topic": topic,
                "args": args,
            },
        )
        assert not rows.isError, f"query_messages({path}, {topic}): {_text(rows)}"
        (count,) = _parsed(rows)
        if count["n"] > 0:
            return f"{path}: {len(topics)} topics, {topic} -> {count['n']} rows"
    raise AssertionError(f"{path}: every topic is empty: {topics}")


async def _check_bag_refusal(session: ClientSession, path: str, image: str) -> str:
    result = await session.call_tool("describe_data_source", {"path": path})
    text = _text(result)
    assert result.isError, f"{path} should need the ros extra on this install: {text}"
    assert "bagel-mcp[ros]" in text and image in text, f"{path}: error names no way out: {text}"
    assert "No module named" not in text, f"{path}: raw import error leaked: {text}"
    return f"{path}: refused, pointing at the ros extra and {image}"


async def _check_reduce_writes_a_bag(
    session: ClientSession, source: str, scratch: pathlib.Path
) -> str:
    """Run a reduce pipeline over the synthetic IMU bag; the backend must reopen its .db3."""
    from bagel_mcp import bags

    config = {
        "name": "pip_smoke_reduce",
        "site": "smoke",
        "asset": "imu",
        "path": source,
        "allow_failure": False,
        "cadence": {"topic": "/imu", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
                "args": {
                    "event_topic": "/imu",
                    "predicate": "\"/imu\"['linear_acceleration']['x'] < -10",
                    "pre_seconds": 1.0,
                    "post_seconds": 1.0,
                },
            }
        ],
    }
    result = await session.call_tool("run_pipeline", {"config": config})
    assert not result.isError, f"run_pipeline: {_text(result)}"
    (summary,) = _parsed(result)
    outputs = [pathlib.Path(path) for path in summary["artifacts"]]
    assert outputs and (outputs[0] / "metadata.yaml").exists(), summary
    assert (scratch / "artifacts") in outputs[0].parents, outputs
    reader = bags.open_reader(outputs[0], ros_version=2)
    assert reader.info.message_count > 0
    source_count = bags.open_reader(source, ros_version=2).info.message_count
    assert reader.info.message_count < source_count, "reduction must drop data"
    kept, total = reader.info.message_count, source_count
    return f"reduce.ros2.db3 wrote {outputs[0].name}: {kept}/{total} messages"


async def run(repo: pathlib.Path, scratch: pathlib.Path) -> list[str]:
    """Drive the installed server over stdio and return one line per check."""
    installed = importlib.util.find_spec("bagel_mcp")
    assert installed and installed.origin, "bagel_mcp is not installed"
    origin = pathlib.Path(installed.origin).resolve()
    assert not origin.is_relative_to(repo.resolve()), (
        f"bagel_mcp imports from the checkout ({origin}), not from the wheel"
    )
    console_script = shutil.which("bagel-mcp", path=str(pathlib.Path(sys.executable).parent))
    assert console_script, "bagel-mcp console script missing from the venv"

    samples = repo / "data" / "sample"
    ros_extra = importlib.util.find_spec("rosbags") is not None
    bag_sources: list[tuple[str, dict]] = []
    if ros_extra:
        ros_bags = _load_fixture_module(repo, "ros_bags")
        bag_sources = [
            (str(samples / "ros2" / "db3"), {}),  # rosbag2 sqlite3 directory, five parts
            (str(samples / "ros2" / "db3_zstd"), {}),  # file-level zstd
            (str(samples / "ros1" / "sample.bag"), {}),
            (str(ros_bags.write_ros2_imu_bag(scratch / "imu_ros2", "sqlite3")), {}),
            (str(ros_bags.write_ros1_imu_bag(scratch / "imu_ros1.bag", "lz4")), {}),
        ]
    sources: list[tuple[str, dict]] = [
        *bag_sources,
        (str(samples / "ros2" / "mcap"), {}),  # rosbag2 MCAP directory, generic reader
        (str(samples / "ros2" / "mcap_zstd"), {}),  # zstd-compressed rosbag2 MCAP
        (str(samples / "copper" / "imu_probe.mcap"), {}),  # bare MCAP file
        # PX4 field descriptions come from a clone of PX4-Autopilot (needs git and
        # the network, as in Docker); the parse path itself does not.
        (str(samples / "px4" / "sample.ulg"), {"download_description": False}),
        (str(samples / "ardupilot" / "sample.bin"), {}),  # topic drill needs ardupilot.org
        (str(samples / "betaflight" / "sample.bbl"), {"log_index": 1}),  # flash chip: n logs/file
        (str(samples / "betaflight" / "sample.BFL"), {}),
        (
            str(samples / "pyarrow" / "csv"),
            {"timestamp_column": "t", "timestamp_format": "seconds"},
        ),
        (str(samples / "waffle" / "robot.waffleform.yaml"), {}),
        *_generated_inputs(repo, scratch),
    ]
    lines: list[str] = []
    parameters = StdioServerParameters(
        command=console_script,
        args=["--transport", "stdio"],
        cwd=str(scratch),
        env={
            # The venv first, then the user's PATH: GitPython (px4 extra) wants git.
            "PATH": os.pathsep.join(
                [str(pathlib.Path(sys.executable).parent), os.environ.get("PATH", "")]
            ),
            "HOME": str(scratch),
            "CACHE_DIRECTORY": str(scratch / "cache"),
            "ARTIFACT_DIRECTORY": str(scratch / "artifacts"),
            "USER_CAPABILITIES_DIRECTORY": str(scratch / "capabilities"),
            "STARTUP_PIPELINES_FILE": "",
        },
    )
    with (scratch / "server-stderr.txt").open("w") as stderr:
        async with stdio_client(parameters, errlog=stderr) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = {tool.name for tool in (await session.list_tools()).tools}
                assert {"describe_data_source", "describe_topic", "query_messages"} <= tools
                lines.append(f"server up over stdio with {len(tools)} tools")

                # Builtin capabilities load from the wheel, from any working directory.
                listed = await session.call_tool("list_agent_capabilities", {})
                assert not listed.isError, _text(listed)
                names = {entry["name"] for entry in _parsed(listed)}
                assert "compose/pipeline" in names, names
                rendered = await session.call_tool(
                    "run_poml_capability",
                    {"poml_path": "./bagel_mcp/agent/compose/pipeline.poml"},
                )
                assert not rendered.isError, _text(rendered)
                lines.append(f"{len(names)} builtin capabilities render from the wheel")

                ardupilot_reason = (
                    None
                    if _ardupilot_metadata_reachable()
                    else "autotest.ardupilot.org unreachable, no field metadata"
                )
                for path, args in sources:
                    reason = ardupilot_reason if path.endswith("sample.bin") else None
                    lines.append(
                        await _check_source(session, path, args, describe_only_reason=reason)
                    )

                logs = await session.call_tool(
                    "read_loggings", {"path": str(samples / "ros" / "log")}
                )
                assert not logs.isError, _text(logs)
                lines.append("ROS text logs read")

                caps = await session.call_tool(
                    "list_pipeline_capabilities", {"include_unavailable": True}
                )
                assert not caps.isError, _text(caps)
                by_module = {entry["module"]: entry for entry in _parsed(caps)}
                assert by_module["bagel_mcp.pipeline.tasks.reduce.mcap"]["available"]
                if ros_extra:
                    assert by_module["bagel_mcp.pipeline.tasks.reduce.ros2.db3"]["available"]
                    lines.append("pipeline capabilities: MCAP and .db3 reduce available")
                    lines.append(
                        await _check_reduce_writes_a_bag(
                            session, str(scratch / "imu_ros2"), scratch
                        )
                    )
                else:
                    assert not by_module["bagel_mcp.pipeline.tasks.reduce.ros2.db3"]["available"]
                    lines.append(
                        "pipeline capabilities: MCAP reduce available, .db3 reduce unavailable"
                    )
                    lines.append(
                        await _check_bag_refusal(
                            session, str(samples / "ros2" / "db3"), "ros2-kilted"
                        )
                    )
                    lines.append(
                        await _check_bag_refusal(
                            session, str(samples / "ros1" / "sample.bag"), "ros1-noetic"
                        )
                    )
    return lines


def main() -> None:
    """CLI entry: print one line per check, exit non-zero on the first failure."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", type=pathlib.Path, required=True, help="the Bagel checkout")
    options = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="bagel-pip-smoke-") as scratch:
        for line in asyncio.run(run(options.repo.resolve(), pathlib.Path(scratch))):
            print(line)  # noqa: T201 -- CI log


if __name__ == "__main__":
    main()
