"""Tests for the agent-capability discovery backing list_agent_capabilities."""

import pathlib

from src.agent.capabilities import list_capabilities


def test_lists_every_poml_under_src_agent() -> None:
    found = {capability["name"] for capability in list_capabilities()}
    on_disk = {
        str(file.relative_to("src/agent").with_suffix(""))
        for file in pathlib.Path("src/agent").rglob("*.poml")
    }
    assert found == on_disk
    assert len(found) >= 5  # compose/pipeline, diagnose/latency, describe/*, examples/woof


def test_every_capability_has_path_that_exists_and_nonempty_summary() -> None:
    for capability in list_capabilities():
        assert pathlib.Path(capability["path"]).exists(), capability
        assert capability["summary"].strip(), capability
        assert capability["summary"].endswith(".")


def test_pipeline_capability_summary_is_first_task_sentence() -> None:
    by_name = {capability["name"]: capability for capability in list_capabilities()}
    summary = by_name["compose/pipeline"]["summary"]
    assert summary.startswith("Turn the user's natural-language request")


def test_results_sorted_by_name() -> None:
    names = [capability["name"] for capability in list_capabilities()]
    assert names == sorted(names)


def test_mcp_tool_returns_discovery_results() -> None:
    import server

    result = server.list_agent_capabilities()
    assert result == list_capabilities()
