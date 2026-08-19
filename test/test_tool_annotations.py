"""Every MCP tool must declare behavior annotations (OpenAI marketplace MCP checks).

readOnlyHint separates the query/describe surface from the artifact writers;
nothing in the toolset deletes user data or touches the open internet, so
destructiveHint and openWorldHint are false across the board -- asserting that
here keeps a future tool from shipping without making the claim consciously.
"""

import server

READ_ONLY_TOOLS = {
    "describe_data_source",
    "describe_topic",
    "query_messages",
    "read_loggings",
    "list_live_topics",
    "run_poml_capability",
    "list_agent_capabilities",
    "list_pipeline_capabilities",
    "preview_pipeline",
}

WRITER_TOOLS = {
    "subscribe_live_topics",
    "save_pipeline",
    "run_pipeline",
    "run_pipeline_batch",
    "export_for_plotjuggler",
    "export_for_rerun",
    "export_for_lichtblick",
    "export_for_lerobot",
    "snap_hardware",
}


def _tools() -> dict:
    return {name: tool for name, tool in server.server._tool_manager._tools.items()}


def test_every_tool_is_classified() -> None:
    assert set(_tools()) == READ_ONLY_TOOLS | WRITER_TOOLS


def test_read_only_tools_are_annotated_read_only() -> None:
    for name in READ_ONLY_TOOLS:
        annotations = _tools()[name].annotations
        assert annotations is not None, f"{name} has no annotations"
        assert annotations.readOnlyHint is True, name


def test_writer_tools_are_annotated_as_local_writers() -> None:
    for name in WRITER_TOOLS:
        annotations = _tools()[name].annotations
        assert annotations is not None, f"{name} has no annotations"
        assert annotations.readOnlyHint is False, name


def test_no_tool_claims_destructive_or_open_world_behavior() -> None:
    for name, tool in _tools().items():
        assert tool.annotations.destructiveHint is False, name
        assert tool.annotations.openWorldHint is False, name
