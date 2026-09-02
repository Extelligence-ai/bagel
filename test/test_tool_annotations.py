"""Every MCP tool must declare behavior annotations (OpenAI marketplace MCP checks).

The matrix below is the source of truth reviewers and clients rely on:
read-only vs writer, whether a retry can repeat an externally visible action
(idempotent), whether existing files can be unlinked (destructive), and
whether caller-selected external endpoints can be reached (open-world).
A new tool fails here until it classifies itself.
"""

import importlib
import sys

import pytest

import server

# name: (read_only, idempotent, destructive, open_world)
EXPECTED = {
    "describe_data_source": (True, True, False, False),
    "describe_topic": (True, True, False, False),
    "query_messages": (True, True, False, False),
    "read_loggings": (True, True, False, False),
    "list_live_topics": (True, True, False, True),
    "subscribe_live_topics": (False, False, True, True),
    "run_poml_capability": (True, True, False, False),
    "list_agent_capabilities": (True, True, False, False),
    "save_agent_capability": (False, True, True, False),
    "list_pipeline_capabilities": (True, True, False, False),
    "preview_pipeline": (True, True, False, False),
    "save_pipeline": (False, True, False, False),
    "run_pipeline": (False, False, False, True),
    "run_pipeline_batch": (False, False, False, True),
    "export_for_plotjuggler": (False, True, False, False),
    "export_for_rerun": (False, True, False, False),
    "export_for_lichtblick": (False, True, False, False),
    "export_for_lerobot": (False, True, False, False),
    "snap_hardware": (False, False, False, False),
    "enroll_fleet_identity": (False, False, False, True),
    "stream_live_topics": (False, False, False, True),
    "stop_live_streams": (False, True, False, True),
    "pause_fleet_streaming": (False, True, False, True),
    "resume_fleet_streaming": (False, True, False, True),
    "describe_stream_status": (True, True, False, False),
    "unenroll_fleet_identity": (False, True, True, False),
}


def _tools() -> dict:
    return dict(server.server._tool_manager._tools.items())


def test_every_tool_is_classified() -> None:
    assert set(_tools()) == set(EXPECTED)


def test_annotations_match_the_declared_matrix() -> None:
    for name, (read_only, idempotent, destructive, open_world) in EXPECTED.items():
        annotations = _tools()[name].annotations
        assert annotations is not None, f"{name} has no annotations"
        got = (
            annotations.readOnlyHint,
            annotations.idempotentHint,
            annotations.destructiveHint,
            annotations.openWorldHint,
        )
        assert got == (read_only, idempotent, destructive, open_world), name


def test_server_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy-import regression: server.py now imports `control` at module scope
    (alongside the existing `fleet_identity` import) to register the fleet
    tools below -- neither drags paho or cryptography in eagerly (see each
    module's own docstring), mirroring test_control.py's/test_service.py's
    sweep idiom.
    """
    stray = [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]
    if stray:
        pytest.skip(f"paho/cryptography already preloaded by another plugin: {stray}")
    monkeypatch.delitem(sys.modules, "server", raising=False)
    importlib.import_module("server")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )
