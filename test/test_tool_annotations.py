"""Every MCP tool must declare behavior annotations (OpenAI marketplace MCP checks).

The matrix below is the source of truth reviewers and clients rely on:
read-only vs writer, whether a retry can repeat an externally visible action
(idempotent), whether existing files can be unlinked (destructive), and
whether caller-selected external endpoints can be reached (open-world).
A new tool fails here until it classifies itself.
"""

import subprocess
import sys

from bagel_mcp import server

# name: (read_only, idempotent, destructive, open_world)
EXPECTED = {
    "describe_data_source": (True, True, False, False),
    "describe_topic": (True, True, False, False),
    "query_messages": (True, True, False, False),
    "read_loggings": (True, True, False, False),
    "compare_logs": (True, True, False, False),
    "check_robot_health": (True, True, False, False),
    "list_live_topics": (True, True, False, True),
    "subscribe_live_topics": (False, False, True, True),
    "unsubscribe_live_topics": (False, True, False, True),
    "run_poml_capability": (True, True, False, False),
    "list_agent_capabilities": (True, True, False, False),
    "save_agent_capability": (False, True, True, False),
    "delete_capability": (False, True, True, False),
    "list_pipeline_capabilities": (True, True, False, False),
    "preview_pipeline": (True, True, False, False),
    "preview_anomalies": (True, True, False, False),
    "save_pipeline": (False, True, False, False),
    "list_pipelines": (True, True, False, False),
    "get_pipeline": (True, True, False, False),
    "delete_pipeline": (False, True, True, False),
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


def test_server_module_does_not_import_paho_or_cryptography_eagerly() -> None:
    """Lazy-import regression: server.py imports the fleet `control` and `identity`
    modules at module scope to register the fleet tools, and neither may drag paho
    or cryptography in eagerly (see each module's own docstring).

    Runs in a fresh interpreter: in the suite, other tests have already imported
    both packages, so a sys.modules sweep here could never fail. pyOpenSSL is
    hidden because botocore (boto3 is a core dependency) loads it, and with it
    cryptography, whenever it is installed (the `live` extra brings it through
    roslibpy[tls]); botocore falls back to the ssl module without it.
    """
    probe = (
        "import sys; sys.modules['OpenSSL'] = None; import bagel_mcp.server; "
        "print(sorted(m for m in sys.modules "
        "if m.split('.')[0] in ('paho', 'cryptography')))"
    )
    result = subprocess.run(  # noqa: S603 -- fixed argv, no user input
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]", result.stdout
