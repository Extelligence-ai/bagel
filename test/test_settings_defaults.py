"""Settings must work without a .env file (issue #158).

The .env used to be baked into every published image because three fields had
no defaults, so Settings() raised at import without it. Any future secret in
.env would then be permanently embedded in public image layers. The fields now
carry defaults matching the repo's .env, so images need no .env at all.
"""

import pathlib
import subprocess
import sys


def test_settings_instantiate_without_env_file(tmp_path: pathlib.Path) -> None:
    """Import settings from a directory with no .env: must not raise."""
    project = subprocess.run(  # noqa: S603 -- trusted sys.executable, no untrusted input
        [sys.executable, "-c", "import pathlib; print(pathlib.Path.cwd())"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    result = subprocess.run(  # noqa: S603 -- trusted sys.executable, no untrusted input
        [
            sys.executable,
            "-c",
            f"import sys; sys.path.insert(0, {project!r}); "
            "from settings import settings; "
            "print(settings.CONTAINER_MODE, settings.MCP_SERVER_HOST, settings.MCP_SERVER_PORT)",
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,  # no .env here
        check=False,  # non-zero return code is asserted on below
    )
    assert result.returncode == 0, result.stderr
    # loopback default: a bare host run must not expose the endpoint (Codex, #160)
    assert result.stdout.strip() == "False 127.0.0.1 8000"


def test_defaults_match_repo_env_file() -> None:
    """Defaults must equal the tracked .env so behavior is unchanged."""
    import pathlib

    env = dict(
        line.split("=", 1)
        for line in pathlib.Path(".env").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    )
    from settings import Settings

    fields = Settings.model_fields
    assert fields["MCP_SERVER_HOST"].default == env["MCP_SERVER_HOST"]
    assert str(fields["MCP_SERVER_PORT"].default) == env["MCP_SERVER_PORT"]
    assert fields["CONTAINER_MODE"].default is False
