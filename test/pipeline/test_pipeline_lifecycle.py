"""Tests for the list_pipelines and delete_pipeline MCP tools.

save_pipeline was a create-only dead end: nothing let a caller discover what
had been saved or remove it. These round out the lifecycle: list_pipelines
mirrors save_pipeline's directory convention (flat `<directory>/<name>.yaml`
files), and delete_pipeline removes exactly one by the same name list_pipelines
reports, refusing path traversal before touching the filesystem.

list_pipelines and delete_pipeline take NO `directory` argument (review
#224/Codex P1): a caller-chosen root would let an MCP caller aim deletion at
any directory on the host. Both are hard-confined to the single trusted root,
`settings.PIPELINES_DIRECTORY` -- the same directory save_pipeline defaults
to -- read live so tests can monkeypatch it, same as
USER_CAPABILITIES_DIRECTORY for agent capabilities. save_pipeline itself
still accepts an explicit `directory` (it's a write tool the caller directs
on purpose); the lifecycle tools never do.

Second-delete contract: delete_pipeline is NOT a silent no-op on a repeat
call -- it raises the same "unknown name" ValueError as any other unknown
name, listing what remains. The idempotentHint annotation refers to the
tool's *effect on state* (the pipeline stays gone either way), not to the
response; this matches conventional DELETE semantics (a second DELETE 404s).
"""

import pathlib

import pytest
import yaml

import server
from settings import settings


@pytest.fixture
def pipelines_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    directory = tmp_path / "pipelines"
    directory.mkdir()
    monkeypatch.setattr(settings, "PIPELINES_DIRECTORY", str(directory))
    return directory


def _config(name: str = "csv_smoke") -> dict:
    return {
        "name": name,
        "site": "test_site",
        "asset": "test_asset",
        "path": "./data/sample/pyarrow/csv/flight.csv",
        "allow_failure": False,
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [
            {
                "module": "src.pipeline.tasks.write_topics_to_file",
                "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                "args": {"topics": ["message"], "output_format": "csv"},
            }
        ],
    }


def _save(name: str = "csv_smoke") -> str:
    """Save via the settings-derived default directory (no `directory` kwarg)."""
    return server.save_pipeline(_config(name), name)


def test_list_pipelines_missing_directory_returns_empty(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "PIPELINES_DIRECTORY", str(tmp_path / "nonexistent"))
    assert server.list_pipelines() == []


def test_list_pipelines_reports_name_path_and_summary(pipelines_dir: pathlib.Path) -> None:
    _save("csv_smoke")
    entries = server.list_pipelines()
    assert len(entries) == 1
    entry = entries[0]
    assert entry["name"] == "csv_smoke"
    assert entry["path"] == str(pipelines_dir / "csv_smoke.yaml")
    assert "1 task" in entry["summary"]
    assert "test_site" in entry["summary"] or "test_asset" in entry["summary"]


def test_list_pipelines_sorted_by_name(pipelines_dir: pathlib.Path) -> None:
    _save("zeta")
    _save("alpha")
    names = [entry["name"] for entry in server.list_pipelines()]
    assert names == ["alpha", "zeta"]


def test_list_pipelines_falls_back_for_unparseable_yaml(pipelines_dir: pathlib.Path) -> None:
    (pipelines_dir / "not_a_pipeline.yaml").write_text("- just\n- a\n- list\n", encoding="utf-8")
    entries = server.list_pipelines()
    assert len(entries) == 1
    assert entries[0]["name"] == "not_a_pipeline"
    assert entries[0]["summary"]  # some fallback text, non-empty


def test_list_pipelines_summary_handles_non_string_fields(pipelines_dir: pathlib.Path) -> None:
    """A hand-edited pipeline can give site/asset a non-string value (Codex/Copilot review)."""
    (pipelines_dir / "weird.yaml").write_text(
        "name: weird\nsite: 123\nasset: 456\ntasks:\n- module: x\n",
        encoding="utf-8",
    )
    entries = server.list_pipelines()
    entry = next(e for e in entries if e["name"] == "weird")
    assert "123" in entry["summary"]
    assert "456" in entry["summary"]


def test_delete_pipeline_round_trip(pipelines_dir: pathlib.Path) -> None:
    _save("csv_smoke")
    assert any(e["name"] == "csv_smoke" for e in server.list_pipelines())

    result = server.delete_pipeline("csv_smoke")
    assert result["name"] == "csv_smoke"
    assert not (pipelines_dir / "csv_smoke.yaml").exists()
    assert not any(e["name"] == "csv_smoke" for e in server.list_pipelines())


def test_delete_pipeline_second_delete_raises(pipelines_dir: pathlib.Path) -> None:
    _save("csv_smoke")
    server.delete_pipeline("csv_smoke")
    with pytest.raises(ValueError, match="csv_smoke"):
        server.delete_pipeline("csv_smoke")


def test_delete_pipeline_unknown_name_lists_available(pipelines_dir: pathlib.Path) -> None:
    _save("csv_smoke")
    with pytest.raises(ValueError, match="csv_smoke"):
        server.delete_pipeline("does_not_exist")


def test_delete_pipeline_has_no_directory_argument() -> None:
    """The trusted-root fix (review #224): the signature no longer accepts one."""
    import inspect

    assert "directory" not in inspect.signature(server.delete_pipeline).parameters
    assert "directory" not in inspect.signature(server.list_pipelines).parameters


def test_delete_pipeline_cannot_target_a_different_directory_via_kwarg(
    pipelines_dir: pathlib.Path,
) -> None:
    """Regression guard for the P1 finding: no kwarg exists to escape the trusted root."""
    with pytest.raises(TypeError):
        server.delete_pipeline("csv_smoke", directory=str(pipelines_dir.parent))  # type: ignore[call-arg]


@pytest.mark.parametrize("bad_name", ["../escape", "..", "a/b", "/etc/passwd"])
def test_delete_pipeline_rejects_traversal(pipelines_dir: pathlib.Path, bad_name: str) -> None:
    _save("csv_smoke")
    outside_marker = pipelines_dir.parent / "escape.yaml"
    with pytest.raises(ValueError):
        server.delete_pipeline(bad_name)
    assert not outside_marker.exists()
    # nothing inside the real directory was touched either
    assert (pipelines_dir / "csv_smoke.yaml").exists()


def test_delete_pipeline_rejects_symlink_escape(
    tmp_path: pathlib.Path, pipelines_dir: pathlib.Path
) -> None:
    """A name that passes the syntax guard but escapes via symlink resolution
    must still be caught by the containment check itself (Copilot review):
    the traversal-string tests above never reach that layer.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.yaml"
    victim.write_text("name: victim\n", encoding="utf-8")
    (pipelines_dir / "escape.yaml").symlink_to(victim)

    with pytest.raises(ValueError, match="outside|escape"):
        server.delete_pipeline("escape")
    assert victim.exists()


def test_delete_pipeline_confinement_checked_before_unlink(
    pipelines_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A traversal name must never reach unlink()."""

    def boom(self: pathlib.Path) -> None:  # pragma: no cover - should never run
        raise AssertionError("unlink should not be called for a traversal name")

    monkeypatch.setattr(pathlib.Path, "unlink", boom)
    with pytest.raises(ValueError):
        server.delete_pipeline("../escape")


def test_save_pipeline_default_directory_follows_settings(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """save_pipeline's default directory is settings.PIPELINES_DIRECTORY, read live."""
    monkeypatch.setattr(settings, "PIPELINES_DIRECTORY", str(tmp_path / "pipelines"))
    output = server.save_pipeline(_config(), "csv_smoke")
    assert output == str(tmp_path / "pipelines" / "csv_smoke.yaml")


def test_save_pipeline_explicit_directory_still_overrides(tmp_path: pathlib.Path) -> None:
    output = server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    loaded = yaml.safe_load(pathlib.Path(output).read_text())
    assert loaded["name"] == "csv_smoke"
