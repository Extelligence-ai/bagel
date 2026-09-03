"""Tests for the list_pipelines and delete_pipeline MCP tools.

save_pipeline was a create-only dead end: nothing let a caller discover what
had been saved or remove it. These round out the lifecycle: list_pipelines
mirrors save_pipeline's directory convention (flat `<directory>/<name>.yaml`
files), and delete_pipeline removes exactly one by the same name list_pipelines
reports, refusing path traversal before touching the filesystem.

Second-delete contract: delete_pipeline is NOT a silent no-op on a repeat call
-- it raises the same "unknown name" ValueError as any other unknown name,
listing what remains. The idempotentHint annotation refers to the tool's
*effect on state* (the pipeline stays gone either way), not to the response;
this matches conventional DELETE semantics (a second DELETE 404s).
"""

import pathlib

import pytest
import yaml

import server


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


def test_list_pipelines_empty_directory_returns_empty(tmp_path: pathlib.Path) -> None:
    assert server.list_pipelines(directory=str(tmp_path / "nonexistent")) == []


def test_list_pipelines_reports_name_path_and_summary(tmp_path: pathlib.Path) -> None:
    server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    entries = server.list_pipelines(directory=str(tmp_path))
    assert len(entries) == 1
    entry = entries[0]
    assert entry["name"] == "csv_smoke"
    assert entry["path"] == str(tmp_path / "csv_smoke.yaml")
    assert "1 task" in entry["summary"]
    assert "test_site" in entry["summary"] or "test_asset" in entry["summary"]


def test_list_pipelines_sorted_by_name(tmp_path: pathlib.Path) -> None:
    server.save_pipeline(_config("zeta"), "zeta", directory=str(tmp_path))
    server.save_pipeline(_config("alpha"), "alpha", directory=str(tmp_path))
    names = [entry["name"] for entry in server.list_pipelines(directory=str(tmp_path))]
    assert names == ["alpha", "zeta"]


def test_list_pipelines_falls_back_for_unparseable_yaml(tmp_path: pathlib.Path) -> None:
    (tmp_path / "not_a_pipeline.yaml").write_text("- just\n- a\n- list\n", encoding="utf-8")
    entries = server.list_pipelines(directory=str(tmp_path))
    assert len(entries) == 1
    assert entries[0]["name"] == "not_a_pipeline"
    assert entries[0]["summary"]  # some fallback text, non-empty


def test_delete_pipeline_round_trip(tmp_path: pathlib.Path) -> None:
    server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    assert any(e["name"] == "csv_smoke" for e in server.list_pipelines(directory=str(tmp_path)))

    result = server.delete_pipeline("csv_smoke", directory=str(tmp_path))
    assert result["name"] == "csv_smoke"
    assert not (tmp_path / "csv_smoke.yaml").exists()
    assert not any(e["name"] == "csv_smoke" for e in server.list_pipelines(directory=str(tmp_path)))


def test_delete_pipeline_second_delete_raises(tmp_path: pathlib.Path) -> None:
    server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    server.delete_pipeline("csv_smoke", directory=str(tmp_path))
    with pytest.raises(ValueError, match="csv_smoke"):
        server.delete_pipeline("csv_smoke", directory=str(tmp_path))


def test_delete_pipeline_unknown_name_lists_available(tmp_path: pathlib.Path) -> None:
    server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    with pytest.raises(ValueError, match="csv_smoke"):
        server.delete_pipeline("does_not_exist", directory=str(tmp_path))


@pytest.mark.parametrize("bad_name", ["../escape", "..", "a/b", "/etc/passwd"])
def test_delete_pipeline_rejects_traversal(tmp_path: pathlib.Path, bad_name: str) -> None:
    server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    outside_marker = tmp_path.parent / "escape.yaml"
    with pytest.raises(ValueError):
        server.delete_pipeline(bad_name, directory=str(tmp_path))
    assert not outside_marker.exists()
    # nothing inside the real directory was touched either
    assert (tmp_path / "csv_smoke.yaml").exists()


def test_delete_pipeline_confinement_checked_before_unlink(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A traversal name must never reach unlink()."""

    def boom(self: pathlib.Path) -> None:  # pragma: no cover - should never run
        raise AssertionError("unlink should not be called for a traversal name")

    monkeypatch.setattr(pathlib.Path, "unlink", boom)
    with pytest.raises(ValueError):
        server.delete_pipeline("../escape", directory=str(tmp_path))


def test_save_pipeline_still_a_valid_yaml_writer(tmp_path: pathlib.Path) -> None:
    """Sanity check the fixture config still round-trips (regression guard)."""
    output = server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    loaded = yaml.safe_load(pathlib.Path(output).read_text())
    assert loaded["name"] == "csv_smoke"
