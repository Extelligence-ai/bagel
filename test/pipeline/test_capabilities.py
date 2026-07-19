"""Tests for pipeline capability introspection."""

from bagel.pipeline import capabilities


def _by_module(entries: list[dict], module: str) -> dict | None:
    return next((entry for entry in entries if entry["module"] == module), None)


def test_discovers_the_sql_gate() -> None:
    # gates.sql has no heavy dependencies, so it is always available.
    entries = capabilities.list_capabilities()
    sql_gate = _by_module(entries, "bagel.pipeline.gates.sql")
    assert sql_gate is not None
    assert sql_gate["available"] is True
    assert sql_gate["kind"] == "gate"
    assert sql_gate["class"] == "SqlQuery"
    param_names = {p["name"] for p in sql_gate["parameters"]}
    assert {"topic", "statement"} <= param_names
    assert all(p["required"] for p in sql_gate["parameters"])


def test_available_entries_carry_parameter_metadata() -> None:
    entries = capabilities.list_capabilities()
    available = [e for e in entries if e.get("available")]
    assert available, "expected at least one importable capability"
    for entry in available:
        assert entry["kind"] in {"task", "gate", "operator"}
        assert "summary" in entry
        for parameter in entry["parameters"]:
            assert {"name", "required", "default", "type"} <= set(parameter)


def test_unavailable_modules_reported_when_requested() -> None:
    # ROS-dependent modules cannot import without ROS; they should be flagged, not crash.
    without = capabilities.list_capabilities(include_unavailable=False)
    with_unavailable = capabilities.list_capabilities(include_unavailable=True)
    assert all(entry.get("available") for entry in without)
    assert len(with_unavailable) >= len(without)


def test_results_sorted_by_module() -> None:
    entries = capabilities.list_capabilities(include_unavailable=True)
    modules = [entry["module"] for entry in entries]
    assert modules == sorted(modules)
