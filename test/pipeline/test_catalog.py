"""Tests for the pipeline module catalog."""

from src.pipeline.catalog import catalog, catalog_as_text


def test_catalog_returns_gates_and_tasks() -> None:
    cat = catalog()
    assert "gates" in cat
    assert "tasks" in cat
    assert len(cat["gates"]) >= 1
    assert len(cat["tasks"]) >= 1


def test_catalog_gate_has_required_keys() -> None:
    cat = catalog()
    for gate in cat["gates"]:
        assert "module" in gate
        assert "class" in gate
        assert "description" in gate
        assert "params" in gate


def test_catalog_task_has_required_keys() -> None:
    cat = catalog()
    for task in cat["tasks"]:
        assert "module" in task
        assert "class" in task
        assert "description" in task
        assert "params" in task


def test_catalog_as_text_contains_modules() -> None:
    text = catalog_as_text()
    assert "SqlQueryGate" in text
    assert "WriteTopicsToFileTask" in text
    assert "src.pipeline.gates.sql" in text
    assert "src.pipeline.tasks.write_topics_to_file" in text


def test_catalog_params_include_types() -> None:
    cat = catalog()
    sql_gate = next(g for g in cat["gates"] if g["class"] == "SqlQueryGate")
    param_names = [p["name"] for p in sql_gate["params"]]
    assert "topic" in param_names
    assert "statement" in param_names
