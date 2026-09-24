"""The compose/anomaly_pipeline capability teaches the LLM the anomaly-gate workflow."""

import pathlib

from src.agent.capabilities import list_capabilities


def _recipe() -> str:
    return pathlib.Path("src/agent/compose/anomaly_pipeline.poml").read_text(encoding="utf-8")


def test_recipe_is_discovered_with_a_summary() -> None:
    by_name = {c["name"]: c for c in list_capabilities()}
    assert "compose/anomaly_pipeline" in by_name
    assert by_name["compose/anomaly_pipeline"]["summary"].startswith("Turn")


def test_recipe_walks_the_calibrate_first_workflow() -> None:
    text = _recipe()
    for step in (
        "describe_topic",
        "preview_anomalies",
        "list_pipeline_capabilities",
        "src.pipeline.gates.anomaly",
        "write_annotations",
    ):
        assert step in text, step


def test_recipe_tells_the_model_to_watch_rates_not_states() -> None:
    text = _recipe().lower()
    assert "rates" in text and "orientation" in text
    assert "max_signals" in text or "signals" in text


def test_recipe_states_the_beta_limits() -> None:
    text = _recipe()
    assert "TYPESAFE_API_KEY" in text
    assert "recorded" in text.lower()
