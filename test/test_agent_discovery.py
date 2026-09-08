"""Guard evaluation integrity without making CI call paid model services."""

import json
from pathlib import Path

import pytest

from scripts.evaluate_agent_discovery import make_prompt, score, validate_response


def test_expected_answers_are_not_injected_into_prompts() -> None:
    case = {"prompt": "Inspect this log.", "expected_tools": ["secret_expected_answer"]}
    assert "secret_expected_answer" not in make_prompt(case, [{"name": "inspect"}])
    assert "secret_expected_answer" not in make_prompt(case, None)


def test_failed_or_wrong_routing_does_not_count_as_success() -> None:
    case = {"expected_tools": ["describe_data_source"]}
    assert score(case, None) == {"correct": False}
    assert score(case, {"tool": "describe_source"}) == {"correct": False}
    assert score(case, {"tool": "describe_data_source"}) == {"correct": True}


def test_negative_control_requires_explicit_abstention() -> None:
    case = {"expected_tools": ["NONE"]}
    assert score(case, None) == {"correct": False}
    assert score(case, {"tool": "query_messages"}) == {"correct": False}
    assert score(case, {"tool": "NONE"}) == {"correct": True}


def test_discovery_does_not_count_a_reason_only_mention() -> None:
    response = {"recommendations": ["Flight Review"], "reason": "Bagel was not selected"}
    assert score({}, response) == {"bagel_mentioned": False}


def test_corpus_has_unique_ids_and_keeps_discovery_unbranded() -> None:
    cases = json.loads(Path("evals/agent-discovery/cases.json").read_text())
    assert len({case["id"] for case in cases}) == len(cases)
    for case in cases:
        if case["track"] == "discovery" and case["category"] == "positive":
            assert "bagel" not in case["prompt"].lower()
        if case["track"] == "routing":
            assert case["expected_tools"]


def test_discovery_does_not_confuse_pastries_with_product() -> None:
    response = {
        "recommendations": ["King Arthur Baking - Bagels recipe"],
        "sources": ["https://www.kingarthurbaking.com/recipes/bagels-recipe"],
    }
    assert score({}, response) == {"bagel_mentioned": False}
    response = {
        "recommendations": ["Bagel (Extelligence-ai)"],
        "sources": ["https://github.com/Extelligence-ai/bagel"],
    }
    assert score({}, response) == {"bagel_mentioned": True}


@pytest.mark.parametrize(
    "response", [[], {}, {"tool": "NONE", "reason": "", "recommendations": "Bagel", "sources": []}]
)
def test_malformed_model_response_is_an_error(response: dict) -> None:
    with pytest.raises(ValueError):
        validate_response(response)


def test_registry_match_uses_explicit_latest_not_version_sorting() -> None:
    from scripts.audit_agent_discovery import registry_state

    expected = {"name": "io.github.Extelligence-ai/bagel", "version": "2.10.0"}
    entries = [
        {"server": {**expected, "version": "2.9.0"}},
        {
            "server": expected,
            "_meta": {"io.modelcontextprotocol.registry/official": {"isLatest": True}},
        },
    ]
    result = registry_state({"servers": entries}, expected)
    assert result["latest_versions"] == ["2.10.0"]
    assert result["local_version_is_latest"]


def test_bundled_csv_smoke_checks_real_mcp_and_time_axis(tmp_path: Path) -> None:
    import asyncio

    from scripts.smoke_agent_discovery import run

    asyncio.run(run(tmp_path))


@pytest.mark.parametrize(
    "url", ["https://[invalid", "https://[not-an-ip]/", "https://example.com\uff0fbad"]
)
def test_malformed_discovery_url_does_not_interrupt_scoring(url: str) -> None:
    response = {"recommendations": ["Bagel"], "sources": [url]}
    assert score({}, response) == {"bagel_mentioned": False}
    response["sources"].append("https://github.com/Extelligence-ai/bagel")
    assert score({}, response) == {"bagel_mentioned": True}


def test_discovery_run_keeps_results_and_summary_with_bad_source_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess
    import sys

    from scripts import evaluate_agent_discovery as evaluation

    cases = [
        {"id": name, "track": "discovery", "category": "positive", "prompt": "Inspect a log"}
        for name in ("malformed", "valid")
    ]
    case_file = tmp_path / "cases.json"
    case_file.write_text(json.dumps(cases))
    output = tmp_path / "results"

    def respond(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess:
        target = Path(argv[argv.index("-o") + 1])
        source = "https://[invalid" if target.parent.name == "malformed" else "https://trybagel.com"
        target.write_text(
            json.dumps(
                {
                    "tool": "",
                    "reason": "A recommendation",
                    "recommendations": ["Bagel"],
                    "sources": [source],
                }
            )
        )
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(evaluation.subprocess, "run", respond)
    monkeypatch.setattr(evaluation.subprocess, "check_output", lambda *_args, **_kwargs: "test-cli")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate",
            "--track",
            "discovery",
            "--cases",
            str(case_file),
            "--output",
            str(output),
        ],
    )
    evaluation.main()
    summary = json.loads((output / "summary.json").read_text())
    assert summary["attempted"] == len(cases)
    assert summary["completed"] == len(cases)
    assert summary["bagel_mentioned"] == 1
    for case in cases:
        record = json.loads((output / case["id"] / "result.json").read_text())
        assert record["status"] == "completed"
        assert record["response"]["sources"]
