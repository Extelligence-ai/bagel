"""Regression checks for the gates that guard quality themselves."""

from contextlib import nullcontext
from unittest.mock import Mock

import pytest

from scripts import seed_databases
from scripts.audit_dependencies import vulnerabilities
from scripts.check_coverage import check_report, expected_files


def test_influx_startup_connection_reset_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_CONTAINER", "ci-owned-container")
    monkeypatch.setattr(seed_databases.subprocess, "run", Mock())
    monkeypatch.setattr(seed_databases.time, "sleep", Mock())
    request = Mock(side_effect=[ConnectionResetError("starting"), nullcontext()])
    monkeypatch.setattr(seed_databases.urllib.request, "urlopen", request)
    seed_databases.main()
    assert request.call_count == 2


def test_expected_coverage_inputs_include_every_matrix_entry() -> None:
    workflow = {
        "jobs": {
            "docker": {"strategy": {"matrix": {"include": [{"service": "iot"}]}}},
            "host-tests": {"strategy": {"matrix": {"python": ["3.10", "3.12"]}}},
        }
    }
    assert expected_files(workflow) == {
        ".coverage.iot",
        ".coverage.host-3.10",
        ".coverage.host-3.12",
    }


@pytest.mark.parametrize(("lines", "branches"), [(63, 100), (100, 49)])
def test_line_and_branch_floors_are_independent(lines: int, branches: int) -> None:
    with pytest.raises(ValueError, match="below"):
        check_report(
            {
                "totals": {
                    "covered_lines": lines,
                    "num_statements": 100,
                    "covered_branches": branches,
                    "num_branches": 100,
                }
            }
        )


def test_coverage_at_both_floors_passes() -> None:
    check_report(
        {
            "totals": {
                "covered_lines": 64,
                "num_statements": 100,
                "covered_branches": 50,
                "num_branches": 100,
            }
        }
    )


@pytest.mark.parametrize("dependencies", [[], [{"name": "unknown", "skip_reason": "not found"}]])
def test_incomplete_audit_cannot_pass(dependencies: list) -> None:
    with pytest.raises(ValueError):
        vulnerabilities({"dependencies": dependencies})


def test_new_advisories_are_distinguished_from_existing_debt() -> None:
    before = vulnerabilities({"dependencies": [{"name": "some_package", "vulns": [{"id": "OLD"}]}]})
    after = vulnerabilities(
        {"dependencies": [{"name": "some-package", "vulns": [{"id": "OLD"}, {"id": "NEW"}]}]}
    )
    assert after - before == {("some-package", "NEW")}
