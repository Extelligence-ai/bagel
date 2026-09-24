"""The publish workflow must actually publish on pushes to main.

Regression: `resolve-audit-baseline` only runs on manual dispatch. On a push it is
skipped, and GitHub skips every job downstream of a skipped job unless that job's
`if` opts out of the implicit success() check. From 2026-09-04 the `docker` job was
skipped on every push to main, so no image (including 2.3.0) was published.
"""

import pathlib

import yaml


def _jobs() -> dict:
    workflow = yaml.safe_load(pathlib.Path(".github/workflows/publish.yaml").read_text())
    return workflow["jobs"]


def test_docker_job_survives_skipped_upstream_jobs() -> None:
    condition = _jobs()["docker"]["if"]
    assert "always()" in condition
    assert "!failure()" in condition
    assert "!cancelled()" in condition


def test_docker_job_still_only_publishes_from_main() -> None:
    assert "github.ref == 'refs/heads/main'" in _jobs()["docker"]["if"]


def test_docker_job_still_waits_for_validation() -> None:
    assert set(_jobs()["docker"]["needs"]) >= {
        "validate-tests",
        "validate-lint",
        "validate-dependencies",
    }
