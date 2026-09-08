"""Reject incomplete coverage input and enforce independent line/branch floors."""

import argparse
import json
from pathlib import Path

import yaml


def expected_files(workflow: dict) -> set[str]:
    """Derive artifact names from the actual test matrix."""
    jobs = workflow["jobs"]
    services = jobs["docker"]["strategy"]["matrix"]["include"]
    versions = jobs["host-tests"]["strategy"]["matrix"]["python"]
    return {f".coverage.{row['service']}" for row in services} | {
        f".coverage.host-{version}" for version in versions
    }


def check_report(report: dict) -> None:
    """Keep the existing 64% line floor and add a separate 50% branch floor."""
    totals = report["totals"]
    for kind, covered, total, floor in (
        ("line", "covered_lines", "num_statements", 64),
        ("branch", "covered_branches", "num_branches", 50),
    ):
        denominator = totals[total]
        if denominator <= 0:
            raise ValueError(f"Missing {kind} coverage")
        percent = 100 * totals[covered] / denominator
        if percent < floor:
            raise ValueError(f"{kind} coverage {percent:.2f}% is below {floor}%")
        print(f"{kind} coverage: {percent:.2f}% (floor {floor}%)")  # noqa: T201 -- CLI summary


def main() -> None:
    """Validate artifacts before combining, or enforce a combined JSON report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, nargs="?")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.report:
        check_report(json.loads(args.report.read_text()))
    elif args.directory:
        workflow = yaml.safe_load(Path(".github/workflows/test.yaml").read_text())
        missing = expected_files(workflow) - {
            p.name for p in args.directory.iterdir() if p.stat().st_size
        }
        if missing:
            raise ValueError(f"Missing coverage inputs: {sorted(missing)}")
    else:
        parser.error("provide a coverage directory or --report")


if __name__ == "__main__":
    main()
