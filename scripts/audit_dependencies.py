"""Block new advisories and retain reports of existing dependency debt."""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def vulnerabilities(report: dict) -> set[tuple[str, str]]:
    """Normalize findings, rejecting empty or incomplete audits."""
    dependencies = report["dependencies"]
    if not dependencies or any("skip_reason" in dep for dep in dependencies):
        raise ValueError("Empty or incomplete dependency audit")
    return {
        (dep["name"].lower().replace("_", "-"), vuln["id"])
        for dep in dependencies
        for vuln in dep["vulns"]
    }


def audit(project: Path, output: Path) -> set[tuple[str, str]]:
    """Audit locked groups for this platform without installing the ML stack."""
    with tempfile.TemporaryDirectory(prefix="bagel-audit-") as temporary:
        requirements = Path(temporary) / "requirements.txt"
        subprocess.run(  # noqa: S603 -- fixed tool and CLI paths
            [  # noqa: S607 -- uv installed by pinned setup-uv
                "uv",
                "export",
                "--frozen",
                "--all-groups",
                "--no-hashes",
                "--no-emit-project",
                "--format",
                "requirements-txt",
                "--output-file",
                str(requirements),
            ],
            cwd=project,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        result = subprocess.run(  # noqa: S603 -- locked auditor; no dependency installation
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--disable-pip",
                "--no-deps",
                "-r",
                str(requirements),
                "--format",
                "json",
                "--output",
                str(output.resolve()),
            ],
            check=False,
        )
        if result.returncode not in (0, 1):
            raise RuntimeError(f"pip-audit failed: {result.returncode}")
        return vulnerabilities(json.loads(output.read_text()))


def main() -> None:
    """Compare against the PR base or prior main commit during publication."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    args = parser.parse_args()
    baseline = audit(args.base, Path("audit-base.json"))
    current = audit(Path.cwd(), Path("audit-current.json"))
    introduced = current - baseline
    print(  # noqa: T201 -- CLI summary
        f"Existing: {len(current & baseline)}; resolved: {len(baseline - current)}; "
        f"new: {sorted(introduced)}"
    )
    if introduced:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
