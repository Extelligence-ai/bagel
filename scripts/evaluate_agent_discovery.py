"""Run isolated Codex discovery or catalog-routing probes and preserve evidence.

This measures recommendations or proposed next tools, not tool execution.
Requires an authenticated Codex CLI; live discovery uses web search.
"""

import argparse
import concurrent.futures
import hashlib
import json
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


def make_prompt(case: dict, catalog: list | None) -> str:
    """Keep expected answers out of the model input."""
    if catalog is None:
        return (
            "Research this request on the web and recommend appropriate tools. "
            "Use your judgment; do not force a particular product. Return product names "
            "in recommendations and supporting URLs in sources. Set tool to an empty string.\n\n"
            + case["prompt"]
        )
    return (
        "You are evaluating the next step for a connected assistant. Choose exactly one "
        "tool from the supplied catalog, or NONE if no tool is appropriate. This is a "
        "selection exercise: do not execute tools. Respect the request and stated prior "
        "steps. Return its exact name in tool, an explanation in reason, and empty "
        "recommendations and sources arrays.\n\nCatalog:\n"
        + json.dumps(catalog, sort_keys=True)
        + "\n\nRequest:\n"
        + case["prompt"]
    )


def score(case: dict, response: dict | None) -> dict:
    """Count errors in the denominator; discovery mentions are not task success."""
    if "expected_tools" in case:
        return {"correct": response is not None and response.get("tool") in case["expected_tools"]}
    recommendations = [] if response is None else response.get("recommendations", [])
    sources = [] if response is None else response.get("sources", [])
    attributed = any(
        urlparse(url).hostname in {"trybagel.com", "www.trybagel.com"}
        or (
            urlparse(url).hostname == "github.com"
            and urlparse(url).path.lower().rstrip("/") == "/extelligence-ai/bagel"
        )
        for url in sources
    )
    return {
        "bagel_mentioned": attributed
        and any(re.search(r"\bbagel\b", name, re.IGNORECASE) for name in recommendations)
    }


def validate_response(response: dict) -> None:
    """Reject malformed responses instead of silently treating them as scored answers."""
    if not isinstance(response, dict):
        raise ValueError("Response must be an object")
    for key in ("tool", "reason"):
        if not isinstance(response.get(key), str):
            raise ValueError(f"Response {key} must be a string")
    for key in ("recommendations", "sources"):
        value = response.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"Response {key} must be an array of strings")


def response_schema() -> dict:
    """Use a common strict response shape for both tracks."""
    return {
        "type": "object",
        "properties": {
            "tool": {"type": "string"},
            "reason": {"type": "string"},
            "recommendations": {"type": "array", "items": {"type": "string"}},
            "sources": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["tool", "reason", "recommendations", "sources"],
        "additionalProperties": False,
    }


def command(output: Path, schema: Path, cwd: str, model: str | None, *, live: bool) -> list[str]:
    """Isolate config, plugins, skills, memory, and tools from the benchmark subject."""
    result = [
        "codex",
        "exec",
        "--ignore-user-config",
        "--ephemeral",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--json",
        "--color",
        "never",
        "-C",
        cwd,
        "--output-schema",
        str(schema),
        "-o",
        str(output),
        "-c",
        'web_search="live"' if live else 'web_search="disabled"',
        "-c",
        "features.skip_host_skill_discovery=true",
    ]
    for feature in ("shell_tool", "apps", "plugins", "memories", "multi_agent", "hooks"):
        result.extend(["--disable", feature])
    if model:
        result.extend(["--model", model])
    return [*result, "-"]


def run_case(case: dict, catalog: list | None, output: Path, model: str | None) -> dict:
    """Run one fresh session, retaining exact inputs, raw events, and failures."""
    directory = output / case["id"]
    directory.mkdir()
    prompt = make_prompt(case, catalog)
    (directory / "prompt.txt").write_text(prompt)
    schema = directory / "schema.json"
    schema.write_text(json.dumps(response_schema(), indent=2))
    started = time.monotonic()
    record = {"id": case["id"], "category": case["category"], "status": "error"}
    response = None
    with tempfile.TemporaryDirectory(prefix="bagel-eval-") as cwd:
        argv = command(directory / "response.json", schema, cwd, model, live=catalog is None)
        (directory / "command.json").write_text(json.dumps(argv, indent=2))
        try:
            with (
                (directory / "events.jsonl").open("w") as stdout,
                (directory / "stderr.txt").open("w") as stderr,
            ):
                completed = subprocess.run(  # noqa: S603 -- fixed CLI; no shell
                    argv,
                    input=prompt,
                    text=True,
                    stdout=stdout,
                    stderr=stderr,
                    timeout=240,
                    check=False,
                )
            record["returncode"] = completed.returncode
            if completed.returncode == 0:
                response = json.loads((directory / "response.json").read_text())
                validate_response(response)
                record["status"] = "completed"
            else:
                record["error"] = "CLI failed; see stderr.txt and events.jsonl"
        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
            record["error"] = str(error)
            response = None
    record.update(score(case, response))
    record.update(response=response, elapsed_seconds=round(time.monotonic() - started, 3))
    (directory / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def main() -> None:
    """Run one track against an explicit catalog snapshot, without overwriting runs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, default=Path("evals/agent-discovery/cases.json"))
    parser.add_argument("--track", choices=("routing", "discovery"), default="routing")
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--model",
        help="Explicit model recommended; otherwise the underlying CLI default is unrecorded",
    )
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 5))
    args = parser.parse_args()
    if args.track == "routing" and args.catalog is None:
        parser.error("--catalog is required for routing")
    if args.track == "discovery" and args.catalog is not None:
        parser.error("discovery must not receive a catalog")
    catalog = None if args.catalog is None else json.loads(args.catalog.read_text())
    cases = [c for c in json.loads(args.cases.read_text()) if c["track"] == args.track]
    if not cases:
        parser.error("the selected track has no cases")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "track": args.track,
        "requested_model": args.model,
        "workers": args.workers,
        "cases_sha256": hashlib.sha256(args.cases.read_bytes()).hexdigest(),
        "catalog_sha256": None
        if args.catalog is None
        else hashlib.sha256(args.catalog.read_bytes()).hexdigest(),
        "cli_version": subprocess.check_output(["codex", "--version"], text=True).strip(),  # noqa: S607
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (output / "cases.json").write_text(json.dumps(cases, indent=2) + "\n")
    if catalog is not None:
        (output / "catalog.json").write_text(json.dumps(catalog, indent=2) + "\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(run_case, c, catalog, output, args.model) for c in cases]
        results = [future.result() for future in futures]
    metric = "correct" if args.track == "routing" else "bagel_mentioned"
    summary = {
        **metadata,
        "attempted": len(results),
        "completed": sum(r["status"] == "completed" for r in results),
        metric: sum(r[metric] for r in results),
        "results": results,
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))  # noqa: T201


if __name__ == "__main__":
    main()
