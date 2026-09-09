"""Snapshot public discovery surfaces without credentials or publishing changes."""

import argparse
import hashlib
import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path

URLS = {
    "registry": "https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.Extelligence-ai/bagel",
    "glama": "https://glama.ai/mcp/servers/Extelligence-ai/bagel",
    "robots": "https://trybagel.com/robots.txt",
    "sitemap": "https://trybagel.com/sitemap.xml",
}


def fetch(url: str, destination: Path) -> dict:
    """Preserve successful and HTTP-error bodies; report network failures explicitly."""
    request = urllib.request.Request(  # noqa: S310 -- fixed HTTPS URLs
        url, headers={"User-Agent": "BagelDiscoveryAudit/1.0"}
    )
    try:
        try:
            response = urllib.request.urlopen(request, timeout=30)  # noqa: S310 -- fixed HTTPS URLs
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            destination.write_bytes(body)
            return {
                "url": url,
                "final_url": response.url,
                "status": response.code,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "body": destination.name,
            }
    except (OSError, urllib.error.URLError) as error:
        return {"url": url, "status": None, "error": str(error)}


def registry_state(payload: dict, expected: dict) -> dict:
    """Use official latest metadata, rather than lexicographic version ordering."""
    entries = [
        entry for entry in payload.get("servers", []) if entry["server"]["name"] == expected["name"]
    ]
    latest = [
        entry["server"]
        for entry in entries
        if entry.get("_meta", {})
        .get("io.modelcontextprotocol.registry/official", {})
        .get("isLatest")
    ]
    return {
        "versions_seen": [entry["server"]["version"] for entry in entries],
        "latest_versions": [entry["version"] for entry in latest],
        "local_version": expected["version"],
        "local_version_is_latest": any(entry["version"] == expected["version"] for entry in latest),
        "local_repository_matches": any(
            entry.get("repository") == expected.get("repository") for entry in latest
        ),
        "pagination_cursor": payload.get("metadata", {}).get("nextCursor"),
    }


def main() -> None:
    """Write a timestamped audit; failures remain visible in the report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"checked_at": datetime.now(timezone.utc).isoformat(), "surfaces": {}}
    for name, url in URLS.items():
        report["surfaces"][name] = fetch(url, args.output / f"{name}.body")
    if report["surfaces"]["registry"]["status"] == HTTPStatus.OK:
        report["registry"] = registry_state(
            json.loads((args.output / "registry.body").read_text()),
            json.loads(Path("server.json").read_text()),
        )
    if report["surfaces"]["glama"]["status"] == HTTPStatus.OK:
        html = (args.output / "glama.body").read_text()
        report["glama_repository_blob_commits"] = sorted(
            set(re.findall(r"/bagel/blob/([a-f0-9]{40})/", html))
        )
    report["limitations"] = [
        "Public page snapshot; no authenticated Glama rescan or rating change requested.",
        "Blob links identify displayed repository snapshots, "
        "not necessarily the tool assessment revision.",
        "A missing robots.txt is not evidence that crawlers are blocked.",
        "HTTP reachability and registry presence do not establish indexing or recommendation rank.",
        "Check pagination_cursor before interpreting a partial registry result.",
    ]
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))  # noqa: T201 -- CLI report


if __name__ == "__main__":
    main()
