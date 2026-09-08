"""Structural tests for the Claude Code plugin manifests."""

import json
import pathlib


def test_plugin_manifest_is_valid_json_with_required_fields() -> None:
    manifest = json.loads(pathlib.Path("plugin/.claude-plugin/plugin.json").read_text())
    assert manifest["name"] == "bagel"
    assert manifest["description"]
    assert manifest["version"]


def test_bundled_mcp_config_points_at_default_local_server() -> None:
    config = json.loads(pathlib.Path("plugin/.mcp.json").read_text())
    bagel = config["mcpServers"]["bagel"]
    # Streamable HTTP: natively supported by Claude Code AND Codex (#168);
    # the default server serves it alongside legacy /sse.
    assert bagel["url"] == "http://localhost:8000/mcp"
    assert bagel["type"] == "http"


def test_marketplace_lists_the_plugin() -> None:
    marketplace = json.loads(pathlib.Path(".claude-plugin/marketplace.json").read_text())
    sources = json.dumps(marketplace)
    assert "bagel" in sources
    assert "./plugin" in sources


def test_codex_manifest_mirrors_the_claude_plugin() -> None:
    codex = json.loads(pathlib.Path("plugin/.codex-plugin/plugin.json").read_text())
    claude = json.loads(pathlib.Path("plugin/.claude-plugin/plugin.json").read_text())
    assert codex["name"] == claude["name"] == "bagel"
    assert codex["version"] == claude["version"]
    assert codex["skills"] == "./skills/"
    # Manifest paths must resolve relative to the plugin root.
    assert (pathlib.Path("plugin") / codex["mcpServers"]).resolve().exists()


def test_codex_manifest_reuses_the_shared_mcp_config() -> None:
    codex = json.loads(pathlib.Path("plugin/.codex-plugin/plugin.json").read_text())
    assert codex["mcpServers"] == "./.mcp.json"
    assert isinstance(codex["author"], dict) and codex["author"]["name"]
    assert isinstance(codex["interface"]["capabilities"], list)
    assert codex["interface"]["capabilities"]


def test_codex_sideload_marketplace_entry_is_codex_shaped() -> None:
    marketplace = json.loads(pathlib.Path(".agents/plugins/marketplace.json").read_text())
    (entry,) = marketplace["plugins"]
    assert entry["name"] == "bagel"
    assert entry["source"] == {"source": "local", "path": "./plugin"}
    assert entry["policy"]["installation"] in {"INSTALLED_BY_DEFAULT", "AVAILABLE", "NOT_AVAILABLE"}
    assert entry["policy"]["authentication"] in {"ON_INSTALL", "ON_USE"}
    assert entry["category"]
