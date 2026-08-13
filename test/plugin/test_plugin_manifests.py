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
    assert bagel["url"] == "http://localhost:8000/sse"


def test_marketplace_lists_the_plugin() -> None:
    marketplace = json.loads(pathlib.Path(".claude-plugin/marketplace.json").read_text())
    sources = json.dumps(marketplace)
    assert "bagel" in sources
    assert "./plugin" in sources
