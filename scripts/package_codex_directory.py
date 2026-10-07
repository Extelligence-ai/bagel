"""Build the skills-only plugin ZIP for the OpenAI Plugins Directory.

The directory only accepts MCP servers on a public HTTPS origin, and Bagel's
server runs on the user's machine (localhost). So the directory package ships
the skills without the MCP connection: no `.mcp.json`, no `mcpServers` in the
Codex manifest. Claude Code and repo-marketplace installs keep using `plugin/`
as-is, connection included.
"""

import argparse
import json
import sys
import zipfile
from pathlib import Path

PLUGIN = Path("plugin")
EXCLUDED = {".mcp.json", ".claude-plugin", ".DS_Store"}


def directory_manifest() -> dict:
    """Return the Codex manifest without the bundled MCP connection."""
    manifest = json.loads((PLUGIN / ".codex-plugin" / "plugin.json").read_text())
    manifest.pop("mcpServers", None)
    return manifest


def package_files() -> list[Path]:
    """Every plugin file that ships in the directory package."""
    return sorted(
        path
        for path in PLUGIN.rglob("*")
        if path.is_file() and not EXCLUDED.intersection(path.relative_to(PLUGIN).parts)
    )


def build(out: Path) -> None:
    """Write the directory ZIP, rooted at the plugin directory."""
    manifest_path = PLUGIN / ".codex-plugin" / "plugin.json"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in package_files():
            name = path.relative_to(PLUGIN).as_posix()
            if path == manifest_path:
                archive.writestr(name, json.dumps(directory_manifest(), indent=2) + "\n")
            else:
                archive.write(path, name)


def main() -> None:
    """Build the package; the version goes in the default file name."""
    version = directory_manifest()["version"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path(f"bagel-plugin-{version}.zip"))
    args = parser.parse_args()
    build(args.out)
    sys.stdout.write(f"{args.out}\n")


if __name__ == "__main__":
    main()
