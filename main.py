#!/usr/bin/env python3
"""
Bagel CLI - Robotics data analysis tool.

Usage:
    bagel run pipeline.yaml       # Run a local pipeline
    bagel server                  # Start MCP server
    bagel cloud login             # Connect to Matcha Cloud
    bagel cloud upload file.mcap  # Upload to cloud
    bagel cloud query "..."       # Query cloud data
"""

import click
import logging
import pathlib
import sys

__version__ = "0.1.0"


# ============================================================================
# Main CLI Group
# ============================================================================

@click.group()
@click.version_option(version="0.1.0", prog_name="bagel")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def app(verbose: bool) -> None:
    """Bagel - Chat with your robotics and drone data using LLMs."""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


# ============================================================================
# Run Command (Pipeline)
# ============================================================================

@app.command()
@click.argument("template", type=click.Path(exists=True))
@click.option("-v", "--var", "variables", multiple=True, help="Template variables (key=value)")
def run(template: str, variables: tuple) -> None:
    """Run a Bagel pipeline from a YAML template.
    
    Example:
        bagel run pipeline.yaml -v site=warehouse -v asset=forklift
    """
    import yaml
    from jinja2 import StrictUndefined, Template, UndefinedError
    from src.pipeline import base
    
    template_path = pathlib.Path(template)
    
    # Parse variables
    var_dict = {}
    for v in variables:
        if "=" not in v:
            click.secho(f"Invalid variable format: {v} (use key=value)", fg="red")
            sys.exit(1)
        key, value = v.split("=", 1)
        var_dict[key] = value
    
    # Render template
    try:
        with open(template_path) as f:
            tmpl = Template(f.read(), undefined=StrictUndefined)
        content = tmpl.render(**var_dict)
    except UndefinedError as e:
        click.secho(f"Missing template variable: {e}", fg="red")
        sys.exit(1)
    
    # Build and run pipeline
    config = yaml.safe_load(content)
    pipeline = base.Pipeline.build(config)
    
    click.echo(f"Running pipeline from {template_path.name}...")
    pipeline.run_all()
    click.secho("✓ Pipeline complete", fg="green")


# ============================================================================
# Server Command (MCP)
# ============================================================================

@app.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--stdio", is_flag=True, help="Use stdio transport (for MCP)")
def server(host: str, port: int, stdio: bool) -> None:
    """Start the Bagel MCP server.
    
    Example:
        bagel server              # HTTP mode
        bagel server --stdio      # MCP stdio mode
    """
    if stdio:
        # MCP stdio mode
        from server import mcp
        import asyncio
        asyncio.run(mcp.run_async(transport="stdio"))
    else:
        # HTTP mode
        import uvicorn
        from server import app as server_app
        click.echo(f"Starting Bagel server on {host}:{port}")
        uvicorn.run(server_app, host=host, port=port)


# ============================================================================
# Cloud Commands (Matcha)
# ============================================================================

@app.group()
def cloud() -> None:
    """Matcha Cloud commands - sync and query your data in the cloud."""
    pass


@cloud.command("login")
@click.option("--api-key", "-k", help="API key (or will prompt)")
@click.option("--api-url", "-u", help="API URL (for custom deployments)")
def cloud_login(api_key: str | None, api_url: str | None) -> None:
    """Configure your Matcha Cloud API key.
    
    Get your API key from: https://matcha.extelligence.ai/settings
    """
    from cloud import config as cloud_config
    from cloud import api as cloud_api
    
    if not api_key:
        click.echo("Get your API key from: https://matcha.extelligence.ai/settings")
        click.echo()
        api_key = click.prompt("Enter your API key", hide_input=True)
    
    if api_url:
        cloud_config.set_api_url(api_url)
    
    cloud_config.set_api_key(api_key)
    
    try:
        client = cloud_api.MatchaClient()
        stats = client.get_stats()
        click.secho("✓ Logged in successfully!", fg="green")
        click.echo(f"  Files: {stats.get('file_count', 0)}")
        click.echo(f"  Config saved to: {cloud_config.CONFIG_FILE}")
    except cloud_api.MatchaAPIError as e:
        cloud_config.clear_config()
        click.secho(f"✗ Invalid API key: {e.message}", fg="red")
        sys.exit(1)


@cloud.command("logout")
def cloud_logout() -> None:
    """Clear stored Matcha Cloud credentials."""
    from cloud import config as cloud_config
    cloud_config.clear_config()
    click.secho("✓ Logged out. Config cleared.", fg="green")


@cloud.command("whoami")
def cloud_whoami() -> None:
    """Show Matcha Cloud authentication status."""
    from cloud import config as cloud_config
    from cloud import api as cloud_api
    
    api_key = cloud_config.get_api_key()
    
    if not api_key:
        click.secho("Not logged in. Run 'bagel cloud login' to authenticate.", fg="yellow")
        sys.exit(1)
    
    click.echo(f"API URL: {cloud_config.get_api_url()}")
    click.echo(f"API Key: {api_key[:8]}...{api_key[-4:]}")
    
    try:
        client = cloud_api.MatchaClient()
        stats = client.get_stats()
        click.echo(f"Files: {stats.get('file_count', 0)}")
        click.secho("✓ Connected", fg="green")
    except cloud_api.MatchaAPIError as e:
        click.secho(f"✗ Connection error: {e.message}", fg="red")


@cloud.command("files")
@click.option("--type", "-t", "file_type", default="all", help="Filter by type")
@click.option("--tag", help="Filter by tag")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def cloud_files(file_type: str, tag: str | None, as_json: bool) -> None:
    """List files in your Matcha Cloud knowledge base."""
    import json
    from cloud import api as cloud_api
    
    try:
        client = cloud_api.MatchaClient()
        files = client.list_files(file_type=file_type)
        
        if tag:
            files = [f for f in files if tag in f.get("tags", [])]
        
        if as_json:
            click.echo(json.dumps(files, indent=2))
            return
        
        if not files:
            click.echo("No files found.")
            return
        
        for f in files[:50]:
            tags = ", ".join(f.get("tags", [])[:3]) or ""
            size = f.get("size", 0)
            size_str = f"{size / 1024 / 1024:.1f}MB" if size > 1024*1024 else f"{size / 1024:.1f}KB"
            click.echo(f"  {f['filename']:<40} {f.get('file_type', '?'):<6} {size_str:<10} {tags}")
        
        if len(files) > 50:
            click.echo(f"\n... and {len(files) - 50} more files")
            
    except cloud_api.MatchaAPIError as e:
        click.secho(f"Error: {e.message}", fg="red")
        sys.exit(1)


@cloud.command("upload")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--tags", "-t", help="Comma-separated tags")
@click.option("--recursive", "-r", is_flag=True, help="Upload directory recursively")
def cloud_upload(file_path: str, tags: str | None, recursive: bool) -> None:
    """Upload files to Matcha Cloud.
    
    Example:
        bagel cloud upload robot.mcap
        bagel cloud upload ./logs/ --recursive --tags production
    """
    from cloud import api as cloud_api
    
    path = pathlib.Path(file_path)
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    
    try:
        client = cloud_api.MatchaClient()
        
        if path.is_dir():
            if not recursive:
                click.secho("Use --recursive to upload directories", fg="yellow")
                sys.exit(1)
            
            extensions = [".mcap", ".bag", ".db3", ".ulg", ".pdf", ".txt", ".md"]
            files_to_upload = []
            for ext in extensions:
                files_to_upload.extend(path.rglob(f"*{ext}"))
            
            if not files_to_upload:
                click.echo("No supported files found.")
                return
            
            click.echo(f"Found {len(files_to_upload)} files to upload")
            
            for f in files_to_upload:
                click.echo(f"  Uploading {f.name}...", nl=False)
                try:
                    client.upload_file(f, tags=tag_list)
                    click.secho(" ✓", fg="green")
                except cloud_api.MatchaAPIError as e:
                    click.secho(f" ✗ {e.message}", fg="red")
        else:
            click.echo(f"Uploading {path.name}...", nl=False)
            result = client.upload_file(path, tags=tag_list)
            click.secho(" ✓", fg="green")
            click.echo(f"  Key: {result['key']}")
            
    except cloud_api.MatchaAPIError as e:
        click.secho(f"Error: {e.message}", fg="red")
        sys.exit(1)


@cloud.command("describe")
@click.argument("file_key")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def cloud_describe(file_key: str, as_json: bool) -> None:
    """Describe a file in Matcha Cloud (metadata, topics, etc)."""
    import json
    from cloud import api as cloud_api
    
    try:
        client = cloud_api.MatchaClient()
        
        # If just filename, try to find full key
        if not file_key.startswith("kb/") and not file_key.startswith("bags/"):
            files = client.list_files()
            matches = [f for f in files if f["filename"] == file_key or file_key in f.get("key", "")]
            if len(matches) == 1:
                file_key = matches[0]["key"]
            elif len(matches) > 1:
                click.echo("Multiple matches:")
                for m in matches[:5]:
                    click.echo(f"  {m['key']}")
                sys.exit(1)
        
        details = client.describe_file(file_key)
        
        if as_json:
            click.echo(json.dumps(details, indent=2, default=str))
            return
        
        click.echo(f"File: {details.get('filename', file_key)}")
        click.echo(f"Type: {details.get('file_type', 'unknown')}")
        
        if details.get("duration"):
            click.echo(f"Duration: {details['duration']:.1f}s")
        
        if details.get("topic_count"):
            click.echo(f"Topics: {details['topic_count']}")
        
        channels = client.get_channels(file_key)
        if channels:
            click.echo("\nChannels:")
            for ch in channels[:20]:
                click.echo(f"  {ch.get('name', ch.get('topic', '?'))}: {ch.get('message_count', '?')} msgs")
                
    except cloud_api.MatchaAPIError as e:
        click.secho(f"Error: {e.message}", fg="red")
        sys.exit(1)


@cloud.command("query")
@click.argument("prompt")
@click.option("--file", "-f", "file_key", help="Focus on specific file")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def cloud_query(prompt: str, file_key: str | None, as_json: bool) -> None:
    """Query your robotics data using natural language.
    
    Example:
        bagel cloud query "what files do I have?"
        bagel cloud query "average pressure" -f fluid.mcap
        bagel cloud query "find anomalies in /odom"
    """
    import json
    from cloud import api as cloud_api
    
    try:
        client = cloud_api.MatchaClient()
        
        click.echo("Thinking...", nl=False)
        result = client.query(prompt, file_key=file_key)
        click.echo("\r" + " " * 20 + "\r", nl=False)
        
        if as_json:
            click.echo(json.dumps(result, indent=2, default=str))
            return
        
        response = result.get("response", result.get("text", str(result)))
        click.echo(response)
        
    except cloud_api.MatchaAPIError as e:
        click.echo("\r" + " " * 20 + "\r", nl=False)
        click.secho(f"Error: {e.message}", fg="red")
        sys.exit(1)


@cloud.command("stats")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def cloud_stats(as_json: bool) -> None:
    """Show Matcha Cloud knowledge base statistics."""
    import json
    from cloud import api as cloud_api
    
    try:
        client = cloud_api.MatchaClient()
        data = client.get_stats()
        
        if as_json:
            click.echo(json.dumps(data, indent=2))
            return
        
        click.echo(f"Files: {data.get('file_count', 0)}")
        
        if data.get("file_types"):
            click.echo("File types:")
            for ft, count in data["file_types"].items():
                click.echo(f"  {ft}: {count}")
        
        if data.get("tags"):
            click.echo(f"Tags: {', '.join(data['tags'][:10])}")
            
    except cloud_api.MatchaAPIError as e:
        click.secho(f"Error: {e.message}", fg="red")
        sys.exit(1)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    app()

