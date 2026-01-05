"""Configuration management for Matcha Cloud."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

CONFIG_DIR = Path.home() / ".bagel"
CONFIG_FILE = CONFIG_DIR / "cloud.json"

DEFAULT_API_URL = "https://matcha-ext.extelligence.ai/api"


def get_config_dir() -> Path:
    """Get or create the config directory."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def load_config() -> dict:
    """Load configuration from file."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_config(config: dict) -> None:
    """Save configuration to file."""
    get_config_dir()
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def get_api_key() -> Optional[str]:
    """Get the API key from config or environment."""
    # Environment variable takes precedence
    env_key = os.environ.get("MATCHA_API_KEY") or os.environ.get("BAGEL_CLOUD_API_KEY")
    if env_key:
        return env_key

    config = load_config()
    return config.get("api_key")


def set_api_key(api_key: str) -> None:
    """Set the API key in config."""
    config = load_config()
    config["api_key"] = api_key
    save_config(config)


def get_api_url() -> str:
    """Get the API URL from config or environment."""
    env_url = os.environ.get("MATCHA_API_URL") or os.environ.get("BAGEL_CLOUD_API_URL")
    if env_url:
        return env_url

    config = load_config()
    return config.get("api_url", DEFAULT_API_URL)


def set_api_url(url: str) -> None:
    """Set the API URL in config."""
    config = load_config()
    config["api_url"] = url
    save_config(config)


def clear_config() -> None:
    """Clear all configuration."""
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()





