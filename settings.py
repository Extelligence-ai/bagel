"""Settings for Bagel."""

import pathlib

from pydantic_settings import BaseSettings, SettingsConfigDict

BYTE = 1
KB = 1024 * BYTE
MB = 1024 * KB
GB = 1024 * MB


class Settings(BaseSettings):
    """Settings for Bagel."""

    model_config = SettingsConfigDict(
        env_prefix="BAGEL_",
        case_sensitive=True,
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # Directory for caching intermediate artifacts
    CACHE_DIRECTORY: str = str(pathlib.Path.home() / ".cache" / "bagel")

    # Minimum number of records per batch in arrow files
    MIN_ARROW_RECORD_BATCH_SIZE_COUNT: int = 500

    # Bytes per record batch in arrow files. Not always respected
    ARROW_RECORD_BATCH_SIZE_BYTES: int = 1 * GB

    # Column name for timestamps in arrow files, i.e., when messages were recorded
    TIMESTAMP_SECONDS_COLUMN_NAME: str = "timestamp_seconds"

    # Local host of bagel
    LOCAL_HOST: str = "0.0.0.0"  # noqa: S104

    # Port of the MCP server (port to listen on for SSE transport)
    MCP_LOCAL_PORT: int = 8000


settings = Settings()

pathlib.Path(settings.CACHE_DIRECTORY).mkdir(parents=True, exist_ok=True)
