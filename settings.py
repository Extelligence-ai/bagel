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
        case_sensitive=True,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Directory name for Bagel artifacts
    ARTIFACT_DIRNAME: str = "artifacts"

    # Directory for Bagel artifacts
    ARTIFACT_DIRECTORY: str = str(pathlib.Path.home() / ".bagel" / ARTIFACT_DIRNAME)

    # Directory for caching intermediate artifacts
    CACHE_DIRECTORY: str = str(pathlib.Path.home() / ".cache" / "bagel")

    # Minimum number of records per batch in arrow files
    MIN_ARROW_RECORD_BATCH_SIZE_COUNT: int = 500

    # Bytes per record batch in arrow files. Not always respected
    ARROW_RECORD_BATCH_SIZE_BYTES: int = 1 * GB

    # Bytes per topic buffer in a topic sink. Always respected
    JSONL_BUFFER_SIZE_PER_TOPIC_BYTES: int = 1 * GB

    # Number of messages to buffer in rosbridge before sending over the WebSocket
    ROSBRIDGE_QUEUE_LENGTH: int = 1000

    # Column name for timestamps in arrow files, i.e., when messages were recorded
    TIMESTAMP_SECONDS_COLUMN_NAME: str = "timestamp_seconds"

    ###############################################
    # S3 configuration for uploading artifacts to #
    # the Extelligence platform.                  #
    ###############################################

    EXTELLIGENCE_S3_BUCKET_NAME: str | None = None  # If not set, artifact upload is disabled

    EXTELLIGENCE_S3_BUCKET_REGION: str | None = None  # If not set, will use default region

    ################################################
    # The default values of the following settings #
    # are specified via the ".env" file.           #
    ################################################

    # Whether running in a container
    CONTAINER_MODE: bool

    # Host of the MCP server
    MCP_SERVER_HOST: str

    # Port of the MCP server
    MCP_SERVER_PORT: int


settings = Settings()

pathlib.Path(settings.CACHE_DIRECTORY).mkdir(parents=True, exist_ok=True)
