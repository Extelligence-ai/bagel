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

    # YAML manifest of live subscriptions (and their standing pipelines) to
    # establish when the server starts, so they survive container restarts.
    # If unset or missing, no startup subscriptions are made.
    STARTUP_PIPELINES_FILE: str | None = None

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

    # Whether to use cloudini for pointcloud compression/decompression by default
    CLOUDINI_ENABLED: bool = True

    # Default quantization resolution in meters for cloudini lossy compression
    CLOUDINI_DEFAULT_RESOLUTION: float = 0.001
    ###############################################
    # S3 configuration for uploading artifacts to #
    # the Extelligence platform.                  #
    ###############################################

    EXTELLIGENCE_S3_BUCKET_NAME: str | None = None  # If not set, artifact upload is disabled

    EXTELLIGENCE_S3_BUCKET_REGION: str | None = None  # If not set, will use default region

    ################################################
    # The following settings default to the values #
    # in the repo's ".env", which Compose also     #
    # interpolates from. Images ship no .env: the  #
    # defaults below are the source of truth for a #
    # running container (#158).                    #
    ################################################

    # Whether running in a container
    CONTAINER_MODE: bool = False

    # Host of the MCP server. 0.0.0.0 is correct INSIDE a container -- the
    # server must listen on all container interfaces for Docker's port
    # publishing to reach it. Host-side exposure is controlled by the
    # 127.0.0.1 prefix on compose.yaml's port mappings, not here.
    MCP_SERVER_HOST: str = "0.0.0.0"  # noqa: S104 -- container-internal default, not a bind call

    # Port of the MCP server
    MCP_SERVER_PORT: int = 8000

    # MCP transport: "sse" (current default, matches the README quickstart) or
    # "streamable-http" (the newer transport; both are supported by MCP SDK v1 and v2)
    MCP_TRANSPORT: str = "sse"


settings = Settings()

pathlib.Path(settings.CACHE_DIRECTORY).mkdir(parents=True, exist_ok=True)
