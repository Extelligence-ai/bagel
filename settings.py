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

    # Directory of user-authored capabilities (.poml or .md files) discovered by
    # list_agent_capabilities alongside the builtins and writable via the
    # save_agent_capability tool. Mounted from the host in compose.yaml so
    # capabilities survive container restarts. Missing directory = builtins only.
    USER_CAPABILITIES_DIRECTORY: str = str(pathlib.Path.home() / ".bagel" / "capabilities")

    # YAML manifest of live subscriptions (and their standing pipelines) to
    # establish when the server starts, so they survive container restarts.
    # If unset or missing, no startup subscriptions are made.
    STARTUP_PIPELINES_FILE: str | None = None

    # Minimum number of records per batch in arrow files
    MIN_ARROW_RECORD_BATCH_SIZE_COUNT: int = 500

    # Bytes per record batch in arrow files. Not always respected
    ARROW_RECORD_BATCH_SIZE_BYTES: int = 1 * GB

    # Hard row-count ceiling per record batch. The byte target above is
    # measured in Arrow bytes, but rows accumulate as Python objects (10-20x
    # overhead) until a batch flushes; with small rows the byte target alone
    # resolves to millions of buffered rows and can OOM the server (#134).
    MAX_ARROW_RECORD_BATCH_SIZE_COUNT: int = 100_000

    # Bytes per topic buffer in a topic sink. Always respected
    JSONL_BUFFER_SIZE_PER_TOPIC_BYTES: int = 1 * GB

    # Max total bytes of cached .arrow query results under
    # <CACHE_DIRECTORY>/data/source_id=*/ . When a new cache file is about to
    # be written and the total exceeds this, the oldest-by-access files are
    # deleted first (cache entries are derived data and rebuild on demand).
    # 0 disables eviction. Live sink buffers, repos, and ARTIFACT_DIRECTORY
    # are never touched by eviction.
    CACHE_MAX_BYTES: int = 20 * GB

    # Max total nominal buffer bytes across all subscribed topics of one live
    # sink. 0 = unbounded (the historical behavior). When set, subscribe()
    # refuses new topics that would exceed it with BufferCapacityExceededError
    # instead of silently growing; note on-disk usage can transiently reach 2x
    # nominal because a rotated overflow file is retained until the next
    # rotation.
    SINK_TOTAL_BUFFER_BYTES: int = 0

    # Number of messages to buffer in rosbridge before sending over the WebSocket
    ROSBRIDGE_QUEUE_LENGTH: int = 1000

    # Column name for timestamps in arrow files, i.e., when messages were recorded
    TIMESTAMP_SECONDS_COLUMN_NAME: str = "timestamp_seconds"

    # Whether to use cloudini for pointcloud compression/decompression by default
    CLOUDINI_ENABLED: bool = True

    # Fleet streaming (beta). False makes the whole publish subsystem inert
    # regardless of configuration: nothing connects, nothing leaves the box.
    FLEET_ENABLED: bool = True

    # Disk budget for the fleet spool's channels lane (store-and-forward
    # outbox under CACHE_DIRECTORY/publish/). Oldest segments are dropped
    # first when over budget; events and heartbeats are never dropped and
    # don't count against this cap. Transient overshoot of one segment
    # (~4 MB) is possible while rotating.
    FLEET_SPOOL_MAX_BYTES: int = 268_435_456

    # Max samples held in the router's bounded queue between the buffer tap
    # and the router thread. On overflow the oldest sample is dropped to make
    # room for the newest; the queue never blocks the tap.
    FLEET_QUEUE_MAX_SAMPLES: int = 10_000

    # Directory holding this robot's fleet enrollment identity: robot.key
    # (mode 0600), robot.crt, ca.crt, identity.yaml (tenant, robot_id,
    # broker_url, enroll_url, expires_at). See src/sink/publish/identity.py.
    FLEET_IDENTITY_DIRECTORY: str = str(pathlib.Path.home() / ".bagel" / "identity")

    # Dev-only escape hatch: allows an unencrypted mqtt:// fleet broker when
    # the host resolves to loopback or a private (RFC1918/RFC4193) address.
    # False in production -- mqtts:// with an enrolled identity is the only
    # transport allowed. See src/sink/publish/connect.py.
    FLEET_DEV_INSECURE: bool = False

    # One-time enrollment token for first-boot fleet enrollment. Consumed at
    # first boot (POSTed to FLEET_ENROLL_URL to obtain identity; once
    # identity.yaml exists this and FLEET_ENROLL_URL are no longer needed)
    # and never persisted -- it never reaches disk or a log line.
    FLEET_ENROLL_TOKEN: str | None = None

    # Enrollment server base URL used for first-boot enrollment (paired with
    # FLEET_ENROLL_TOKEN) and identity renewal. See src/sink/publish/identity.py.
    FLEET_ENROLL_URL: str | None = None

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

    # Host of the MCP server. Defaults to loopback so a bare `uv run
    # server.py` outside Compose never exposes the unauthenticated endpoint
    # on all interfaces. Containers NEED 0.0.0.0 for Docker port publishing;
    # compose.yaml injects MCP_SERVER_HOST=0.0.0.0 explicitly per service.
    MCP_SERVER_HOST: str = "127.0.0.1"

    # Port of the MCP server
    MCP_SERVER_PORT: int = 8000

    # MCP transport: "both" (default) serves legacy SSE at /sse and streamable
    # HTTP at /mcp on one port, so SSE-configured clients and streamable-only
    # clients (Codex's native MCP client) connect without configuration (#168).
    # Set "sse" or "streamable-http" to pin a single transport.
    MCP_TRANSPORT: str = "both"


settings = Settings()

pathlib.Path(settings.CACHE_DIRECTORY).mkdir(parents=True, exist_ok=True)
