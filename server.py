"""Entry point for the Bagel MCP server."""

import pathlib
from typing import Any

import duckdb
import yaml
from mcp.server.fastmcp import FastMCP
from poml import poml

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve
from src.di.types.topic_sink import TopicSink, guess_host, guess_port
from src.pipeline import base, batch, capabilities, plotjuggler, windows
from src.sink import startup

server = FastMCP(
    name="Bagel MCP Server",
    host=settings.MCP_SERVER_HOST,
    port=settings.MCP_SERVER_PORT,
)


@server.tool(
    title="Describe a data source",
    description=(
        "Summarize a data source without returning its messages. "
        "Includes: a brief summary, basic metadata (start time, message count, config parameters), "
        "and a list of available topics. "
        "Excludes: detailed topic definitions or actual messages."
    ),
)
def describe_data_source(path: str, args: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Generate a structured summary of a data source.

    Provides high-level metadata and a list of topics available in the data source.
    This tool is useful for initial inspection before exploring specific topics.
    It does **not** include detailed topic definitions or actual messages.

    Args:
        path (str): Filesystem path or URL to the data source.
        args (dict[str, Any] | None, optional): Additional constructor arguments
            used to create the `SourceFactory` and `TopicRegistry`.

    Returns:
        list[dict[str, Any]]: A structured description of the data source with keys:
            - `Summary`: Short overview of the data source
            - `Metadata`: Basic metadata (e.g., start time, message count, configuration parameters)
            - `Topics`: List of available topic names grouped by their semantic meaning

    Examples:
        As an LLM prompt:
            Describe the data source at "./data/sample/ros2/mcap".

        As a Python call:
            >>> describe_data_source("./data/sample/ros2/mcap")

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    return poml(
        "./src/agent/describe/data_source.poml",
        context={
            "metadata": factory.metadata,
            "topics": registry.available_topics(factory.build()),
        },
    )


@server.tool(
    title="Describe a topic in a data source",
    description=(
        "Generate a structured summary of a topic without returning its messages. "
        "Includes: short summary, DuckDB schema, original IDL definition, and "
        "guidelines for SQL queries. Excludes: actual topic data."
    ),
)
def describe_topic(
    path: str, topic: str, args: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Summarize a topic's structure and schema.

    Provides metadata about a topic in the data source to help the LLM
    understand how to query it. This includes a textual summary, schema,
    IDL definition, and SQL query guidelines. It does **not** return
    actual topic messages.

    Args:
        path (str): Filesystem path or URL to the data source.
        topic (str): The name of the topic to describe.
        args (dict[str, Any] | None, optional): Additional constructor arguments
            used to create the `SourceFactory` and `TopicRegistry`.

    Returns:
        list[dict[str, Any]]: A structured description of the topic, including:
            - `Summary`: Short description of the topic
            - `DuckDB Schema`: Column names and types
            - `Topic Definition`: Original IDL (Interface Definition Language)
            - `Guidelines for DuckDB SQL Generation`: Hints for writing DuckDB SQL queries

    Examples:
        As an LLM prompt:
            Describe the topic `/odom` in "./data/sample/ros2/mcap".

        As a Python call:
            >>> describe_topic("./data/sample/ros2/mcap", topic="/odom")

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    data_source = factory.build()
    relation = dataset.to_duckdb(factory, registry, [topic], empty=True)

    return poml(
        "./src/agent/describe/topic.poml",
        context={
            "topic_name": topic,
            "type_name": registry.native_type_name(topic, data_source),
            "message_count": registry.message_count(topic, data_source),
            "duckdb_schema": {
                name: str(type_)
                for name, type_ in zip(relation.columns, relation.dtypes, strict=True)
            },
            "topic_definition": registry.describe(topic, data_source),
        },
    )


@server.tool(
    title="Query topic messages with SQL",
    description=(
        "Run a DuckDB SQL query on messages from a single topic in a data source. "
        "Returns the query results as structured dictionaries. "
        "Use this tool to answer user questions about message data, "
        "including filtering, aggregation, and downsampling."
    ),
)
def query_messages(  # noqa: PLR0913
    path: str,
    sql_statement: str,
    topic: str,
    start_seconds: float | None = None,
    end_seconds: float | None = None,
    args: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Query messages from a topic using DuckDB SQL.

    Loads the specified topic into DuckDB as a table, executes the SQL statement,
    and returns the result as a list of dictionaries.

    To manage large datasets:
    - Use `start_seconds` and `end_seconds` to select a time window.
    - Use downsampling techniques such as time-binning or selecting every N-th record.
    - Prefer SQL **aggregations** (e.g., AVG, MIN, MAX, COUNT) to summarize data.

    Args:
        path (str): Filesystem path or URL to the data source.
        sql_statement (str): SQL query to execute against the topic table.
        topic (str): The topic name (also used as the DuckDB table name and the column name).
        start_seconds (float | None, optional): Start time seconds (inclusive).
            If None, starts from the beginning. Defaults to None.
        end_seconds (float | None, optional): End time seconds (inclusive).
            If None, reads until the end. Defaults to None.
        args (dict[str, Any] | None, optional): Additional constructor arguments
            used to create the `SourceFactory` and `TopicRegistry`.

    Returns:
        list[dict[str, Any]]: Query results as a list of dictionaries with column-value pairs.

    Examples:
        As an LLM prompt:
            What is the average linear velocity from `/turtle1/pose` in "./data/sample/ros2/mcap".

        As a Python call:
            >>> query_messages(
            ...     "./data/sample/ros2/mcap",
            ...     "SELECT AVG("/turtle1/pose".linear_velocity) as avg_lv FROM "/turtle1/pose"",
            ...     "/turtle1/pose",
            ... )

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})

    relation = dataset.to_duckdb(factory, registry, [topic], start_seconds, end_seconds)
    duckdb.register(topic, relation)
    result = duckdb.sql(sql_statement)
    return result.to_df().to_dict(orient="records")


@server.tool(
    title="Read logging messages from a data source",
    description=(
        "Extract INFO, WARN, and ERROR messages from a data source. "
        "Supports optional time filtering. Use for debugging or diagnostics."
    ),
)
def read_loggings(
    path: str,
    start_seconds: float | None = None,
    end_seconds: float | None = None,
    args: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Read system log messages from a data source.

    Retrieves log entries (INFO, WARN, ERROR) from the given data source. An
    optional time window can be specified. Not all sources provide logs—if no
    logging dataset exists, check if logs are available as a topic and use
    `query_messages` instead.

    Args:
        path (str): Filesystem path or URL to the data source.
        start_seconds (float | None, optional): Start time seconds (inclusive).
            If None, starts from the beginning. Defaults to None.
        end_seconds (float | None, optional): End time seconds (inclusive).
            If None, reads until the end. Defaults to None.
        args (dict[str, Any] | None, optional): Additional constructor arguments
            used to create the `SourceFactory` and `TopicRegistry`.

    Returns:
        list[dict[str, Any]]: A list of logging messages, where each dictionary
            typically contains timestamp, severity (e.g., INFO, WARN, ERROR), and
            message content fields.

    Examples:
        As an LLM prompt:
            Read all ERROR messages from the PX4 ULog at "./data/sample/px4/sample.ulg".

        As a Python call:
            >>> read_loggings("./data/sample/px4/sample.ulg")

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    dataset = module.provide(f"{BaseModule.LOGGING_DATASET.value}.{ds_type.value}", {})
    relation = dataset.to_duckdb(factory, registry, start_seconds, end_seconds)
    return relation.to_df().to_dict(orient="records")


@server.tool(
    title="List available live topics",
    description=(
        "Use this tool to inspect a live data stream and list the topics that "
        "can be subscribed to. Helpful before starting a subscription."
    ),
)
def list_live_topics(
    type_: str,
    host: str | None = None,
    port: int | None = None,
    args: dict[str, Any] | None = None,
) -> list[str]:
    """List available topics from a live data stream.

    Connects to a live streaming service (e.g., ROS bridge, PX4 telemetry)
    and retrieves the list of topics that are currently available for
    subscription. This is typically used to discover which topics exist
    before calling `subscribe_live_topics`.

    Args:
        type_ (str): The type of `TopicSink` to use (e.g., ROS1, ROS2, PX4). For the full
            list of supported types, see `TopicSink` in `src/di/types/topic_sink.py`.
        host (str | None, optional): Hostname of the live data stream service. If None,
            the default host is inferred.
        port (int | None, optional): Port number of the live data stream service. If None,
            the default port is inferred.
        args (dict[str, Any] | None, optional): Additional constructor arguments for
            creating the `TopicSink`.

    Returns:
        list[str]: A list of available topic names that can be subscribed to.

    Examples:
        As an LLM prompt:
            List the available topics in a ROS2 bridge on host `127.0.0.1`.

        As a Python call:
            >>> list_live_topics("ros2.bridge", host="127.0.0.1")

    """
    ts_type = TopicSink(type_)
    sink = module.provide(
        f"{BaseModule.TOPIC_SINK.value}.{ts_type.value}",
        {
            "host": host or guess_host(ts_type),
            "port": port or guess_port(ts_type),
            **(args or {}),
        },
    )
    return sink.available_topics


@server.tool(
    title="Subscribe to live topic messages",
    description=(
        "Use this tool to connect to a live data stream and subscribe to one or more topics. "
        "Messages are written to a local sink directory, which can be used later as input "
        "for other tools (via the `path` argument in SourceFactory). Optionally attach a "
        "pipeline config to create a STANDING pipeline that runs on incoming messages -- "
        "e.g. an on_event cadence that captures and uploads a window around every anomaly."
    ),
)
def subscribe_live_topics(  # noqa: PLR0913
    type_: str,
    topics: list[str] | None = None,
    host: str | None = None,
    port: int | None = None,
    overwrite: bool = False,
    args: dict[str, Any] | None = None,
    pipeline: dict[str, Any] | None = None,
) -> str:
    """Subscribe to real-time messages from a live data stream.

    Establishes a connection to a live streaming service (e.g., ROS bridge, telemetry server)
    and subscribes to the specified topics. The subscribed messages are persisted to a local
    sink directory for subsequent analysis or playback.

    Args:
        type_ (str): The type of `TopicSink` to use (e.g., ROS1, ROS2, MQTT). For the full
            list of supported types, see `TopicSink` in `src/di/types/topic_sink.py`.
        topics (list[str] | None, optional): The topics to subscribe to. If None, subscribes
            to all available topics.
        host (str | None, optional): Hostname of the live data stream service. If None,
            the default host is inferred.
        port (int | None, optional): Port number of the live data stream service. If None,
            the default port is inferred.
        overwrite (bool, optional): If True, overwrite any existing topic buffer directory,
            i.e., clear out the disk buffer of the selected topics. Defaults to False.
        args (dict[str, Any] | None, optional): Additional constructor arguments for
            creating the `TopicSink`.
        pipeline (dict[str, Any] | None, optional): A pipeline configuration (the same
            structure `run_pipeline` accepts) to run as a STANDING pipeline on incoming
            messages for the life of the subscription. Its `cadence.topic` must be among
            the subscribed topics; its `path` defaults to the sink directory so tasks read
            the live buffer. Use an `on_event` cadence (with `debounce`/`forward`) for
            edge recording. Defaults to None.

    Returns:
        str: Filesystem path to the sink directory where subscribed messages are stored.
            This path can later be passed as the `path` argument to the `SourceFactory` when
            using other tools.

    Examples:
        As an LLM prompt:
            Subscribe to `freezer/1/status` from the MQTT broker, and every time temp rises
            above -15 for 2 minutes, snapshot the last 30 seconds to a CSV.

        As a Python call:
            >>> subscribe_live_topics("mqtt", topics=["freezer/1/status"], pipeline={...})

    """
    ts_type = TopicSink(type_)
    sink = module.provide(
        f"{BaseModule.TOPIC_SINK.value}.{ts_type.value}",
        {
            "host": host or guess_host(ts_type),
            "port": port or guess_port(ts_type),
            **(args or {}),
        },
    )
    startup.subscribe_with_pipeline(sink, topics, pipeline, overwrite=overwrite)
    return str(sink.directory)


@server.tool(
    title="Run a capability defined in a POML file",
    description=(
        "Use this tool to run a predefined capability described in a `.poml` file. "
        "The file specifies task instructions and output formats. "
        "Optional context values can be injected to customize its behavior."
    ),
)
def run_poml_capability(
    poml_path: str,
    poml_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a structured capability from a POML file.

    Loads a `.poml` file containing instructions written in the POML
    (Prompt-Oriented Markup Language) format. The file defines the task the
    LLM should perform and how the output should be structured. This tool
    produces a ready-to-use prompt for LLM execution.

    Optionally, a context dictionary can be passed to substitute values in
    the POML template, enabling dynamic parameterization.

    Args:
        poml_path (str): Filesystem path to the `.poml` file containing the
            capability definition.
        poml_context (dict[str, Any] | None, optional): Key-value pairs injected
            into the POML file to customize behavior. Defaults to None.

    Raises:
        FileNotFoundError: If the `.poml` file cannot be found.

    Returns:
        list[dict[str, Any]]: A structured prompt representation, typically in the format:
            [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}]

    Examples:
        As an LLM prompt:
            Run the capability './src/agent/examples/woof.poml' on the ROS2 bag
            './data/sample/ros2/mcap'.

        As a Python call:
            >>> run_poml_capability("./src/agent/examples/woof.poml", {"foo": "bar"})

    """
    poml_file = pathlib.Path(poml_path)
    if not poml_file.exists():
        raise FileNotFoundError(poml_file)
    return poml(poml_file, context=poml_context)


@server.tool(
    title="List pipeline capabilities",
    description=(
        "List the tasks and gates available to compose a data pipeline, including "
        "each one's module path, kind (task or gate), constructor parameters, and a "
        "short summary. Use this before authoring a pipeline so the correct `module` "
        "and `args` are chosen instead of guessed."
    ),
)
def list_pipeline_capabilities(include_unavailable: bool = False) -> list[dict[str, Any]]:
    """List the tasks and gates that can be composed into a pipeline.

    Each capability is a building block referenced by its `module` path in a pipeline
    YAML. Tasks perform actions (e.g. snippet or reduce a bag); gates decide whether
    downstream tasks run. The reported `parameters` map directly to a task/gate's
    `args` in the pipeline config.

    Args:
        include_unavailable (bool, optional): If True, also list modules that cannot be
            imported in this environment (e.g. ROS tasks without ROS installed), marked
            with `available: False`. Defaults to False.

    Returns:
        list[dict[str, Any]]: Capability descriptions, each with `module`, `kind`,
            `class`, `summary`, `parameters`, and `available`.

    Examples:
        As an LLM prompt:
            What pipeline tasks can I use?

        As a Python call:
            >>> list_pipeline_capabilities()

    """
    return capabilities.list_capabilities(include_unavailable=include_unavailable)


@server.tool(
    title="Preview an event-driven data reduction",
    description=(
        "Dry-run an event-windowed reduction WITHOUT writing any files. Detects the "
        "rising-edge events where a SQL predicate becomes true on a topic, builds "
        "pre/post windows around them, merges overlaps, and reports how much data would "
        "be kept. Use this to audit a reduce/snippet pipeline before running it."
    ),
)
def preview_pipeline(  # noqa: PLR0913
    path: str,
    event_topic: str,
    predicate: str,
    pre_seconds: float,
    post_seconds: float = 0.0,
    debounce_seconds: float = 0.0,
    args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Preview the reduction that a pipeline would perform, without writing anything.

    Evaluates `predicate` against every message of `event_topic`, finds the rising-edge
    events (False -> True transitions), builds `[event - pre_seconds, event + post_seconds]`
    windows, merges overlapping windows, and returns the resulting event count, kept
    windows, and kept fraction of the source. Nothing is written to disk.

    Args:
        path (str): Filesystem path or URL to the data source.
        event_topic (str): The topic to evaluate the predicate against.
        predicate (str): A SQL boolean expression over `event_topic` columns, e.g.
            "linear_acceleration_x < -10".
        pre_seconds (float): Seconds to keep before each event.
        post_seconds (float, optional): Seconds to keep after each event. Defaults to 0.0.
        debounce_seconds (float, optional): Minimum seconds between consecutive events;
            closer events are coalesced. Defaults to 0.0.
        args (dict[str, Any] | None, optional): Additional constructor arguments used to
            create the `SourceFactory` and `TopicRegistry`.

    Returns:
        dict[str, Any]: A summary with `event_count`, `events` (timestamps), `intervals`
            (kept windows as start/end seconds), `kept_seconds`, `total_seconds`, and
            `kept_fraction`.

    Examples:
        As an LLM prompt:
            Preview keeping 10s before and after every deceleration below -10 on `/imu`.

        As a Python call:
            >>> preview_pipeline("./data/sample/ros2/mcap", "/imu",
            ...                  "linear_acceleration_x < -10", pre_seconds=10, post_seconds=10)

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})

    relation = dataset.to_duckdb(factory, registry, [event_topic])
    ts_column = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    rows = relation.project(f"{ts_column} AS ts, ({predicate}) AS hit").fetchall()

    timestamps = [float(row[0]) for row in rows]
    span_seconds = (max(timestamps) - min(timestamps)) if timestamps else 0.0

    plan = windows.plan_reduction(
        rows, pre_seconds, post_seconds, span_seconds, min_gap_seconds=debounce_seconds
    )
    return {
        "event_count": len(plan["events"]),
        "events": plan["events"],
        "intervals": [
            {"start_seconds": start, "end_seconds": end} for start, end in plan["intervals"]
        ],
        "kept_seconds": plan["kept_seconds"],
        "total_seconds": plan["total_seconds"],
        "kept_fraction": plan["kept_fraction"],
    }


@server.tool(
    title="Save a pipeline to a YAML file",
    description=(
        "Persist a pipeline configuration to a YAML file so it can be reused, edited, or "
        "run later with `run.py`. Returns the path to the written file."
    ),
)
def save_pipeline(
    config: dict[str, Any], name: str, directory: str = "pipelines"
) -> str:
    """Write a pipeline configuration to a YAML file.

    Args:
        config (dict[str, Any]): The pipeline configuration (the same structure `run_pipeline`
            accepts and `run.py` loads): `name`, `site`, `asset`, `path`, `allow_failure`,
            `cadence`, and `tasks`.
        name (str): The pipeline file name (without extension), in lower_snake_case.
        directory (str, optional): Directory to write the file into. Created if missing.
            Defaults to "pipelines".

    Returns:
        str: The path to the written YAML file.

    Raises:
        ValueError: If `name` is not a valid lower_snake_case identifier.

    Examples:
        As an LLM prompt:
            Save this pipeline as "hard_decel_reduce".

    """
    from src import artifacts

    if not artifacts.is_lower_snake_case(name):
        raise ValueError(f"Pipeline name '{name}' must be lower_snake_case.")

    output_directory = pathlib.Path(directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_file = output_directory / f"{name}.yaml"
    with open(output_file, "w") as stream:
        yaml.safe_dump(config, stream, sort_keys=False)
    return str(output_file)


@server.tool(
    title="Run a pipeline",
    description=(
        "Build and run a pipeline from a configuration and return the artifact paths it "
        "produced. Prefer running `preview_pipeline` first for event-driven reductions so "
        "the effect is audited before anything is written."
    ),
)
def run_pipeline(config: dict[str, Any]) -> dict[str, Any]:
    """Build and run a pipeline, returning the artifacts it produced.

    Args:
        config (dict[str, Any]): The pipeline configuration with `name`, `site`, `asset`,
            `path`, `allow_failure`, `cadence`, and `tasks` (and optional `gates`). This is
            the same structure accepted by `save_pipeline` and loaded by `run.py`.

    Returns:
        dict[str, Any]: A summary with `pipeline` (name), `status` ("completed"), and
            `artifacts` (the paths produced by the pipeline's tasks).

    Examples:
        As an LLM prompt:
            Run the reduce pipeline I just previewed.

    """
    pipeline = base.Pipeline.build(config)
    produced = pipeline.run_all()
    return {
        "pipeline": pipeline.name,
        "status": "completed",
        "artifacts": [str(path) for path in produced],
    }


@server.tool(
    title="Run a pipeline across many data sources (batch)",
    description=(
        "Run one pipeline configuration against many data sources -- explicit paths or glob "
        "patterns like 'logs/*'. Each source is processed independently; a failure on one "
        "source is reported but does not stop the batch. Returns per-source results and a "
        "summary. For an event reduction, preview a representative source first."
    ),
)
def run_pipeline_batch(config: dict[str, Any], paths: list[str]) -> dict[str, Any]:
    """Run a pipeline config against every matching data source.

    Args:
        config (dict[str, Any]): The base pipeline config (same structure as `run_pipeline`).
            Its `path` is overridden for each source, so it need not be set.
        paths (list[str]): Data source paths or glob patterns (e.g. ["logs/*.mcap"]). Globs
            are expanded; patterns that match nothing are treated as literal paths.

    Returns:
        dict[str, Any]: A summary with `sources`, `completed`, `failed`, `artifacts` (total
            produced), and `results` (a per-source list with `path`, `status`, and either
            `artifacts` or `error`).

    Examples:
        As an LLM prompt:
            Reduce every bag under "./logs" with this pipeline.

        As a Python call:
            >>> run_pipeline_batch(config, ["./logs/*"])

    """
    expanded = batch.expand_paths(paths)
    results = batch.run_batch(config, expanded)
    return batch.summarize(results)


@server.tool(
    title="Export an event window for PlotJuggler",
    description=(
        "Export a time window of topic data as a PlotJuggler session: a flattened CSV "
        "(one scalar column per signal) plus a layout file with the curves pre-added "
        "and the window pre-framed. Opening the returned command shows the event "
        "already plotted and zoomed. Use after preview_pipeline to hand an event to a "
        "human for visual inspection."
    ),
)
def export_for_plotjuggler(  # noqa: PLR0913
    path: str,
    topics: list[str],
    start_seconds: float,
    end_seconds: float,
    name: str = "event",
    signals: list[str] | None = None,
    args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Export a time window as a ready-to-open PlotJuggler session.

    Writes a flattened CSV of the window (columns named `topic/field/subfield`, the
    naming PlotJuggler users expect) and a layout `.xml` that references the CSV,
    pre-adds the signal curves, and frames the time range -- so PlotJuggler opens the
    event already plotted and zoomed.

    Args:
        path (str): Filesystem path or URL to the data source.
        topics (list[str]): The topics to include in the export.
        start_seconds (float): Window start (also the plot's framed x range start).
        end_seconds (float): Window end.
        name (str, optional): Session name, used for the output files and the tab
            title. Defaults to "event".
        signals (list[str] | None, optional): Flattened signal names to plot (e.g.
            "/imu/linear_acceleration/x"). If None, all numeric signals are plotted,
            capped at 8. Defaults to None.
        args (dict[str, Any] | None, optional): Additional constructor arguments used
            to create the `SourceFactory` and `TopicRegistry`.

    Returns:
        dict[str, Any]: `csv` and `layout` paths, the plotted `curves`, and `command`
            -- run it (or double-click the layout) to open the session in PlotJuggler.

    Examples:
        As an LLM prompt:
            Show me the second brake event in PlotJuggler.

        As a Python call:
            >>> export_for_plotjuggler("./flight.mcap", ["/imu"], 118.9, 138.9)

    """
    ds_type = resolve(path)
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **(args or {})}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", args or {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})

    relation = dataset.to_duckdb(
        factory, registry, topics, start_seconds=start_seconds, end_seconds=end_seconds
    )
    return plotjuggler.export_window(
        relation,
        name=name,
        start_seconds=start_seconds,
        end_seconds=end_seconds,
        signals=signals,
    )


if __name__ == "__main__":
    if settings.STARTUP_PIPELINES_FILE and pathlib.Path(settings.STARTUP_PIPELINES_FILE).exists():
        # Standing pipelines: re-establish subscriptions (and their attached pipelines)
        # on boot, so they survive container restarts.
        startup.start(settings.STARTUP_PIPELINES_FILE)
    server.run(transport="sse")
