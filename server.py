"""Entry point for the Bagel MCP server."""

import pathlib
from typing import Any

import duckdb
from mcp.server.fastmcp import FastMCP
from poml import poml

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve
from src.di.types.topic_sink import TopicSink, guess_host, guess_port

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
    factory = module.provide(BaseModule.SOURCE_FACTORY, ds_type, {"path": path, **(args or {})})
    registry = module.provide(BaseModule.TOPIC_REGISTRY, ds_type, args or {})
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
    factory = module.provide(BaseModule.SOURCE_FACTORY, ds_type, {"path": path, **(args or {})})
    registry = module.provide(BaseModule.TOPIC_REGISTRY, ds_type, args or {})
    dataset = module.provide(BaseModule.MESSAGE_DATASET, ds_type, {})
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
        end_seconds (float | None, optional): End time seconds (exclusive).
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
    factory = module.provide(BaseModule.SOURCE_FACTORY, ds_type, {"path": path, **(args or {})})
    registry = module.provide(BaseModule.TOPIC_REGISTRY, ds_type, args or {})
    dataset = module.provide(BaseModule.MESSAGE_DATASET, ds_type, {})

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
        end_seconds (float | None, optional): End time seconds (exclusive).
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
    factory = module.provide(BaseModule.SOURCE_FACTORY, ds_type, {"path": path, **(args or {})})
    registry = module.provide(BaseModule.TOPIC_REGISTRY, ds_type, args or {})
    dataset = module.provide(BaseModule.LOGGING_DATASET, ds_type, {})
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
        BaseModule.TOPIC_SINK,
        ts_type,
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
        "for other tools (via the `path` argument in SourceFactory)."
    ),
)
def subscribe_live_topics(  # noqa: PLR0913
    type_: str,
    topics: list[str] | None = None,
    host: str | None = None,
    port: int | None = None,
    overwrite: bool = False,
    args: dict[str, Any] | None = None,
) -> str:
    """Subscribe to real-time messages from a live data stream.

    Establishes a connection to a live streaming service (e.g., ROS bridge, telemetry server)
    and subscribes to the specified topics. The subscribed messages are persisted to a local
    sink directory for subsequent analysis or playback.

    Args:
        type_ (str): The type of `TopicSink` to use (e.g., ROS1, ROS2, PX4). For the full
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

    Returns:
        str: Filesystem path to the sink directory where subscribed messages are stored.
            This path can later be passed as the `path` argument to the `SourceFactory` when
            using other tools.

    Examples:
        As an LLM prompt:
            Subscribe to the `/odom` and `/scan` topics from a ROS2 bridge.

        As a Python call:
            >>> subscribe_live_topics("ros2.bridge", topics=["/odom", "/scan"])

    """
    ts_type = TopicSink(type_)
    sink = module.provide(
        BaseModule.TOPIC_SINK,
        ts_type,
        {
            "host": host or guess_host(ts_type),
            "port": port or guess_port(ts_type),
            **(args or {}),
        },
    )
    sink.subscribe(topics, overwrite=overwrite)
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


if __name__ == "__main__":
    server.run(transport="sse")
