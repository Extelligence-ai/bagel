"""Entry point for the Bagel MCP server."""

import pathlib
import textwrap
from typing import Any

import duckdb
from mcp.server.fastmcp import FastMCP
from poml import poml

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve

server = FastMCP(
    name="Bagel MCP Server",
    host=settings.LOCAL_HOST,
    port=settings.MCP_LOCAL_PORT,
)


@server.tool(
    title="Generate a summary of the data source from its metadata and available topics.",
    description=(
        textwrap.dedent(
            """\
            The result of this MCP tool:

            Includes:
            - A brief summary of the data source
            - Basic metadata (e.g., start time, message count, configuration parameters)
            - A list of available topics

            Excludes:
            - Detailed information about specific topics (e.g., topic definitions)
            - Actual topic messages
            """
        )
    ),
)
def describe_data_source(path: str, args: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Describe the data source.

    Args:
        path (str): Path or URL to the data source.
        args (dict[str, Any] | None, optional): Constructor arguments used to create
            SourceFactory and TopicRegistry.

    Returns:
        list[dict[str, Any]]: The formatted prompt to send to the LLMs.

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
    title="Generate a summary of the topic in the data source.",
    description=textwrap.dedent(
        """\
        The result of this MCP tool:

        Includes:
        - A brief summary of the topic
        - The DuckDB schema of the topic
        - The topic definition in its original IDL (Interface Definition Language) format
        - Guidelines for generating DuckDB SQL queries for the topic

        Excludes:
        - Actual topic messages
        """
    ),
)
def describe_topic(
    path: str, topic: str, args: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Describe a topic in the data source.

    Args:
        path (str): Path or URL to the data source.
        topic (str): The topic name the user is interested in.
        args (dict[str, Any] | None, optional): Constructor arguments used to create
            SourceFactory and TopicRegistry.

    Returns:
        list[dict[str, Any]]: The formatted prompt to send to the LLMs.

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
    title="Query topic messages using a DuckDB SQL query.",
    description=textwrap.dedent(
        """\
        Execute a SQL query against the DuckDB table of topic messages to answer the user's prompt.

        Notes:
        - Only one topic can be queried at a time.
        - If the query result is too large, use the `start_seconds` and `end_seconds` arguments to
          paginate the results.
        """
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
    """Query topic messages using DuckDB SQL.

    Args:
        path (str): Path or URL to the data source.
        sql_statement (str): The DuckDB SQL query to execute.
        topic (str): The topic name to query. This is also the DuckDB table name.
        start_seconds (float | None, optional): The start time seconds (inclusive).
            If None, starts from the beginning.
        end_seconds (float | None, optional): The end time seconds (exclusive).
            If None, reads until the end.
        args (dict[str, Any] | None, optional): Constructor arguments used to create
            SourceFactory and TopicRegistry.

    Returns:
        list[dict[str, Any]]: The query results as a list of dictionaries.

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
    title="Read the logging messages in a data source.",
    description="Extracts system loggings (e.g., INFO, WARN, ERROR) from the given data source.",
)
def read_loggings(
    path: str,
    start_seconds: float | None = None,
    end_seconds: float | None = None,
    args: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Read the logging messages in a data source.

    Args:
        path (str): Path or URL to the data source.
        start_seconds (float | None, optional): Start time seconds (inclusive).
            If None, starts from the beginning.
        end_seconds (float | None, optional): End time seconds (exclusive).
            If None, reads until the end.
        args (dict[str, Any] | None, optional): Constructor arguments used to create
            SourceFactory and TopicRegistry.

    Returns:
        list[dict[str, Any]]: The logging messages in the data source.

    """
    ds_type = resolve(path)
    factory = module.provide(BaseModule.SOURCE_FACTORY, ds_type, {"path": path, **(args or {})})
    registry = module.provide(BaseModule.TOPIC_REGISTRY, ds_type, args or {})
    dataset = module.provide(BaseModule.LOGGING_DATASET, ds_type, {})
    relation = dataset.to_duckdb(factory, registry, start_seconds, end_seconds)
    return relation.to_df().to_dict(orient="records")


@server.tool(
    title="Execute a POML-defined capability.",
    description=(
        "Run a capability defined in a .poml file. The POML file provides structured instructions "
        "that the LLM will interpret and execute.."
    ),
)
def run_poml_capability(
    poml_path: str,
    poml_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a capability defined in a POML file.

    This function loads a `.poml` file containing structured instructions and
    returns a formatted prompt for LLM execution. Optional context data can be
    injected into the POML template to parameterize its behavior.

    Args:
        poml_path (str): The path to the `.poml` file that defines the capability instructions.
        poml_context (dict[str, Any] | None, optional): Key-value pairs to inject
            into the POML template for dynamic parameterization. Defaults to None.

    Raises:
        FileNotFoundError: If the specified `.poml` file does not exist.

    Returns:
        list[dict[str, Any]]: The formatted prompt to send to the LLMs.

    """
    poml_file = pathlib.Path(poml_path)
    if not poml_file.exists():
        raise FileNotFoundError(poml_file)
    return poml(poml_file, context=poml_context)


if __name__ == "__main__":
    server.run(transport="sse")
