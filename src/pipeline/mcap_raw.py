"""Raw MCAP record passthrough utilities.

Shared by the MCAP reduce and snippet tasks: iterate raw (schema, channel, message)
records out of MCAP files and copy a selection of them verbatim into a new MCAP file.
Messages are never decoded or re-encoded, so no middleware typesupport is needed and
any profile (ros1, ros2, protobuf, ...) passes through unchanged.
"""

import pathlib
from collections.abc import Iterable, Iterator

from mcap.reader import make_reader
from mcap.records import Channel, Message, Schema
from mcap.writer import Writer

RawRecord = tuple[Schema | None, Channel, Message]


def in_intervals(log_time_ns: int, intervals_ns: list[tuple[int, int]]) -> bool:
    """Return True if a nanosecond log time falls within any interval."""
    return any(start <= log_time_ns <= end for start, end in intervals_ns)


def iter_raw_messages(
    mcap_files: Iterable[pathlib.Path], topics: list[str] | None = None
) -> Iterator[RawRecord]:
    """Yield raw (schema, channel, message) records from MCAP files in log-time order.

    Args:
        mcap_files: The .mcap files to read, in order.
        topics: Topics to include. If None, all topics are included.

    """
    keep_topics = set(topics) if topics else None
    for mcap_file in mcap_files:
        with open(mcap_file, "rb") as stream:
            reader = make_reader(stream)
            for schema, channel, message in reader.iter_messages(
                topics=topics, log_time_order=True
            ):
                if keep_topics is not None and channel.topic not in keep_topics:
                    continue
                yield schema, channel, message


def write_raw_messages(output_file: pathlib.Path, records: Iterable[RawRecord]) -> int:
    """Copy raw records verbatim into a new MCAP file, returning the message count.

    Schema and channel definitions are registered once on first sight and copied
    unchanged; message payload bytes pass through untouched.
    """
    count = 0
    with open(output_file, "wb") as output_stream:
        writer = Writer(output_stream)
        writer.start()
        schema_ids: dict[int, int] = {}  # source schema id -> output schema id
        channel_ids: dict[int, int] = {}  # source channel id -> output channel id

        for schema, channel, message in records:
            if schema is not None and schema.id not in schema_ids:
                schema_ids[schema.id] = writer.register_schema(
                    name=schema.name, encoding=schema.encoding, data=schema.data
                )
            if channel.id not in channel_ids:
                channel_ids[channel.id] = writer.register_channel(
                    topic=channel.topic,
                    message_encoding=channel.message_encoding,
                    schema_id=schema_ids.get(schema.id, 0) if schema else 0,
                    metadata=dict(channel.metadata),
                )
            writer.add_message(
                channel_id=channel_ids[channel.id],
                log_time=message.log_time,
                data=message.data,
                publish_time=message.publish_time,
                sequence=message.sequence,
            )
            count += 1
        writer.finish()
    return count
