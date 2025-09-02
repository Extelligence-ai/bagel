"""Read messages from a Betaflight .bbl file by topic."""

from collections.abc import Iterator

import pyarrow as pa

from settings import settings
from src.convert.converter import MessageConverter
from src.reader.betaflight.bbl.reader import BblReader
from src.reader.topic import TopicMessageReader


class TopicMessageReader(TopicMessageReader, BblReader):
    """Read messages from a Betaflight .bbl file by topic."""

    def _iter_record_batches(  # noqa: PLR0913
        self,
        topics: list[str],
        start_seconds: float | None,
        end_seconds: float | None,
        ffill: bool,
        schema: pa.Schema,
        converters: dict[str, MessageConverter],
    ) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches for the specified topics and time range."""
        batch_size = settings.MIN_ARROW_RECORD_BATCH_SIZE_COUNT
        batch = {column: [] for column in schema.names}
        record = {column: None for column in schema.names}

        messages = self._iter_messages(topics, start_seconds, end_seconds, timestamps_only=False)

        for timestamp, topic, message in messages:
            if not ffill:
                record = {column: None for column in schema.names}
            record[settings.ROBOLOG_ID_COLUMN_NAME] = self.robolog_id
            record[settings.TIMESTAMP_SECONDS_COLUMN_NAME] = timestamp
            record[topic] = converters[topic].to_dict(message)

            for column, value in record.items():
                batch[column].append(value)

            if len(batch[settings.ROBOLOG_ID_COLUMN_NAME]) >= batch_size:
                record_batch = pa.RecordBatch.from_pydict(batch, schema=schema)
                batch_size = self._estimate_record_batch_size_count(record_batch)
                batch = {column: [] for column in schema.names}
                yield record_batch

        if batch[settings.ROBOLOG_ID_COLUMN_NAME]:
            yield pa.RecordBatch.from_pydict(batch, schema=schema)
