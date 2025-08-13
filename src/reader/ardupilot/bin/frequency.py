"""Calculate the frequency of messages in an ArduPilot .bin file by topic."""

from collections.abc import Iterator

import pyarrow as pa

from settings import settings
from src.reader.ardupilot.bin.reader import BinReader
from src.reader.frequency import TopicFrequencyReader


class TopicFrequencyReader(TopicFrequencyReader, BinReader):
    """Calculate the frequency of messages in an ArduPilot .bin file by topic."""

    def _iter_record_batches(
        self,
        topics: list[str],
        start_seconds: float | None,
        end_seconds: float | None,
        schema: pa.Schema,
    ) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches for the specified topics and time range."""
        batch_size = settings.MIN_ARROW_RECORD_BATCH_SIZE_COUNT
        batch = {column: [] for column in schema.names}
        latest = {topic: None for topic in topics}

        messages = self._iter_messages(topics, start_seconds, end_seconds, timestamps_only=True)

        for timestamp, topic, _ in messages:
            record = {colname: None for colname in schema.names}
            record[settings.ROBOLOG_ID_COLUMN_NAME] = self.robolog_id
            record[settings.TIMESTAMP_SECONDS_COLUMN_NAME] = timestamp
            if latest[topic] is not None:
                record[topic] = timestamp - latest[topic]
            latest[topic] = timestamp

            for column, value in record.items():
                batch[column].append(value)

            if len(batch[settings.ROBOLOG_ID_COLUMN_NAME]) >= batch_size:
                record_batch = pa.RecordBatch.from_pydict(batch, schema=schema)
                batch_size = self._estimate_record_batch_size_count(record_batch)
                batch = {column: [] for column in schema.names}
                yield record_batch

        if batch[settings.ROBOLOG_ID_COLUMN_NAME]:
            yield pa.RecordBatch.from_pydict(batch, schema=schema)
