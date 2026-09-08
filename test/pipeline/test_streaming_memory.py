"""Reader memory and ordering regressions over generated CSV data."""

import pathlib
import tracemalloc

from src.message.pyarrow.csv import MessageDataset
from src.source.pyarrow.csv import SourceFactory


def test_csv_reader_memory_does_not_scale_with_decoded_file(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "wide.csv"
    with path.open("w") as stream:
        stream.write("t,value\n")
        for index in range(120_000):
            stream.write(f"{index},{'x' * 500}\n")
    source = SourceFactory(str(path), timestamp_column="t", timestamp_format="seconds").build()
    tracemalloc.start()
    try:
        count = sum(1 for _ in MessageDataset()._messages(source, ["message"], None, None))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert count == 120_000
    assert peak < 40 * 1024 * 1024, f"decoded Python memory grew to {peak} bytes"


def test_streaming_sort_preserves_ties_and_filters_window(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "unordered.csv"
    path.write_text("t,value\n3,last\n1,first\n2,a\n2,b\n0,excluded\n")
    source = SourceFactory(str(path), timestamp_column="t", timestamp_format="seconds").build()
    messages = list(MessageDataset()._messages(source, ["message"], 1, 2))
    assert [(timestamp, row["value"]) for _, timestamp, row in messages] == [
        (1, "first"),
        (2, "a"),
        (2, "b"),
    ]
