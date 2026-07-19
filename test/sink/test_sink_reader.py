"""Unit tests for TopicSinkReader and the downstream bagel.sink adapters (issue #72)."""

import pathlib

import duckdb
import pyarrow as pa
import pytest
import yaml

from bagel.di import module
from bagel.di.types.base_module import BaseModule
from bagel.di.types.data_source import DataSource, resolve
from bagel.sink import base as sink_base
from bagel.sink.buffer import TopicBufferWriter
from bagel.sink.reader import TopicSinkReader
from bagel.source import errors

STRUCT = pa.struct([pa.field("x", pa.float64()), pa.field("note", pa.string())])


@pytest.fixture()
def sink_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """A real sink directory: magic metadata plus two written topic buffers."""
    (tmp_path / "metadata.yaml").write_text(yaml.safe_dump({"magic": "BAGEL_SINK"}))
    for topic in ["/imu", "/battery"]:
        writer = TopicBufferWriter(
            path=tmp_path,
            topic=topic,
            type_name="test/Msg",
            definition="float64 x\nstring note",
            struct=STRUCT,
            buffer_size_bytes=None,
            overwrite=True,
            pipeline=None,
            extract_timestamp=lambda msg: msg["x"],
        )
        for i in range(4):
            writer.append({"x": float(i), "note": topic})
    return tmp_path


def test_sink_reader_lists_topics_and_reads(sink_dir: pathlib.Path) -> None:
    reader = TopicSinkReader(str(sink_dir))

    assert sorted(reader.subscribed_topics()) == ["/battery", "/imu"]
    assert reader.metadata["magic"] == "BAGEL_SINK"

    messages = list(reader.reader("/imu").messages())
    assert len(messages) == 4
    assert messages[0][1]["note"] == "/imu"

    with pytest.raises(sink_base.TopicNotFoundError):
        reader.reader("/nope")


def test_sink_directory_resolves_and_full_adapter_chain_reads(sink_dir: pathlib.Path) -> None:
    # GIVEN the sink resolves to the bagel.sink data source type
    ds_type = resolve(str(sink_dir))
    assert ds_type is DataSource.BAGEL_SINK

    # WHEN reading through the same DI chain the MCP tools use
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": str(sink_dir)}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})

    data_source = factory.build()
    assert registry.available_topics(data_source) == ["/battery", "/imu"]
    assert registry.struct("/imu", data_source) == STRUCT
    assert registry.message_count("/imu", data_source) is None  # unbounded stream contract

    # THEN SQL over the sink returns what the writer wrote
    relation = dataset.to_duckdb(factory, registry, ["/battery"])
    duckdb.register("battery", relation)
    result = duckdb.sql("SELECT MAX(\"/battery\"['x']) AS peak FROM battery").fetchone()
    assert result[0] == 3.0


def test_source_factory_rejects_non_sink_directories(tmp_path: pathlib.Path) -> None:
    from bagel.source.bagel.sink import SourceFactory

    with pytest.raises(FileNotFoundError):
        SourceFactory(str(tmp_path / "missing"))

    (tmp_path / "metadata.yaml").write_text(yaml.safe_dump({"magic": "NOT_A_SINK"}))
    with pytest.raises(errors.InvalidPathError):
        SourceFactory(str(tmp_path))
