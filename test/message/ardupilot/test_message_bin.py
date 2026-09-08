from src.message.ardupilot.bin import MessageDataset
from src.source.ardupilot.bin import SourceFactory
from src.topic.ardupilot.bin import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (69769, 66)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["AETR"])

    # THEN
    assert relation.shape == (1028, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1754307092.581066
    )

    # THEN
    assert relation.shape == (8, 66)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.to_df().shape == (0, 66)


def test_char_array_bytes_are_decoded_to_text() -> None:
    """pymavlink may hand back bytes for char[] fields it could not decode (#199)."""
    from src.message.ardupilot.bin import _text

    assert _text(b"ArduPlane V4.6.3\x00\x00") == "ArduPlane V4.6.3"
    assert _text(bytearray(b"PA\x06\xff")) == "PA\x06�"
    assert _text(12) == 12
