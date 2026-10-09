from bagel_mcp.source.ros2 import db3


def test_should_build_db3_directory() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3/")

    # WHEN
    reader = factory.build()

    # THEN
    assert reader.info.message_count == 6074
    assert next(reader.raw_messages(reader.info.topic_names, None, None), None) is not None


def test_should_build_db3_file() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3/part_0.db3")

    # WHEN
    reader = factory.build()

    # THEN
    assert reader.info.message_count == 1246
    assert next(reader.raw_messages(reader.info.topic_names, None, None), None) is not None


def test_should_build_db3_zstd_directory() -> None:
    """A directory recorded with file-level zstd compression reads as it is."""
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3_zstd/")

    # WHEN
    reader = factory.build()

    # THEN
    assert factory.compression_format == "zstd"
    assert factory.compression_mode == "file"
    assert reader.info.message_count == 6
    assert sum(1 for _ in reader.raw_messages(reader.info.topic_names, None, None)) == 6


def test_should_build_db3_zstd_file() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")

    # WHEN
    reader = factory.build()

    # THEN
    assert reader.info.message_count == 6
    assert next(reader.raw_messages(reader.info.topic_names, None, None), None) is not None
