from bagel_mcp.source.ros2 import mcap


def test_should_build_mcap_directory() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap/")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 15


def test_should_build_mcap_file() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap/part_0.mcap")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 15


def test_should_build_mcap_zstd_directory() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap_zstd/")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 6


def test_should_build_mcap_zstd_file() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap_zstd/part_0.mcap.zstd")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 6
