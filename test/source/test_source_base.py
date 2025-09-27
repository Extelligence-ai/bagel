import pathlib

from src.source import base


class MockBoundedSourceFactory(base.BoundedSourceFactory):
    @property
    def uuid(self) -> str:
        return "00000000-1111-2222-3333-444444444444"

    def build(self) -> object:
        return object()

    @property
    def total_message_count(self) -> int:
        return 100

    @property
    def start_seconds(self) -> float:
        return 0.0

    @property
    def end_seconds(self) -> float:
        return 10.0


class MockFileBasedSourceFactory(base.FileBasedSourceFactory):
    def build(self) -> object:
        return object()

    @property
    def total_message_count(self) -> int:
        return 100

    @property
    def start_seconds(self) -> float:
        return 0.0

    @property
    def end_seconds(self) -> float:
        return 10.0

    def validate_path(self) -> tuple[bool, Exception | None]:
        return True, None


def test_bounded_source_factory() -> None:
    # GIVEN
    factory = MockBoundedSourceFactory()

    # WHEN
    metadata = factory.metadata

    # THEN
    assert metadata["total_message_count"] == 100
    assert metadata["start_seconds"] == 0.0
    assert metadata["end_seconds"] == 10.0
    assert metadata["duration_seconds"] == 10.0
    assert factory.duration_seconds == 10.0


def test_file_based_source_factory() -> None:
    # GIVEN
    path = pathlib.Path("data/sample/ros1/sample.bag")
    factory = MockFileBasedSourceFactory(path)

    # WHEN
    metadata = factory.metadata

    # THEN"
    assert metadata["path"] == str(path)
    assert metadata["size_bytes"] == 12464
    assert factory.uuid == "ab827c6b-e15a-5c8d-875b-593104b7f29b"
    assert factory.path == path
    assert factory.size_bytes == 12464
