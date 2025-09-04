import pathlib
import tempfile

from src.source import base


class MockLocalFileSystemSourceFactory(base.LocalFileSystemSourceFactory):
    def build(self) -> object: ...

    def validate_path(self) -> tuple[bool, Exception | None]:
        return True, None


def test_should_generate_uuid_based_on_file_content() -> None:
    with tempfile.NamedTemporaryFile(mode="wb") as file:
        # GIVEN
        pathlib.Path(file.name).write_text("Test content")

        # WHEN
        factory = MockLocalFileSystemSourceFactory(path=file.name)

        # THEN
        assert factory.uuid == "fe743461-ce80-5ed6-961a-f6cc6938d5e9"


def test_should_generate_uuid_based_on_directory_content() -> None:
    with tempfile.TemporaryDirectory() as directory:
        # GIVEN
        pathlib.Path(directory, "file1.txt").write_text("Test content 1")
        pathlib.Path(directory, "file2.txt").write_text("Test content 2")

        # WHEN
        factory = MockLocalFileSystemSourceFactory(path=directory)

        # THEN
        assert factory.uuid == "2fce74a4-036c-5e5f-a57c-1531f7991c31"
