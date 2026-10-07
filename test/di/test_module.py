import pytest

from src.di import module


class Cat:
    def __init__(self, name: str, sound: str = "meow") -> None:
        self.name = name
        self.sound = sound


def test_should_construct_module() -> None:
    # GIVEN
    args = {"name": "cat", "sound": "woof??"}

    # WHEN
    cat = module.construct(Cat, args)

    # THEN
    assert isinstance(cat, Cat)
    assert cat.name == "cat"
    assert cat.sound == "woof??"


def test_should_ignore_unexpected_args() -> None:
    # GIVEN
    args = {"name": "cat", "color": "black"}

    # WHEN
    cat = module.construct(Cat, args)

    # THEN
    assert isinstance(cat, Cat)
    assert cat.name == "cat"
    assert cat.sound == "meow"


def test_should_raise_if_missing_required_args() -> None:
    # GIVEN
    args = {}

    # WHEN / THEN
    with pytest.raises(ValueError, match="Missing required constructor arguments: name"):
        module.construct(Cat, args)


def test_missing_optional_package_names_the_image_and_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # GIVEN an MDF source whose asammdf import fails, as in an image before 2.4.2
    def missing(name: str) -> None:
        raise ModuleNotFoundError("No module named 'asammdf'", name="asammdf")

    monkeypatch.setattr(module.importlib, "import_module", missing)

    # WHEN / THEN the error says how to get it in and out of Docker
    with pytest.raises(ModuleNotFoundError) as raised:
        module.provide("src.source.factory.automotive.mf4", {"path": "x.mf4"})
    assert "2.4.2" in str(raised.value)
    assert "uv sync --group automotive" in str(raised.value)
    assert raised.value.name == "asammdf"


def test_missing_unrelated_package_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN a missing package that no optional group installs
    def missing(name: str) -> None:
        raise ModuleNotFoundError("No module named 'nope'", name="nope")

    monkeypatch.setattr(module.importlib, "import_module", missing)

    # WHEN / THEN the original error passes through unchanged
    with pytest.raises(ModuleNotFoundError, match="^No module named 'nope'$"):
        module.import_module("src.whatever")
