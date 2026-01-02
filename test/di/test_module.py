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
