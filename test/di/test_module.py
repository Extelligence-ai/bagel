import pytest

from bagel_mcp.di import module


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


def test_missing_optional_package_names_the_pip_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN a PX4 source whose pyulog import fails, as on a pip install without the px4 extra
    def missing(name: str) -> None:
        raise ModuleNotFoundError("No module named 'pyulog'", name="pyulog")

    monkeypatch.setattr(module.importlib, "import_module", missing)

    # WHEN / THEN the error names the extra to install
    with pytest.raises(ModuleNotFoundError, match=r"pip install 'bagel-mcp\[px4\]'") as raised:
        module.import_module("bagel_mcp.source.px4.ulog")
    assert raised.value.name == "pyulog"


def test_missing_required_package_is_raised_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN a missing package that no extra provides
    def missing(name: str) -> None:
        raise ModuleNotFoundError("No module named 'nope'", name="nope")

    monkeypatch.setattr(module.importlib, "import_module", missing)

    # WHEN / THEN the original error passes through
    with pytest.raises(ModuleNotFoundError, match="No module named 'nope'"):
        module.import_module("bagel_mcp.anything")


@pytest.mark.parametrize(
    ("import_path", "image"),
    [("bagel_mcp.sink.ros2.bridge", "ros2-kilted"), ("bagel_mcp.sink.ros1.bridge", "ros1-noetic")],
)
def test_live_ros_on_pip_points_to_docker(
    monkeypatch: pytest.MonkeyPatch, import_path: str, image: str
) -> None:
    # GIVEN a pip install, where roslibpy (a Docker-only companion) is missing
    def missing(name: str) -> None:
        raise ModuleNotFoundError("No module named 'roslibpy'", name="roslibpy")

    monkeypatch.setattr(module.importlib, "import_module", missing)

    # WHEN / THEN the error says live topics run in Docker and names the service
    with pytest.raises(ModuleNotFoundError, match=f"docker compose run --service-ports {image}"):
        module.import_module(import_path)
