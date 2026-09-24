"""Gates can attach annotations that downstream tasks read (`Gate.annotations`)."""

import pathlib
import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest
from google.protobuf.wrappers_pb2 import DoubleValue
from mcap_protobuf.writer import Writer as ProtobufWriter

from src.di import module
from src.pipeline import base

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000


@pytest.fixture
def mcap_path(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "log.mcap"
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(4):
            timestamp_ns = int((EPOCH + i) * SECOND_NS)
            writer.write_message(
                topic="/x",
                message=DoubleValue(value=float(i)),
                log_time=timestamp_ns,
                publish_time=timestamp_ns,
            )
    return path


class Labeller(base.Gate):
    """Passes and labels every evaluation with its timestamp."""

    def __init__(self, key: str, passes: bool = True) -> None:
        self._key = key
        self._passes = passes
        self._last: float | None = None

    def setup(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        pass

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        self._last = asof_seconds
        return self._passes

    def annotations(self) -> dict:
        return {self._key: self._last}


class PlainGate(base.Gate):
    """A gate that does not override `annotations`."""

    def setup(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        pass

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        return True


SEEN: list[dict] = []
RECORD: dict = {"probabilities": {"x": 0.5}}


class Nested(base.Gate):
    """Passes and reports a nested record that it keeps a reference to."""

    def setup(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        pass

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        return True

    def annotations(self) -> dict:
        return RECORD


class Tamper(base.Task):
    """Edits a nested value of the annotations it is handed."""

    def setup(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        pass

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        self.gate_annotations["nested"]["probabilities"]["x"] = 999.0
        SEEN.append(dict(self.gate_annotations))


class Recorder(base.Task):
    """Records the annotations it sees on each execution."""

    def setup(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        pass

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        SEEN.append(dict(self.gate_annotations))


@pytest.fixture(autouse=True)
def _operators(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    SEEN.clear()
    operators = {
        "labeller": Labeller,
        "plain": PlainGate,
        "recorder": Recorder,
        "nested": Nested,
        "tamper": Tamper,
    }
    for name, cls in operators.items():
        fake = types.ModuleType(f"fake_{name}")
        fake.register = lambda cls=cls, name=name: module.global_registry.__setitem__(  # type: ignore[attr-defined]
            f"fake_{name}", cls
        )
        monkeypatch.setitem(sys.modules, f"fake_{name}", fake)
    yield


def _run(path: pathlib.Path, gates: list[dict], tasks: list[dict] | None = None) -> None:
    config = {
        "name": "annotations",
        "site": "s",
        "asset": "a",
        "path": str(path),
        "allow_failure": False,
        "cadence": {"topic": "/x", "when": {"every": 2, "unit": "second"}},
        "gates": gates,
        "tasks": tasks or [{"module": "fake_recorder"}],
    }
    base.Pipeline.build(config).run_all()


def test_tasks_receive_annotations_from_passing_gates(mcap_path: pathlib.Path) -> None:
    _run(mcap_path, [{"module": "fake_labeller", "args": {"key": "label"}}])
    assert SEEN == [{"labeller": {"label": EPOCH}}, {"labeller": {"label": EPOCH + 2}}]


def test_annotations_from_several_gates_are_kept_apart_by_gate_name(
    mcap_path: pathlib.Path,
) -> None:
    # Two gates that both report a `label` must not overwrite each other (Codex P1).
    _run(
        mcap_path,
        [
            {"module": "fake_labeller", "args": {"key": "label"}},
            {"module": "fake_labeller", "name": "second_gate", "args": {"key": "label"}},
        ],
    )
    assert SEEN[0] == {"labeller": {"label": EPOCH}, "second_gate": {"label": EPOCH}}


def test_gates_without_annotations_contribute_nothing(mcap_path: pathlib.Path) -> None:
    _run(mcap_path, [{"module": "fake_plain"}])
    assert SEEN == [{}, {}]


def test_tasks_do_not_run_when_a_gate_fails(mcap_path: pathlib.Path) -> None:
    _run(mcap_path, [{"module": "fake_labeller", "args": {"key": "k", "passes": False}}])
    assert SEEN == []


def test_tasks_cannot_mutate_annotations(mcap_path: pathlib.Path) -> None:
    task = Recorder()
    with pytest.raises(TypeError):
        task.gate_annotations["x"] = 1  # type: ignore[index]


def test_default_gate_annotations_are_empty() -> None:
    assert PlainGate().annotations() == {}


def test_two_annotating_gates_with_the_same_name_are_rejected(mcap_path: pathlib.Path) -> None:
    # Pipeline.build does not require unique operator names; two annotating gates that
    # share one would silently overwrite each other's record (Codex P2).
    with pytest.raises(ValueError, match="labeller"):
        _run(
            mcap_path,
            [
                {"module": "fake_labeller", "args": {"key": "a"}},
                {"module": "fake_labeller", "args": {"key": "b"}},
            ],
        )


def test_tasks_cannot_alter_a_gate_record_through_nested_values(mcap_path: pathlib.Path) -> None:
    # MappingProxyType only guards the outer mapping; nested dicts must be copies so a
    # task's edits never reach the gate's own record or the next task (Codex P2).
    RECORD["probabilities"]["x"] = 0.5
    _run(mcap_path, [{"module": "fake_nested"}], tasks=[{"module": "fake_tamper"}])
    assert RECORD == {"probabilities": {"x": 0.5}}


def test_a_gate_named_asof_seconds_cannot_shadow_the_timestamp(mcap_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="asof_seconds"):
        _run(
            mcap_path,
            [{"module": "fake_labeller", "name": "asof_seconds", "args": {"key": "k"}}],
            tasks=[{"module": "src.pipeline.tasks.write_annotations"}],
        )
