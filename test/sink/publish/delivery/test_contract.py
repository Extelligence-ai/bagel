"""Fleet service interoperability vectors and restricted capture inputs."""

import json
from pathlib import Path

import pytest

from src.sink.publish.delivery import contract

VECTORS = json.loads(Path(__file__).with_name("contract-v1.json").read_text())


@pytest.mark.parametrize("vector", VECTORS)
def test_protocol_vectors(vector: dict) -> None:
    spec = contract.validate(vector["spec"])
    assert contract.canonical(spec) == vector["canonical"]
    assert contract.digest(spec) == vector["sha256"]
    native = contract.native(spec, "/local/buffer", "asset-1")
    assert native["tasks"][0]["module"] == "src.pipeline.tasks.write_topics_to_file"
    assert native["tasks"][0]["upload"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"module": "os"},
        {"source": "http://remote"},
        {"topics": ["#"]},
        {"lookback_seconds": True},
        {"trigger": {**VECTORS[0]["spec"]["trigger"], "field": ["x'); DROP TABLE robots;--"]}},
        {"trigger": {**VECTORS[0]["spec"]["trigger"], "value": float("nan")}},
        {"trigger": {**VECTORS[0]["spec"]["trigger"], "value": 10**400}},
    ],
)
def test_rejects_unbounded_or_executable_inputs(change: dict) -> None:
    with pytest.raises(ValueError):
        contract.validate({**VECTORS[0]["spec"], **change})
