"""Streams config: typed validation of the manifest's `streams:` section.

Load-time failures are StreamConfigError(field, reason). Predicate strings
are deliberately NOT validated here (SQL predicates surface errors at first
evaluation, matching src/pipeline/gates/sql.py convention).
"""

import pyarrow as pa
import pytest

from src.sink.publish import StreamConfigError, config

IMU = pa.struct(
    [
        pa.field(
            "linear_acceleration",
            pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.float64())]),
        ),
        pa.field("frame_id", pa.string()),
        pa.field("calibrated", pa.bool_()),
        pa.field("readings", pa.list_(pa.float64())),
    ]
)


class TestStreamConfigError:
    def test_carries_field_and_reason(self) -> None:
        e = StreamConfigError("channels[0].fields", "unknown field 'z'")
        assert e.field == "channels[0].fields"
        assert e.reason == "unknown field 'z'"
        assert str(e) == "streams config: channels[0].fields: unknown field 'z'"


class TestResolvePath:
    def test_nested_scalar_resolves(self) -> None:
        ap = config.resolve_path(IMU, "linear_acceleration.x", field_label="f")
        assert ap.path == ["linear_acceleration", "x"]
        assert ap.pa_type == pa.float64()

    def test_top_level_scalar_resolves(self) -> None:
        ap = config.resolve_path(IMU, "frame_id", field_label="f")
        assert ap.path == ["frame_id"]
        assert ap.pa_type == pa.string()

    def test_unknown_segment_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="unknown field 'z'"):
            config.resolve_path(IMU, "linear_acceleration.z", field_label="channels[0].fields")

    def test_traversing_through_scalar_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="not a struct"):
            config.resolve_path(IMU, "frame_id.sub", field_label="f")


class TestClassify:
    @pytest.mark.parametrize(
        ("pa_type", "expected"),
        [
            (pa.float64(), "number"),
            (pa.float32(), "number"),
            (pa.int32(), "number"),
            (pa.uint8(), "number"),
            (pa.bool_(), "bool"),
            (pa.string(), "string"),
            (pa.large_string(), "string"),
        ],
    )
    def test_scalars(self, pa_type: pa.DataType, expected: str) -> None:
        assert config.classify(pa_type) == expected

    @pytest.mark.parametrize(
        "pa_type",
        [
            pa.list_(pa.float64()),
            pa.struct([pa.field("a", pa.int8())]),
            pa.binary(),
            pa.timestamp("us"),
        ],
    )
    def test_non_scalars_raise(self, pa_type: pa.DataType) -> None:
        with pytest.raises(ValueError, match="not a streamable scalar"):
            config.classify(pa_type)
