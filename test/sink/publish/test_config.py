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


class TestChannelRule:
    def test_fields_rule_builds(self) -> None:
        r = config.ChannelRule.build(
            {"topic": "/imu", "fields": ["linear_acceleration.x"], "rate_hz": 5}
        )
        assert r.topic == "/imu"
        assert r.fields == ["linear_acceleration.x"]
        assert r.rate_hz == 5.0
        assert r.renames == {}

    def test_geo_rule_builds_with_rename(self) -> None:
        r = config.ChannelRule.build(
            {
                "topic": "/odom",
                "geo": {"lat": "pose.x", "lon": "pose.y"},
                "rate_hz": 1,
                "as": {"geo": "position"},
            }
        )
        assert r.geo == {"lat": "pose.x", "lon": "pose.y"}
        assert r.renames == {"geo": "position"}

    @pytest.mark.parametrize("rate", [0, -1, 50.001, 1000])
    def test_rate_out_of_range_raises(self, rate: int | float) -> None:
        with pytest.raises(StreamConfigError, match="rate_hz"):
            config.ChannelRule.build({"topic": "/imu", "fields": ["a"], "rate_hz": rate})

    def test_rate_of_exactly_50_is_allowed(self) -> None:
        assert (
            config.ChannelRule.build({"topic": "/t", "fields": ["a"], "rate_hz": 50}).rate_hz
            == 50.0
        )

    def test_fields_and_geo_together_raise(self) -> None:
        with pytest.raises(StreamConfigError, match="exactly one of"):
            config.ChannelRule.build(
                {"topic": "/t", "fields": ["a"], "geo": {"lat": "a", "lon": "b"}, "rate_hz": 1}
            )

    def test_neither_fields_nor_geo_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="exactly one of"):
            config.ChannelRule.build({"topic": "/t", "rate_hz": 1})

    def test_geo_missing_lon_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="lat.*lon|lon"):
            config.ChannelRule.build({"topic": "/t", "geo": {"lat": "a"}, "rate_hz": 1})

    def test_missing_topic_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="topic"):
            config.ChannelRule.build({"fields": ["a"], "rate_hz": 1})


class TestEventRule:
    def test_full_event_builds(self) -> None:
        e = config.EventRule.build(
            {
                "name": "hard_decel",
                "topic": "/imu",
                "predicate": "\"/imu\"['linear_acceleration']['x'] < -10",
                "pre_seconds": 10,
                "post_seconds": 10,
                "debounce_seconds": 2,
                "artifact": "mcap",
            }
        )
        assert e.name == "hard_decel"
        assert e.artifact == "mcap"

    def test_minimal_event_builds(self) -> None:
        e = config.EventRule.build({"name": "n", "topic": "/t", "predicate": "true"})
        assert e.pre_seconds == 0.0 and e.artifact is None

    def test_unknown_artifact_kind_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="artifact"):
            config.EventRule.build(
                {"name": "n", "topic": "/t", "predicate": "true", "artifact": "avi"}
            )

    def test_negative_window_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="pre_seconds"):
            config.EventRule.build(
                {"name": "n", "topic": "/t", "predicate": "true", "pre_seconds": -1}
            )

    def test_missing_predicate_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="predicate"):
            config.EventRule.build({"name": "n", "topic": "/t"})


class TestStreamsConfigBuild:
    def test_full_manifest_section_builds(self) -> None:
        cfg = config.StreamsConfig.build(
            {
                "broker": "mqtts://fleet.example.com:8883",
                "flush_interval_s": 2,
                "channels": [{"topic": "/imu", "fields": ["linear_acceleration.x"], "rate_hz": 5}],
                "events": [{"name": "n", "topic": "/imu", "predicate": "true"}],
            }
        )
        assert cfg.broker == "mqtts://fleet.example.com:8883"
        assert cfg.flush_interval_s == 2.0
        assert len(cfg.channels) == 1 and len(cfg.events) == 1

    def test_defaults(self) -> None:
        cfg = config.StreamsConfig.build(
            {"channels": [{"topic": "/t", "fields": ["a"], "rate_hz": 1}]}
        )
        assert cfg.broker is None and cfg.flush_interval_s == 1.0 and cfg.events == []

    @pytest.mark.parametrize("url", ["http://x", "ftp://x:1", "not a url", "mqtts://"])
    def test_bad_broker_url_raises(self, url: str) -> None:
        with pytest.raises(StreamConfigError, match="broker"):
            config.StreamsConfig.build({"broker": url, "channels": []})

    def test_plain_mqtt_scheme_is_syntactically_ok(self) -> None:
        # Whether mqtt:// is *allowed* is enrollment's dev-mode rule (step 6);
        # here it only has to parse.
        assert config.StreamsConfig.build(
            {"broker": "mqtt://localhost:1883", "channels": []}
        ).broker

    def test_zero_flush_interval_raises(self) -> None:
        with pytest.raises(StreamConfigError, match="flush_interval_s"):
            config.StreamsConfig.build({"flush_interval_s": 0, "channels": []})

    def test_error_labels_carry_list_index(self) -> None:
        with pytest.raises(StreamConfigError, match=r"channels\[1\]"):
            config.StreamsConfig.build(
                {
                    "channels": [
                        {"topic": "/a", "fields": ["x"], "rate_hz": 1},
                        {"topic": "/b", "fields": ["y"], "rate_hz": 99},
                    ]
                }
            )
