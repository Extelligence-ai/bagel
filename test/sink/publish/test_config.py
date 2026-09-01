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

ODOM = pa.struct(
    [
        pa.field(
            "pose",
            pa.struct(
                [
                    pa.field("x", pa.float64()),
                    pa.field("y", pa.float64()),
                    pa.field("z", pa.float64()),
                ]
            ),
        )
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

    def test_unknown_top_level_key_raises(self) -> None:
        with pytest.raises(StreamConfigError, match=r"unknown keys \['az'\]"):
            config.ChannelRule.build(
                {"topic": "/t", "fields": ["a"], "rate_hz": 1, "az": {"lat": "a", "lon": "b"}}
            )


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

    def test_unknown_top_level_key_raises(self) -> None:
        with pytest.raises(StreamConfigError, match=r"unknown keys \['severity'\]"):
            config.EventRule.build(
                {"name": "n", "topic": "/t", "predicate": "true", "severity": "high"}
            )


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

    def test_event_error_labels_carry_list_index(self) -> None:
        with pytest.raises(StreamConfigError, match=r"events\[1\]"):
            config.StreamsConfig.build(
                {
                    "channels": [],
                    "events": [
                        {"name": "a", "topic": "/t", "predicate": "true"},
                        {"name": "b", "topic": "/t", "predicate": "true", "pre_seconds": -1},
                    ],
                }
            )

    def test_null_channels_is_treated_as_empty(self) -> None:
        cfg = config.StreamsConfig.build({"channels": None})
        assert cfg.channels == []

    def test_null_events_is_treated_as_empty(self) -> None:
        cfg = config.StreamsConfig.build({"channels": [], "events": None})
        assert cfg.events == []

    def test_non_list_channels_raises_typed(self) -> None:
        with pytest.raises(StreamConfigError, match="streams.channels"):
            config.StreamsConfig.build({"channels": "x"})

    def test_non_list_events_raises_typed(self) -> None:
        with pytest.raises(StreamConfigError, match="streams.events"):
            config.StreamsConfig.build({"channels": [], "events": {"a": 1}})


class TestResolve:
    def _cfg(self, channels: list) -> config.StreamsConfig:
        return config.StreamsConfig.build({"channels": channels})

    def test_scalar_channels_resolve_with_default_names(self) -> None:
        cfg = self._cfg(
            [{"topic": "/imu", "fields": ["linear_acceleration.x", "frame_id"], "rate_hz": 5}]
        )
        out = cfg.resolve({"/imu": IMU})
        assert [(c.name, c.type) for c in out] == [
            ("imu.linear_acceleration.x", "number"),
            ("imu.frame_id", "string"),
        ]
        assert out[0].source_topic == "/imu"
        assert out[0].source_field == "linear_acceleration.x"
        assert out[0].paths["value"].path == ["linear_acceleration", "x"]
        assert out[0].rate_hz == 5.0

    def test_rename_overrides_default(self) -> None:
        cfg = self._cfg(
            [
                {
                    "topic": "/imu",
                    "fields": ["linear_acceleration.x"],
                    "rate_hz": 5,
                    "as": {"linear_acceleration.x": "accel.x"},
                }
            ]
        )
        assert cfg.resolve({"/imu": IMU})[0].name == "accel.x"

    def test_geo_channel_resolves(self) -> None:
        cfg = self._cfg(
            [{"topic": "/nav/odom", "geo": {"lat": "pose.x", "lon": "pose.y"}, "rate_hz": 1}]
        )
        (c,) = cfg.resolve({"/nav/odom": ODOM})
        assert c.name == "odom.geo" and c.type == "geo"
        assert set(c.paths) == {"lat", "lon"}
        assert c.rate_hz == 1.0

    def test_geo_channel_with_alt_resolves(self) -> None:
        cfg = self._cfg(
            [
                {
                    "topic": "/nav/odom",
                    "geo": {"lat": "pose.x", "lon": "pose.y", "alt": "pose.z"},
                    "rate_hz": 1,
                }
            ]
        )
        (c,) = cfg.resolve({"/nav/odom": ODOM})
        assert set(c.paths) == {"lat", "lon", "alt"}
        assert c.type == "geo"

    def test_geo_path_to_non_number_raises(self) -> None:
        cfg = self._cfg(
            [{"topic": "/imu", "geo": {"lat": "frame_id", "lon": "calibrated"}, "rate_hz": 1}]
        )
        with pytest.raises(StreamConfigError, match="geo.*number|number"):
            cfg.resolve({"/imu": IMU})

    def test_non_scalar_field_raises(self) -> None:
        cfg = self._cfg([{"topic": "/imu", "fields": ["readings"], "rate_hz": 1}])
        with pytest.raises(StreamConfigError, match="not a streamable scalar"):
            cfg.resolve({"/imu": IMU})

    def test_unknown_topic_raises(self) -> None:
        cfg = self._cfg([{"topic": "/nope", "fields": ["a"], "rate_hz": 1}])
        with pytest.raises(StreamConfigError, match="unknown topic"):
            cfg.resolve({"/imu": IMU})

    def test_duplicate_channel_names_raise(self) -> None:
        cfg = self._cfg(
            [
                {"topic": "/imu", "fields": ["frame_id"], "rate_hz": 1},
                {
                    "topic": "/imu",
                    "fields": ["linear_acceleration.x"],
                    "rate_hz": 1,
                    "as": {"linear_acceleration.x": "imu.frame_id"},
                },
            ]
        )
        with pytest.raises(StreamConfigError, match="duplicate channel name"):
            cfg.resolve({"/imu": IMU})

    def test_duplicate_default_names_raise(self) -> None:
        cfg = self._cfg(
            [
                {"topic": "/imu", "fields": ["frame_id"], "rate_hz": 1},
                {"topic": "/imu", "fields": ["frame_id"], "rate_hz": 2},
            ]
        )
        with pytest.raises(StreamConfigError, match="duplicate channel name"):
            cfg.resolve({"/imu": IMU})

    def test_typo_rename_key_raises(self) -> None:
        cfg = self._cfg(
            [
                {
                    "topic": "/imu",
                    "fields": ["frame_id"],
                    "rate_hz": 1,
                    "as": {"frme_id": "n"},
                }
            ]
        )
        with pytest.raises(StreamConfigError, match="matches no field path"):
            cfg.resolve({"/imu": IMU})

    def test_typo_geo_rename_key_raises(self) -> None:
        cfg = self._cfg(
            [
                {
                    "topic": "/nav/odom",
                    "geo": {"lat": "pose.x", "lon": "pose.y"},
                    "rate_hz": 1,
                    "as": {"gep": "position"},
                }
            ]
        )
        with pytest.raises(StreamConfigError, match="matches no field path"):
            cfg.resolve({"/nav/odom": ODOM})


class TestLoadStreams:
    def test_manifest_without_streams_returns_none(self) -> None:
        assert config.load_streams({"subscriptions": []}) is None

    def test_manifest_with_streams_builds(self) -> None:
        manifest = {
            "subscriptions": [],
            "streams": {"channels": [{"topic": "/imu", "fields": ["frame_id"], "rate_hz": 1}]},
        }
        cfg = config.load_streams(manifest)
        assert isinstance(cfg, config.StreamsConfig) and len(cfg.channels) == 1

    def test_yaml_round_trip(self, tmp_path: object) -> None:
        import yaml

        text = """
streams:
  broker: mqtts://fleet.example.com:8883
  flush_interval_s: 1
  channels:
    - topic: /imu
      fields: [linear_acceleration.x, linear_acceleration.y]
      rate_hz: 5
  events:
    - name: hard_decel
      topic: /imu
      predicate: '"/imu"[''linear_acceleration''][''x''] < -10'
      pre_seconds: 10
      post_seconds: 10
      debounce_seconds: 2
      artifact: mcap
"""
        f = tmp_path / "startup.yaml"
        f.write_text(text)
        cfg = config.load_streams(yaml.safe_load(f.read_text()))
        assert cfg.broker.startswith("mqtts://")
        assert cfg.channels[0].rate_hz == 5.0
        assert cfg.events[0].name == "hard_decel"

    def test_invalid_streams_raises_not_swallows(self) -> None:
        with pytest.raises(StreamConfigError):
            config.load_streams({"streams": {"channels": [{"topic": "/t", "rate_hz": 1}]}})
