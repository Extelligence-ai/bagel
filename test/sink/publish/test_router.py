"""SampleQueue: bounded, drop-oldest, counted (spec §4).

RouterCore: extraction, rate-capping, batch-building (spec §2/§4).
"""

import importlib
import pathlib
import sys
import time

import pyarrow as pa
import pytest

from src.message.base import AccessPath
from src.sink.publish import router as router_mod
from src.sink.publish.config import ResolvedChannel
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.router import RouterCore, SampleQueue, StreamRouter, extract_value
from src.sink.publish.spool import Spool


class TestSampleQueue:
    def test_put_and_drain_fifo(self) -> None:
        q = SampleQueue(maxsize=10)
        for i in range(3):
            q.put(("/imu", float(i), {"n": i}))
        got = q.drain(max_items=10, timeout_s=0.01)
        assert [s[1] for s in got] == [0.0, 1.0, 2.0]
        assert q.depth == 0

    def test_overflow_drops_oldest_and_counts(self) -> None:
        q = SampleQueue(maxsize=3)
        for i in range(5):
            q.put(("/imu", float(i), {}))
        assert q.dropped == 2
        got = q.drain(max_items=10, timeout_s=0.01)
        assert [s[1] for s in got] == [2.0, 3.0, 4.0]

    def test_drain_respects_max_items(self) -> None:
        q = SampleQueue(maxsize=10)
        for i in range(6):
            q.put(("/t", float(i), {}))
        got = q.drain(max_items=4, timeout_s=0.01)
        assert len(got) == 4 and q.depth == 2

    def test_drain_times_out_empty(self) -> None:
        q = SampleQueue(maxsize=4)
        assert q.drain(max_items=4, timeout_s=0.01) == []

    def test_as_tap_feeds_queue(self) -> None:
        q = SampleQueue(maxsize=4)
        tap = q.as_tap()
        tap("/imu", 1.0, {"x": 1})
        assert q.depth == 1


def test_router_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.router", raising=False)
    importlib.import_module("src.sink.publish.router")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)


def _chan(
    name: str, topic: str, path: list[str], rate_hz: float, type_: str = "number"
) -> ResolvedChannel:
    return ResolvedChannel(
        name=name,
        type=type_,
        source_topic=topic,
        source_field=".".join(path),
        rate_hz=rate_hz,
        paths={"value": AccessPath(path=path, pa_type=pa.float64())},
    )


class TestExtractValue:
    def test_nested(self) -> None:
        assert extract_value({"a": {"b": 3.5}}, ["a", "b"]) == 3.5

    def test_missing_raises_keyerror(self) -> None:
        import pytest

        with pytest.raises(KeyError):
            extract_value({"a": {}}, ["a", "b"])


class TestRateCapAndBatch:
    def test_last_sample_per_interval_wins(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], rate_hz=1.0)], flush_interval_s=1.0)
        # three samples inside the same 1 Hz slot: last wins
        core.offer("/imu", 10.1, {"x": 1.0})
        core.offer("/imu", 10.5, {"x": 2.0})
        core.offer("/imu", 10.9, {"x": 3.0})
        # next slot
        core.offer("/imu", 11.2, {"x": 4.0})
        batch = core.flush(t_batch=12.0)
        assert [(s["t"], s["v"]) for s in batch["samples"]] == [(10.9, 3.0), (11.2, 4.0)]
        assert batch["v"] == 1 and "seq" not in batch

    def test_channels_from_other_topics_ignored(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], 5.0)], flush_interval_s=1.0)
        core.offer("/other", 1.0, {"x": 9.9})
        assert core.flush(2.0) is None

    def test_missing_field_skipped_and_counted(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], 5.0)], flush_interval_s=1.0)
        core.offer("/imu", 1.0, {"y": 1.0})
        assert core.skipped == 1 and core.flush(2.0) is None

    def test_geo_channel_builds_object(self) -> None:
        chan = ResolvedChannel(
            name="odom.geo",
            type="geo",
            source_topic="/odom",
            source_field="lat=a,lon=b",
            rate_hz=1.0,
            paths={
                "lat": AccessPath(path=["a"], pa_type=pa.float64()),
                "lon": AccessPath(path=["b"], pa_type=pa.float64()),
            },
        )
        core = RouterCore([chan], flush_interval_s=1.0)
        core.offer("/odom", 3.0, {"a": 37.1, "b": -122.0})
        (sample,) = core.flush(4.0)["samples"]
        assert sample["c"] == "odom.geo" and sample["v"] == {"lat": 37.1, "lon": -122.0}

    def test_flush_empties_and_orders_across_channels(self) -> None:
        core = RouterCore(
            [_chan("a.x", "/a", ["x"], 10.0), _chan("b.x", "/b", ["x"], 10.0)],
            flush_interval_s=1.0,
        )
        core.offer("/b", 2.0, {"x": 2.0})
        core.offer("/a", 1.0, {"x": 1.0})
        batch = core.flush(3.0)
        assert [s["t"] for s in batch["samples"]] == [1.0, 2.0]
        assert core.flush(4.0) is None

    def test_should_flush_on_size_or_interval(self) -> None:
        core = RouterCore([_chan("a.x", "/a", ["x"], 50.0)], flush_interval_s=1.0, max_samples=2)
        core.flush(0.0)  # set _last_flush
        assert not core.should_flush(now=0.5, pending_count=1)
        assert core.should_flush(now=0.5, pending_count=2)
        assert core.should_flush(now=1.6, pending_count=0)

    def test_late_sample_for_already_emitted_slot_is_dropped(self) -> None:
        # Slot-survival: once a (channel, slot) has been popped by flush(), a
        # later-arriving offer() for that same slot -- or an earlier one --
        # must not resurrect it into a future batch. Without this guard, a
        # reordered/late sample could re-emit stale data for a slot the
        # subscriber already received.
        core = RouterCore([_chan("imu.x", "/imu", ["x"], rate_hz=1.0)], flush_interval_s=1.0)
        core.offer("/imu", 10.9, {"x": 3.0})  # slot 10
        batch = core.flush(t_batch=11.0)
        assert [(s["t"], s["v"]) for s in batch["samples"]] == [(10.9, 3.0)]

        # A late sample lands for the same (already-emitted) slot 10.
        core.offer("/imu", 10.95, {"x": 99.0})
        assert core.pending_count == 0
        assert core.flush(t_batch=12.0) is None

        # The next slot still works normally afterward.
        core.offer("/imu", 11.4, {"x": 5.0})
        batch2 = core.flush(t_batch=13.0)
        assert [(s["t"], s["v"]) for s in batch2["samples"]] == [(11.4, 5.0)]


class FakePublisher(Publisher):
    """In-memory Publisher double: records calls, can be told to fail."""

    def __init__(
        self, *, connect_should_fail: bool = False, channel_publish_delay_s: float = 0.0
    ) -> None:
        self.connect_should_fail = connect_should_fail
        self.connect_calls = 0
        self.schema_calls: list[dict] = []
        self.channel_calls: list[dict] = []
        self._fail_channel_publishes = 0
        self._channel_publish_delay_s = channel_publish_delay_s
        self._connected = False

    def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_should_fail:
            raise PublishError("connect failed")
        self._connected = True

    def publish(
        self, kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
    ) -> None:
        if kind == "schema":
            self.schema_calls.append(payload)
        elif kind == "channels":
            if self._channel_publish_delay_s:
                time.sleep(self._channel_publish_delay_s)
            if self._fail_channel_publishes > 0:
                self._fail_channel_publishes -= 1
                raise PublishError("channels publish failed")
            self.channel_calls.append(payload)
        else:
            raise AssertionError(f"FakePublisher: unexpected kind {kind!r}")

    def close(self) -> None:
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def fail_next_channel_publishes(self, n: int) -> None:
        self._fail_channel_publishes = n


def _router(
    tmp_path: pathlib.Path,
    *,
    rate_hz: float = 1.0,
    flush_interval_s: float = 0.0,
    publisher: Publisher | None = None,
) -> tuple[StreamRouter, SampleQueue, Spool, FakePublisher]:
    core = RouterCore(
        [_chan("imu.x", "/imu", ["x"], rate_hz=rate_hz)], flush_interval_s=flush_interval_s
    )
    q = SampleQueue(maxsize=100)
    spool = Spool(tmp_path / "spool")
    pub = publisher if publisher is not None else FakePublisher()
    router = StreamRouter(core, q, spool, pub, schema_payload={"v": 1, "channels": []})
    return router, q, spool, pub


class TestStreamRouterTick:
    def test_flow_through_acked(self, tmp_path: pathlib.Path) -> None:
        router, q, spool, pub = _router(tmp_path)

        q.put(("/imu", 1.0, {"x": 1.0}))
        router._tick(now=10.0)

        assert router.online
        assert pub.connect_calls == 1
        assert pub.schema_calls == [{"v": 1, "channels": []}]
        assert len(pub.channel_calls) == 1
        assert pub.channel_calls[0]["seq"] == 1
        assert [s["v"] for s in pub.channel_calls[0]["samples"]] == [1.0]
        assert list(spool.pending("channels")) == []

    def test_offline_accumulate_and_backoff_grows(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Deterministic full jitter: always the upper bound, so next_attempt
        # is exactly now + backoff and the test can drive `now` precisely.
        monkeypatch.setattr(router_mod.random, "uniform", lambda _lo, hi: hi)
        pub = FakePublisher(connect_should_fail=True)
        router, q, spool, _ = _router(tmp_path, publisher=pub)

        q.put(("/imu", 1.0, {"x": 1.0}))
        router._tick(now=0.0)
        assert not router.online
        assert router.backoff == 2.0  # min(60, 1.0 * 2)
        assert pub.connect_calls == 1
        assert len(list(spool.pending("channels"))) == 1  # accumulated, unacked

        # Before the backoff deadline (next_attempt == 0.0 + 2.0): no retry.
        q.put(("/imu", 2.0, {"x": 2.0}))
        router._tick(now=0.1)
        assert not router.online
        assert router.backoff == 2.0
        assert pub.connect_calls == 1
        assert len(list(spool.pending("channels"))) == 2  # still accumulating offline

        # At/after the deadline: another attempt, backoff grows again.
        router._tick(now=2.0)
        assert not router.online
        assert router.backoff == 4.0  # min(60, 2.0 * 2)
        assert pub.connect_calls == 2

    def test_recover_republishes_schema_then_replays_spooled_batches_in_order(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(router_mod.random, "uniform", lambda _lo, hi: hi)
        pub = FakePublisher(connect_should_fail=True)
        router, q, spool, _ = _router(tmp_path, publisher=pub)

        q.put(("/imu", 1.0, {"x": 1.0}))
        router._tick(now=0.0)  # fails; batch seq=1 spooled while offline
        assert not router.online and router.backoff == 2.0

        q.put(("/imu", 2.0, {"x": 2.0}))
        router._tick(now=0.1)  # before deadline (2.0): no reconnect attempt
        assert not router.online and pub.connect_calls == 1

        pub.connect_should_fail = False
        q.put(("/imu", 3.0, {"x": 3.0}))
        router._tick(now=2.0)  # deadline reached: recovers, then replays + the live one

        assert router.online
        assert router.backoff == 1.0  # reset on successful reconnect
        assert pub.schema_calls == [{"v": 1, "channels": []}]
        assert [c["seq"] for c in pub.channel_calls] == [1, 2, 3]
        assert [s["v"] for c in pub.channel_calls for s in c["samples"]] == [1.0, 2.0, 3.0]
        assert list(spool.pending("channels")) == []

    def test_publish_failure_mid_replay_stops_and_reschedules(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A failure partway through spool.pending() must not spin: the router
        # goes offline immediately, leaving later seqs unacked for next time.
        monkeypatch.setattr(router_mod.random, "uniform", lambda _lo, hi: hi)
        pub = FakePublisher()
        router, q, spool, _ = _router(tmp_path, publisher=pub)

        q.put(("/imu", 1.0, {"x": 1.0}))
        router._tick(now=0.0)  # online, seq=1 published+acked
        assert router.online and pub.channel_calls[-1]["seq"] == 1

        pub.fail_next_channel_publishes(1)
        q.put(("/imu", 2.0, {"x": 2.0}))
        router._tick(now=1.0)  # seq=2 spooled, then publish fails -> offline
        assert not router.online
        assert router.backoff == 2.0
        assert [seq for seq, _ in spool.pending("channels")] == [2]

        router._tick(now=1.0)  # before deadline (1.0 + 2.0 = 3.0): no retry, no spin
        assert pub.connect_calls == 1  # unchanged: still the initial connect from tick 1
        assert not router.online

        router._tick(now=3.0)  # deadline reached: recovers and replays seq 2
        assert router.online
        assert [c["seq"] for c in pub.channel_calls] == [1, 2]
        assert list(spool.pending("channels")) == []

    def test_pump_checks_stop_event_between_replay_iterations(self, tmp_path: pathlib.Path) -> None:
        # A post-outage backlog can hold thousands of records (the channels
        # lane is size-capped, not count-capped); _pump must notice
        # self._stop_event mid-replay rather than draining the whole
        # backlog first.
        router, _q, spool, pub = _router(tmp_path)
        for seq in range(1, 6):
            spool.append("channels", seq, {"seq": seq, "v": 1, "samples": []})
        router._online = True  # skip _reconnect(); exercise the replay loop directly

        publish = pub.publish

        def publish_then_stop_after_two(
            kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
        ) -> None:
            publish(kind, payload, retain=retain, timeout_s=timeout_s)
            if kind == "channels" and len(pub.channel_calls) == 2:
                router._stop_event.set()

        pub.publish = publish_then_stop_after_two  # type: ignore[method-assign]

        router._pump(now=0.0)

        assert [c["seq"] for c in pub.channel_calls] == [1, 2]
        # The loop broke before seq 3: unacked, still spooled, correct watermark.
        assert [seq for seq, _ in spool.pending("channels")] == [3, 4, 5]

    def test_pump_returns_immediately_when_stop_event_already_set(
        self, tmp_path: pathlib.Path
    ) -> None:
        # Narrows the stop/reconnect race: if stop() lands while a blocked
        # connect() is in flight, _pump must not fall through into a full
        # publish pass once that connect() finally resolves and _online
        # flips true -- it should bail out before ever calling _reconnect()
        # or publishing anything.
        router, _q, spool, pub = _router(tmp_path)
        spool.append("channels", 1, {"seq": 1, "v": 1, "samples": []})
        router._stop_event.set()

        router._pump(now=0.0)

        assert pub.connect_calls == 0
        assert pub.channel_calls == []
        assert not router.online
        assert [seq for seq, _ in spool.pending("channels")] == [1]


class TestStreamRouterThread:
    def test_stop_joins_promptly(self, tmp_path: pathlib.Path) -> None:
        router, _q, _spool, _pub = _router(tmp_path, flush_interval_s=1.0)
        router.start()
        time.sleep(0.05)

        start = time.monotonic()
        router.stop()
        elapsed = time.monotonic() - start

        assert not router.is_alive()
        assert elapsed < 2.0

    def test_stop_joins_promptly_mid_large_replay_backlog(self, tmp_path: pathlib.Path) -> None:
        # Regression for the normal-path violation: a large post-outage
        # backlog draining slowly must still let stop() honor its
        # join(timeout=5) bound, not just the connect()-blocking edge case.
        publish_delay_s = 0.05
        total_records = 40
        pub = FakePublisher(channel_publish_delay_s=publish_delay_s)
        router, _q, spool, _ = _router(tmp_path, publisher=pub)
        for seq in range(1, total_records + 1):
            spool.append("channels", seq, {"seq": seq, "v": 1, "samples": []})

        router.start()
        time.sleep(publish_delay_s * 3)  # a handful of records go out, nowhere near all 40

        start = time.monotonic()
        router.stop()
        elapsed = time.monotonic() - start

        assert not router.is_alive()
        assert elapsed < 5.0
        remaining = [seq for seq, _ in spool.pending("channels")]
        assert remaining  # stopped mid-backlog: work was left for next start
        assert len(remaining) < total_records
        # The unacked tail is a contiguous run up to total_records -- no gaps,
        # no out-of-order acks.
        assert remaining == list(range(remaining[0], total_records + 1))


class TestStreamRouterStopFlushesAcceptedSamples:
    def _start_and_let_initial_tick_pass(self, router: StreamRouter) -> None:
        """Start the router and wait past its first tick.

        `RouterCore._last_flush` starts at 0.0, and `run()` drives `_tick`
        with a real `time.time()` -- so the very first tick's
        `should_flush()` is unconditionally True (any real epoch timestamp
        minus 0.0 dwarfs `flush_interval_s`), flushing an (empty) batch and
        setting `_last_flush` to a real recent timestamp. Only after that
        can a *subsequent* sample reliably sit in `_pending` without
        crossing a long `flush_interval_s`'s boundary.
        """
        router.start()
        time.sleep(0.3)

    def test_stop_spools_samples_offered_but_below_flush_threshold(
        self, tmp_path: pathlib.Path
    ) -> None:
        # Codex review (3906982911): stop() used to only set the stop event
        # and join -- it never forced the queue/core's already-accepted
        # samples into the spool first, so a sample the tap had accepted
        # (offer()'d into RouterCore._pending) but that hadn't yet crossed a
        # flush boundary was silently dropped on stop(). A long
        # flush_interval_s (10s) guarantees should_flush() would say "not
        # yet" on any tick that happened to run before stop() lands.
        router, q, spool, pub = _router(tmp_path, flush_interval_s=10.0)
        self._start_and_let_initial_tick_pass(router)

        q.put(("/imu", 1.0, {"x": 1.0}))
        deadline = time.monotonic() + 2.0
        while router._core.pending_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert router._core.pending_count == 1  # accepted by offer(), not yet flushed

        router.stop()

        assert list(spool.pending("channels"))  # the accepted sample was spooled, not lost
        batch = next(payload for _seq, payload in spool.pending("channels"))
        assert [s["v"] for s in batch["samples"]] == [1.0]

    def test_stop_on_an_idle_router_with_nothing_pending_is_a_clean_noop(
        self, tmp_path: pathlib.Path
    ) -> None:
        router, _q, spool, _pub = _router(tmp_path, flush_interval_s=10.0)
        self._start_and_let_initial_tick_pass(router)

        router.stop()  # must not raise even with nothing to flush

        assert list(spool.pending("channels")) == []

    def test_pause_discard_still_discards_after_stop_flushes_pending(
        self, tmp_path: pathlib.Path
    ) -> None:
        # pause(discard=True) is FleetService's job (it acks past the
        # channels lane's last written seq after stop()) -- this just
        # documents that stop()'s new flush-on-stop doesn't fight that: the
        # newly-spooled batch is still ack-able/discardable like any other.
        router, q, spool, _pub = _router(tmp_path, flush_interval_s=10.0)
        self._start_and_let_initial_tick_pass(router)
        q.put(("/imu", 1.0, {"x": 1.0}))
        deadline = time.monotonic() + 2.0
        while router._core.pending_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)

        router.stop()
        assert list(spool.pending("channels"))  # flushed on stop

        last_seq = spool.next_seq("channels") - 1
        spool.ack("channels", last_seq)
        assert list(spool.pending("channels")) == []  # discard still works


class TestStreamRouterStopResetsOnline:
    def test_stop_resets_online_to_false(self, tmp_path: pathlib.Path) -> None:
        # Codex review (3906982938): StreamRouter._online was never reset on
        # stop(), so FleetService.status() kept reporting online: true for a
        # router thread that had already cleanly stopped.
        router, q, _spool, pub = _router(tmp_path, flush_interval_s=0.01)
        router.start()
        q.put(("/imu", 1.0, {"x": 1.0}))
        deadline = time.monotonic() + 2.0
        while not router.online and time.monotonic() < deadline:
            time.sleep(0.01)
        assert router.online  # sanity: actually connected before stop()

        router.stop()

        assert router.online is False


class TestStreamRouterStopJoinTimeout:
    def test_stop_joins_with_timeout_longer_than_publish_wait(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3906982943): stop()'s join(timeout=5) can't outlast a
        # 10s QoS-1 publish wait (wait_for_publish(timeout=10.0) inside
        # MqttPublisher.publish), so termination wasn't actually guaranteed
        # before a caller (e.g. pause()/resume()) proceeded. The join timeout
        # must exceed that 10s bound.
        router, _q, _spool, _pub = _router(tmp_path, flush_interval_s=1.0)

        join_calls: list[float | None] = []
        real_join = router.join

        def spy_join(timeout: float | None = None) -> None:
            join_calls.append(timeout)
            real_join(timeout)

        monkeypatch.setattr(router, "join", spy_join)

        router.start()
        router.stop()

        assert join_calls
        assert join_calls[0] is not None and join_calls[0] > 10.0

    def test_stop_logs_warning_when_thread_does_not_terminate(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        router, _q, _spool, _pub = _router(tmp_path, flush_interval_s=1.0)
        # Don't actually start the thread -- is_alive() is always False for a
        # never-started Thread, which is the wrong shape for this test. Fake
        # is_alive() to simulate a stuck thread that survives the join.
        monkeypatch.setattr(router, "is_alive", lambda: True)
        monkeypatch.setattr(router, "join", lambda timeout=None: None)

        with caplog.at_level(logging.WARNING):
            router.stop()

        warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("did not terminate" in r.getMessage() for r in warnings_)


class TestStreamRouterDeathIsVisible:
    """A bug in core/spool (not a PublishError) must not die silently.

    run() wraps _tick in a broad except: log, record _fatal_error, exit the
    loop. `alive` (thread running AND no fatal error) and `last_error` make
    this visible to FleetService.status() instead of leaving a router that
    looks merely "not yet reconnected" forever.
    """

    def test_non_publish_error_kills_thread_and_is_visible(self, tmp_path: pathlib.Path) -> None:
        router, q, _spool, _pub = _router(tmp_path, flush_interval_s=1.0)

        def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("core exploded")

        router._core.offer = boom  # type: ignore[method-assign]
        q.put(("/imu", 1.0, {"x": 1.0}))

        assert router.alive is False  # never started yet: not alive, but no error either
        assert router.last_error is None

        router.start()
        deadline = time.monotonic() + 2.0
        while router.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)

        assert not router.is_alive()
        assert router.alive is False
        assert router.last_error is not None
        assert "core exploded" in router.last_error

        # stop() on an already-dead thread must not raise (no exception escapes).
        router.stop()
        assert not router.is_alive()
