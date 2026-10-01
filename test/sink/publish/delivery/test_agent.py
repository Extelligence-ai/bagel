from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from src.sink.publish.delivery import contract
from src.sink.publish.delivery.main import Agent, SupersededError
from src.sink.publish.delivery.runtime import RuntimeFailureError

SPEC = dict(
    schema=contract.SCHEMA,
    name="capture",
    source="sensors",
    topics=["temperature"],
    trigger=dict(kind="periodic", topic="temperature", every_seconds=5),
    lookback_seconds=10,
    output_format="csv",
)


class Channel:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.identity = dict(
            tenant_id="t1",
            robot_id="r1",
            asset_id="a1",
            installation_id="i1",
            credential_generation=1,
        )
        self.job = dict(
            **self.identity,
            target_id="target1",
            revision=1,
            last_sequence=0,
            spec=SPEC,
            digest=contract.digest(SPEC),
            runtime=contract.RUNTIME,
            expires_at=(now + timedelta(hours=1)).isoformat(),
        )
        self.reports = []
        self.fail_status = None
        self.code = 500
        self.polls = 0
        self.supersede = False

    def current_identity(self) -> tuple[str, str]:
        return "t1", "r1"

    def request(self, method: str, route: str, body: dict | None = None) -> dict:
        if method == "GET":
            self.polls += 1
            if self.supersede and self.polls > 1:
                return dict(identity=self.identity, job=None)
            return dict(
                identity=self.identity,
                job=deepcopy(self.job),
                server_time=datetime.now(timezone.utc).isoformat(),
            )
        if route == "inventory":
            self.inventory = deepcopy(body)
            return {"accepted": True}
        if body["status"] == self.fail_status:
            raise httpx.HTTPStatusError(
                "lost response",
                request=httpx.Request("POST", "https://control/report"),
                response=httpx.Response(self.code),
            )
        self.reports.append(deepcopy(body))
        self.job["last_sequence"] = body["sequence"]
        return {"accepted": True}


class Runtime:
    digest = None

    def __init__(self) -> None:
        self.execution = {}
        self.calls = []
        self.fail = False
        self.prepared_stopped = False

    def inventory(self) -> dict:
        return {
            "protocol": 2,
            "runtime": contract.RUNTIME,
            "sources": {"sensors": ["temperature"]},
            "current_digest": self.digest,
        }

    def prepare(self, job: dict) -> Runtime:
        self.calls.append("prepare")
        if self.fail:
            raise RuntimeFailureError("unavailable topic")
        return self

    def activate(self, prepared: object) -> None:
        self.calls.append("activate")
        self.digest = contract.digest(SPEC)

    def restore(self, job: dict) -> None:
        self.calls.append("restore")
        self.digest = job["digest"]

    def stop(self) -> None:
        self.prepared_stopped = True


def test_ack_only_after_activation_retry_lost_ack_and_restart(tmp_path: Path) -> None:
    channel, runtime = Channel(), Runtime()
    channel.fail_status = "active"
    agent = Agent(tmp_path, channel, runtime)
    with pytest.raises(httpx.HTTPStatusError):
        agent.tick()
    assert runtime.calls == ["prepare", "activate"]
    assert agent.state["active"]["digest"] == runtime.digest
    assert agent.state["pending_report"]["status"] == "active"
    channel.fail_status = None
    agent.tick()
    assert runtime.calls == ["prepare", "activate"]
    assert channel.reports[-1]["status"] == "active"
    restarted = Runtime()
    Agent(tmp_path, channel, restarted).restore()
    assert restarted.digest == runtime.digest and restarted.calls == ["restore"]


@pytest.mark.parametrize(
    "case",
    [
        "bad_digest",
        "expired",
        "foreign_generation",
        "superseded",
        "preparation_failure",
        "report_conflict",
        "offline",
    ],
)
def test_invalid_or_unconfirmed_delivery_never_activates(tmp_path: Path, case: str) -> None:
    channel, runtime = Channel(), Runtime()
    runtime.digest = "previous"
    if case == "bad_digest":
        channel.job["digest"] = "bad"
    if case == "expired":
        channel.job["expires_at"] = "2000-01-01T00:00:00+00:00"
    if case == "foreign_generation":
        channel.job["credential_generation"] = 99
    if case == "superseded":
        channel.supersede = True
    if case == "preparation_failure":
        runtime.fail = True
    if case in ("report_conflict", "offline"):
        channel.fail_status = "applying"
        channel.code = 409 if case == "report_conflict" else 503
    agent = Agent(tmp_path, channel, runtime)
    if case in ("foreign_generation", "report_conflict", "offline"):
        with pytest.raises((RuntimeFailureError, SupersededError, httpx.HTTPStatusError)):
            agent.tick()
    else:
        agent.tick()
    assert "activate" not in runtime.calls
    assert runtime.digest == "previous"
    assert not any(r["status"] == "active" for r in channel.reports)
    if case in ("report_conflict", "offline", "superseded"):
        assert runtime.prepared_stopped
    if case == "preparation_failure":
        agent.tick()
        assert runtime.calls == ["prepare"]  # no endless retry of known bad release


def test_interrupted_pending_activation_restores_only_prior_durable_active(tmp_path: Path) -> None:
    channel, runtime = Channel(), Runtime()
    agent = Agent(tmp_path, channel, runtime)
    prior = {**channel.job, "target_id": "prior", "revision": 1}
    agent.state.update(active=prior, desired={**channel.job, "target_id": "pending", "revision": 2})
    agent.save()
    restarted = Agent(tmp_path, channel, runtime)
    restarted.restore()
    assert runtime.calls == ["restore"] and runtime.digest == prior["digest"]
    # Changing controller identity must not restore the prior controller's journal.
    channel.current_identity = lambda: ("t1", "replacement")
    runtime.calls.clear()
    restarted.restore()
    assert runtime.calls == []


def test_failed_activation_restores_previous_worker_and_cleans_failed_restore(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from src.sink.publish.delivery.runtime import BagelRuntime

    class Worker:
        def __init__(self, name: str, fail: bool = False) -> None:
            self.job, self.fail, self.stopped = {"digest": name}, fail, False

        def stop(self) -> None:
            self.stopped = True

        def activate(self) -> None:
            if self.fail:
                raise RuntimeFailureError("activation interrupted")

    old, broken, restored = Worker("old"), Worker("new", True), Worker("old")
    runtime = BagelRuntime(tmp_path, tmp_path / "sources.json")
    runtime.worker = old
    monkeypatch.setattr(runtime, "prepare", lambda job: restored)
    with pytest.raises(RuntimeFailureError):
        runtime.activate(broken)
    assert old.stopped and broken.stopped and runtime.worker is restored
    failed_restore = Worker("old", True)
    monkeypatch.setattr(runtime, "prepare", lambda job: failed_restore)
    with pytest.raises(RuntimeFailureError):
        runtime.activate(Worker("new", True))
    assert failed_restore.stopped and runtime.worker is None


def test_gateway_denial_is_durable_across_restart(tmp_path: Path) -> None:
    channel, runtime = Channel(), Runtime()
    agent = Agent(tmp_path, channel, runtime)
    agent.tick()
    agent.deny()
    recovered = Runtime()
    restarted = Agent(tmp_path, channel, recovered)
    restarted.restore()
    assert recovered.calls == []
    assert restarted.state["credential_denied"]


def test_stop_admission_and_restart_never_resurrect_capture(tmp_path: Path) -> None:
    channel, runtime = Channel(), Runtime()
    agent = Agent(tmp_path, channel, runtime)
    agent.tick()
    channel.job.update(target_id="stop1", revision=2, operation="stop")
    channel.fail_status, channel.code = "applying", 409
    with pytest.raises(SupersededError):
        agent.tick()
    assert not runtime.prepared_stopped
    channel.fail_status = None
    agent.tick()
    assert runtime.prepared_stopped
    assert channel.reports[-1]["status"] == "stopped"
    restarted = Runtime()
    Agent(tmp_path, channel, restarted).restore()
    assert restarted.calls == []


def test_same_digest_new_revision_needs_admission(tmp_path: Path) -> None:
    channel, runtime = Channel(), Runtime()
    agent = Agent(tmp_path, channel, runtime)
    agent.tick()
    channel.job.update(target_id="new", revision=2)
    channel.fail_status, channel.code = "applying", 409
    with pytest.raises(SupersededError):
        agent.tick()
    assert agent.state["active"]["target_id"] == "target1"


def test_inventory_contains_only_allowlisted_source_metadata(tmp_path: Path) -> None:
    import json

    from src.sink.publish.delivery.runtime import BagelRuntime

    sources = tmp_path / "sources.json"
    sources.write_text(
        json.dumps(
            {
                "sources": {
                    "sensors": {
                        "sink": "mqtt",
                        "host": "secret",
                        "port": 1883,
                        "topics": ["temperature"],
                        "args": {"password": "secret"},
                    },
                    "unsupported": {"sink": "other"},
                }
            }
        )
    )
    result = BagelRuntime(tmp_path, sources).inventory()
    assert result == {
        "protocol": 2,
        "runtime": contract.RUNTIME,
        "sources": {"sensors": ["temperature"]},
        "current_digest": None,
    }
