"""Delivery authorization, retry and durable recovery semantics."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from src.sink.publish.applications import contract
from src.sink.publish.applications.main import Agent
from src.sink.publish.applications.runtime import DockerRuntime
from test.sink.publish.applications.test_application_runtime import BINDING, PAIR, Engine


class Channel:
    def __init__(self) -> None:
        self.jobs = []
        self.reports = []
        self.inventory = None
        self.deny = False
        self.reject_apply = False
        self.lose_report = False
        self.time = datetime.now(timezone.utc)

    def current_identity(self) -> tuple[str, str]:
        return "t1", "robot1"

    def request(self, method: str, route: str, body: dict | None = None) -> dict:
        if self.deny or (self.reject_apply and route == "report" and body["status"] == "applying"):
            status = 403 if self.deny else 409
            response = httpx.Response(
                status, request=httpx.Request(method, "https://fleet.example")
            )
            response.raise_for_status()
        if route == "next":
            return {
                "identity": {**BINDING, "robot_id": "robot1", "asset_id": "asset1"},
                "server_time": self.time.isoformat(),
                "jobs": self.jobs,
            }
        if route == "inventory":
            self.inventory = body["inventory"]
        if route == "report":
            self.reports.append(deepcopy(body))
            if self.lose_report:
                self.lose_report = False
                raise httpx.ConnectError("lost response")
        return {"accepted": True}


def provision(tmp_path: Path) -> tuple[Agent, Channel, Engine]:
    engine = Engine()
    channel = Channel()
    ready = tmp_path / "ready"
    ready.write_text("ready")
    config = {
        "applications": {
            "inspection": {
                "registry_prefixes": ["registry.example/"],
                "ready_file": str(ready),
                "health_timeout_seconds": 2,
            }
        }
    }
    agent = Agent(
        tmp_path / "agent", channel, config, lambda *args: DockerRuntime(*args, run=engine.run)
    )
    agent.tick()
    channel.jobs = [
        {
            "target_id": "target1",
            "sequence": 0,
            "application": "inspection",
            "desired": PAIR,
            "desired_digest": contract.digest(PAIR),
            "previous": None,
            "expected_inventory": contract.digest(channel.inventory),
            "expires_at": (channel.time + timedelta(hours=1)).isoformat(),
            "spec": {"runtime": "docker-application-v1", "platforms": ["linux/arm64"]},
        }
    ]
    return agent, channel, engine


def test_success_reports_health_only_after_running_container(tmp_path: Path) -> None:
    agent, channel, engine = provision(tmp_path)
    agent.tick()
    assert [r["status"] for r in channel.reports] == ["verifying", "applying", "healthy"]
    assert channel.reports[-1]["observed_digest"] == contract.digest(PAIR)
    assert len([c for c in engine.commands if c[0] == "create"]) == 1
    agent.tick()
    assert len([c for c in engine.commands if c[0] == "create"]) == 1


def test_denied_or_superseded_authorization_never_activates(tmp_path: Path) -> None:
    agent, channel, engine = provision(tmp_path)
    channel.deny = True
    with pytest.raises(httpx.HTTPStatusError):
        agent.tick()
    assert not any(c[0] == "create" for c in engine.commands)
    channel.deny = False
    channel.reject_apply = True
    with pytest.raises(httpx.HTTPStatusError):
        agent.tick()
    assert not any(c[0] == "create" for c in engine.commands)


def test_retry_keeps_report_sequence_and_body(tmp_path: Path) -> None:
    agent, channel, _ = provision(tmp_path)
    channel.lose_report = True
    with pytest.raises(httpx.ConnectError):
        agent.tick()
    first = deepcopy(channel.reports[-1])
    agent.tick()
    assert channel.reports[1] == first
    assert channel.reports[-1]["status"] == "healthy"


def test_expiry_and_changed_inventory_fail_before_application_effects(tmp_path: Path) -> None:
    agent, channel, engine = provision(tmp_path)
    channel.time += timedelta(hours=2)
    agent.tick()
    assert channel.reports[-1]["status"] == "failed"
    assert not any(c[0] == "create" for c in engine.commands)


def test_interrupted_attempt_is_not_silently_reapplied(tmp_path: Path) -> None:
    agent, channel, engine = provision(tmp_path)
    engine.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        agent.tick()
    restarted = Agent(
        agent.root, channel, agent.config, lambda *args: DockerRuntime(*args, run=engine.run)
    )
    restarted.tick()
    assert channel.reports[-1]["status"] == "failed"
    assert not any(c["State"]["Running"] for c in engine.containers.values())
    assert len([c for c in engine.commands if c[0] == "create"]) == 1


def test_restart_recovers_previous_pair_before_network_authorization(tmp_path: Path) -> None:
    agent, channel, engine = provision(tmp_path)
    agent.tick()
    previous = deepcopy(PAIR)
    desired = deepcopy(PAIR)
    desired["software"]["version"] = "2"
    channel.jobs = [
        {
            **channel.jobs[0],
            "target_id": "target2",
            "desired": desired,
            "desired_digest": contract.digest(desired),
            "previous": previous,
            "expected_inventory": contract.digest(agent.runtimes["inspection"].inventory()),
        }
    ]
    engine.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        agent.tick()
    channel.deny = True
    restarted = Agent(
        agent.root, channel, agent.config, lambda *args: DockerRuntime(*args, run=engine.run)
    )
    assert restarted.runtimes["inspection"].current() == (previous, True)
    with pytest.raises(httpx.HTTPStatusError):
        restarted.tick()
    assert sum(c["State"]["Running"] for c in engine.containers.values()) == 1
