"""Recovery invariants for the opt-in application installer."""

from copy import deepcopy
from pathlib import Path

import pytest

from src.sink.publish.applications import contract
from src.sink.publish.applications.runtime import DockerRuntime

IMAGE = "registry.example/app@sha256:" + "a" * 64
PAIR = {"software": {"version": "1", "image": IMAGE, "model_mode": "bundled"}, "model": None}
BINDING = {"tenant_id": "t1", "installation_id": "i1", "credential_generation": 1}


class Engine:
    def __init__(self) -> None:
        self.containers = {}
        self.commands = []
        self.unhealthy = False
        self.interrupt = False

    def run(self, args: list[str], timeout: int = 300) -> bytes:  # noqa: C901, PLR0911 -- fake Docker command dispatcher
        import json

        self.commands.append(args)
        if args[0] == "info":
            return b"linux/arm64"
        if args[0] == "pull":
            return b""
        if args[:2] == ["image", "inspect"]:
            return json.dumps(
                [{"RepoDigests": [args[2]], "Config": {"Healthcheck": {"Test": ["CMD", "true"]}}}]
            ).encode()
        if args[:2] == ["container", "ls"]:
            return "\n".join(self.containers).encode()
        if args[:2] == ["container", "inspect"]:
            if args[2] not in self.containers:
                raise RuntimeError("missing container")
            return json.dumps([self.containers[args[2]]]).encode()
        if args[0] == "create":
            name = args[args.index("--name") + 1]
            sha = next(a.split("=", 1)[1] for a in args if a.startswith("bagel.release="))
            self.containers[name] = {
                "Config": {"Image": args[-1], "Labels": {"bagel.release": sha}},
                "State": {"Running": False, "Health": {"Status": "starting"}},
            }
            return name.encode()
        name = args[-1]
        if args[0] == "start":
            self.containers[name]["State"] = {
                "Running": True,
                "Health": {"Status": "unhealthy" if self.unhealthy else "healthy"},
            }
            if self.interrupt:
                self.interrupt = False
                raise KeyboardInterrupt("power loss")
            self.unhealthy = False
        if args[0] == "stop":
            self.containers[name]["State"]["Running"] = False
        if args[0] == "rm":
            self.containers.pop(name)
        return b""


def runtime(tmp_path: Path, engine: Engine) -> DockerRuntime:
    ready = tmp_path / "ready"
    ready.write_text("ready")
    return DockerRuntime(
        tmp_path / "runtime",
        "inspection",
        {
            "registry_prefixes": ["registry.example/"],
            "ready_file": str(ready),
            "health_timeout_seconds": 2,
        },
        BINDING,
        engine.run,
    )


def test_activation_reports_real_health_and_persists(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    assert app.inventory()["current"] is None
    prepared = app.prepare(PAIR, "t1")
    app.activate(prepared)
    assert app.current() == (PAIR, True)
    assert runtime(tmp_path, engine).current() == (PAIR, True)
    create = next(c for c in engine.commands if c[0] == "create")
    assert "--read-only" in create and "--cap-drop" in create
    assert "/var/run/docker.sock" not in " ".join(create)


@pytest.mark.parametrize("interruption", [False, True])
def test_failure_or_interruption_restores_previous_healthy_pair(
    tmp_path: Path, interruption: bool
) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    app.activate(app.prepare(PAIR, "first"))
    next_pair = deepcopy(PAIR)
    next_pair["software"]["version"] = "2"
    prepared = app.prepare(next_pair, "second")
    engine.interrupt = interruption
    engine.unhealthy = not interruption
    with pytest.raises((RuntimeError, KeyboardInterrupt)):
        app.activate(prepared)
    recovered = runtime(tmp_path, engine)
    assert recovered.current() == (PAIR, True)
    assert "transition" not in recovered.state
    assert sum(c["State"]["Running"] for c in engine.containers.values()) == 1


def test_readiness_and_allowlist_reject_without_stopping_application(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    app.activate(app.prepare(PAIR, "first"))
    pair = deepcopy(PAIR)
    pair["software"]["image"] = "evil.example/app@sha256:" + "b" * 64
    with pytest.raises(ValueError, match="approved registry"):
        app.prepare(pair, "second")
    (tmp_path / "ready").write_text("busy")
    with pytest.raises(ValueError, match="ready"):
        app.activate(app.prepare(PAIR, "third"))
    assert app.current() == (PAIR, True)
    assert not any(c[0] == "stop" for c in engine.commands)


def test_installation_change_cannot_adopt_existing_runtime(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    app.activate(app.prepare(PAIR, "first"))
    with pytest.raises(ValueError, match="identity changed"):
        DockerRuntime(
            app.root, "inspection", app.config, {**BINDING, "credential_generation": 2}, engine.run
        )


def test_model_only_preserves_software_and_bundled_rejects_model_update() -> None:
    spec = {
        "schema": contract.SCHEMA,
        "name": "model",
        "version": "2",
        "kind": "model",
        "application": "inspection",
        "runtime": "docker-application-v1",
        "platforms": ["linux/arm64"],
        "model": {
            "version": "2",
            "uri": "oci://registry.example/model@sha256:" + "b" * 64,
            "sha256": "c" * 64,
            "contract": "vision-v1",
        },
    }
    inv = {
        "runtime": "docker-application-v1",
        "platform": "linux/arm64",
        "ready": True,
        "healthy": True,
        "current": PAIR,
    }
    with pytest.raises(ValueError, match="bundles"):
        contract.resolve(spec, inv)
    old = {
        "software": {**PAIR["software"], "model_mode": "external", "model_contract": "vision-v1"},
        "model": {**spec["model"], "version": "1"},
    }
    resolved = contract.resolve(spec, {**inv, "current": old})
    assert resolved["software"] == old["software"]
    assert resolved["model"]["version"] == "2"


@pytest.mark.parametrize("previous", [False, True])
def test_recovery_waits_for_readiness_before_any_container_changes(
    tmp_path: Path, previous: bool
) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    if previous:
        app.activate(app.prepare(PAIR, "first"))
    candidate = app.prepare(PAIR, "candidate")
    engine.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        app.activate(candidate)
    (tmp_path / "ready").write_text("busy")
    before = len(engine.commands)
    with pytest.raises(ValueError, match="readiness"):
        app.recover()
    assert len(engine.commands) == before
    assert "transition" in app.state
    assert engine.containers[candidate["container"]]["State"]["Running"]
    (tmp_path / "ready").write_text("ready")
    app.recover()
    assert app.current() == ((PAIR, True) if previous else (None, False))


def test_daemon_failure_retains_first_install_recovery_journal(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    engine.interrupt = True
    candidate = app.prepare(PAIR, "candidate")
    with pytest.raises(KeyboardInterrupt):
        app.activate(candidate)

    def unavailable(args: list[str], timeout: int = 300) -> bytes:
        raise RuntimeError("Docker daemon unavailable")

    app.run = unavailable
    with pytest.raises(RuntimeError, match="unavailable"):
        app.recover()
    assert "transition" in app.state
    assert engine.containers[candidate["container"]]["State"]["Running"]
    app.run = engine.run
    app.recover()
    assert app.current() == (None, False)
    assert candidate["container"] not in engine.containers


def test_stopped_application_is_not_reported_as_running(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    app.activate(app.prepare(PAIR, "first"))
    engine.run(["stop", app.state["active"]["container"]])
    assert app.current() == (None, False)


def test_explicit_rollback_restores_a_missing_container(tmp_path: Path) -> None:
    engine = Engine()
    app = runtime(tmp_path, engine)
    app.activate(app.prepare(PAIR, "first"))
    engine.containers.pop(app.state["active"]["container"])
    app.activate(app.prepare(PAIR, "restore"), recovery=True)
    assert app.current() == (PAIR, True)
