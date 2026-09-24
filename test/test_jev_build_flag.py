"""The on-robot Jev model is opt-in at image build time (JEV_MODE build arg).

CPU-only robots keep the default images, which must not pull torch/transformers;
GPU robots use a `-jev` image built with `JEV_MODE=true`. The decide gate's remote
backend works in both.
"""

import os
import pathlib
import re
import stat
import subprocess

import yaml

DOCKERFILES = sorted(pathlib.Path("docker").glob("Dockerfile.*"))
JEV_SERVICE = "ros2-jazzy-jev"


def _sync_args(tmp_path: pathlib.Path, env: dict[str, str], *groups: str) -> list[str]:
    """Run docker/sync-deps.sh with a fake `uv` that records its arguments."""
    fake_uv = tmp_path / "uv"
    fake_uv.write_text('#!/usr/bin/env bash\necho "$@"\n', encoding="utf-8")
    fake_uv.chmod(fake_uv.stat().st_mode | stat.S_IEXEC)
    result = subprocess.run(  # noqa: S603
        ["bash", "docker/sync-deps.sh", "false", *groups],  # noqa: S607
        env={**os.environ, **env, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.split()


def _services() -> dict:
    return yaml.safe_load(pathlib.Path("compose.yaml").read_text(encoding="utf-8"))["services"]


def test_jev_group_holds_the_model_runtime() -> None:
    # No tomllib on Python 3.10 (still supported), so read the group textually.
    pyproject = pathlib.Path("pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r"^jev = \[(.*?)^\]", pyproject, re.MULTILINE | re.DOTALL)
    assert match, "no `jev` dependency group in pyproject.toml"
    assert "torch" in match.group(1)
    assert "transformers" in match.group(1)


def test_sync_adds_the_jev_group_only_when_jev_mode_is_true(tmp_path: pathlib.Path) -> None:
    assert "jev" not in _sync_args(tmp_path, {}, "ros2")
    assert "jev" not in _sync_args(tmp_path, {"JEV_MODE": "false"}, "ros2")
    args = _sync_args(tmp_path, {"JEV_MODE": "true"}, "ros2")
    assert args[args.index("jev") - 1] == "--group"
    assert "ros2" in args


def test_every_dockerfile_accepts_the_jev_mode_build_arg() -> None:
    offenders = []
    for dockerfile in DOCKERFILES:
        text = dockerfile.read_text(encoding="utf-8")
        if "sync-deps.sh" not in text:
            continue
        arg = text.find("ARG JEV_MODE")
        sync = text.find('bash ./sync-deps.sh "${DEV_MODE}"')
        if arg == -1 or arg > sync:
            offenders.append(str(dockerfile))
    assert not offenders, f"Dockerfiles without ARG JEV_MODE before sync-deps: {offenders}"


def test_default_images_do_not_build_jev() -> None:
    enabled = [
        name
        for name, service in _services().items()
        if str(service.get("build", {}).get("args", {}).get("JEV_MODE", "false")).lower() == "true"
    ]
    assert enabled == [JEV_SERVICE]


def test_jev_image_is_built_and_published_by_ci() -> None:
    for workflow in ("docker-build.yaml", "publish.yaml"):
        text = pathlib.Path(".github/workflows", workflow).read_text(encoding="utf-8")
        assert JEV_SERVICE in text, f"{workflow} does not build {JEV_SERVICE}"
