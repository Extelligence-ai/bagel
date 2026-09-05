"""Every test file must be reachable by CI (issue #161).

CI runs pytest in two places: inside each service image (the `docker` job,
over whatever the Dockerfiles COPY in) and on the host (the `host-tests` job,
over the paths on its pytest command line). A test file on neither list never
runs, and pytest cannot fail a test it cannot see -- 42 files sat in that gap
for months (issue #161). This guard makes the gap a CI failure instead of a
silent omission: add new test paths to the host job's pytest invocation in
.github/workflows/test.yaml, or to a Dockerfile COPY list, or they don't merge.
"""

import pathlib
import re

import yaml

_COPY_TEST_PATH = re.compile(r"\s*COPY\b.*?\s(test/\S+)\s+\./")


def _repo_test_files() -> set[str]:
    return {str(p) for p in pathlib.Path("test").rglob("*.py") if "__pycache__" not in p.parts}


def _expand(arg: str) -> set[str]:
    """Expand one pytest path argument or COPY source into concrete .py files."""
    if "*" in arg:
        return {str(p) for p in pathlib.Path().glob(arg) if p.suffix == ".py"}
    path = pathlib.Path(arg)
    if path.is_dir():
        return {str(p) for p in path.rglob("*.py") if "__pycache__" not in p.parts}
    return {arg} if path.suffix == ".py" else set()


def _host_job_files() -> set[str]:
    """Files the host-tests job's pytest invocation reaches."""
    workflow = yaml.safe_load(
        pathlib.Path(".github/workflows/test.yaml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["host-tests"]["steps"]
    commands = [s["run"] for s in steps if "pytest" in s.get("run", "")]
    assert commands, "host-tests job has no pytest step"
    reached: set[str] = set()
    for command in commands:
        for arg in command.split():
            if arg == "test" or arg.startswith("test/"):
                reached |= _expand(arg)
    return reached


def _tested_dockerfiles() -> set[pathlib.Path]:
    """Dockerfiles backing services that actually run pytest in test.yaml.

    Only entries in the test matrix count; building alone is insufficient.
    """
    workflow = yaml.safe_load(
        pathlib.Path(".github/workflows/test.yaml").read_text(encoding="utf-8")
    )
    services = {
        entry["service"] for entry in workflow["jobs"]["docker"]["strategy"]["matrix"]["include"]
    }
    compose = yaml.safe_load(pathlib.Path("compose.yaml").read_text(encoding="utf-8"))
    return {
        pathlib.Path(compose["services"][service]["build"]["dockerfile"])
        for service in services
        if service in compose["services"]
    }


def _image_files() -> set[str]:
    """Files reachable inside at least one pytest-running image via COPY."""
    reached: set[str] = set()
    for dockerfile in _tested_dockerfiles():
        for line in dockerfile.read_text(encoding="utf-8").splitlines():
            match = _COPY_TEST_PATH.match(line)
            if match:
                reached |= _expand(match.group(1))
    return reached


def test_every_test_file_is_reachable_by_ci() -> None:
    unreachable = sorted(_repo_test_files() - _host_job_files() - _image_files())
    assert not unreachable, (
        f"{len(unreachable)} test file(s) run neither on the host nor in any "
        f"image -- add them to the host-tests pytest paths in "
        f".github/workflows/test.yaml or to a Dockerfile COPY list: "
        f"{unreachable}"
    )


def test_host_job_paths_exist() -> None:
    """A stale path in the host job would silently shrink what runs."""
    workflow = yaml.safe_load(
        pathlib.Path(".github/workflows/test.yaml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["host-tests"]["steps"]
    for step in steps:
        if "pytest" not in step.get("run", ""):
            continue
        for arg in step["run"].split():
            if (arg == "test" or arg.startswith("test/")) and "*" not in arg:
                assert pathlib.Path(arg).exists(), f"host-tests pytest path does not exist: {arg}"


def test_image_files_only_counts_images_that_run_pytest() -> None:
    """Arrow and IoT must run tests, not merely build production images."""
    tested = {str(dockerfile) for dockerfile in _tested_dockerfiles()}
    assert tested, "expected at least one pytest-running dockerfile"
    assert "docker/Dockerfile.arrow" in tested
    assert "docker/Dockerfile.iot" in tested
    assert "docker/Dockerfile.ros2" in tested


def test_env_guard_allows_env_example() -> None:
    """Copilot on #163: the .env guard must not reject .env.example."""
    from test.test_compose_bindings import COPY_ENV_PATTERN

    assert COPY_ENV_PATTERN.match("COPY .env ./.env")
    assert COPY_ENV_PATTERN.match("COPY --chown=u:u .env ./.env")
    assert not COPY_ENV_PATTERN.match("COPY .env.example ./.env.example")
