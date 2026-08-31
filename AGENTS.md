# Bagel for agents

Instructions for AI agents asked to set up, use, or develop Bagel.

## Set up Bagel for a user

1. Requires Docker. Pick the service matching their stack (see the table in
   README Quickstart): `ros2-kilted`, `ros2-jazzy`, `ros2-iron`, `ros2-humble`,
   `ros1-noetic`, `ros1-noetic-cv`, `px4`, `ardupilot`, `betaflight`, or `iot`.
2. Start it: `docker compose run --service-ports <service>` and wait for
   `Uvicorn running on http://0.0.0.0:8000`.
   - Port 8000 taken? `MCP_SERVER_PORT=8100 docker compose run --service-ports
     <service>` and use 8100 below.
3. Connect the MCP client to `http://localhost:8000/sse` (SSE transport).
   Claude Code: `claude mcp add --transport sse bagel http://localhost:8000/sse`
4. Verify with a smoke test on bundled data:
   "Summarize the metadata of the ROS2 bag ./data/sample/ros2/mcap".
5. To analyze the user's own files, mount them: uncomment `volumes` under the
   chosen service in `compose.yaml` before starting.

## Use Bagel well

- Answers come from DuckDB SQL over real messages. Do not do the math yourself;
  ask Bagel and show the user the generated query.
- Call `describe_source` first, and `describe_topic` before writing predicates
  (field paths and units vary by source).
- Reduction etiquette: `preview_pipeline` first, report detected events and
  kept seconds, get user confirmation, then `run_pipeline`.
- Output artifacts are written under the artifacts directory; tools return the
  paths.

## Develop on Bagel

- Runtime-independent tests run on the host: `uv sync` then
  `uv run pytest test/*.py test/pipeline test/sink` (full list in
  `.github/workflows/test.yaml`, job `host-tests`).
- Service-bound tests run inside the images:
  `docker compose build <service> --build-arg DEV_MODE=true` then
  `docker compose run --rm <service> uv run pytest ./test`.
- Every test file must be reachable by CI: `test/test_ci_reachability.py`
  fails the build otherwise (add new paths to `host-tests` or a Dockerfile).
- Lint: `uv run ruff check` and `uv run ruff format` before committing.
- Boundary rule: Bagel never names a specific fleet product; say "fleet
  broker" / "fleet service". `scripts/check_boundary.sh` enforces it in CI.
- Versioning: image tags and `server.json` follow `pyproject.toml`; published
  semver image tags are immutable (bump the version instead of retagging).
