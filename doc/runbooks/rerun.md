# Rerun integration

[Rerun](https://rerun.io) is the open-source visualization SDK the robotics and
robot-learning communities are converging on. Bagel exports any time window as a
Rerun recording (`.rrd`): every scalar signal becomes a time series, and the viewer
opens with the full signal tree ready to explore.

## Setup

The export needs the optional `rerun-sdk` dependency, and the viewer itself:

```bash
uv sync --group viz          # the SDK, for the export
uvx rerun-sdk@latest --help  # or: pip install rerun-sdk / the desktop app
```

## From a prompt

> Find the hardest brake in ./flight_042 and show it to me in Rerun.

Behind the scenes this chains `preview_pipeline` (find the event) with
`export_for_rerun` (write the window):

```text
export_for_rerun(
  path="./flight_042", topics=["/imu"],
  start_seconds=118.9, end_seconds=138.9, name="brake event 2",
)
-> { "rrd": "~/.bagel/artifacts/rerun/brake_event_2/brake_event_2.rrd",
     "signals": ["/imu/linear_acceleration/x", ...],
     "command": "rerun '~/.bagel/artifacts/rerun/brake_event_2/brake_event_2.rrd'" }
```

Run the returned command and the event is on screen.

Signal names are the flattened `topic/field/subfield` paths: the same naming as the
[PlotJuggler export](./plotjuggler.md), so the two integrations are interchangeable:
pick the viewer your team already uses. Pass `signals=[...]` to narrow the export;
by default every numeric signal in the selected topics is included.

## Docker note

If Bagel runs in Docker, artifacts land in `~/.bagel/artifacts` on the host (the
compose services mount it), so the returned `rerun` command works as-is on your
desktop.
