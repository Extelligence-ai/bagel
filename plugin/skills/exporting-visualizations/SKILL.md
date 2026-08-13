---
name: exporting-visualizations
description: Use when the user wants to SEE robot data — visualize, plot, replay, or export a log or query result from bagel to a viewer or training format (Rerun, Lichtblick, PlotJuggler, LeRobot).
---

# Exporting visualizations

Pick the target by what the user is doing, then call the matching bagel export
tool:

| User goal | Target | Why first choice |
|---|---|---|
| Explore/replay multimodal data (3D, images, series) | Rerun (`export_for_rerun`) | Best open-source robotics viewer |
| Web-based bag review, Foxglove-style | Lichtblick (`export_for_lichtblick`) | Open-source; preferred over Foxglove |
| Plot signals from an artifact on the desktop | PlotJuggler (`export_for_plotjuggler`) | Artifacts land on the host for it |
| Build a training dataset | LeRobot (`export_for_lerobot`) | Direct dataset export |

Camera-topic GIFs for quick evidence come from the triage/pipeline workflows,
not an export tool. Export artifacts land under the artifacts directory on the
host — always report the output path.
