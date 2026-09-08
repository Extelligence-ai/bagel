# Lichtblick / Foxglove integration

[Lichtblick](https://github.com/lichtblick-suite/lichtblick) is BMW's open-source
fork of Foxglove Studio (Apache-2.0). Bagel exports any time window as a Lichtblick
session: an MCAP file plus a layout with the plot series and time/value ranges
pre-set, so the event is on screen as soon as you import both. The same files open
in Foxglove, which shares the layout format.

## From a prompt

> Find the hardest brake in ./flight_042 and show it to me in Lichtblick.

Behind the scenes this chains `preview_pipeline` (find the event) with
`export_for_lichtblick` (write the window):

```text
export_for_lichtblick(
  path="./flight_042", topics=["/imu"],
  start_seconds=118.9, end_seconds=138.9, name="brake event 2",
)
-> { "mcap": ".../lichtblick/brake_event_2/brake_event_2.mcap",
     "layout": ".../lichtblick/brake_event_2/brake_event_2_layout.json",
     "curves": ["/imu.linear_acceleration.x", ...],
     "instructions": "Open Lichtblick, drop in the .mcap, then Layouts -> Import from file." }
```

## Opening the session

1. Open Lichtblick (desktop or web) and drag the exported `.mcap` in.
2. **Layouts → Import from file** → the exported `_layout.json`.

The Plot panel opens with the event's signals plotted and the time/value ranges
framed to the window.

## Notes

- The MCAP uses JSON-encoded channels (`jsonschema` schemas): readable by
  Lichtblick, Foxglove, and Bagel itself, so you can keep querying the exported
  window with SQL.
- Signal names in `signals=[...]` use the flattened `topic/field/subfield`
  convention shared with the [PlotJuggler](./plotjuggler.md) and
  [Rerun](./rerun.md) exports; pick the viewer your team already uses.
- If Bagel runs in Docker, artifacts land in `~/.bagel/artifacts` on the host
  (the compose services mount it).

## Troubleshooting

- **Plot panel opens empty** with the exported layout: clear the plot's X-axis
  min/max (panel settings) and it will auto-fit. Exported layouts currently
  write absolute-epoch X bounds while Lichtblick's timestamp axis uses elapsed
  seconds, which parks the preset viewport far from the data: a fix in
  `export_for_lichtblick` is tracked. Y-axis framing is unaffected.
