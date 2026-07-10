# Automotive: ASAM MDF (.mf4)

MDF is the measurement format the automotive world runs on — CANape, INCA, Vector
tooling, and most DAQ hardware write it. Bagel reads `.mf4` (and older MDF)
files directly: **channel groups become topics, channels become fields**, and every
prompt that works on a ROS bag works on a measurement.

## Setup

The MDF reader needs the optional `automotive` dependency group:

```bash
uv sync --group automotive
```

Everything is pure pip (`asammdf`) — no vendor tooling required.

## Prompts to try

> Summarize the measurement at ./drive_042.mf4

> What's the max EngineSpeed in ./drive_042.mf4, and when did it happen?

> Find every hard deceleration below -3 m/s² in the Vehicle group and show me
> VehicleSpeed 10 seconds around each one.

Channel units and comments from the file ride along into the schema, so the LLM
knows `EngineSpeed` is in rpm without being told.

## Notes

- Timestamps are exposed as **absolute epoch seconds** (the file's measurement
  start time plus each sample's offset), consistent with every other Bagel source —
  so time windows and cross-source comparisons behave as expected.
- Unnamed channel groups appear as `ChannelGroup_<index>`.
- Raw CAN logs (`.blf` / `.asc`) with DBC decoding are the planned follow-up; if
  you need them, [open a ticket](https://github.com/Extelligence-ai/bagel/issues)
  or 👍 the existing one.
