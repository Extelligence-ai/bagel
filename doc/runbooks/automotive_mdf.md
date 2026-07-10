# Automotive: ASAM MDF (.mf4) and raw CAN (.blf / .asc) — beta

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

## Raw CAN captures with a DBC

Vector BLF and ASC captures work too — pass the DBC that describes the bus, and
**DBC messages become topics, signals become fields** with physical values
(scaling applied) and units attached:

> Using ./vehicle.dbc, what's the max EngineSpeed in ./drive.blf?

which calls `query_messages(path="./drive.blf", args={"dbc": "./vehicle.dbc"}, ...)`.
Frames whose IDs aren't in the DBC are skipped (and counted in the logs), so a
partial DBC still works.

## Beta status

Both readers are verified against files we generate with the same libraries that
read them (`asammdf`, `python-can`) — real CANape/INCA/Vector-produced captures
haven't crossed our test bench yet. If you have one, trying Bagel on it and
[reporting what happens](https://github.com/Extelligence-ai/bagel/issues) is the
single most useful contribution.

## Notes

- Timestamps are exposed as **absolute epoch seconds** (the file's measurement
  start time plus each sample's offset), consistent with every other Bagel source —
  so time windows and cross-source comparisons behave as expected.
- Unnamed channel groups appear as `ChannelGroup_<index>`.
- MDF4 files with DBC-referenced CAN raw frames inside are best decoded by your
  DAQ tool into channel groups first; direct in-MDF CAN decoding is demand-driven.
