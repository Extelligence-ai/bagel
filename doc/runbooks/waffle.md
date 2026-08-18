# Waffle Iron: chat with your robot's hardware — EXPERIMENTAL BETA

[Waffle Iron](https://github.com/arunvenkatadri/waffle-iron) is infrastructure as
code for robots: a `robot.waffleform.yaml` declares the robot's full hardware
state — compute, actuators, sensors, firmware, calibration, software, URDF.
Bagel reads WaffleForms directly: **component categories become topics,
components become rows**, so hardware state is queryable exactly like a bag.

## Prompts to try

> Describe the robot declared in ./robot.waffleform.yaml

> Which sensors are on firmware older than 5.14?

> What ROS packages and versions does warehouse-amr-07 run?

Under the hood these are the ordinary `describe_data_source` / `query_messages`
tools; each category (`compute`, `actuators`, `sensors`, `software`,
`calibration`, plus a `robot` identity row) is a topic with a union schema over
its components.

## Snapshots become history

A WaffleForm is a snapshot: every row carries the file's modification time. Snap
periodically (`waffle snap` on a pipeline cadence) and accumulate the files, and
hardware state becomes a time series — *"when did robot 7's camera firmware
change?"* becomes a SQL question.

## Fleet-scale

Point the batch tools at a fleet's worth of forms:

> Across ./fleet/*.waffleform.yaml, which robots still run nav2 1.1.12?

## Experimental beta status

This is the first slice of the Waffle Iron integration (WaffleForm as a data
source). Version fields are strings — compare exact versions, not lexicographic
ranges (`'5.9' > '5.14'` is true for strings). Planned next: `waffle verify` as
a tool, periodic snap pipelines, live spec-vs-reality contradiction alerts, and
hardware provenance stamped into LeRobot exports. Interfaces may change without
notice while waffle-iron itself is v0.x.
