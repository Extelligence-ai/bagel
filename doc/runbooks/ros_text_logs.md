# Inspect ROS text logs without opening a bag

ROS writes plain-text log files to disk (`~/.ros/log` and its per-run
subdirectories). Bagel reads these directly, so you can inspect INFO/WARN/ERROR
messages without spinning up a bag -- useful when you just want to know what went
wrong before deciding which data to pull.

## Supported formats

Bagel auto-detects `.log` files (or directories containing them) written in any
of the common ROS formats:

| Format | Example line |
| --- | --- |
| ROS 2 rcutils (per-node) | `[INFO] [1662400000.100000000] [talker]: Publishing: 'Hello'` |
| ROS 2 `launch.log` | `1662400000.04 [INFO] [launch]: process started with pid [123]` |
| ROS 1 `rosout.log` | `1662400001.12 INFO /rosout [rosout.cpp:100(main)] [topics: /rosout] started` |
| ROS 1 roscpp (per-node) | `[ INFO] [1662400000.100000000]: waiting for service` |
| ROS 1 rospy (per-node) | `[rospy.client][INFO] 2022-09-05 12:00:00,100: init_node` |

Unrecognized lines are folded into the previous record, so multi-line messages
(tracebacks) stay attached to the entry that produced them. Formats that carry no
node name in the line take it from the file name instead.

## Prompts to try

Point your MCP client at a log path and ask:

> Read all ERROR messages from ~/.ros/log

> What went wrong in ~/.ros/log/2026-07-09-08-14-02 between t=100 and t=130?

Each node that logged becomes a topic, so SQL works too:

> Count WARN messages per node in ~/.ros/log using SQL

Under the hood these use the `read_loggings` and `query_messages` tools; each
record carries `level`, the multi-line `message`, and the source `file`.

## From log to data

Logs tell you *when* something went wrong; the bag has the *what*. A natural
follow-up prompt:

> Find the first ERROR in ~/.ros/log, then preserve the 30 seconds before and
> after that timestamp from ./rosbag2_2026_07_09 and drop the rest.

This chains `read_loggings` (on the logs) with `run_pipeline` in reduce mode
(on the bag) -- see [data_reduction.md](data_reduction.md).

## Try it on the bundled sample

```
Read all ERROR messages from data/sample/ros/log
```
