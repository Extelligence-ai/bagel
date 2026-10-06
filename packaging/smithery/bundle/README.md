# Bagel local MCP bundle

This bundle connects a stdio MCP client to a locally running Bagel server over
SSE. It uses the bundled, pinned `mcp-remote` bridge; it does not install or start
Bagel, host a server, or require a Matcha account.

## Prerequisites

- Docker and a running Bagel service, following the [Bagel quickstart](https://github.com/Extelligence-ai/bagel#readme).
- Your data mounted into that Bagel container.
- Node.js 22.12 or later, supplied by the MCP client or installed locally.

Choose the matching Docker service for ROS, PX4, ArduPilot, Betaflight, or IoT.
The default connection is `http://127.0.0.1:8000/sse`. Change the bundle's port
setting if you started Bagel on another port. Only loopback connections are used.

## Verify

Ask: "Describe the available topics in ./data/sample/ros2/mcap."
Use a sample supported by your chosen Docker service. Paths refer to files
inside the Bagel container.

## Privacy and costs

The bridge communicates with Bagel on your machine. Results are returned to the
AI client you chose and are subject to that client's data handling. Bagel's
external integrations have their own data handling and cost implications.
Bagel is free software; AI services and optional external integrations may charge.

## Licensing

The wrapper is Apache-2.0. Bundled dependencies retain their own licenses,
included under node_modules. The bundle version is independent of Bagel's
server version.
