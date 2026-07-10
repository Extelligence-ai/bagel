# Security Policy

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub's private vulnerability reporting](https://github.com/Extelligence-ai/bagel/security/advisories/new)
— do not open a public issue with details. If that route is unavailable to you,
open a bare issue asking for a private channel (no details) and a maintainer will
provide one.

We aim to acknowledge reports within **3 business days**. Please include a working
proof of concept and the exact code paths involved; reports without a reproducible
PoC are hard to act on.

Bagel tracks a single rolling branch (`stage`); fixes land there and in the next
published Docker images.

## Threat model (what counts)

Bagel is an MCP server that an LLM drives to read and transform **local, trusted
data** on behalf of the person operating it. Some behaviors that look alarming are
the product working as intended:

**In scope — we want these reports:**
- Reaching Bagel's tools **without the operator's consent** (e.g., anything
  exploitable by a remote party when the server is deployed as documented)
- Escaping the operator's intent: crafted *data files* (bags, MCAP, logs, MQTT
  payloads) that achieve code execution or read/write files the operator never
  pointed Bagel at
- Path handling that escapes the artifact/cache directories
- Secrets leakage (broker credentials, DB DSNs, webhook URLs, cloud keys) into
  artifacts, logs, or tool output

**Out of scope — by design, not vulnerabilities:**
- Executing operator-supplied SQL over operator-specified data — that is the
  product (`query_messages` is an intentional SQL interface, as documented)
- Reading files the operator's own prompts point Bagel at
- Anything requiring the attacker to already control the MCP client or the
  operator's machine

## Deployment hardening

- The MCP endpoint has **no authentication layer** — treat it like a database
  socket. Bind it to localhost (`MCP_SERVER_HOST=127.0.0.1`) or keep it inside a
  trusted network; never expose the port to the public internet. The compose files
  publish it on the local host only for the documented single-machine setup.
- Treat pipeline configs and startup manifests as sensitive: they can contain
  broker credentials, DSNs, and webhook URLs.
- Cloud upload tasks use your ambient credentials (AWS/GCS/Azure SDK chains);
  scope those credentials to the destination buckets.
