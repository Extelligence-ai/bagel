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

Bagel tracks a single rolling branch (`main`); fixes land there and in the next
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
- Compose publishes the MCP, Jupyter, and (opt-in) ollama ports on `127.0.0.1`
  only. Docker's port publishing bypasses host firewalls, so an unprefixed
  mapping would expose the unauthenticated endpoint to every device on the
  network. To share an instance deliberately, remove the `127.0.0.1:` prefix
  and put an authenticated, TLS-terminating proxy in front of it.

The `save_agent_capability` tool is the endpoint's first tool whose writes are
driven by LLM-authored content rather than caller-specified paths or configuration.
Its writes are confined to `.poml`/`.md` files under the configured
`USER_CAPABILITIES_DIRECTORY` (name slugs are validated; path traversal and
absolute paths are rejected), and builtin capabilities are immutable through
it. As with every tool on this endpoint, it is intended for trusted-network
deployment.

- Treat pipeline configs and startup manifests as sensitive: they can contain
  broker credentials, DSNs, and webhook URLs.
- Cloud upload tasks use your ambient credentials (AWS/GCS/Azure SDK chains);
  scope those credentials to the destination buckets.
- Published images contain no `.env`. Runtime configuration comes from the
  defaults in `settings.py`, Compose's `environment:`/`env_file:`, or the
  process environment — so a secret added to a local `.env` is never baked
  into a published image layer.
