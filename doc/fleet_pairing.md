# Controller pairing

A fleet service can issue a short-lived pairing command for an existing asset:

```sh
python -m src.sink.publish.pairing --url https://fleet.example/enroll --code PAIRING_CODE
```

Run this inside the same Python environment and persistent identity volume as the
Bagel server. Docker example: `docker exec -it BAGEL_CONTAINER
/home/ubuntu/runtime/.venv/bin/python -m src.sink.publish.pairing ...`.
This command is part of the fleet development branch, not older published images.

The client generates a P-256 key locally and durably saves the pending request
(mode 0600) before contacting the service. It reports a persistent generated
controller ID, host/platform, Bagel version/build, and readable Linux DMI metadata.
Inside Docker it does not claim container/VM hardware metadata as physical robot
metadata. Unknown hardware serials remain absent. It prints the public-key
fingerprint; an operator compares that fingerprint in the fleet service and
confirms the asset association before the service issues a certificate.

The private key never leaves the controller. POST `/v1/pairing` carries the
pairing ID/token, signed CSR, discovery and an ECDSA/SHA-256 proof over UTF-8 bytes:
`bagel.pairing.v1\n` followed by canonical JSON containing `pairing_id`, `csr_pem`,
and `discovery` (sorted keys, compact separators, ASCII escaping). The service
pins the first valid claim; retries use identical CSR/discovery. An
`awaiting_confirmation` response keeps polling; `issued` supplies the existing
fleet enrollment identity response. The returned certificate must match the saved
key and tenant/robot CN before identity is installed.

On interruption use `python -m src.sink.publish.pairing --resume` in the same
environment. Do not generate another key for the same pairing. After installation
Bagel asks the local SSE MCP server to reload existing stream rules; if unavailable,
restart the Bagel server. Pairing does not invent stream rules or physical serials.
An existing enrolled identity is never overwritten. Use fleet access withdrawal
and replacement before connecting the controller to a different asset. A new host
must receive its own identity, not a copy of this directory.

## Browser setup

An installer can enable the local browser flow with
`FLEET_PAIRING_UI_ENABLED=true` and the trusted `FLEET_ENROLL_URL`. Publish the
server port on loopback only, for example `127.0.0.1:8000:8000` in Docker. Do not
expose this unauthenticated local setup service on a network interface.

The fleet portal opens `/fleet/connect#code=ENCODED_CODE` on
that local server. The user clicks **Connect this Bagel**, compares the fingerprint
on the portal, and approves there. Bagel creates the key, polls for approval,
installs the certificate, and reloads the existing stream configuration. No shell
command or container name is required. The page immediately removes the fragment
from history, keeps no secret in browser storage, and requires an explicit click
before starting. The start endpoint accepts no service URL from the browser; it always uses the
installer-configured enrollment service.

A worker keeps going when the tab closes. Pending attempts resume on server
restart; the browser can also retry with the same durable key. An already enrolled
controller cannot be overwritten. Setup is opt-in and only accepts loopback Host
headers with same-origin/CSRF-checked mutations. This initial browser flow serves
locally accessible controllers; remote claiming is not implemented.
