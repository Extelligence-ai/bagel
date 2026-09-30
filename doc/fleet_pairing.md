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
