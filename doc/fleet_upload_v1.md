# Fleet file upload v1

Paired devices can send captured files to their fleet workspace without cloud
access keys. Managed fleet captures queue uploads automatically. Other pipelines
can add `src.pipeline.tasks.upload.fleet` with source, run_id, origin, and optional
provenance (build, capture times, triggers, anomaly labels, and pipeline revision).
Generic S3/GCS/Azure tasks keep their existing behavior.

The client uses the HTTPS origin of the saved enrollment URL. POST
`/fleet/uploads/presigned` with certificate PEM, Unix timestamp, payload, and a
base64 ECDSA SHA-256 signature over UTF-8:
`fleet-upload-v1\nPOST\n<route>\n<timestamp>\n<canonical payload>`.
Canonical JSON sorts keys, uses compact separators and ASCII escapes, and rejects
nonfinite numbers. The service must verify both possession and active recorded
issuance; a certificate string alone is not authentication.

The payload includes filename (relative path), size_bytes, SHA-256 hex digest,
run_id, and origin (real/sim/synthetic). The service assigns organization, robot,
bucket, and immutable object key. Send the returned PUT URL and headers, including
the checksum and If-None-Match, then POST `/fleet/uploads/confirm` with upload_id.
A PUT 412 after a lost response is safe to confirm; the service verifies the
existing object. Requests expire after five minutes; every retry obtains fresh
proof and authorization. The current transport supports single files up to 5 GiB.

Managed captures persist receipts alongside their release artifacts. The upload
thread retries independently of capture/control reconciliation and preserves
receipts across worker stops and process restarts. Enrollment mismatches retain
files locally. Successful confirmation renames the receipt to `.confirmed`;
source artifacts remain under the existing disk quota. Failed receipts rotate
behind unattempted work. No new credential or customer terminal step is required.
