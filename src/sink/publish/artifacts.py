"""Bounded on-disk store for fleet event artifacts: MCAP with JSON-encoded channels.

Each stored artifact is one event window written to its own MCAP file with a
single ``jsonschema``-encoded channel -- the exact wire format
``src/pipeline/lichtblick.py``'s ``write_mcap`` produces (``register_schema``
with ``encoding="jsonschema"``, ``register_channel`` with
``message_encoding="json"``, JSON message payloads), so an artifact opens in
any tool that reads JSON-encoded MCAP. That format is reused rather than
duplicated: ``jsonschema_type`` is imported from ``lichtblick.py``.

Raw-record passthrough (``mcap_raw``) is deliberately NOT used here: the live
tap that feeds event windows carries already-decoded dicts, not raw MCAP
message bytes, so there is nothing to copy through -- don't "optimize" this
into a raw-bytes path later; there are no raw bytes available at this layer.

``ArtifactStore`` mirrors ``Spool.for_robot``'s identifier validation and
resolved-containment discipline, but is deliberately rooted OUTSIDE the spool
tree (``CACHE_DIRECTORY/publish-artifacts``, not
``CACHE_DIRECTORY/publish``): ``Spool.stats()`` lane-scans its root's
subdirectories, and an artifacts directory sitting inside a spool root would
be misread as a spool lane.
"""

import json
import os
import pathlib
import tempfile

import pyarrow as pa
from mcap.writer import Writer

from settings import settings
from src.pipeline.lichtblick import jsonschema_type

SECOND_NS = 1_000_000_000


def write_event_mcap(
    path: pathlib.Path,
    topic: str,
    struct: pa.StructType,
    samples: list[tuple[float, dict]],
) -> int:
    """Write one MCAP file holding a single JSON-encoded channel.

    Mirrors ``lichtblick.write_mcap``'s writer usage exactly (same
    ``profile``/``library`` strings, same schema/channel registration, same
    per-message JSON encoding) so the result is standard JSON-encoded MCAP.

    Args:
        path: Destination file (overwritten if it exists).
        topic: Channel/schema name.
        struct: Arrow struct type describing one sample's shape; converted to
            a JSON-Schema fragment via ``lichtblick.jsonschema_type``.
        samples: ``(timestamp_seconds, payload)`` pairs, in the order to write.
            ``log_time = int(timestamp_seconds * 1e9)``.

    Returns:
        The number of messages written (``len(samples)``).

    """
    with open(path, "wb") as stream:
        writer = Writer(stream)
        writer.start(profile="", library="bagel")
        schema_id = writer.register_schema(
            name=topic,
            encoding="jsonschema",
            data=json.dumps(jsonschema_type(struct)).encode(),
        )
        channel_id = writer.register_channel(
            topic=topic, message_encoding="json", schema_id=schema_id
        )
        for timestamp_seconds, payload in samples:
            log_time = int(timestamp_seconds * SECOND_NS)
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=json.dumps(payload).encode(),
            )
        writer.finish()
    return len(samples)


def _validate_identifier(value: str, *, max_segments: int) -> None:
    """Mirror ``Spool.for_robot``'s traversal guard for a ``/``-delimited identifier.

    Non-empty, no leading ``/``, at most ``max_segments`` slash-delimited
    segments, no segment is empty, ``.`` or ``..``.
    """
    if not value:
        raise ValueError("must be non-empty")
    if value.startswith("/"):
        raise ValueError("must not start with /")
    segments = value.split("/")
    if len(segments) > max_segments:
        raise ValueError(f"must have at most {max_segments - 1} /")
    for segment in segments:
        if not segment or segment in (".", ".."):
            raise ValueError("segments must be non-empty and not . or ..")


class ArtifactStore:
    """Bounded on-disk store of event-window MCAP artifacts for one robot."""

    def __init__(self, root: pathlib.Path, max_bytes: int) -> None:
        """Initialize the store, creating ``root`` (and parents) if needed.

        Args:
            root: Directory holding this store's artifacts.
            max_bytes: Total byte budget; ``_evict()`` drops the oldest files
                (by mtime) once this is exceeded.

        """
        self._root = pathlib.Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes

    @classmethod
    def for_robot(cls, robot: str) -> "ArtifactStore":
        """Create an ArtifactStore for a robot, rooted outside the spool tree.

        Args:
            robot: Robot identifier in shape ``tenant/robot`` or ``robot``
                (one ``/`` max). Must not contain ``.``, ``..``, or be empty.
                No leading ``/``.

        Returns:
            ArtifactStore rooted at
            ``CACHE_DIRECTORY/publish-artifacts/<robot>`` with the cap from
            ``settings.FLEET_ARTIFACTS_MAX_BYTES``.

        Raises:
            ValueError: If robot contains path traversal or is malformed.

        """
        max_segments = 2  # tenant/robot or robot (0 vs 1 slash)
        _validate_identifier(robot, max_segments=max_segments)

        base = (pathlib.Path(settings.CACHE_DIRECTORY) / "publish-artifacts").resolve()
        root = (base / robot).resolve()
        if not root.is_relative_to(base):
            raise ValueError(f"robot path escapes base directory: {root}")

        return cls(root, max_bytes=settings.FLEET_ARTIFACTS_MAX_BYTES)

    def store(
        self,
        name: str,
        event_id: str,
        topic: str,
        struct: pa.StructType,
        samples: list[tuple[float, dict]],
    ) -> pathlib.Path | None:
        """Write one event window as ``<name>-<event_id>.mcap``, then evict.

        Writes to a sibling tempfile and ``os.replace``s it into place so a
        crash mid-write never leaves a torn ``.mcap`` a collector might ship.

        Args:
            name: Rule name (from config); re-validated with the same
                single-segment traversal rules as ``for_robot`` -- belt and
                braces, since config-sourced values still reach a filesystem
                path.
            event_id: Event identifier; re-validated with the same
                single-segment traversal rules as ``name`` -- it's always a
                uuid4 string from our own emitter, so this should never fire
                in practice, but a slash or ``..`` spliced unvalidated into
                the filename is still a path-shaped value from outside this
                function and gets the same belt-and-braces treatment.
            topic: Passed through to ``write_event_mcap``.
            struct: Passed through to ``write_event_mcap``.
            samples: Passed through to ``write_event_mcap``.

        Returns:
            The written path, or ``None`` (writing nothing) if the finished
            tempfile's size alone exceeds ``max_bytes``.

        Raises:
            ValueError: If ``name`` or ``event_id`` fails the traversal guard.

        """
        _validate_identifier(name, max_segments=1)
        _validate_identifier(event_id, max_segments=1)

        target = self._root / f"{name}-{event_id}.mcap"
        fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=self._root)
        tmp_path = pathlib.Path(tmp_name)
        os.close(fd)
        try:
            write_event_mcap(tmp_path, topic, struct, samples)
            if tmp_path.stat().st_size > self._max_bytes:
                return None
            os.replace(tmp_path, target)
        finally:
            tmp_path.unlink(missing_ok=True)

        self._evict(exclude=target)
        return target

    def _evict(self, exclude: pathlib.Path) -> None:
        """Unlink the oldest artifact(s) by mtime while over budget.

        ``exclude`` (the file ``store()`` just wrote) is never a candidate --
        excluded by identity, not by mtime ordering, so it can never be
        evicted regardless of what its mtime happens to read as (a
        same-second write racing an older file's forged/rolled-back mtime,
        or any other mtime-luck coincidence). mtime ordering picks the
        oldest only among the REMAINING files. Missing-file races (a
        concurrent evictor or collector) are tolerated via
        ``missing_ok=True``.
        """
        while True:
            stats = self.stats()
            if stats["bytes"] <= self._max_bytes:
                return
            candidates = [p for p in self._root.glob("*.mcap") if p != exclude]
            if not candidates:
                return
            oldest = min(candidates, key=lambda p: p.stat().st_mtime)
            oldest.unlink(missing_ok=True)

    def stats(self) -> dict[str, int]:
        """Return ``{"bytes": total, "files": count}`` for this store."""
        files = list(self._root.glob("*.mcap"))
        return {
            "bytes": sum(p.stat().st_size for p in files),
            "files": len(files),
        }
