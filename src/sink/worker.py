"""Run a live pipeline's fires on a worker thread, off the transport's ingest thread.

A transport callback (paho's network thread, a ROS executor) must return quickly or
messages back up. Pipelines do DuckDB queries, write files, upload to a bucket and may
ask a decision backend such as Jev, any of which can take seconds. The buffer writer
therefore only decides *when* a pipeline fires and hands the timestamp to this worker,
which runs the fires one at a time, in order.

The queue is bounded two ways, so a stalled pipeline cannot grow memory or read a
window that has already rotated out of the topic buffer:

- `LIVE_PIPELINE_MAX_PENDING`: when full, the oldest waiting fire is dropped.
- `LIVE_PIPELINE_MAX_LAG_SECONDS`: a fire that waited longer than this is dropped
  when its turn comes.

Dropped fires are counted in `pipeline.summary.dropped` and logged.
"""

import collections
import logging
import threading
import time
from typing import Any

from settings import settings


class PipelineWorker:
    """One background thread running one pipeline's fires in submission order."""

    def __init__(self, pipeline: Any) -> None:  # noqa: ANN401 -- duck-typed Pipeline
        """Start the worker thread for `pipeline`."""
        self._pipeline = pipeline
        self._pending: collections.deque[tuple[float, float]] = collections.deque()
        self._condition = threading.Condition()
        self._busy = False
        self._stopped = False
        self._thread = threading.Thread(
            target=self._run, name=f"pipeline:{getattr(pipeline, 'name', '?')}", daemon=True
        )
        self._thread.start()

    @property
    def stopped(self) -> bool:
        """True once `stop()` was called."""
        return self._stopped

    def submit(self, asof_seconds: float) -> None:
        """Queue a fire at `asof_seconds`; never blocks on the pipeline."""
        with self._condition:
            if self._stopped:
                raise RuntimeError(f"Pipeline worker for '{self._pipeline.name}' is stopped")
            self._pending.append((asof_seconds, time.monotonic()))
            while len(self._pending) > settings.LIVE_PIPELINE_MAX_PENDING:
                dropped, _ = self._pending.popleft()
                self._drop(dropped, "the queue is full")
            self._condition.notify_all()

    def drain(self, timeout_seconds: float | None = None) -> bool:
        """Wait until every queued fire has run; False if `timeout_seconds` ran out first."""
        deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
        with self._condition:
            while self._pending or self._busy:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
        return True

    def stop(self) -> None:
        """Stop accepting fires and end the thread once the fire in progress finishes.

        Fires still queued are discarded; call `drain()` first to run them.
        """
        with self._condition:
            self._stopped = True
            self._pending.clear()
            self._condition.notify_all()

    def _drop(self, asof_seconds: float, why: str) -> None:
        self._pipeline.summary.dropped += 1
        logging.warning(
            "Pipeline '%s' dropped the fire at %.4f s: %s (%d dropped so far)",
            self._pipeline.name,
            asof_seconds,
            why,
            self._pipeline.summary.dropped,
        )

    def _next(self) -> float | None:
        """Return the next fire to run, or None once stopped."""
        with self._condition:
            while True:
                if self._stopped:
                    return None
                while self._pending:
                    asof_seconds, queued_at = self._pending.popleft()
                    if time.monotonic() - queued_at > settings.LIVE_PIPELINE_MAX_LAG_SECONDS:
                        self._drop(asof_seconds, "it waited longer than the allowed lag")
                        continue
                    self._busy = True
                    return asof_seconds
                self._condition.notify_all()  # wake drain(): nothing pending, not busy
                self._condition.wait()

    def _run(self) -> None:
        while (asof_seconds := self._next()) is not None:
            try:
                self._pipeline.run_at(asof_seconds)
            except Exception:
                # run_at already counted the failure in the summary; a live stream keeps
                # going rather than losing every later fire.
                logging.exception(
                    "Pipeline '%s' failed at %.4f s", self._pipeline.name, asof_seconds
                )
            finally:
                with self._condition:
                    self._busy = False
                    self._condition.notify_all()
