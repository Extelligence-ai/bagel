"""Progress bar that tracks data transfer."""

import math
import sys
import threading

import humanize

SOLID = "⣿"
EMPTY = "⣀"


class Progress:
    """Progress bar that tracks data transfer."""

    def __init__(  # noqa: PLR0913
        self,
        source: str,
        destination: str,
        total_bytes: int,
        source_type: str | None = None,
        destination_type: str | None = None,
        report_progress: bool = True,
        progress_bar_n_chars: int = 40,
        solid: str = SOLID,
        empty: str = EMPTY,
    ) -> None:
        """Initiate a progress bar for file transfer.

        Args:
            source (str): Path of the source being transferred.
            destination (str): Path of the destination being transferred to.
            total_bytes (int): Total number of bytes to be transferred.
            source_type (str | None, optional): Annotation for the source type.
            destination_type (str | None, optional): Annotation for the destination type.
            report_progress (bool, optional): Whether to report progress to stdout.
            progress_bar_n_chars (int, optional): Number of characters in the progress bar.
            solid (str, optional): Character to represent completed portion.
            empty (str, optional): Character to represent uncompleted portion.

        """
        self._source = source
        self._destination = destination
        self._total_bytes = total_bytes
        self._source_type = source_type
        self._destination_type = destination_type
        self._report_progress = report_progress
        self._bytes_dispatched = 0
        self._progress_bar_n_chars = progress_bar_n_chars
        self._solid = solid
        self._empty = empty
        self._lock = threading.Lock()

    def __call__(self, dispatched_bytes: int) -> None:
        """Update progress bar with the latest bytes increment."""
        with self._lock:
            self._bytes_dispatched += dispatched_bytes

            if self._total_bytes == 0:
                ratio = 1.0
            else:
                ratio = float(self._bytes_dispatched) / float(self._total_bytes)

            n_solids = math.floor(ratio * self._progress_bar_n_chars)
            n_empties = self._progress_bar_n_chars - n_solids
            percentage = f"{round(ratio * 100, 1)}%".rjust(6)
            progress_bar = f"[{self._solid * n_solids}{self._empty * n_empties}] {percentage}"

            source = (
                f"{self._source}"
                if not self._source_type
                else f"{self._source} ({self._source_type})"
            )

            destination = (
                f"{self._destination}"
                if not self._destination_type
                else f"{self._destination} ({self._destination_type})"
            )

            if self._bytes_dispatched < self._total_bytes:
                if self._report_progress:
                    sys.stdout.write(f"{progress_bar} Dispatching {source}\r")
            else:
                sys.stdout.write(
                    f"{progress_bar} Dispatched {source} to {destination}.\n"
                    f"Total size dispatched: {humanize.naturalsize(self._total_bytes)}\n"
                )

            sys.stdout.flush()
