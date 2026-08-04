"""Resource-limit behavior (#134): memory scales with the query, not the file.

Before hardening, parse_file() slurped the whole log via read_text().splitlines()
(~3x file size peak: raw string + line list + records). After: it iterates the
open handle, so peak is ~the records themselves plus one line. The test uses
long messages so records ≈ file size; the old implementation peaks ≥3x and the
new one ~1.2x, so a 2x threshold cleanly separates them.
"""

import pathlib
import tracemalloc

from src.source.ros.parse import parse_file


def test_parse_file_streams_instead_of_slurping(tmp_path: pathlib.Path) -> None:
    message = "x" * 10_000
    line = f"[INFO] [1662400000.100000000] [talker]: {message}\n"
    log = tmp_path / "big.log"
    with open(log, "w", encoding="utf-8") as f:
        for _ in range(2_000):
            f.write(line)
    size = log.stat().st_size  # ~20 MB

    tracemalloc.start()
    records = parse_file(log)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(records) == 2_000
    assert records[0].message == message
    assert peak < 2 * size, f"peak {peak} vs file {size}: parse_file is not streaming"
