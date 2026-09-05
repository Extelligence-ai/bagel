"""Seed CI-owned disposable databases; never run against a user's database."""

import os
import subprocess
import time
import urllib.error
import urllib.request


def main() -> None:
    """Make identical readings with two rising edges and a twenty-minute span."""
    subprocess.run(  # noqa: S603 -- fixed command, CI-owned container identifier
        [  # noqa: S607 -- Docker installed on hosted runner
            "docker",
            "exec",
            os.environ["POSTGRES_CONTAINER"],
            "psql",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE TABLE readings AS SELECT now() - interval '20 minutes' "
            "+ i * interval '1 minute' AS time, "
            "CASE WHEN i IN (3,4,12,13) THEN 40.0 ELSE 20.0 END AS temp "
            "FROM generate_series(0,20) AS i;",
        ],
        check=True,
    )
    start = int(time.time()) - 1200
    lines = "\n".join(
        f"readings temp={40.0 if i in (3, 4, 12, 13) else 20.0} {start + i * 60}" for i in range(21)
    )
    request = urllib.request.Request(
        "http://localhost:8181/api/v3/write_lp?db=telemetry&precision=second",
        data=lines.encode(),
        method="POST",
    )
    attempts = 30
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=5):  # noqa: S310 -- fixed localhost CI service
                return
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
            time.sleep(2)


if __name__ == "__main__":
    main()
