"""Generate a deterministic corpus of malformed MCAP files."""

import pathlib

MCAP_MAGIC = b"\x89MCAP0\r\n"


def write_corpus(dest: pathlib.Path) -> list[pathlib.Path]:
    """Write malformed MCAP files into ``dest`` and return their paths."""
    dest.mkdir(parents=True, exist_ok=True)
    cases: dict[str, bytes] = {
        "empty.mcap": b"",
        "magic_only.mcap": MCAP_MAGIC,
        "bad_magic.mcap": b"\x00NOPE0\r\n" + b"\x00" * 64,
        "truncated_header.mcap": MCAP_MAGIC + b"\x01\x05",
        "truncated_midchunk.mcap": MCAP_MAGIC + b"\x06" + b"\xff" * 200,
        "no_end_magic.mcap": MCAP_MAGIC + b"\x00" * 512,
        "zstd_bomb.mcap": MCAP_MAGIC + b"\x28\xb5\x2f\xfd" + b"\x00" * 128,
    }
    paths = []
    for name, data in cases.items():
        p = dest / name
        p.write_bytes(data)
        paths.append(p)
    return paths
