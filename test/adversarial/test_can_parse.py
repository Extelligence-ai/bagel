"""Adversarial/fuzz tests for the raw CAN log source (issue #134).

``CanLog`` has TWO distinct untrusted-input surfaces:

1. The DBC schema, parsed by ``cantools.database.load_file`` in
   ``CanLog.__init__``. Characterization showed a malformed/garbage/empty DBC
   raises ``cantools.database.errors.UnsupportedDatabaseFormatError`` (a
   ``cantools.errors.Error`` subclass), while a DBC path with an extension
   cantools doesn't recognize (e.g. ``.txt``) raises a plain ``ValueError``
   from cantools' own format-dispatch code -- not a domain error either way.

2. The CAN capture (``.blf``/``.asc``), opened and iterated by
   ``can.LogReader`` and decoded via ``cantools``'s
   ``Database.decode_message`` in ``CanLog.records``. Characterization
   showed:
   - A ``.blf`` with the correct ``LOGG`` magic but a corrupt/truncated body
     raises ``struct.error`` (on very short files) or ``ValueError``
     (`"read length must be non-negative or -1"`, on files with an
     internally-inconsistent header) from deep inside python-can's BLF
     reader, both at ``can.LogReader(...)`` construction time.
   - A ``.blf``/``.asc`` path whose extension python-can doesn't recognize
     raises ``ValueError`` ("No read support for unknown log format ...").
   - A syntactically-parseable ``.asc`` whose frame line has a non-hex data
     byte raises ``ValueError`` ("invalid literal for int() with base 16")
     during frame iteration (not at construction).
   - A frame whose declared data length doesn't match its DBC message
     definition raises ``cantools.database.errors.DecodeError`` ("Wrong data
     size: N instead of M bytes") from ``decode_message``. Unlike the
     structural failures above, this is a per-frame problem: ``CanLog``
     skips and counts that one frame (mirroring how unknown-arbitration-id
     frames are handled) rather than aborting the whole capture.

Structural capture failures (can't open/iterate the container at all) must
never reach a caller as a raw library traceback -- they come out as a single
clean, typed error (``errors.InvalidPathError``).
"""

import pathlib

import pytest

pytest.importorskip("can")
pytest.importorskip("cantools")

import can
import cantools

from src.source import errors
from src.source.automotive import can as can_source

VALID_DBC = """VERSION ""

NS_ :

BS_:

BU_: ECU

BO_ 100 EngineData: 8 ECU
 SG_ Speed : 0|16@1+ (1,0) [0|65535] "" ECU
"""


def _write_valid_dbc(tmp_path: pathlib.Path) -> pathlib.Path:
    dbc = tmp_path / "valid.dbc"
    dbc.write_text(VALID_DBC)
    return dbc


def _write_blf(tmp_path: pathlib.Path, name: str, messages: list[can.Message]) -> pathlib.Path:
    path = tmp_path / name
    with can.BLFWriter(str(path)) as writer:
        for msg in messages:
            writer.on_message_received(msg)
    return path


def _write_valid_blf(tmp_path: pathlib.Path, *, data: bytes = bytes(range(8))) -> pathlib.Path:
    msg = can.Message(arbitration_id=0x64, data=data, is_extended_id=False, timestamp=1.0)
    return _write_blf(tmp_path, "valid.blf", [msg])


# ---------------------------------------------------------------------------
# Surface 1: DBC schema parsing (CanLog.__init__ -> cantools.database.load_file)
# Before hardening: raw cantools.database.errors.UnsupportedDatabaseFormatError
# or a raw ValueError escape from CanLog.__init__.
# ---------------------------------------------------------------------------


def test_garbage_dbc_raises_clean_error(tmp_path: pathlib.Path) -> None:
    dbc = tmp_path / "garbage.dbc"
    dbc.write_bytes(b"this is not a dbc file \x00\x01\x02 garbage {{{ ]] not valid at all\n")
    blf = _write_valid_blf(tmp_path)

    with pytest.raises(errors.InvalidPathError):
        can_source.CanLog(path=str(blf), dbc=str(dbc))


def test_empty_dbc_raises_clean_error(tmp_path: pathlib.Path) -> None:
    dbc = tmp_path / "empty.dbc"
    dbc.write_bytes(b"")
    blf = _write_valid_blf(tmp_path)

    with pytest.raises(errors.InvalidPathError):
        can_source.CanLog(path=str(blf), dbc=str(dbc))


def test_dbc_with_unrecognized_extension_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """cantools dispatches load format by extension; ``.txt`` raises a raw ValueError."""
    dbc = tmp_path / "garbage.txt"
    dbc.write_bytes(b"garbage")
    blf = _write_valid_blf(tmp_path)

    with pytest.raises(errors.InvalidPathError):
        can_source.CanLog(path=str(blf), dbc=str(dbc))


def test_missing_dbc_raises_file_not_found_already(tmp_path: pathlib.Path) -> None:
    """Already clean: cantools itself raises a plain FileNotFoundError. Regression guard."""
    blf = _write_valid_blf(tmp_path)

    with pytest.raises(FileNotFoundError):
        can_source.CanLog(path=str(blf), dbc=str(tmp_path / "does_not_exist.dbc"))


# ---------------------------------------------------------------------------
# Surface 2: CAN capture parsing (CanLog.records -> can.LogReader + decode_message)
# Before hardening: raw struct.error / ValueError from python-can, or a raw
# cantools.database.errors.DecodeError from decode_message.
# ---------------------------------------------------------------------------


def test_empty_blf_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Before hardening: struct.error escapes can.LogReader(...) construction."""
    dbc = _write_valid_dbc(tmp_path)
    blf = tmp_path / "empty.blf"
    blf.write_bytes(b"")

    log = can_source.CanLog(path=str(blf), dbc=str(dbc))
    with pytest.raises(errors.InvalidPathError):
        _ = log.records


def test_corrupt_blf_header_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Has the LOGG magic but an internally-inconsistent header.

    Before hardening: ValueError("read length must be non-negative or -1")
    escapes can.LogReader(...) construction.
    """
    dbc = _write_valid_dbc(tmp_path)
    blf = tmp_path / "corrupt_header.blf"
    blf.write_bytes(b"LOGG" + b"\x00" * 20 + b"garbage garbage garbage not a real blf structure")

    log = can_source.CanLog(path=str(blf), dbc=str(dbc))
    with pytest.raises(errors.InvalidPathError):
        _ = log.records


def test_unrecognized_capture_extension_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Before hardening: ValueError("No read support for unknown log format ...")."""
    dbc = _write_valid_dbc(tmp_path)
    capture = tmp_path / "capture.weird"
    capture.write_bytes(b"garbage")

    log = can_source.CanLog(path=str(capture), dbc=str(dbc))
    with pytest.raises(errors.InvalidPathError):
        _ = log.records


def test_asc_with_non_hex_frame_data_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Syntactically valid ASC header, but a frame data byte isn't valid hex.

    Before hardening: ValueError("invalid literal for int() with base 16:
    'ZZ'") escapes during frame iteration (not construction).
    """
    dbc = _write_valid_dbc(tmp_path)
    asc = tmp_path / "bad_frame.asc"
    asc.write_text(
        "date Mon Jan 1 00:00:00 2024\n"
        "base hex  timestamps absolute\n"
        "no internal events logged\n"
        "\n"
        "   1.000000 1  100             Rx   d 8 01 02 03 04 05 06 07 ZZ\n"
    )

    log = can_source.CanLog(path=str(asc), dbc=str(dbc))
    with pytest.raises(errors.InvalidPathError):
        _ = log.records


def test_frame_data_length_mismatch_is_skipped_not_fatal(tmp_path: pathlib.Path) -> None:
    """One bad-known-id frame among several good ones must not nuke the capture.

    A frame for a known DBC message whose data is shorter than the message
    length makes decode_message raise cantools.database.errors.DecodeError
    ("Wrong data size: 2 instead of 8 bytes"). This is a per-frame problem,
    not a structural one: CanLog.records should skip and count that single
    frame (like it already does for unknown arbitration IDs) and still
    return the good frames, rather than raising for the whole capture.
    """
    dbc = _write_valid_dbc(tmp_path)
    good = [
        can.Message(arbitration_id=0x64, data=bytes(range(8)), is_extended_id=False, timestamp=t)
        for t in (1.0, 2.0, 3.0)
    ]
    bad = can.Message(arbitration_id=0x64, data=b"\x01\x02", is_extended_id=False, timestamp=4.0)
    blf = _write_blf(tmp_path, "mixed.blf", [*good, bad])

    log = can_source.CanLog(path=str(blf), dbc=str(dbc))
    records = log.records

    assert len(records) == len(good)
    assert all(name == "EngineData" for _, name, _ in records)


def test_missing_capture_file_raises_file_not_found_already(tmp_path: pathlib.Path) -> None:
    """Already clean: python-can itself raises a plain FileNotFoundError. Regression guard."""
    dbc = _write_valid_dbc(tmp_path)
    log = can_source.CanLog(path=str(tmp_path / "does_not_exist.blf"), dbc=str(dbc))

    with pytest.raises(FileNotFoundError):
        _ = log.records


def test_asc_with_only_unrecognized_lines_is_already_clean(tmp_path: pathlib.Path) -> None:
    """python-can's ASC reader is lenient: garbage lines are silently skipped,
    not raised. Already clean (no exception, just zero records). Kept as a
    regression guard against accidentally over-tightening the capture guard.
    """
    dbc = _write_valid_dbc(tmp_path)
    asc = tmp_path / "garbage.asc"
    asc.write_text("this is not an asc file\nrandom garbage\n\x00\x01\x02")

    log = can_source.CanLog(path=str(asc), dbc=str(dbc))
    assert log.records == []


# ---------------------------------------------------------------------------
# Sanity: genuinely valid DBC + capture still builds and decodes normally --
# hardening the parse-failure paths must not affect the happy path.
# ---------------------------------------------------------------------------


def test_valid_dbc_and_capture_still_decode_correctly(tmp_path: pathlib.Path) -> None:
    dbc = _write_valid_dbc(tmp_path)
    blf = _write_valid_blf(tmp_path)

    log = can_source.CanLog(path=str(blf), dbc=str(dbc))
    records = log.records

    assert len(records) == 1
    timestamp, name, signals = records[0]
    assert isinstance(timestamp, float)
    assert name == "EngineData"
    assert signals == {"Speed": 256}
    assert isinstance(log.database, cantools.database.can.database.Database)
