"""Adversarial/fuzz tests for the ASAM MDF4 source (issue #134).

A malformed ``.mf4`` file must produce a CLEAN, typed error from
``SourceFactory`` -- either ``errors.InvalidPathError`` (or another
clearly-typed domain error) -- never a raw ``asammdf``-internal traceback
(``struct.error``, ``UnicodeDecodeError``, ``AttributeError``,
``UnboundLocalError``, etc.) and never a hang.

Call-flow note: ``SourceFactory.__init__`` calls ``super().__init__(path)``
first, which (via ``base.FileBasedSourceFactory.__init__``) already invokes
``validate_path()`` and raises cleanly if the file doesn't exist, isn't a
file, or lacks the ``MDF_MAGIC`` header. So files with no/wrong magic are
*already* clean (see the "already clean" section below). The real gap is a
file that HAS the correct 8-byte magic but a corrupt/truncated body: it
passes ``validate_path()`` and then ``MDF(path)`` in ``__init__`` explodes
with whatever low-level exception ``asammdf``'s binary parser happens to hit
at the point of corruption -- observed, across different truncation points
of a real minimal MDF4 file, to be any of: ``struct.error``,
``UnicodeDecodeError``, ``AttributeError``, ``ValueError``,
``UnboundLocalError``. There is no common asammdf-specific base exception
across these -- they're raised from deep inside asammdf's block-unpacking
code as plain Python parsing errors.
"""

import pathlib

import pytest

pytest.importorskip("asammdf")

from src.source import errors
from src.source.automotive import mf4

MDF_MAGIC = b"MDF     "


# ---------------------------------------------------------------------------
# Vector 1 (PRIMARY CRASH): valid MDF_MAGIC header, corrupt/garbage body.
# Before hardening: raises whatever asammdf's parser raises (struct.error /
# UnicodeDecodeError / AttributeError / ValueError / UnboundLocalError,
# depending on exactly where the corruption falls) directly out of __init__.
# ---------------------------------------------------------------------------


def test_valid_magic_with_garbage_body_raises_clean_error(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "corrupt_body.mf4"
    bad.write_bytes(MDF_MAGIC + b"\x00\xff\x10\x20" * 50)

    with pytest.raises(errors.InvalidPathError):
        mf4.SourceFactory(str(bad))


def test_valid_magic_truncated_right_after_magic_raises_clean_error(
    tmp_path: pathlib.Path,
) -> None:
    """Only the 8-byte magic is present; the mandatory ID block is truncated.

    Before hardening: ``struct.error: unpack requires a buffer of 64 bytes``
    escapes from asammdf's ``FileIdentificationBlock`` unpacking.
    """
    bad = tmp_path / "truncated_after_magic.mf4"
    bad.write_bytes(MDF_MAGIC)

    with pytest.raises(errors.InvalidPathError):
        mf4.SourceFactory(str(bad))


def test_real_mdf_file_truncated_at_various_offsets_raises_clean_error(
    tmp_path: pathlib.Path,
) -> None:
    """Truncate a genuinely valid MDF4 file at many offsets.

    Characterization (against the unhardened code) showed this produces a
    grab-bag of distinct raw exception types depending on exactly where the
    cut falls: struct.error, AttributeError, ValueError, UnboundLocalError.
    After hardening, every one of these must come out as a single clean
    InvalidPathError instead.
    """
    import numpy as np
    from asammdf import MDF, Signal

    full_path = tmp_path / "real.mf4"
    working = MDF(version="4.10")
    signal = Signal(
        samples=np.array([1.0, 2.0, 3.0]),
        timestamps=np.array([0.0, 1.0, 2.0]),
        name="test_channel",
    )
    working.append([signal])
    working.save(str(full_path), overwrite=True)
    working.close()

    data = full_path.read_bytes()
    assert len(data) > 100  # sanity: the fixture file isn't trivially tiny

    for offset in (8, 16, 24, 40, 64, 100, 150, 300, 500, len(data) - 1):
        truncated = tmp_path / f"trunc_{offset}.mf4"
        truncated.write_bytes(data[:offset])

        with pytest.raises(errors.InvalidPathError):
            mf4.SourceFactory(str(truncated))


# ---------------------------------------------------------------------------
# Vectors that are ALREADY CLEAN via validate_path() -- kept as regression
# guards so future refactors of __init__'s ordering can't silently reopen
# the crash.
# ---------------------------------------------------------------------------


def test_empty_file_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    empty = tmp_path / "empty.mf4"
    empty.write_bytes(b"")

    with pytest.raises(errors.InvalidPathError):
        mf4.SourceFactory(str(empty))


def test_no_mdf_magic_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    not_mdf = tmp_path / "not_mdf.mf4"
    not_mdf.write_bytes(b"NOTMDF!!" + b"\x00" * 50)

    with pytest.raises(errors.InvalidPathError):
        mf4.SourceFactory(str(not_mdf))


def test_missing_file_raises_file_not_found_already(tmp_path: pathlib.Path) -> None:
    missing = tmp_path / "does_not_exist.mf4"

    with pytest.raises(FileNotFoundError):
        mf4.SourceFactory(str(missing))


def test_directory_instead_of_file_raises_clean_error_already(
    tmp_path: pathlib.Path,
) -> None:
    directory = tmp_path / "a_directory.mf4"
    directory.mkdir()

    with pytest.raises(errors.PathNotFileError):
        mf4.SourceFactory(str(directory))


# ---------------------------------------------------------------------------
# Sanity: a genuinely valid MDF4 file still builds and works normally --
# hardening the corrupt-body path must not affect the happy path.
# ---------------------------------------------------------------------------


def test_valid_mdf_file_still_builds_correctly(tmp_path: pathlib.Path) -> None:
    import asammdf
    import numpy as np
    from asammdf import Signal

    full_path = tmp_path / "valid.mf4"
    working = asammdf.MDF(version="4.10")
    signal = Signal(
        samples=np.array([1.0, 2.0, 3.0]),
        timestamps=np.array([0.0, 1.0, 2.0]),
        name="test_channel",
    )
    working.append([signal])
    working.save(str(full_path), overwrite=True)
    working.close()

    factory = mf4.SourceFactory(str(full_path))

    assert factory.total_message_count == 3
    assert isinstance(factory.build(), asammdf.MDF)
