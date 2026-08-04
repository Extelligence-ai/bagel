"""Adversarial/fuzz tests for the PyArrow CSV/JSON sources (issue #134).

A malformed CSV/JSON input must produce a CLEAN, typed error --
``errors.InvalidPathError`` (or another clearly-typed domain error) -- never a
raw ``pyarrow.lib.ArrowInvalid``/``ArrowTypeError`` traceback, an uncontrolled
crash (e.g. a raw ``UnicodeDecodeError``), or a hang.

Call-flow / boundary characterization (empirically determined against the
unhardened code):

- ``pyarrow.dataset.dataset(...)`` (the ``ds.dataset()`` call) is LAZY about
  reading row *data*, but it is NOT lazy about schema inference: for CSV and
  JSON formats it must read (at least part of) the file immediately to infer
  a schema. Whether that inference failure surfaces as an exception depends
  entirely on ``exclude_invalid_files``:

  * ``exclude_invalid_files=True`` (the default): PyArrow performs its own
    internal per-file validity check during dataset construction and
    silently EXCLUDES any file that fails it. ``SourceFactory._build()``
    (``src/source/pyarrow/base.py``) now closes that silent-data-loss gap
    (#134): if EVERY candidate file was excluded, the resulting dataset
    would be indistinguishable from "path legitimately has no matching
    events", so ``_build()`` raises a typed ``errors.InvalidPathError``
    instead of returning an empty dataset. If only SOME files under a
    directory were excluded, the good files still build normally, but the
    drop is surfaced via a ``logging.warning`` call and recorded as
    ``excluded_file_count`` in ``SourceFactory.metadata`` so it isn't
    silently invisible to a caller.
  * ``exclude_invalid_files=False`` (an explicit, first-class, user-settable
    constructor argument on both ``csv.SourceFactory`` and
    ``json.SourceFactory`` -- see their docstrings, which already warn "this
    will incur IO ... resulting in an error at scan time" when disabled):
    a single malformed file raises ``pyarrow.lib.ArrowInvalid`` directly out
    of ``ds.dataset(...)`` inside ``SourceFactory._build()``
    (``src/source/pyarrow/base.py``). This is the PRIMARY crash boundary.
  * Also with ``exclude_invalid_files=False``: a DIRECTORY containing one
    valid file (whose schema is used) and one malformed file that still
    passes the lightweight Python-side ``is_csv_file``/``is_json_file``
    sniffer, will construct successfully (schema comes from the first good
    file) but raises ``pyarrow.lib.ArrowInvalid`` later, at
    ``data_source.dataset.to_table()`` inside
    ``src/message/pyarrow/base.py``'s ``MessageDataset._messages()``. This is
    the SECONDARY crash boundary.

- Separately (not a pyarrow exception at all): binary garbage with a
  ``.json`` extension was found to raise a raw, un-typed
  ``UnicodeDecodeError`` straight out of ``SourceFactory.__init__()`` (via
  ``validate_path()`` -> ``is_json_file()`` -> ``is_json_lines_file()`` /
  ``is_standard_json_file()`` in ``src/di/types/data_source.py``), because
  those two helpers only caught ``json.JSONDecodeError`` and not the
  ``UnicodeDecodeError`` that invalid UTF-8 bytes trigger during
  ``f.readline()`` / ``path.read_text()``. The sibling ``is_csv_file()`` in
  the same module already guards against exactly this
  (``except (csv.Error, UnicodeDecodeError)``), so this was a pre-existing
  gap in the JSON-specific helpers, now closed to match.

Both `pyarrow.lib.ArrowInvalid` and `pyarrow.lib.ArrowTypeError` (and other
Arrow-specific exceptions) share the common base `pyarrow.lib.ArrowException`
(itself unrelated to `ArrowInvalid`'s incidental `ValueError` parentage), so
that is what the hardening catches.
"""

import logging
import pathlib

import pytest

from src.di.types import data_source as data_source_types
from src.message.pyarrow.base import MessageDataset
from src.source import errors
from src.source.pyarrow import csv as csv_source
from src.source.pyarrow import json as json_source

# ---------------------------------------------------------------------------
# Vector 1 (PRIMARY CRASH -- _build() / ds.dataset()): a single malformed
# file, with exclude_invalid_files=False so PyArrow doesn't silently drop it.
# ---------------------------------------------------------------------------


def test_csv_ragged_rows_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Ragged rows (inconsistent column counts) that survive the CSV sniffer.

    A handful of well-formed rows establishes the dialect for
    ``csv.Sniffer()``; a single short row later in the same file is enough
    to make PyArrow's real CSV parser raise, but not necessarily enough to
    trip the lenient Python sniffer used by ``validate_path()``.
    """
    bad = tmp_path / "ragged.csv"
    rows = ["a,b,c", *[f"{i},{i + 1},{i + 2}" for i in range(20)], "99,100"]
    bad.write_text("\n".join(rows) + "\n")

    factory = csv_source.SourceFactory(str(bad), exclude_invalid_files=False)
    with pytest.raises(errors.InvalidPathError):
        factory.build()


def test_csv_unterminated_quote_raises_clean_error(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "unterminated.csv"
    rows = ["a,b,c", *[f"{i},{i + 1},{i + 2}" for i in range(20)], '99,"100,200']
    bad.write_text("\n".join(rows) + "\n")

    factory = csv_source.SourceFactory(str(bad), exclude_invalid_files=False)
    with pytest.raises(errors.InvalidPathError):
        factory.build()


def test_json_array_instead_of_ndjson_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """PyArrow's JSON reader expects newline-delimited JSON, not a JSON array."""
    bad = tmp_path / "array.json"
    bad.write_text('[{"a": 1}, {"a": 2}]')

    factory = json_source.SourceFactory(str(bad), exclude_invalid_files=False)
    with pytest.raises(errors.InvalidPathError):
        factory.build()


def test_json_type_conflicting_rows_raises_clean_error(tmp_path: pathlib.Path) -> None:
    """Same key changes type (number -> object) across NDJSON rows."""
    bad = tmp_path / "mixed_types.json"
    bad.write_text('{"a": 1}\n{"a": {"b": 2}}\n')

    factory = json_source.SourceFactory(str(bad), exclude_invalid_files=False)
    with pytest.raises(errors.InvalidPathError):
        factory.build()


def test_json_broken_second_line_raises_clean_error(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "broken_line.json"
    bad.write_text('{"a": 1}\n{"a": 2\n{"a": 3}\n')

    factory = json_source.SourceFactory(str(bad), exclude_invalid_files=False)
    with pytest.raises(errors.InvalidPathError):
        factory.build()


# ---------------------------------------------------------------------------
# Vector 2 (SECONDARY CRASH -- to_table() inside message/pyarrow/base.py):
# a directory where the dataset schema comes from a valid file, but a
# malformed sibling file (which still passes the Python-side sniffer) blows
# up only once the dataset is actually scanned.
# ---------------------------------------------------------------------------


def test_directory_with_malformed_sibling_raises_clean_error_at_to_table(
    tmp_path: pathlib.Path,
) -> None:
    directory = tmp_path / "mixed"
    directory.mkdir()
    (directory / "a_good.csv").write_text("a,b,c\n1,2,3\n4,5,6\n")
    rows = ["a,b,c", *[f"{i},{i + 1},{i + 2}" for i in range(20)], "99,100"]
    (directory / "z_bad.csv").write_text("\n".join(rows) + "\n")

    factory = csv_source.SourceFactory(str(directory), exclude_invalid_files=False)
    data_source = factory.build()  # succeeds: schema comes from the good file

    message_dataset = MessageDataset()
    with pytest.raises(errors.InvalidPathError):
        list(
            message_dataset._messages(
                data_source,
                topics=["message"],
                start_seconds_inclusive=None,
                end_seconds_inclusive=None,
            )
        )


# ---------------------------------------------------------------------------
# Vector 3: binary garbage with a .json extension -- not a pyarrow exception
# at all, but a raw UnicodeDecodeError leaking out of the JSON sniffer
# helpers in src/di/types/data_source.py, straight out of
# SourceFactory.__init__() (via validate_path()).
# ---------------------------------------------------------------------------


def test_json_binary_garbage_raises_clean_error_not_unicode_decode_error(
    tmp_path: pathlib.Path,
) -> None:
    bad = tmp_path / "binary.json"
    bad.write_bytes(bytes(range(256)) * 4)

    with pytest.raises(ValueError):
        json_source.SourceFactory(str(bad))


def test_is_json_lines_file_does_not_raise_on_invalid_utf8(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "binary.json"
    bad.write_bytes(bytes(range(256)) * 4)

    assert data_source_types.is_json_lines_file(bad) is False


def test_is_standard_json_file_does_not_raise_on_invalid_utf8(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "binary.json"
    bad.write_bytes(bytes(range(256)) * 4)

    assert data_source_types.is_standard_json_file(bad) is False


# ---------------------------------------------------------------------------
# Vectors that are ALREADY CLEAN (or already don't crash) -- kept as
# regression guards so future refactors can't silently reopen them.
# ---------------------------------------------------------------------------


def test_csv_binary_garbage_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "binary.csv"
    bad.write_bytes(bytes(range(256)) * 4)

    with pytest.raises(ValueError):
        csv_source.SourceFactory(str(bad))


def test_csv_empty_file_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")

    with pytest.raises(ValueError):
        csv_source.SourceFactory(str(empty))


def test_json_empty_file_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")

    with pytest.raises(ValueError):
        json_source.SourceFactory(str(empty))


def test_json_invalid_syntax_raises_clean_error_already(tmp_path: pathlib.Path) -> None:
    bad = tmp_path / "invalid.json"
    bad.write_text("{not valid json!!!")

    with pytest.raises(ValueError):
        json_source.SourceFactory(str(bad))


def test_csv_header_only_file_does_not_crash_already(tmp_path: pathlib.Path) -> None:
    """Header-only CSV: builds fine and yields zero rows, no crash."""
    header_only = tmp_path / "header_only.csv"
    header_only.write_text("a,b,c\n")

    factory = csv_source.SourceFactory(str(header_only))
    data_source = factory.build()
    message_dataset = MessageDataset()
    messages = list(
        message_dataset._messages(
            data_source,
            topics=["message"],
            start_seconds_inclusive=None,
            end_seconds_inclusive=None,
        )
    )
    assert messages == []


def test_malformed_single_file_raises_instead_of_silently_dropping(
    tmp_path: pathlib.Path,
) -> None:
    """#134: a source whose every file is excluded is an error, not 0 rows.

    Uses the same ragged-rows fixture as ``test_csv_ragged_rows_raises_clean_error``:
    it passes the lightweight Python-side ``is_csv_file`` sniffer (so
    ``SourceFactory.__init__`` accepts the path) but PyArrow's own internal
    validity check excludes it during ``ds.dataset()`` discovery under the
    default ``exclude_invalid_files=True``.
    """
    bad = tmp_path / "ragged.csv"
    rows = ["a,b,c", *[f"{i},{i + 1},{i + 2}" for i in range(20)], "99,100"]
    bad.write_text("\n".join(rows) + "\n")

    factory = csv_source.SourceFactory(str(bad))  # default exclude_invalid_files=True
    with pytest.raises(errors.InvalidPathError, match="excluded as invalid"):
        factory.build()


def test_directory_with_one_bad_file_warns_and_counts(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """#134: partial drops keep working but leave a warning and metadata count."""
    (tmp_path / "good.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    rows = ["a,b,c", *[f"{i},{i + 1},{i + 2}" for i in range(20)], "99,100"]
    (tmp_path / "ragged.csv").write_text("\n".join(rows) + "\n")
    factory = csv_source.SourceFactory(str(tmp_path))
    with caplog.at_level(logging.WARNING):
        built = factory.build()
    assert built.dataset.count_rows() == 1
    assert factory.metadata["excluded_file_count"] == 1
    assert any("excluded" in record.message for record in caplog.records)


def test_metadata_excluded_count_defaults_to_zero(tmp_path: pathlib.Path) -> None:
    (tmp_path / "good.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    factory = csv_source.SourceFactory(str(tmp_path))
    assert factory.metadata["excluded_file_count"] == 0


# ---------------------------------------------------------------------------
# Sanity: valid CSV/JSON files still build and materialize correctly --
# hardening the crash boundaries must not affect the happy path.
# ---------------------------------------------------------------------------


def test_valid_csv_file_still_builds_and_materializes_correctly(tmp_path: pathlib.Path) -> None:
    good = tmp_path / "good.csv"
    good.write_text("a,b,c\n1,2,3\n4,5,6\n")

    factory = csv_source.SourceFactory(str(good))
    data_source = factory.build()
    message_dataset = MessageDataset()
    messages = list(
        message_dataset._messages(
            data_source,
            topics=["message"],
            start_seconds_inclusive=None,
            end_seconds_inclusive=None,
        )
    )
    assert len(messages) == 2
    assert {msg["a"] for _, _, msg in messages} == {1, 4}


def test_valid_json_file_still_builds_and_materializes_correctly(tmp_path: pathlib.Path) -> None:
    good = tmp_path / "good.json"
    good.write_text('{"a": 1}\n{"a": 2}\n')

    factory = json_source.SourceFactory(str(good))
    data_source = factory.build()
    message_dataset = MessageDataset()
    messages = list(
        message_dataset._messages(
            data_source,
            topics=["message"],
            start_seconds_inclusive=None,
            end_seconds_inclusive=None,
        )
    )
    assert len(messages) == 2
    assert {msg["a"] for _, _, msg in messages} == {1, 2}
