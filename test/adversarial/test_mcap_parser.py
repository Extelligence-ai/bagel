"""Malformed-MCAP tests: the parser must never crash uncontrollably."""

import pathlib

import pytest
import zstandard
from mcap.exceptions import McapError

from src.source import mcap
from . import make_corpus


def test_corpus_is_generated(tmp_path: pathlib.Path) -> None:
    files = make_corpus.write_corpus(tmp_path)
    assert len(files) >= 5
    assert all(f.exists() for f in files)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> list[pathlib.Path]:
    return make_corpus.write_corpus(tmp_path_factory.mktemp("corpus"))


def test_malformed_mcap_raises_controlled_errors(corpus: list[pathlib.Path]) -> None:
    """Feed every malformed file through the real parser entry point.

    ``mcap.summaries()`` is the function production code actually calls (via
    ``SourceFactory._statistics`` / ``topic_information``) to read the MCAP
    summary section. Calling it here exercises the whole real path:
    ``McapBag.mcap_files`` (including the zstd-decompress step for
    ``*.mcap.zstd`` inputs) followed by ``make_reader(stream).get_summary()``.
    This is a genuine characterization test -- it documents what the parser
    actually does with each malformed file, rather than a raw byte read that
    never touches the parser.
    """
    behavior: dict[str, str] = {}
    for path in corpus:
        bag = mcap.McapBag(path=path)
        try:
            # Real parse entry point -- see docstring above.
            result = mcap.summaries(bag)
            behavior[path.name] = f"parsed without raising -> {result!r}"
        except (ValueError, OSError, EOFError, McapError, zstandard.ZstdError) as exc:
            # Controlled failure modes:
            #   - McapError (and its subclasses InvalidMagic, EndOfFile,
            #     RecordLengthLimitExceeded, DecoderNotFoundError,
            #     UnsupportedCompressionError) are raised by the mcap
            #     library itself for malformed containers.
            #   - OSError is raised by the mcap library's internal reads
            #     when a stream is truncated mid-record.
            #   - zstandard.ZstdError is raised when the zstd-decompress
            #     step (McapBag.mcap_files -> decompress()) hits a corrupt
            #     or invalid compressed frame.
            # All of these are typed, documented failures -- not crashes.
            behavior[path.name] = f"controlled failure: {type(exc).__name__}: {exc}"
        # Anything NOT in the except tuple above (RecursionError, MemoryError,
        # a raw struct.error, a segfault, ...) is an uncontrolled crash: it
        # propagates out of this try/except and fails the test. That would be
        # a genuine robustness finding to record, not something to silently
        # widen the except clause to swallow.

    print("\nMCAP parser behavior on the malformed corpus:")
    for name, outcome in behavior.items():
        print(f"  {name}: {outcome}")

    # Every corpus file produced a controlled outcome (clean parse or a typed
    # exception from the mcap/zstandard libraries) -- documented above.
    assert len(behavior) == len(corpus)
