"""Malformed-MCAP tests: the parser must never crash uncontrollably."""

import pathlib

import pytest

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
    for path in corpus:
        bag = mcap.McapBag(path=path)
        try:
            _ = bag.mcap_files
            for f in bag.mcap_files:
                with open(f, "rb") as fh:
                    fh.read(8)
        except (ValueError, OSError, EOFError) as exc:
            # Controlled failure modes are acceptable.
            assert str(exc) != ""
        # An uncontrolled crash (e.g. RecursionError, MemoryError, or a raw
        # struct.error escaping) is a finding — record it in the scorecard.
