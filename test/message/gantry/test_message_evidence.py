import pathlib

import pytest

from src.message.gantry import evidence as message
from src.source.gantry import evidence as source
from test._fixtures import gantry_evidence

CREATED_AT_SECONDS = 1785606730.0  # 2026-08-01T17:52:10+00:00


@pytest.fixture()
def bundle(tmp_path: pathlib.Path) -> source.EvidenceBundle:
    path = gantry_evidence.write_bundle(tmp_path / "bundle")
    return source.SourceFactory(path=str(path)).build()


def test_should_stamp_events_with_their_own_time(bundle: source.EvidenceBundle) -> None:
    # WHEN
    rows = list(message.MessageDataset()._messages(bundle, ["events"], None, None))

    # THEN
    assert [r[1] for r in rows] == sorted(r[1] for r in rows)
    first_topic, first_seconds, first_row = rows[0]
    assert first_topic == "events"
    assert first_seconds == CREATED_AT_SECONDS
    assert first_row["kind"] == "submission.created"


def test_should_stamp_static_tables_with_the_submissions_birth(
    bundle: source.EvidenceBundle,
) -> None:
    # WHEN
    rows = list(message.MessageDataset()._messages(bundle, ["signal_pairs"], None, None))

    # THEN every row carries the same deterministic instant
    assert {r[1] for r in rows} == {CREATED_AT_SECONDS}


def test_should_stamp_gates_with_their_start(bundle: source.EvidenceBundle) -> None:
    # WHEN
    rows = list(message.MessageDataset()._messages(bundle, ["gates"], None, None))

    # THEN
    by_gate = {r[2]["gate"]: r[1] for r in rows}
    assert by_gate["g2"] < by_gate["g3"]


def test_should_merge_topics_in_time_order(bundle: source.EvidenceBundle) -> None:
    # WHEN
    rows = list(
        message.MessageDataset()._messages(bundle, ["events", "gates", "ladder"], None, None)
    )

    # THEN
    seconds = [r[1] for r in rows]
    assert seconds == sorted(seconds)
    assert {r[0] for r in rows} == {"events", "gates", "ladder"}


def test_should_respect_time_windows(bundle: source.EvidenceBundle) -> None:
    # GIVEN a window that keeps only the g3 gate row
    start = CREATED_AT_SECONDS + 45  # after g2 started (t+10s), before g3 (t+50s)

    # WHEN
    rows = list(message.MessageDataset()._messages(bundle, ["gates"], start, None))

    # THEN
    assert [r[2]["gate"] for r in rows] == ["g3"]
