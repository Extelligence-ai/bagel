import pathlib

import pytest

from src.source.gantry import evidence as source
from src.topic import base
from src.topic.gantry import evidence as topic
from test._fixtures import gantry_evidence


@pytest.fixture()
def bundle(tmp_path: pathlib.Path) -> source.EvidenceBundle:
    path = gantry_evidence.write_bundle(tmp_path / "bundle")
    return source.SourceFactory(path=str(path)).build()


def test_should_list_every_table_as_a_topic(bundle: source.EvidenceBundle) -> None:
    # WHEN
    topics = topic.TopicRegistry().available_topics(bundle)

    # THEN
    assert topics == sorted(gantry_evidence.MANIFEST["tables"])


def test_should_count_messages_from_the_table(bundle: source.EvidenceBundle) -> None:
    # WHEN
    count = topic.TopicRegistry().message_count("signal_pairs", bundle)

    # THEN
    assert count == 3


def test_should_attach_column_docs_to_the_struct(bundle: source.EvidenceBundle) -> None:
    # WHEN
    struct = topic.TopicRegistry().struct("signal_pairs", bundle)

    # THEN
    field = struct.field("error_shuffled")
    assert b"detached" in field.metadata[base.DESCRIPTION_KEY.encode()]


def test_should_carry_the_gates_verdict_into_the_description(bundle: source.EvidenceBundle) -> None:
    # WHEN
    description = topic.TopicRegistry().describe("signal_pairs", bundle)

    # THEN the topic explains itself and quotes the verdict it evidences
    assert "held-out clip" in description
    assert "shuffled control on 3 of 3" in description


def test_should_raise_for_unknown_topic(bundle: source.EvidenceBundle) -> None:
    # WHEN / THEN
    with pytest.raises(base.TopicNotFoundError):
        topic.TopicRegistry().struct("nope", bundle)
