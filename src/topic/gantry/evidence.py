"""A topic registry for Gantry Bench evidence bundles."""

import pyarrow as pa

from src.di import module
from src.source.gantry.evidence import EvidenceBundle
from src.topic import base

#: Which gate of the harness produced each table, for wiring that gate's
#: one-line verdict into the topic description.
TABLE_GATE = {
    "signal_pairs": "g2",
    "ladder": "g3",
    "ladder_vs_baseline": "g3",
}

#: What each table is, in words an agent can act on.
TABLE_DOCS = {
    "gates": "One row per gate of the gauntlet (g0 intake, g1 data report, g2 signal "
    "check, g3 robot test): status, one-line verdict, timing and cost.",
    "findings": "Every finding any gate raised: a dotted code, its severity, a plain "
    "summary, and the prescription for fixing it.",
    "measures": "Every number a gate measured, with sample size, confidence interval "
    "bounds, units and method. A measurement is not a complaint; gates that measure "
    "a lot and flag nothing are good outcomes.",
    "abstained": "Feedback modules that declined to judge, with their reason. Kept "
    "because a report that silently omits what it could not judge reads as a clean "
    "bill of health.",
    "signal_pairs": "The signal check's working: one row per held-out clip, the "
    "prediction error of a fit on the real data (error_yours) against the same fit "
    "with actions detached from frames (error_shuffled). better=true means the real "
    "data won that clip. The margin is error_shuffled - error_yours.",
    "ladder": "The robot test's ladder: one row per rung per arm, with wins, trials, "
    "success rate and its confidence interval. Rungs order from first contact to "
    "solved; an unmeasured arm has no rate, not a zero.",
    "ladder_vs_baseline": "The per-rung contrast against the baseline arm: how many "
    "paired scenes separated the arms, with any test detail as JSON.",
    "events": "The submission's timeline: uploads, gate queued/started/finished, "
    "retries. The only genuinely temporal topic in the bundle.",
    "coach": "Generated advice: 'point' rows are improvement suggestions, 'fix' rows "
    "answer a specific finding code.",
}

#: Column-level context, attached as Arrow field metadata for schema readers.
COLUMN_DOCS = {
    "signal_pairs": {
        "episode": "held-out episode id, never seen by the fit being scored",
        "error_yours": "mean squared error predicting held-out actions from frames",
        "error_shuffled": "same fit, same clips, actions detached from their frames",
        "better": "whether the real pairing beat its own shuffled control on this clip",
    },
    "ladder": {
        "rung": "task stage, from first contact to solved",
        "rung_index": "position of the rung on the ladder, 0 first",
        "arm": "which trained policy: the submitted data's, a control, or the baseline",
        "measured": "false means no rollouts produced this number; absent, not zero",
        "rate": "wins / n on this rung",
        "unmeasured": "scenes with no reading for this rung",
    },
    "measures": {
        "measure": "dotted name of the quantity, e.g. held_out.error_your_data",
        "n": "sample size behind the value",
        "ci_lo": "lower bound of the 95% interval, empty where none was computed",
        "ci_hi": "upper bound of the 95% interval, empty where none was computed",
    },
}


def _topic(bundle: EvidenceBundle, topic: str) -> pa.Table:
    if topic not in bundle.tables:
        raise base.TopicNotFoundError(topic)
    return bundle.tables[topic]


class TopicRegistry(base.TopicRegistry):
    """A topic registry for Gantry Bench evidence bundles.

    Each table of the bundle is one topic. Descriptions carry the owning
    gate's one-line verdict from the manifest, so an agent reading the topic
    list already holds the conclusion its queries will be auditing.
    """

    def available_topics(self, data_source: EvidenceBundle) -> list[str]:
        """Return a list of available topic names."""
        return sorted(data_source.tables)

    def native_type_name(self, topic: str, data_source: EvidenceBundle) -> str:
        """Return the native type name for the given topic."""
        _topic(data_source, topic)
        return f"gantry.bench/{topic}"

    def message_count(self, topic: str, data_source: EvidenceBundle) -> int:
        """Return the number of messages for the given topic."""
        return _topic(data_source, topic).num_rows

    def struct(self, topic: str, data_source: EvidenceBundle) -> pa.StructType:
        """Return the PyArrow StructType for the given topic, with column docs."""
        table = _topic(data_source, topic)
        docs = COLUMN_DOCS.get(topic, {})
        fields = [
            pa.field(
                f.name,
                f.type,
                metadata={base.DESCRIPTION_KEY: docs[f.name]} if f.name in docs else None,
            )
            for f in table.schema
        ]
        return pa.struct(fields)

    def describe(self, topic: str, data_source: EvidenceBundle) -> str:
        """Return a human-readable description of the given topic."""
        _topic(data_source, topic)
        text = TABLE_DOCS.get(topic, f"Evidence table '{topic}' from a Gantry Bench verdict.")
        gate_key = TABLE_GATE.get(topic)
        for gate in data_source.manifest.get("gates", []):
            if gate.get("key") == gate_key and gate.get("summary"):
                text += f" The gate's verdict: {gate['summary']}"
        return text


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
