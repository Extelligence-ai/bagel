"""A miniature Gantry Bench evidence bundle for tests.

Shapes mirror what Gantry Bench's exporter writes (bench/api/app/evidence.py
in github.com/ShaikZaidHaaris/gantry): a manifest.json carrying the magic,
the submission header, per-gate verdicts and a per-table column-type map,
beside one CSV per table.
"""

import json
import pathlib

MANIFEST = {
    "magic": "GANTRY_EVIDENCE",
    "format_version": 1,
    "generated_at": "2026-08-02T10:00:00+00:00",
    "submission": {
        "id": "sample_two_handed",
        "name": "Two-handed pick",
        "status": "awaiting_user",
        "current_gate": "g3",
        "created_at": "2026-08-01T17:52:10+00:00",
        "benchmark": {"key": "pick_dual_bottles", "name": "Pick dual bottles"},
        "demo": True,
    },
    "dataset": {"version": 1, "bytes": 9888178, "detected": {"episodes": 58, "fps": 25.0}},
    "gates": [
        {
            "key": "g2",
            "name": "Signal check",
            "status": "passed",
            "summary": "your data beat its own shuffled control on 3 of 3 held-out clips",
        },
        {
            "key": "g3",
            "name": "Robot test",
            "status": "passed",
            "summary": "solved 4/50 scenes against the baseline's 12/100",
        },
    ],
    "g3_context": {"alpha": 0.05, "has_control": False, "has_baseline": True},
    "coach_model": "",
    "tables": {
        "gates": {
            "file": "gates.csv",
            "rows": 2,
            "columns": {
                "gate": "string",
                "name": "string",
                "status": "string",
                "summary": "string",
                "trials": "int",
                "cost_cents": "int",
                "started_at": "timestamp",
                "finished_at": "timestamp",
            },
        },
        "signal_pairs": {
            "file": "signal_pairs.csv",
            "rows": 3,
            "columns": {
                "episode": "string",
                "error_yours": "float",
                "error_shuffled": "float",
                "better": "bool",
            },
        },
        "ladder": {
            "file": "ladder.csv",
            "rows": 4,
            "columns": {
                "rung": "string",
                "rung_index": "int",
                "arm": "string",
                "measured": "bool",
                "wins": "int",
                "n": "int",
                "rate": "float",
                "ci_lo": "float",
                "ci_hi": "float",
                "unmeasured": "int",
            },
        },
        "events": {
            "file": "events.csv",
            "rows": 3,
            "columns": {
                "ts": "timestamp",
                "kind": "string",
                "gate": "string",
                "detail": "json",
            },
        },
    },
}

CSVS = {
    "gates.csv": (
        "gate,name,status,summary,trials,cost_cents,started_at,finished_at\n"
        "g2,Signal check,passed,beat its own shuffled control,0,0,"
        "2026-08-01T17:52:20+00:00,2026-08-01T17:52:30+00:00\n"
        "g3,Robot test,passed,solved 4/50 scenes,50,0,"
        "2026-08-01T17:53:00+00:00,2026-08-01T19:00:00+00:00\n"
    ),
    "signal_pairs.csv": (
        "episode,error_yours,error_shuffled,better\n"
        "ep_000,0.016,0.033,true\n"
        "ep_001,0.014,0.024,true\n"
        "ep_002,0.031,0.029,false\n"
    ),
    "ladder.csv": (
        "rung,rung_index,arm,measured,wins,n,rate,ci_lo,ci_hi,unmeasured\n"
        "lifted,0,your data,true,50,50,1.0,0.9286,1.0,0\n"
        "lifted,0,baseline,false,,0,,,,100\n"
        "solved,4,your data,true,4,50,0.08,0.0316,0.1892,0\n"
        "solved,4,baseline,true,12,100,0.12,0.0702,0.1976,0\n"
    ),
    "events.csv": (
        "ts,kind,gate,detail\n"
        '2026-08-01T17:52:10+00:00,submission.created,,"{""name"": ""Two-handed pick""}"\n'
        "2026-08-01T17:52:20+00:00,gate.started,g2,{}\n"
        '2026-08-01T17:52:30+00:00,gate.finished,g2,"{""status"": ""passed""}"\n'
    ),
}


def write_bundle(directory: pathlib.Path) -> pathlib.Path:
    """Write the miniature bundle into ``directory`` and return it."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(json.dumps(MANIFEST, indent=1))
    for name, content in CSVS.items():
        (directory / name).write_text(content)
    return directory
