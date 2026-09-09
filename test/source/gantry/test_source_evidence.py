import json
import pathlib

import pyarrow as pa
import pytest

from src.di.types import data_source
from src.source import errors
from src.source.gantry import evidence
from test._fixtures import gantry_evidence


def test_should_resolve_gantry_evidence_directory(tmp_path: pathlib.Path) -> None:
    # GIVEN
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")

    # WHEN
    result = data_source.resolve(str(bundle))

    # THEN
    assert result == data_source.DataSource.GANTRY_EVIDENCE


def test_should_not_resolve_directory_without_magic(tmp_path: pathlib.Path) -> None:
    # GIVEN
    directory = tmp_path / "bundle"
    directory.mkdir()
    (directory / "manifest.json").write_text(json.dumps({"magic": "SOMETHING_ELSE"}))

    # WHEN / THEN
    assert not data_source.is_gantry_evidence_directory(directory)


def test_should_build_every_declared_table(tmp_path: pathlib.Path) -> None:
    # GIVEN
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")

    # WHEN
    built = evidence.SourceFactory(path=str(bundle)).build()

    # THEN
    assert set(built.tables) == set(gantry_evidence.MANIFEST["tables"])
    for name, spec in gantry_evidence.MANIFEST["tables"].items():
        assert built.tables[name].num_rows == spec["rows"]


def test_should_type_columns_from_the_manifest(tmp_path: pathlib.Path) -> None:
    # GIVEN
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")

    # WHEN
    pairs = evidence.SourceFactory(path=str(bundle)).build().tables["signal_pairs"]

    # THEN
    assert pairs.schema.field("error_yours").type == pa.float64()
    assert pairs.schema.field("better").type == pa.bool_()


def test_should_read_empty_cells_as_nulls_not_zeros(tmp_path: pathlib.Path) -> None:
    # GIVEN an unmeasured ladder arm, whose rate is absent rather than zero
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")

    # WHEN
    ladder = evidence.SourceFactory(path=str(bundle)).build().tables["ladder"]
    unmeasured = ladder.to_pylist()[1]

    # THEN
    assert unmeasured["measured"] is False
    assert unmeasured["rate"] is None
    assert unmeasured["wins"] is None


def test_should_surface_verdicts_in_metadata(tmp_path: pathlib.Path) -> None:
    # GIVEN
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")

    # WHEN
    metadata = evidence.SourceFactory(path=str(bundle)).metadata

    # THEN
    assert metadata["submission"]["id"] == "sample_two_handed"
    assert metadata["g3_context"]["has_control"] is False
    assert any("shuffled control" in g["summary"] for g in metadata["gates"])


def test_should_refuse_directory_without_manifest(tmp_path: pathlib.Path) -> None:
    # GIVEN
    directory = tmp_path / "not_a_bundle"
    directory.mkdir()

    # WHEN / THEN
    with pytest.raises(errors.InvalidPathError):
        evidence.SourceFactory(path=str(directory))


def test_should_refuse_manifest_that_lies_about_a_table(tmp_path: pathlib.Path) -> None:
    # GIVEN a declared table whose file is missing
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    (bundle / "signal_pairs.csv").unlink()

    # WHEN / THEN
    with pytest.raises(errors.InvalidPathError, match="signal_pairs"):
        evidence.SourceFactory(path=str(bundle)).build()


def test_should_refuse_manifest_that_lies_about_row_counts(tmp_path: pathlib.Path) -> None:
    # GIVEN a table with fewer rows than the manifest declares
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    lines = (bundle / "signal_pairs.csv").read_text().splitlines()
    (bundle / "signal_pairs.csv").write_text("\n".join(lines[:-1]) + "\n")

    # WHEN / THEN
    with pytest.raises(errors.InvalidPathError, match="declares"):
        evidence.SourceFactory(path=str(bundle)).build()


@pytest.mark.parametrize("escape", ["parent", "absolute", "symlink"])
def test_table_paths_cannot_escape_bundle(tmp_path: pathlib.Path, escape: str) -> None:
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    outside = tmp_path / "outside.csv"
    outside.write_text((bundle / "signal_pairs.csv").read_text())
    filename = "../outside.csv" if escape == "parent" else str(outside)
    if escape == "symlink":
        (bundle / "link.csv").symlink_to(outside)
        filename = "link.csv"
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"]["signal_pairs"]["file"] = filename
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(errors.InvalidPathError, match="within the bundle"):
        evidence.SourceFactory(path=str(bundle)).build()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("file", None),
        ("columns", []),
        ("columns", {"episode": "unknown"}),
        ("rows", -1),
        ("rows", True),
        ("rows", "3"),
    ],
)
def test_invalid_table_specs_raise_format_errors(
    tmp_path: pathlib.Path, field: str, value: object
) -> None:
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"]["signal_pairs"][field] = value
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(errors.InvalidPathError):
        evidence.SourceFactory(path=str(bundle)).build()


@pytest.mark.parametrize("tables", [None, [], {"signal_pairs": None}])
def test_invalid_table_mapping_raises_format_error(tmp_path: pathlib.Path, tables: object) -> None:
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"] = tables
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(errors.InvalidPathError):
        evidence.SourceFactory(path=str(bundle)).build()


@pytest.mark.parametrize(
    "header", ["wrong,error_yours,error_shuffled,better", "episode,episode,error_shuffled,better"]
)
def test_csv_columns_must_match_manifest(tmp_path: pathlib.Path, header: str) -> None:
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    csv_path = bundle / "signal_pairs.csv"
    _, rows = csv_path.read_text().split("\n", 1)
    csv_path.write_text(header + "\n" + rows)

    with pytest.raises(errors.InvalidPathError, match="columns"):
        evidence.SourceFactory(path=str(bundle)).build()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("submission", None),
        ("dataset", []),
        ("g3_context", False),
        ("gates", {}),
        ("gates", [None]),
    ],
)
def test_invalid_metadata_is_rejected(tmp_path: pathlib.Path, key: str, value: object) -> None:
    bundle = gantry_evidence.write_bundle(tmp_path / "bundle")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest[key] = value
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(errors.InvalidPathError):
        evidence.SourceFactory(path=str(bundle))


def test_bundle_can_be_described_and_queried_through_server(tmp_path: pathlib.Path) -> None:
    import server

    path = str(gantry_evidence.write_bundle(tmp_path / "bundle"))
    assert server.describe_data_source(path)
    assert server.describe_topic(path, "signal_pairs")
    result = server.query_messages(
        path=path,
        topic="signal_pairs",
        sql_statement=(
            'SELECT COUNT(*) AS n FROM "signal_pairs" WHERE "signal_pairs"[\'better\'] = true'
        ),
    )
    assert result == [{"n": 2}]
