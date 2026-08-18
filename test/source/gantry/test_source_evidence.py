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
