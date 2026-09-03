"""Build provenance: settings for build_id and vcs_ref in heartbeats/events (fleet step 8)."""

import importlib
import sys

import pytest

from settings import settings


def test_provenance_module_does_not_import_paho_or_cryptography(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """provenance.py must not drag paho or cryptography at import time."""
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.provenance", raising=False)
    importlib.import_module("src.sink.publish.provenance")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


class TestBuildProvenance:
    def test_both_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", None)
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        assert build_provenance() is None

    def test_build_id_only_returns_dict_with_build_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        result = build_provenance()
        assert result == {"build_id": "abc123"}
        assert "vcs_ref" not in result

    def test_build_id_and_vcs_ref_returns_both(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", "v2.2.3-4-gdeadbeef")
        result = build_provenance()
        assert result == {"build_id": "abc123", "vcs_ref": "v2.2.3-4-gdeadbeef"}

    def test_empty_string_build_id_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        assert build_provenance() is None

    def test_whitespace_only_build_id_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "  ")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        assert build_provenance() is None

    def test_build_id_set_empty_vcs_ref_returns_build_id_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", "")
        result = build_provenance()
        assert result == {"build_id": "abc123"}
        assert "vcs_ref" not in result

    def test_vcs_ref_only_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", None)
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", "v2.2.3")
        assert build_provenance() is None

    def test_build_id_with_whitespace_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "  abc123  ")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        result = build_provenance()
        assert result == {"build_id": "abc123"}

    def test_vcs_ref_with_whitespace_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.sink.publish.provenance import build_provenance

        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", "  v2.2.3  ")
        result = build_provenance()
        assert result == {"build_id": "abc123", "vcs_ref": "v2.2.3"}
