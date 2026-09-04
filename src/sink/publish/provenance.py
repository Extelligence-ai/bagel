"""Build provenance: settings-sourced build_id and vcs_ref for heartbeats/events.

The `build_provenance()` function reads `BAGEL_BUILD_ID` and `BAGEL_VCS_REF` at
CALL time (not import time, so tests can monkeypatch settings), returning:
- `None` if `build_id` is unset/empty/whitespace-only (the required key)
- `None` if only `vcs_ref` is set (build_id is required; vcs_ref is optional)
- `{"build_id": <stripped>}` if `build_id` alone is set and non-empty
- `{"build_id": <stripped>, "vcs_ref": <stripped>}` if both are set and non-empty

Images may bake these settings at build time via environment variables.
"""

from settings import settings


def build_provenance() -> dict | None:
    """Read BAGEL_BUILD_ID and BAGEL_VCS_REF, return provenance dict or None.

    build_id is the required key; a lone vcs_ref (no build_id) returns None.
    Both values are stripped of leading/trailing whitespace.
    Empty strings and whitespace-only strings are treated as unset (None).
    """
    build_id = settings.BAGEL_BUILD_ID
    vcs_ref = settings.BAGEL_VCS_REF

    # Strip and check build_id
    if build_id is not None:
        build_id = build_id.strip()
    if not build_id:  # None or empty string after strip
        return None

    # Build the result with the required build_id
    result = {"build_id": build_id}

    # Add vcs_ref only if it's set and non-empty
    if vcs_ref is not None:
        vcs_ref = vcs_ref.strip()
        if vcs_ref:
            result["vcs_ref"] = vcs_ref

    return result
