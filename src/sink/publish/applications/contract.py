"""Vendor-neutral, immutable application releases and compatibility resolution.

A release is desired state, not installation evidence. Only an authenticated
installer report can establish what is running. Software can contain its models
or consume a separately versioned model artifact.
"""

import json
import re
from urllib.parse import urlsplit

from src.sink.publish.delivery.contract import canonical, digest

__all__ = ["canonical", "digest"]

MAX_LABEL = 128
ASCII_CONTROL = 32
MAX_IMAGE = 512
MAX_URI = 2048
MAX_PLATFORMS = 16

SCHEMA = "fleet.application.v1"
KINDS = ("software", "model", "software_model")
ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
SHA = re.compile(r"^[a-f0-9]{64}$")
IMAGE = re.compile(r"^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$")


def keys(value: dict, required: tuple[str, ...], optional: tuple[str, ...] = ()) -> None:
    """Reject unknown and missing contract fields."""
    if (
        not isinstance(value, dict)
        or set(value) - set(required) - set(optional)
        or set(required) - set(value)
    ):
        raise ValueError("Unexpected or missing release fields.")


def label(value: str, name: str) -> None:
    """Validate a printable version or display label."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > MAX_LABEL
        or any(ord(c) < ASCII_CONTROL for c in value)
    ):
        raise ValueError(f"{name} must be 1-128 printable characters.")


def identifier(value: str, name: str) -> None:
    """Validate a local application or runtime identifier."""
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise ValueError(f"{name} must be a short lowercase identifier.")


def software(value: dict) -> dict:
    """Validate a pinned software image and its model interface."""
    keys(value, ("version", "image", "model_mode"), ("model_contract", "bundled_model"))
    label(value["version"], "Software version")
    if (
        not isinstance(value["image"], str)
        or len(value["image"]) > MAX_IMAGE
        or not IMAGE.fullmatch(value["image"])
    ):
        raise ValueError("Software image must be pinned with @sha256 and contain no credentials.")
    if value["model_mode"] not in ("bundled", "external", "none"):
        raise ValueError("Choose bundled, external, or no model for this software.")
    if value["model_mode"] == "external":
        identifier(value.get("model_contract"), "Model interface")
        if "bundled_model" in value:
            raise ValueError("External models cannot also be declared bundled.")
    elif "model_contract" in value:
        raise ValueError("Only software with external models declares a model interface.")
    if "bundled_model" in value:
        if value["model_mode"] != "bundled":
            raise ValueError("Bundled model metadata requires bundled software.")
        keys(value["bundled_model"], ("name", "version"))
        for key, val in value["bundled_model"].items():
            label(val, "Bundled model " + key)
    return value


def model(value: dict) -> dict:
    """Validate an immutable external model reference."""
    keys(value, ("version", "uri", "sha256", "contract"), ("format", "entrypoint"))
    label(value["version"], "Model version")
    identifier(value["contract"], "Model interface")
    if not isinstance(value["sha256"], str) or not SHA.fullmatch(value["sha256"]):
        raise ValueError("Model artifact requires a full SHA-256 checksum.")
    if not isinstance(value["uri"], str) or len(value["uri"]) > MAX_URI:
        raise ValueError("Model artifact URI is invalid.")
    uri = urlsplit(value["uri"])
    if (
        uri.scheme not in ("https", "s3", "oci", "fleet")
        or not uri.hostname
        or uri.username
        or uri.password
        or uri.query
        or uri.fragment
        or any(c.isspace() for c in value["uri"])
    ):
        raise ValueError("Use an HTTPS, S3, or OCI URI without credentials or query parameters.")
    if uri.scheme == "fleet":
        from src.sink.publish.applications.artifacts import safe_path

        if (
            not re.fullmatch(r"fleet://models/[a-f0-9]{64}", value["uri"])
            or value.get("format") not in ("file", "zip")
            or not safe_path(value.get("entrypoint", ""))
        ):
            raise ValueError("Invalid Fleet model artifact, format or entry file.")
    elif "format" in value or "entrypoint" in value:
        raise ValueError("Artifact format and entry file apply only to Fleet models.")
    return value


def validate(spec: dict) -> dict:
    """Validate and canonicalize an application release."""
    keys(
        spec,
        ("schema", "name", "version", "kind", "application", "runtime", "platforms"),
        ("software", "model"),
    )
    if spec["schema"] != SCHEMA or spec["kind"] not in KINDS:
        raise ValueError("Choose a software, model, or software + model release.")
    for key in ("name", "version"):
        label(spec[key], key.title())
    for key in ("application", "runtime"):
        identifier(spec[key], key.title())
    if (
        not isinstance(spec["platforms"], list)
        or not spec["platforms"]
        or len(spec["platforms"]) > MAX_PLATFORMS
        or any(
            p not in ("linux/amd64", "linux/arm64", "darwin/arm64", "darwin/amd64")
            for p in spec["platforms"]
        )
        or len(set(spec["platforms"])) != len(spec["platforms"])
    ):
        raise ValueError("Choose distinct supported operating system/architecture pairs.")
    has_software = spec["kind"] in ("software", "software_model")
    has_model = spec["kind"] in ("model", "software_model")
    if ("software" in spec) != has_software or ("model" in spec) != has_model:
        raise ValueError("Artifacts must match the selected deployment type.")
    if has_software:
        software(spec["software"])
    if has_model:
        model(spec["model"])
    if (
        spec["runtime"] == "docker-application-v1"
        and has_model
        and not spec["model"]["uri"].startswith("fleet://models/")
        and (
            not spec["model"]["uri"].startswith("oci://")
            or not IMAGE.fullmatch(spec["model"]["uri"][6:])
        )
    ):
        raise ValueError(
            "Docker models require an OCI image pinned by digest, containing /model/model.bin."
        )
    if (
        has_software
        and has_model
        and not spec["model"]["uri"].startswith("fleet://models/")
        and (
            spec["software"]["model_mode"] != "external"
            or spec["software"]["model_contract"] != spec["model"]["contract"]
        )
    ):
        raise ValueError("Software and model interfaces must match for a coordinated release.")
    return json.loads(canonical(spec))


def validate_state(state: dict) -> dict:
    """Validate the complete compatible software/model pair."""
    keys(state, ("software", "model"))
    software(state["software"])
    if state["software"]["model_mode"] == "external":
        model(state["model"])
        if state["model"]["contract"] != state["software"]["model_contract"]:
            raise ValueError("Application and model interfaces do not match.")
    elif state["model"] is not None:
        raise ValueError("Bundled/no-model software cannot run a separate model artifact.")
    return state


def resolve(spec: dict, inventory: dict) -> dict:
    """Return a complete desired pair; unchanged components are explicitly pinned."""
    validate(spec)
    if (
        inventory.get("runtime") != spec["runtime"]
        or inventory.get("platform") not in spec["platforms"]
    ):
        raise ValueError("Installer runtime or robot platform is incompatible.")
    if inventory.get("ready") is not True:
        raise ValueError("Robot installer has not reported that activation is safe.")
    current = inventory.get("current")
    if current is not None:
        validate_state(current)
    app = spec.get("software") or (current or {}).get("software")
    if not app:
        raise ValueError("Model-only deployment requires reported running software.")
    artifact = spec.get("model") if "model" in spec else (current or {}).get("model")
    if app["model_mode"] != "external":
        artifact = None
        if spec["kind"] == "model":
            raise ValueError(
                "This application bundles its model or does not accept external models."
            )
    if (
        artifact
        and artifact["uri"].startswith("fleet://")
        and "fleet-model-v1" not in inventory.get("capabilities", [])
    ):
        raise ValueError("Update the Bagel application worker to support Fleet depot models.")
    return validate_state({"software": app, "model": artifact})
