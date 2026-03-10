"""Decode cloudini-compressed pointcloud topics and write to standard formats."""

import logging
import pathlib
from enum import Enum

from settings import settings
from src.artifacts import short_digest
from src.di import module
from src.pipeline import base

_HAS_CLOUDINI = True
try:
    import wasmtime  # noqa: F401

    _HAS_CLOUDINI = True
except ImportError:
    _HAS_CLOUDINI = False


class OutputFormat(Enum):
    """Supported pointcloud output formats."""

    CSV = "csv"
    NPZ = "npz"


def _cloudini_available() -> bool:
    """Return True if cloudini dependencies are installed and globally enabled."""
    if not _HAS_CLOUDINI:
        logging.warning(
            "cloudini dependencies (wasmtime, numpy) are not installed. "
            "Install them with: uv sync --group cloudini"
        )
        return False
    if not settings.CLOUDINI_ENABLED:
        logging.info("Cloudini is disabled globally via CLOUDINI_ENABLED=false.")
        return False
    return True


def _create_decoder(wasm_path: str) -> object:
    """Create a CloudiniDecoder instance.

    The cloudini_decoder module is vendored from
    https://github.com/facontidavide/cloudini/blob/main/cloudini_py/cloudini_decoder.py
    and requires a path to the cloudini WASM binary.

    Args:
        wasm_path: Path to the cloudini_wasm.wasm file.

    Returns:
        A CloudiniDecoder instance.

    """
    from src.pipeline.tasks.cloudini.vendor.cloudini_decoder import CloudiniDecoder

    return CloudiniDecoder(wasm_path)


class DecodePointCloudTask(base.Task):
    """Decode cloudini-compressed pointcloud messages from MCAP topics.

    Reads CompressedPointCloud2 messages, decodes them via cloudini, and writes
    the resulting pointclouds to CSV or NumPy .npz files.

    Opt-out mechanisms:
        1. Do not install the ``cloudini`` dependency group — the task logs a warning and skips.
        2. Set ``cloudini: false`` in the YAML task args to disable per-task.
        3. Set ``CLOUDINI_ENABLED=false`` in ``.env`` to disable globally (default is enabled).

    """

    def __init__(
        self,
        topics: list[str],
        output_directory: str,
        wasm_path: str,
        output_format: str = "npz",
        cloudini: bool = True,
    ) -> None:
        """Initialize the task.

        Args:
            topics: Pointcloud topics to decode. Must contain at least one topic.
            output_directory: Directory to write decoded pointcloud files.
            wasm_path: Path to the cloudini_wasm.wasm binary file.
            output_format: Output format — "csv" or "npz". Defaults to "npz".
            cloudini: Whether to use cloudini for this task. Defaults to True.
                Set to False to skip decoding (YAML-level opt-out).

        Raises:
            ValueError: If topics is empty.

        """
        if len(topics) == 0:
            raise ValueError("'topics' must contain at least one pointcloud topic name.")
        self._topics = topics
        self._output_directory = pathlib.Path(output_directory)
        self._output_format = OutputFormat(output_format)
        self._cloudini_enabled = cloudini
        self._wasm_path = wasm_path

        self._factory = None
        self._registry = None
        self._dataset = None

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Set up data source dependencies."""
        from src.di.types.base_module import BaseModule
        from src.di.types.data_source import resolve

        ds_type = resolve(path)
        self._factory = module.provide(
            f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path, **kwargs}
        )
        self._registry = module.provide(
            f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {**kwargs}
        )
        self._dataset = module.provide(
            f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {**kwargs}
        )

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Decode compressed pointclouds and write to files."""
        if not self._cloudini_enabled:
            logging.info("Cloudini disabled for this task via YAML config (cloudini: false).")
            return
        if not _cloudini_available():
            return

        decoder = _create_decoder(self._wasm_path)

        data_source = self._factory.build()
        start_seconds = None
        if (
            lookback
            and lookback.unit != base.Unit.FRAME
            and asof_seconds - lookback.to_seconds() >= 0
        ):
            start_seconds = asof_seconds - lookback.to_seconds()

        messages = self._dataset._messages(
            data_source, self._topics, start_seconds, asof_seconds
        )

        digest = short_digest([*self._topics, str(lookback)])
        output_dir = self._output_directory / f"timestamp_seconds={asof_seconds}" / digest
        output_dir.mkdir(parents=True, exist_ok=True)

        n_decoded = 0
        for topic, timestamp_seconds, message in messages:
            raw_data = self._extract_raw_data(message)
            if raw_data is None:
                continue

            try:
                point_cloud, header = decoder.decode_message(raw_data)
            except Exception:
                logging.exception(
                    "Failed to decode pointcloud on topic '%s' at %.4f seconds",
                    topic,
                    timestamp_seconds,
                )
                continue

            safe_topic = topic.replace("/", "_").lstrip("_")
            filename = f"{safe_topic}_{timestamp_seconds:.6f}"
            self._write_pointcloud(point_cloud, header, output_dir, filename)
            n_decoded += 1

        logging.info(
            "Decoded %d pointclouds from topics %s to %s",
            n_decoded,
            self._topics,
            output_dir,
        )

    @staticmethod
    def _extract_raw_data(message: object) -> bytes | None:
        """Extract raw bytes from a message, handling both ROS and dict formats."""
        if isinstance(message, dict):
            return message.get("data")
        if hasattr(message, "data"):
            data = message.data
            if isinstance(data, bytes):
                return data
            if isinstance(data, list | tuple):
                return bytes(data)
        return None

    def _write_pointcloud(
        self,
        point_cloud: object,
        header: dict,
        output_dir: pathlib.Path,
        filename: str,
    ) -> None:
        """Write a decoded pointcloud to the configured output format."""
        import numpy as np

        match self._output_format:
            case OutputFormat.NPZ:
                output_file = output_dir / f"{filename}.npz"
                np.savez_compressed(output_file, points=point_cloud, **header)

            case OutputFormat.CSV:
                output_file = output_dir / f"{filename}.csv"
                if point_cloud.dtype.names:
                    import csv

                    with open(output_file, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(point_cloud.dtype.names)
                        for row in point_cloud:
                            writer.writerow(row)
                else:
                    np.savetxt(output_file, point_cloud, delimiter=",")


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = DecodePointCloudTask
