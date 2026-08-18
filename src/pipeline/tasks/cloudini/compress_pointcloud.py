"""Compress a ROS2 MCAP bag's pointcloud topics using the cloudini CLI."""

import logging
import pathlib
import shlex
import shutil
import subprocess

from settings import settings
from src.di import module
from src.pipeline import base
from src.source.mcap import McapBag

CLOUDINI_CONVERTER = "cloudini_rosbag_converter"


def _converter_available() -> bool:
    """Return True if cloudini compression is enabled and the CLI is installed."""
    if not settings.CLOUDINI_ENABLED:
        logging.info("Cloudini is disabled globally via CLOUDINI_ENABLED=false.")
        return False
    if shutil.which(CLOUDINI_CONVERTER) is None:
        logging.warning(
            "'%s' was not found on PATH; skipping pointcloud compression. "
            "Build it from https://github.com/facontidavide/cloudini (cloudini_ros).",
            CLOUDINI_CONVERTER,
        )
        return False
    return True


class CompressPointCloud(base.ArtifactMixin, base.Task):
    """Compress a ROS2 MCAP bag's PointCloud2 topics into CompressedPointCloud2.

    Wraps cloudini's ``cloudini_rosbag_converter -f <in> -o <out> -c``, which rewrites
    every ``sensor_msgs/PointCloud2`` topic in an MCAP bag as the much smaller
    ``point_cloud_interfaces/CompressedPointCloud2`` encoding; all other topics pass
    through unchanged. To analyze the compressed topics with Bagel afterwards, run the
    ``decode_pointcloud`` task on the result (see the cloudini runbook): Bagel does not
    decode cloudini transparently during ingestion.

    Compression uses cloudini's built-in default quantization: the converter
    CLI exposes no resolution option (verified against cloudini_ros), so the
    ``CLOUDINI_DEFAULT_RESOLUTION`` setting does not apply to this task.

    The source must be a ROS2 MCAP bag. The ``cloudini_rosbag_converter`` binary must be
    on PATH; if it is missing, or cloudini is disabled via ``CLOUDINI_ENABLED``, the task
    logs and skips rather than failing the pipeline.
    """

    def __init__(self, cloudini: bool = True) -> None:
        """Initialize the task.

        Args:
            cloudini (bool, optional): Per-task opt-out. If False, the task skips (useful
                to disable compression for a single pipeline without changing the global
                setting). Defaults to True.

        """
        self._enabled = cloudini

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """No data-source dependencies; the task operates on the bag file directly."""

    def execute(
        self, asof_seconds: float, lookback: base.Lookback | None
    ) -> list[pathlib.Path] | None:
        """Compress the source bag's pointcloud topics into a new MCAP bag."""
        if not self._enabled:
            logging.info("CompressPointCloud opted out for this pipeline (cloudini: false).")
            return None
        if not _converter_available():
            return None

        # rosbag2 MCAP directories are a supported source layout: resolve the
        # contained .mcap segment(s) rather than passing the directory to the
        # converter (Codex on #142).
        source = pathlib.Path(self.path)
        if source.is_dir():
            inputs = McapBag(path=source).mcap_files
            if not inputs:
                logging.info("No .mcap files under %s; nothing to compress.", source)
                return None
        else:
            inputs = [source]

        base_output = self.artifact_path(asof_seconds, ".mcap")
        outputs: list[pathlib.Path] = []
        for index, input_file in enumerate(inputs):
            output_file = (
                base_output
                if len(inputs) == 1
                else base_output.with_name(f"{base_output.stem}_part{index:02d}.mcap")
            )
            # -y: artifact_path is deterministic, so a retry finds the previous
            # output in place; without it the converter prompts and a
            # non-interactive pipeline blocks (Copilot on #142).
            command = [
                CLOUDINI_CONVERTER,
                "-f",
                str(input_file),
                "-o",
                str(output_file),
                "-c",
                "-y",
            ]

            result = subprocess.run(  # noqa: S603
                command,
                check=True,  # raise CalledProcessError if nonzero exit
                text=True,
                capture_output=True,
            )

            logging.debug(shlex.join(result.args))
            if result.stdout.strip():
                logging.debug(result.stdout.strip())
            if output_file.exists():
                outputs.append(output_file)
                logging.info("Wrote %s", output_file)

        if not outputs:
            # The converter exits 0 without writing when the bag has no
            # pointcloud topics; reporting a nonexistent artifact would make
            # a downstream upload raise FileNotFoundError (Copilot on #142).
            logging.info("No pointcloud topics in %s; nothing to compress.", self.path)
            return None

        return outputs


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = CompressPointCloud
