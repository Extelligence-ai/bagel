"""Generate a GIF file from images in a topic."""

import logging
import pathlib

from PIL import Image, ImageDraw, ImageFont

from src import artifacts
from src.di import module
from src.pipeline import base, images


class GenerateGif(images.TopicImageMixin, base.Task):
    """Generate a GIF file from images in a topic."""

    def __init__(self, topic: str) -> None:
        """Initialize the task.

        Args:
            topic (str): The image topic to generate a GIF file from.

        """
        self._topic = topic

        # Styling options for the text box below each image
        self._strip_height = 40
        self._padding = 4
        self._font = ImageFont.load_default()
        self._fill_text = (255, 255, 255, 255)

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""

        def _text_size(draw, text, font) -> tuple[int, int]:  # noqa: ANN001
            bbox = draw.textbbox((0, 0), text, font=font)
            return bbox[2] - bbox[0], bbox[3] - bbox[1]

        frames = []
        first_image = None
        gif_files = []

        for _, timestamp_seconds, image in self.to_images(
            topics=[self._topic], asof_seconds=asof_seconds, lookback=lookback
        ):
            timestamp_text = f"{timestamp_seconds:.8f}"

            img = image.convert("RGBA")
            W, H = img.size  # noqa: N806

            annotated = Image.new("RGBA", (W, H + self._strip_height), (0, 0, 0, 255))
            annotated.paste(img, (0, 0))
            draw = ImageDraw.Draw(annotated)

            _, topic_h = _text_size(draw, self._topic, self._font)
            ts_w, _ = _text_size(draw, timestamp_text, self._font)

            y_text = H + (self._strip_height - topic_h) // 2
            draw.text(
                (self._padding, y_text),
                self._topic,
                font=self._font,
                fill=self._fill_text,
            )
            draw.text(
                (W - ts_w - self._padding, y_text),
                timestamp_text,
                font=self._font,
                fill=self._fill_text,
            )

            frames.append(annotated)
            first_image = first_image or annotated

        if frames and first_image:
            gif_file = artifacts.pipeline_task_artifact_path(
                self.pipeline,
                self.name,
                self.site,
                self.asset,
                self.log_id,
                asof_seconds,
                ".gif",
            )
            gif_file.parent.mkdir(parents=True, exist_ok=True)
            first_image.save(
                gif_file,
                save_all=True,
                append_images=frames[1:],
                duration=100,
                loop=0,
            )
            logging.info("Wrote %s", gif_file)
            gif_files.append(gif_file)

        return gif_files


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = GenerateGif
