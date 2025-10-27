"""Estimate depth on detected objects to determine whether they are too close."""

import logging

import numpy as np
import torch
from transformers import (
    AutoImageProcessor,
    AutoModelForDepthEstimation,
    YolosForObjectDetection,
    YolosImageProcessor,
)

from src.di import module
from src.pipeline import base, images

YOLO_MODEL_ID = "hustvl/yolos-tiny"
DEPTH_MODEL_ID = "depth-anything/Depth-Anything-V2-Small-hf"

yolo_processor = YolosImageProcessor.from_pretrained(YOLO_MODEL_ID)
yolo_model = YolosForObjectDetection.from_pretrained(YOLO_MODEL_ID)
depth_processor = AutoImageProcessor.from_pretrained(DEPTH_MODEL_ID)
depth_model = AutoModelForDepthEstimation.from_pretrained(DEPTH_MODEL_ID)


class ObjectTooClose(images.TopicImageMixin, base.Gate):
    """Estimate depth on detected objects to determine whether they are too close.

    The lookback window may include multiple images, each with several detected objects.
    The gate returns True if any detected object is too close.

    """

    def __init__(
        self,
        topic: str,
        labels: list[str] | None = None,
        probability: float = 0.5,
        closeness: float | None = None,
    ) -> None:
        """Initialize the gate.

        Args:
            topic (str): The image topic to evaluate.
            labels (list[str] | None, optional): The YOLO labels to detect, e.g., ["person", "car"].
                If None, all available labels are used. Defaults to None.
            probability (float, optional): The minimum probability for a detection to be considered.
                Must be between 0 and 1. Defaults to 0.5.
            closeness (float | None, optional): The closeness threshold. Must be between 0 and 1.
                1 means very close, 0 means very far. If None, the median closeness of the
                entire image is used. Defaults to None.

        Raises:
            ValueError: If the labels is an empty list.
            ValueError: If any of the specified labels are not supported by the YOLO model.
            ValueError: If probability is not between 0 and 1.
            ValueError: If closeness is not between 0 and 1.

        """
        self._topic = topic

        if labels is not None and not labels:
            raise ValueError("Labels list cannot be empty")
        self._yolo_labels = yolo_model.config.id2label
        available_labels = list(set(self._yolo_labels.values()))
        labels = labels or available_labels
        wrong_labels = [label for label in labels if label not in available_labels]
        if wrong_labels:
            raise ValueError(
                f"Labels not supported: {wrong_labels}. Available labels: {available_labels}"
            )
        self._labels = labels

        if not (0.0 <= probability <= 1.0):
            raise ValueError("Probability must be between 0 and 1")
        self._probability = probability

        if closeness is not None and not (0.0 <= closeness <= 1.0):
            raise ValueError("Closeness must be between 0 and 1")
        self._closeness = closeness

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        """Evaluate whether the gating criteria is met at the given time."""
        for _, _, image in self.to_images(
            topics=[self._topic], asof_seconds=asof_seconds, lookback=lookback
        ):
            # Object detection
            yolo_inputs = yolo_processor(images=image, return_tensors="pt")
            with torch.no_grad():
                yolo_outputs = yolo_model(**yolo_inputs)
            detections = yolo_processor.post_process_object_detection(
                yolo_outputs,
                target_sizes=torch.tensor([image.size[::-1]]),
                threshold=self._probability,
            )[0]

            # Depth estimation
            depth_inputs = depth_processor(images=image, return_tensors="pt")
            with torch.no_grad():
                depth_outputs = depth_model(**depth_inputs)
            depth_map = depth_outputs.predicted_depth.squeeze().cpu().numpy()
            depth_map = (depth_map - depth_map.min()) / (depth_map.max() - depth_map.min())
            closeness_threshold = self._closeness or np.median(depth_map)

            # Check if any object is too close
            for probability, label, bbox in zip(
                detections["scores"], detections["labels"], detections["boxes"], strict=True
            ):
                yolo_label = self._yolo_labels[label.item()]
                x0, y0, x1, y1 = map(int, bbox.tolist())
                region = depth_map[y0:y1, x0:x1]
                closeness = np.max(region) if region.size > 0 else None
                if (
                    yolo_label in self._labels
                    and closeness is not None
                    and closeness >= closeness_threshold
                ):
                    logging.info(
                        "Found '%s' object probability=%.4f closeness=%.4f timestamp=%f",
                        yolo_label,
                        probability,
                        closeness,
                        asof_seconds,
                    )
                    return True

        return False


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ObjectTooClose
