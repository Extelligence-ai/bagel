"""EXPERIMENTAL: embedders for physical-AI event windows.

The defensible version embeds the signal or vision content of an event (a time-series or
vision encoder). That needs models and is deferred. This module ships a dependency-free
text embedder so the whole index-and-search loop runs end to end today, behind an
`Embedder` Protocol that a real encoder can drop into later.

Nothing here is part of a release. APIs will change.
"""

import hashlib
import math
import re
from typing import Protocol, runtime_checkable

_TOKEN = re.compile(r"[a-z0-9]+")


@runtime_checkable
class Embedder(Protocol):
    """Turns an event into a fixed-length, L2-normalized vector."""

    @property
    def dim(self) -> int:
        """The dimensionality of the vectors this embedder produces."""
        ...

    def embed(self, text: str) -> list[float]:
        """Return the embedding for the given text."""
        ...


class HashingEmbedder:
    """Deterministic hashing-trick text embedder. No ML dependencies.

    Bag-of-words with signed feature hashing, then L2 normalization. It is good enough to
    prove the index and the search loop and to write tests against; it is not a semantic
    model. Swap it for a sentence encoder (text) or a time-series encoder (signal) once the
    interface has earned its keep.
    """

    def __init__(self, dim: int = 256) -> None:
        """Initialize the embedder.

        Args:
            dim (int): The output vector dimensionality. Must be positive.

        Raises:
            ValueError: If dim is not positive.

        """
        if dim <= 0:
            raise ValueError(f"dim must be positive, got {dim}")
        self._dim = dim

    @property
    def dim(self) -> int:
        """The output vector dimensionality."""
        return self._dim

    def embed(self, text: str) -> list[float]:
        """Embed text into an L2-normalized vector via signed feature hashing."""
        vector = [0.0] * self._dim
        for token in _TOKEN.findall(text.lower()):
            digest = hashlib.blake2b(token.encode("utf-8"), digest_size=5).digest()
            bucket = int.from_bytes(digest[:4], "big") % self._dim
            vector[bucket] += 1.0 if digest[4] & 1 else -1.0

        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0.0:
            return vector
        return [value / norm for value in vector]
