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


class SentenceTransformerEmbedder:
    """A local sentence-embedding model. Edge-first: runs on CPU, nothing leaves the box.

    Wraps `sentence-transformers`, an optional dependency, so the default small model
    (all-MiniLM-L6-v2, 384 dims) runs on a robotics companion computer without a GPU or an
    API key. Unlike `HashingEmbedder`, it captures meaning, so "hard brake" and "sudden
    deceleration" land close even with no shared words.

    Install the dependency to use it:

        pip install sentence-transformers

    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        """Load the model.

        Args:
            model_name (str): A sentence-transformers model id. The default is small and
                CPU-friendly for edge deployment.

        Raises:
            ImportError: If `sentence-transformers` is not installed.

        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as error:
            raise ImportError(
                "SentenceTransformerEmbedder requires the 'sentence-transformers' package. "
                "Install it with: pip install sentence-transformers"
            ) from error

        self._model = SentenceTransformer(model_name)
        self._dim = int(self._model.get_sentence_embedding_dimension())

    @property
    def dim(self) -> int:
        """The model's output dimensionality."""
        return self._dim

    def embed(self, text: str) -> list[float]:
        """Embed text into an L2-normalized vector using the local model."""
        vector = self._model.encode([text], normalize_embeddings=True)[0]
        return [float(value) for value in vector]


def make_embedder(
    kind: str = "hashing", *, dim: int = 256, model_name: str = "all-MiniLM-L6-v2"
) -> Embedder:
    """Build an embedder by name, so pipeline configs can choose one with a primitive arg.

    Args:
        kind (str): "hashing" (default, dependency-free) or "sentence-transformer" (local
            semantic model, needs the optional dependency).
        dim (int): Vector dimensionality for the hashing embedder (ignored by the model).
        model_name (str): The sentence-transformers model id.

    Raises:
        ValueError: If `kind` is not recognized.

    """
    if kind == "hashing":
        return HashingEmbedder(dim=dim)
    if kind in ("sentence-transformer", "sentence_transformer", "st"):
        return SentenceTransformerEmbedder(model_name=model_name)
    raise ValueError(f"unknown embedder kind: {kind!r} (use 'hashing' or 'sentence-transformer')")
