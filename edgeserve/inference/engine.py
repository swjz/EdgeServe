"""Abstract inference-engine interface.

EdgeServe's LLM operators are backend-agnostic: HuggingFace transformers
today, vLLM / SGLang on GPU boxes tomorrow. Each backend implements
`InferenceEngine`; operators consume the ABC so swap-in is one line.

`CacheHandle` is an opaque type owned by the engine. Operators never look
inside -- they only export/import it via `serialize_cache` / `deserialize_cache`,
which is what travels through `edgeserve.semantic_cache`.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

CacheHandle = Any  # engine-specific; opaque to operators.


class InferenceEngine(ABC):
    @property
    @abstractmethod
    def device(self) -> str:
        """'cpu' | 'mps' | 'cuda[:idx]'."""

    @abstractmethod
    def tokenize(self, text: str) -> List[int]:
        """Return a flat list of token ids (no batch dimension, no specials padding)."""

    @abstractmethod
    def detokenize(self, token_ids: List[int]) -> str:
        ...

    @abstractmethod
    def prefill(
        self, token_ids: List[int], cache: Optional[CacheHandle] = None,
    ) -> CacheHandle:
        """Forward-pass `token_ids`, appending onto an optional existing cache.

        Returns the updated cache handle. Callers that only want the cache
        (no generation) can stop here.
        """

    @abstractmethod
    def generate(
        self,
        token_ids: List[int],
        max_new_tokens: int,
        cache: Optional[CacheHandle] = None,
        greedy: bool = True,
    ) -> Tuple[List[int], CacheHandle]:
        """Generate `max_new_tokens` continuing from `cache` (or from scratch).

        `token_ids` are appended to the cache BEFORE generation begins. Returns
        (new_token_ids, final_cache). New tokens exclude the input prompt.
        """

    @abstractmethod
    def serialize_cache(self, cache: CacheHandle) -> bytes:
        """Bytes that can be shipped to another node and imported back."""

    @abstractmethod
    def deserialize_cache(self, blob: bytes) -> CacheHandle:
        """Inverse of serialize_cache. Loads onto `self.device`."""
