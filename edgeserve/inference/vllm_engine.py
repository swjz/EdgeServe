"""vLLM implementation of `InferenceEngine`.

Scope (as of this commit):
  * tokenize / detokenize / generate / prefill: fully supported via vllm.LLM.
  * KV export/import (serialize_cache / deserialize_cache): NOT supported.

vLLM owns its KV cache internally via PagedAttention -- blocks are allocated
inside the engine and there is no safe, version-stable public API for
lifting a sequence's KV out of the block manager and loading it back into
a different vLLM instance. Proper cross-process transfer requires writing
a vLLM `KVConnectorBase_V1` (see
`vllm.distributed.kv_transfer.kv_connector.v1`) that plugs EdgeServe's
`SemanticCacheClient` into vLLM's save/load hooks. That's tracked as a
separate roadmap item.

What this adapter IS useful for:
  * Running vLLM with `enable_prefix_caching=True` as a single-process
    baseline in `benchmark_kv_routing.py` -- measures vLLM's internal
    radix prefix-cache ceiling on the same N-agent / shared-doc workload.
  * Any operator (`LLMCompute`, future `VLLMCompute`) that just needs
    tokens-in-tokens-out on a single vLLM instance.
"""

from typing import List, Optional, Tuple

from edgeserve.inference.engine import CacheHandle, InferenceEngine


class VLLMEngine(InferenceEngine):
    """Thin InferenceEngine wrapper around vllm.LLM.

    Args mirror vLLM's own constructor where it matters (dtype, gpu_memory
    utilization, max_model_len) and expose prefix caching as a first-class
    toggle because that's the central knob for the comparisons we care about.
    """

    def __init__(
        self,
        model_id: str,
        device: str = 'cuda',
        dtype: str = 'bfloat16',
        enable_prefix_caching: bool = True,
        gpu_memory_utilization: float = 0.5,
        max_model_len: Optional[int] = None,
        **extra_llm_kwargs,
    ):
        from vllm import LLM, SamplingParams

        if device != 'cuda':
            raise ValueError(f'VLLMEngine requires device=cuda, got {device}')

        self._model_id = model_id
        self._device = device
        self._SamplingParams = SamplingParams

        llm_kwargs = dict(
            model=model_id,
            dtype=dtype,
            enable_prefix_caching=enable_prefix_caching,
            gpu_memory_utilization=gpu_memory_utilization,
        )
        if max_model_len is not None:
            llm_kwargs['max_model_len'] = max_model_len
        llm_kwargs.update(extra_llm_kwargs)
        self._llm = LLM(**llm_kwargs)
        self._tokenizer = self._llm.get_tokenizer()

    @property
    def device(self) -> str:
        return self._device

    @property
    def eos_token_id(self) -> Optional[int]:
        return self._tokenizer.eos_token_id

    def tokenize(self, text: str) -> List[int]:
        return self._tokenizer.encode(text, add_special_tokens=False)

    def detokenize(self, token_ids: List[int]) -> str:
        return self._tokenizer.decode(token_ids, skip_special_tokens=True)

    def prefill(
        self, token_ids: List[int], cache: Optional[CacheHandle] = None,
    ) -> CacheHandle:
        """Run a 1-token generation to force vLLM to prefill and populate
        its internal prefix cache. Returns a sentinel; vLLM's block manager
        owns the actual state.

        `cache` is ignored -- vLLM looks up its own radix prefix cache on
        every request and hits automatically when the token prefix matches
        something seen before.
        """
        if not token_ids:
            return None
        # vLLM 0.19 accepts a bare list[int] (or list of list[int]) as `prompts`.
        self._llm.generate(
            prompts=[list(token_ids)],
            sampling_params=self._SamplingParams(max_tokens=1, temperature=0.0),
            use_tqdm=False,
        )
        return _SENTINEL_VLLM_CACHE

    def generate(
        self,
        token_ids: List[int],
        max_new_tokens: int,
        cache: Optional[CacheHandle] = None,
        greedy: bool = True,
    ) -> Tuple[List[int], CacheHandle]:
        if max_new_tokens <= 0:
            self.prefill(token_ids, cache=cache)
            return [], _SENTINEL_VLLM_CACHE
        sp = self._SamplingParams(
            max_tokens=max_new_tokens,
            temperature=0.0 if greedy else 1.0,
        )
        outputs = self._llm.generate(
            prompts=[list(token_ids)],
            sampling_params=sp,
            use_tqdm=False,
        )
        out = outputs[0].outputs[0]
        return list(out.token_ids), _SENTINEL_VLLM_CACHE

    def serialize_cache(self, cache: CacheHandle) -> bytes:
        raise NotImplementedError(
            'VLLMEngine does not support KV export -- vLLM owns its KV blocks '
            'internally via PagedAttention. To use EdgeServe Semantic Cache '
            'Routing with vLLM, implement a vllm KVConnectorBase_V1 that '
            'wraps SemanticCacheClient (see '
            'vllm.distributed.kv_transfer.kv_connector.v1.base). Tracked on '
            'the roadmap.'
        )

    def deserialize_cache(self, blob: bytes) -> CacheHandle:
        raise NotImplementedError(
            'VLLMEngine does not support KV import -- see serialize_cache '
            'for the connector path forward.'
        )


_SENTINEL_VLLM_CACHE = object()  # marker returned as the CacheHandle
