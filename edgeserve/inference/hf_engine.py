"""HuggingFace transformers implementation of `InferenceEngine`.

Keeps the KV cache in the modern `transformers.DynamicCache` form (or legacy
tuple on older versions). Serialization goes through the existing safetensors
codec in `edgeserve.semantic_cache.kv_io` so bytes produced by this engine
are directly transportable by `SemanticCacheClient`.
"""

from typing import List, Optional, Tuple

from edgeserve.inference.engine import CacheHandle, InferenceEngine


class HFEngine(InferenceEngine):
    def __init__(self, model_id: str, device: str = 'cpu', dtype=None):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self._model_id = model_id
        self._device = device
        self._tokenizer = AutoTokenizer.from_pretrained(model_id)
        if self._tokenizer.pad_token_id is None:
            self._tokenizer.pad_token = self._tokenizer.eos_token
        self._model = AutoModelForCausalLM.from_pretrained(
            model_id, torch_dtype=dtype or torch.float32,
        )
        self._model.to(device)
        self._model.eval()
        self._torch = torch

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

    def _ids_tensor(self, token_ids: List[int]):
        return self._torch.tensor([token_ids], dtype=self._torch.long, device=self._device)

    def prefill(
        self, token_ids: List[int], cache: Optional[CacheHandle] = None,
    ) -> CacheHandle:
        if not token_ids:
            return cache
        ids = self._ids_tensor(token_ids)
        with self._torch.no_grad():
            out = self._model(input_ids=ids, past_key_values=cache, use_cache=True)
        return out.past_key_values

    def generate(
        self,
        token_ids: List[int],
        max_new_tokens: int,
        cache: Optional[CacheHandle] = None,
        greedy: bool = True,
    ) -> Tuple[List[int], CacheHandle]:
        if max_new_tokens <= 0:
            cache = self.prefill(token_ids, cache=cache)
            return [], cache

        # Feed the prompt in (may be empty when caller already prefilled).
        if token_ids:
            ids = self._ids_tensor(token_ids)
            with self._torch.no_grad():
                out = self._model(input_ids=ids, past_key_values=cache, use_cache=True)
            cache = out.past_key_values
            last_logits = out.logits[:, -1, :]
        else:
            # No new prompt tokens -- caller handed us a cache; we still need
            # a "next-token" logits. Re-run the last cached position by doing
            # one step forward with a zero-length input is not supported, so
            # we require at least one prompt token in practice. Raise.
            raise ValueError('generate() requires at least one prompt token')

        new_tokens: List[int] = []
        eos = self.eos_token_id
        for _ in range(max_new_tokens):
            if greedy:
                next_tok = int(last_logits.argmax(dim=-1).item())
            else:
                probs = self._torch.softmax(last_logits, dim=-1)
                next_tok = int(self._torch.multinomial(probs, num_samples=1).item())
            new_tokens.append(next_tok)
            if eos is not None and next_tok == eos:
                break
            step_ids = self._torch.tensor([[next_tok]], dtype=self._torch.long, device=self._device)
            with self._torch.no_grad():
                out = self._model(input_ids=step_ids, past_key_values=cache, use_cache=True)
            cache = out.past_key_values
            last_logits = out.logits[:, -1, :]

        return new_tokens, cache

    def serialize_cache(self, cache: CacheHandle) -> bytes:
        from edgeserve.semantic_cache.kv_io import save_past_key_values
        return save_past_key_values(cache)

    def deserialize_cache(self, blob: bytes) -> CacheHandle:
        from edgeserve.semantic_cache.kv_io import load_past_key_values
        return load_past_key_values(blob, device=self._device, as_cache=True)
