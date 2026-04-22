"""Safetensors-backed codec for HuggingFace `past_key_values` tensors.

`past_key_values` is the canonical KV-cache payload exchanged between EdgeServe
nodes for Semantic Cache Routing. We serialize the (layer-indexed) key/value
tensor pairs to a single safetensors blob so it can be shipped as-is over the
per-node HTTP server (see `edgeserve.semantic_cache.http_server`).

Shape assumption: HF decoder models return
    past_key_values: Tuple[Tuple[Tensor, Tensor], ...]  # per-layer (k, v)

We accept the legacy tuple format and also any object exposing `.to_legacy_cache()`
(e.g. transformers >=4.36 `DynamicCache`). On load we always return the tuple.
"""

from typing import Any, Tuple


def _as_legacy(past_key_values: Any) -> Tuple[Tuple[Any, Any], ...]:
    """Coerce either a legacy tuple or a DynamicCache-like object to (k, v) pairs.

    - transformers <4.36: `past_key_values` is already tuple[tuple[k, v], ...]
    - transformers 4.36-4.56: `DynamicCache.to_legacy_cache()` returns that shape
    - transformers >=4.57: DynamicCache iterates as 3-tuples (k, v, meta) and
      exposes `.layers[i].keys` / `.values`. `to_legacy_cache` is gone.
    """
    if hasattr(past_key_values, 'to_legacy_cache'):
        return past_key_values.to_legacy_cache()
    # DynamicCache (4.57+): pull from .layers if present.
    if hasattr(past_key_values, 'layers'):
        return tuple((L.keys, L.values) for L in past_key_values.layers)
    # Iterable of per-layer tuples. Tolerate both 2-tuple and longer forms
    # (4.57 yields (k, v, meta)).
    out = []
    for item in past_key_values:
        out.append((item[0], item[1]))
    return tuple(out)


def save_past_key_values(past_key_values: Any) -> bytes:
    """Serialize `past_key_values` to a safetensors byte string.

    Tensors go to CPU in a single batched transfer to minimize PCIe overhead
    on multi-layer KV caches (tens of layers * 2 tensors each would otherwise
    cost a launch per layer).
    """
    from safetensors.torch import save

    legacy = _as_legacy(past_key_values)
    flat = {}
    for i, kv in enumerate(legacy):
        k, v = kv[0], kv[1]
        # .cpu() only if not already on CPU -- avoids a no-op copy for CPU backends.
        k_cpu = k.detach().contiguous() if k.device.type == 'cpu' else k.detach().contiguous().cpu()
        v_cpu = v.detach().contiguous() if v.device.type == 'cpu' else v.detach().contiguous().cpu()
        flat[f'k.{i}'] = k_cpu
        flat[f'v.{i}'] = v_cpu
    metadata = {'num_layers': str(len(legacy))}
    return save(flat, metadata=metadata)


def load_past_key_values(
    blob: bytes, device: str = 'cpu', as_cache: bool = True
) -> Any:
    """Load a safetensors blob back into a KV cache.

    Returns a modern `transformers.DynamicCache` if available (the format
    accepted by HF forward passes as of transformers >=4.36). Set
    `as_cache=False` to force the legacy tuple-of-tuples form.

    Fast path: `safetensors.torch.load(blob)` reads directly from bytes
    (no tempfile), and tensors are moved to `device` with a single .to()
    per layer. For `device='cuda'` this is one H2D copy, dominated by
    PCIe bandwidth rather than serialization.
    """
    from safetensors.torch import load as st_load

    tensors = st_load(blob)
    num_layers = sum(1 for key in tensors if key.startswith('k.'))
    legacy = tuple(
        (tensors[f'k.{i}'].to(device, non_blocking=True),
         tensors[f'v.{i}'].to(device, non_blocking=True))
        for i in range(num_layers)
    )

    if not as_cache:
        return legacy
    try:
        from transformers.cache_utils import DynamicCache
        return DynamicCache(legacy)
    except (ImportError, AttributeError, TypeError):
        return legacy
