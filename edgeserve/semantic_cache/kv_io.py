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
    """Serialize `past_key_values` to a safetensors byte string."""
    from safetensors.torch import save

    legacy = _as_legacy(past_key_values)
    flat = {}
    for i, kv in enumerate(legacy):
        k, v = kv[0], kv[1]
        flat[f'k.{i}'] = k.detach().contiguous().cpu()
        flat[f'v.{i}'] = v.detach().contiguous().cpu()
    metadata = {'num_layers': str(len(legacy))}
    return save(flat, metadata=metadata)


def load_past_key_values(
    blob: bytes, device: str = 'cpu', as_cache: bool = True
) -> Any:
    """Load a safetensors blob back into a KV cache.

    Returns a modern `transformers.DynamicCache` if available (the format
    accepted by HF forward passes as of transformers >=4.36). Set
    `as_cache=False` to force the legacy tuple-of-tuples form.
    """
    from safetensors import safe_open
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(delete=False, suffix='.safetensors') as f:
        f.write(blob)
        tmp_path = f.name
    try:
        with safe_open(tmp_path, framework='pt', device=device) as st:
            num_layers = int(st.metadata().get('num_layers', 0))
            if num_layers == 0:
                num_layers = sum(1 for key in st.keys() if key.startswith('k.'))
            legacy = tuple(
                (st.get_tensor(f'k.{i}'), st.get_tensor(f'v.{i}'))
                for i in range(num_layers)
            )
    finally:
        os.unlink(tmp_path)

    if not as_cache:
        return legacy
    try:
        from transformers.cache_utils import DynamicCache
        # transformers >=4.57 accepts `ddp_cache_data` as the first arg:
        # an iterable of per-layer (k, v) tuples. Older versions had
        # DynamicCache.from_legacy_cache(legacy); we avoid depending on it.
        return DynamicCache(legacy)
    except (ImportError, AttributeError, TypeError):
        return legacy
