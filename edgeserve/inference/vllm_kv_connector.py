"""EdgeServeKVConnector: a vllm.KVConnectorBase_V1 that routes KV cache
blocks through EdgeServe's SemanticCacheClient (Pulsar-backed discovery +
HTTP / same-host transport).

Mirrors the block-layout handling of vLLM's own `ExampleConnector` (the
debug connector in vllm.distributed.kv_transfer.kv_connector.v1.example_connector),
but replaces its per-host shared-disk storage with EdgeServe's
Semantic Cache Routing primitives.

Flow on a cache MISS (new prompt):
  1. scheduler: _found_match returns False -> request is flagged is_store
  2. worker: save_kv_layer() extracts each layer's KV by slot_mapping and
     stashes bytes in `_pending_saves[request_hash][layer_name]`
  3. worker: wait_for_save() serializes all layers for each pending request
     and calls SemanticCacheClient.publish({request_hash}, bytes)

Flow on a cache HIT (new prompt whose prefix hash we've seen):
  1. scheduler: catalog.lookup({request_hash}) returns a header
     -> get_num_new_matched_tokens returns the aligned token count
  2. vLLM's scheduler pre-allocates block_ids for those tokens
  3. scheduler: build_connector_meta adds is_store=False + block_ids
  4. worker: start_load_kv() calls SemanticCacheClient.resolve_into to
     get the bytes (same-host safe_open fast path when available),
     decodes per-layer tensors, and scatters into the paged buffer
     via `slot_mapping`.

Registering with vLLM
---------------------
    from edgeserve.inference.vllm_kv_connector import register
    register()  # idempotent

    from vllm import LLM
    from vllm.config import KVTransferConfig
    llm = LLM(
        model='Qwen/Qwen2.5-1.5B',
        enable_prefix_caching=True,
        kv_transfer_config=KVTransferConfig(
            kv_connector='EdgeServeKVConnector',
            kv_connector_extra_config={
                'pulsar_url': 'pulsar://localhost:6650',
                'topic': 'kvcache-headers',
                'local_cache_path': '/tmp/edgeserve-kv',
                'node_id': 'worker-A',
            },
        ),
    )
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

import torch

try:
    from vllm.config import VllmConfig
    from vllm.distributed.kv_events import KVCacheEvent
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
        KVConnectorMetadata,
        KVConnectorRole,
    )
    _HAVE_VLLM = True
except ImportError:
    _HAVE_VLLM = False
    class KVConnectorBase_V1:  # type: ignore[no-redef]
        pass
    class KVConnectorMetadata:  # type: ignore[no-redef]
        pass
    class KVConnectorRole:  # type: ignore[no-redef]
        SCHEDULER = 'scheduler'
        WORKER = 'worker'
    class KVCacheEvent:  # type: ignore[no-redef]
        pass

if TYPE_CHECKING:
    from vllm.forward_context import ForwardContext
    from vllm.v1.attention.backend import AttentionMetadata
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.core.sched.output import SchedulerOutput
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.outputs import KVConnectorOutput
    from vllm.v1.request import Request


# ---------------------------------------------------------------------------
# Side-channel: user-declared semantic entity tags per vLLM request.
#
# Two modes:
#
# 1. Per-request (AsyncEngine / custom request_id):
#   set_request_entities("req-42", {"doc_id:wiki_42", "lang:en"})
#   # then pass request_id="req-42" via AsyncEngine.generate()
#
# 2. Next-request (LLM.generate() which auto-assigns IDs):
#   set_next_request_entities({"doc_id:wiki_42", "lang:en"})
#   llm.generate(prompts=[...])   # entities consumed by first request
#
# The scheduler picks these up during get_num_new_matched_tokens and includes
# them in both the bloom publish (so entity-based consumers can find this
# entry) and the entity-first lookup path (so consumers that declare the same
# tags hit this entry without needing the exact prefix hash).
_REQUEST_ENTITIES: dict[str, frozenset[str]] = {}
_NEXT_REQUEST_ENTITIES: frozenset[str] = frozenset()


def set_request_entities(request_id: str, entities: Iterable[str]) -> None:
    """Attach semantic entity tags to a specific vLLM request_id."""
    _REQUEST_ENTITIES[request_id] = frozenset(entities)


def clear_request_entities(request_id: str) -> None:
    """Remove a request's entity tags (called automatically after publish)."""
    _REQUEST_ENTITIES.pop(request_id, None)


def set_next_request_entities(entities: Iterable[str]) -> None:
    """Attach semantic entity tags to the *next* request (any request_id).

    Use this with `LLM.generate()` which auto-assigns request IDs.  The
    entities are consumed by the first incoming request and then cleared.
    """
    global _NEXT_REQUEST_ENTITIES
    _NEXT_REQUEST_ENTITIES = frozenset(entities)


logger = logging.getLogger(__name__)
# Ensure INFO-level diagnostics land in vLLM's subprocess logs. Without this,
# our logger only emits at WARNING by default.
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        '[edgeserve_kv %(levelname)s] %(message)s'
    ))
    logger.addHandler(_h)
    logger.propagate = False


# ---------------------------------------------------------------------------
# Metadata shuttled from scheduler to worker per step.

@dataclass
class _ReqSpec:
    """What the worker needs to know for one scheduled request this step."""
    request_hash: str           # hex digest; catalog key
    token_ids: list[int]        # aligned to block_size
    slot_mapping: torch.Tensor  # flat [num_aligned_tokens]
    is_store: bool
    # User-declared semantic entity tags for this request (may be empty).
    # Included in the bloom on publish; used as alternate lookup key on load.
    user_entities: frozenset[str] = field(default_factory=frozenset)
    # For entity-based hits: the block UUID the scheduler found, so the worker
    # can fetch it directly without re-running the bloom lookup.
    hit_block_uuid: Optional[str] = None


class EdgeServeKVMetadata(KVConnectorMetadata):
    """Per-step blob describing what the worker must load/save."""
    def __init__(self) -> None:
        if _HAVE_VLLM:
            super().__init__()
        self.requests: list[_ReqSpec] = []


def _align_to_block(num_tokens: int, block_size: int) -> int:
    """Align to block boundary (below). Matches ExampleConnector convention."""
    if num_tokens <= 0:
        return 0
    return (num_tokens - 1) // block_size * block_size


def _hash_token_ids(token_ids: list[int]) -> str:
    """Canonical hash of a token-id prefix. Used as the catalog entity
    tag and as the prefix_hash field of the published CacheHeader."""
    arr = torch.tensor(token_ids, dtype=torch.long).numpy().tobytes()
    return hashlib.sha256(arr).hexdigest()


def _slot_mapping_from_blocks(
    block_ids: list[int], block_size: int, num_tokens: int,
) -> torch.Tensor:
    """Replicated from ExampleConnector.ReqMeta.make_meta."""
    block_ids_tensor = torch.tensor(block_ids, dtype=torch.long)
    num_blocks = block_ids_tensor.shape[0]
    block_offsets = torch.arange(0, block_size, dtype=torch.long)
    slot = (
        block_offsets.reshape((1, block_size))
        + block_ids_tensor.reshape((num_blocks, 1)) * block_size
    )
    return slot.flatten()[:num_tokens]


# ---------------------------------------------------------------------------
# Tensor gather/scatter helpers, lifted from ExampleConnector and adapted.

def _slice_first_n_tokens(
    tensor: torch.Tensor, n: int, attn_metadata: Any,
) -> torch.Tensor:
    """Slice the first N tokens from a saved KV tensor. Axis depends on the
    attention backend that saved the tensor (see `_extract_kv_from_layer`):
    Flash has token axis = 1, others axis = 0.

    We detect Flash by dim count + leading dim == 2 to avoid importing the
    backend classes in the hot path; cheap enough.
    """
    if tensor.dim() == 3 and tensor.shape[0] == 2:
        # Flash-style (2, tokens, hidden)
        return tensor[:, :n]
    return tensor[:n]


def _extract_kv_from_layer(
    layer: torch.Tensor,
    slot_mapping: torch.Tensor,
    attn_metadata: Any,
    block_size: int,
) -> torch.Tensor:
    """Gather tokens along the paged buffer's token axis."""
    try:
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import (
            TritonAttentionMetadata,
        )
    except ImportError:
        MLACommonMetadata = object
        TritonAttentionMetadata = object

    if isinstance(attn_metadata, MLACommonMetadata):
        num_pages, page_size = layer.shape[0], layer.shape[1]
        return layer.reshape(num_pages * page_size, -1)[slot_mapping, ...]
    elif isinstance(attn_metadata, TritonAttentionMetadata):
        block_idxs = slot_mapping // block_size
        offsets = slot_mapping % block_size
        return layer[block_idxs, :, offsets]
    # Default FlashAttention: (2, num_pages, page_size, ...)
    num_pages, page_size = layer.shape[1], layer.shape[2]
    return layer.reshape(2, num_pages * page_size, -1)[:, slot_mapping, ...]


def _inject_kv_into_layer(
    dst_layer: torch.Tensor,
    src_kv: torch.Tensor,
    slot_mapping: torch.Tensor,
    attn_metadata: Any,
    block_size: int,
) -> None:
    try:
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import (
            TritonAttentionMetadata,
        )
    except ImportError:
        MLACommonMetadata = object
        TritonAttentionMetadata = object

    shape = dst_layer.shape
    if isinstance(attn_metadata, MLACommonMetadata):
        num_pages, page_size = shape[0], shape[1]
        dst = dst_layer.reshape(num_pages * page_size, -1)
        dst[slot_mapping, ...] = src_kv.to(dst.device, non_blocking=True)
    elif isinstance(attn_metadata, TritonAttentionMetadata):
        block_idxs = slot_mapping // block_size
        offsets = slot_mapping % block_size
        dst_layer[block_idxs, :, offsets] = src_kv.to(
            dst_layer.device, non_blocking=True
        )
    else:
        num_pages, page_size = shape[1], shape[2]
        dst = dst_layer.reshape(2, num_pages * page_size, -1)
        dst[:, slot_mapping, ...] = src_kv.to(dst.device, non_blocking=True)


# ---------------------------------------------------------------------------
# Scheduler side: decides which requests need load vs store.

def _engine_provenance(vllm_config: "VllmConfig", block_size: int) -> dict:
    """Extract Phase-7.0 engine-provenance fields from a VllmConfig.

    Any field we can't resolve becomes None (or 0 for block_size), which the
    header validator treats as "don't care" on that dimension.  Callers pass
    the returned dict as keyword args to publish() / catalog.lookup().
    """
    out = {
        'model_id': None, 'model_version': None,
        'tokenizer_hash': None, 'block_size': block_size,
    }
    try:
        model_config = getattr(vllm_config, 'model_config', None)
        if model_config is not None:
            out['model_id'] = getattr(model_config, 'model', None)
            # HF revision / commit; may be None for local paths.
            out['model_version'] = getattr(model_config, 'revision', None)
            # dtype disambiguates bf16 vs fp16 KV that would otherwise silently
            # mix; fold it into model_version so mismatches force a miss.
            dtype = getattr(model_config, 'dtype', None)
            if dtype is not None:
                out['model_version'] = f'{out["model_version"] or ""}+dtype={dtype}'
    except Exception:
        pass
    return out


class _Scheduler:
    def __init__(self, vllm_config: "VllmConfig",
                 kv_cache_config: "KVCacheConfig | None",
                 client: "EdgeServeBackendClient",
                 block_size: int) -> None:
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config
        self._client = client
        self._block_size = block_size
        self._provenance = _engine_provenance(vllm_config, block_size)
        # request_id -> Request (we've decided to load external KV on this req)
        self._requests_need_load: dict[str, Any] = {}
        # request_id -> matched prefix length (in tokens) from the last
        # get_num_new_matched_tokens call. build_connector_meta uses this
        # to size slot_mapping correctly when the match length is shorter
        # than the current prompt.
        self._matched_len: dict[str, int] = {}
        # Cache to avoid re-hashing in this step
        self._hash_cache: dict[str, str] = {}
        # request_id -> block UUID str for entity-based hits (scheduler found
        # the header via entity tags; pass UUID to worker for direct fetch).
        self._entity_hit_uuid: dict[str, str] = {}

    def _req_hash(self, request: "Request", upto: Optional[int] = None) -> str:
        token_ids = list(request.prompt_token_ids or [])
        if upto is not None:
            token_ids = token_ids[:upto]
        key = (request.request_id, upto)
        if key not in self._hash_cache:
            self._hash_cache[str(key)] = _hash_token_ids(token_ids)
        return self._hash_cache[str(key)]

    def _catalog_has(self, request_hash: str) -> bool:
        """Return True iff a catalog header exactly covers this prefix hash.

        Phase 7.0: the catalog now post-filters bloom-positive candidates on
        the publisher's explicit prefix-hash list AND on engine provenance
        (model_id / model_version / block_size).  A bloom false positive or a
        cross-model KV can no longer satisfy this check.
        """
        hits = self._client.client.catalog.lookup(
            {request_hash},
            exact_validate=True,
            engine_model_id=self._provenance.get('model_id'),
            engine_model_version=self._provenance.get('model_version'),
            engine_tokenizer_hash=self._provenance.get('tokenizer_hash'),
            engine_block_size=self._provenance.get('block_size', 0),
        )
        return bool(hits)

    def _any_prefix_in_catalog(self, aligned_tokens: list) -> bool:
        """Check if any block-aligned prefix of `aligned_tokens` is cached."""
        bs = self._block_size
        for n in range(len(aligned_tokens), bs - 1, -bs):
            if self._catalog_has(_hash_token_ids(aligned_tokens[:n])):
                return True
        return False

    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int,
    ) -> tuple[Optional[int], bool]:
        """Find the best cached KV for this request.

        Lookup order:
          1. Entity-first: if the request has user-declared entity tags, check
             the catalog for any header whose bloom covers ALL declared tags.
             The header's num_tokens tells us the coverage; we use it directly.
          2. Prefix-hash fallback: walk block-aligned prefix lengths from
             longest to shortest, checking the hash at each boundary.

        Publishers emit multi-boundary prefix hashes AND user entity tags so
        that either path can find the same entry.
        """
        token_ids = list(request.prompt_token_ids or [])
        bs = self._block_size
        max_aligned = _align_to_block(len(token_ids) - 1, bs)
        if max_aligned <= num_computed_tokens:
            return 0, False

        # --- Entity-first path ---
        global _NEXT_REQUEST_ENTITIES
        user_ents = _REQUEST_ENTITIES.get(request.request_id, frozenset())
        if not user_ents and _NEXT_REQUEST_ENTITIES:
            user_ents = _NEXT_REQUEST_ENTITIES
            _NEXT_REQUEST_ENTITIES = frozenset()  # consume once
            _REQUEST_ENTITIES[request.request_id] = user_ents  # persist for build_connector_meta
        if user_ents:
            hits = self._client.client.catalog.lookup(
                user_ents,
                exact_validate=True,
                engine_model_id=self._provenance.get('model_id'),
                engine_model_version=self._provenance.get('model_version'),
                engine_tokenizer_hash=self._provenance.get('tokenizer_hash'),
                engine_block_size=self._provenance.get('block_size', 0),
            )
            if hits:
                best = hits[0]  # catalog ranks by most-recent
                n = best.num_tokens
                if n > num_computed_tokens and n <= max_aligned:
                    logger.info(
                        'EdgeServe entity HIT for request %s via tags %s '
                        '(%d tokens, block %s)',
                        request.request_id, user_ents, n, best.block_uuid,
                    )
                    self._matched_len[request.request_id] = n
                    self._entity_hit_uuid[request.request_id] = str(best.block_uuid)
                    return n - num_computed_tokens, False

        # --- Prefix-hash fallback ---
        for n in range(max_aligned, max(num_computed_tokens, bs) - 1, -bs):
            h = _hash_token_ids(token_ids[:n])
            if self._catalog_has(h):
                logger.info(
                    'EdgeServe prefix HIT for request %s (%d of %d tokens matched)',
                    request.request_id, n, len(token_ids),
                )
                self._matched_len[request.request_id] = n
                return n - num_computed_tokens, False
        return 0, False

    def update_state_after_alloc(
        self, request: "Request", blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        if num_external_tokens > 0:
            self._requests_need_load[request.request_id] = request

    def build_connector_meta(
        self, scheduler_output: "SchedulerOutput",
    ) -> KVConnectorMetadata:
        meta = EdgeServeKVMetadata()
        total_load = 0
        total_store = 0

        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])
            # block_ids[0]: blocks for this sequence in the default KV group
            if not new_req.block_ids or not new_req.block_ids[0]:
                continue
            block_ids = new_req.block_ids[0]

            if new_req.req_id in self._requests_need_load:
                # Use the prefix length the scheduler matched (may be shorter
                # than the full aligned prompt when doc+suffix is longer than
                # the cached doc).
                matched_n = self._matched_len.get(
                    new_req.req_id,
                    _align_to_block(len(token_ids) - 1, self._block_size),
                )
                aligned_tokens = token_ids[:matched_n]
                req_hash = _hash_token_ids(aligned_tokens)
                slot = _slot_mapping_from_blocks(
                    block_ids, self._block_size, matched_n,
                )
                meta.requests.append(_ReqSpec(
                    request_hash=req_hash,
                    token_ids=aligned_tokens,
                    slot_mapping=slot,
                    is_store=False,
                    user_entities=_REQUEST_ENTITIES.get(new_req.req_id, frozenset()),
                    hit_block_uuid=self._entity_hit_uuid.get(new_req.req_id),
                ))
                total_load += 1
            else:
                num_to_check = _align_to_block(len(token_ids) - 1, self._block_size)
                if num_to_check <= 0:
                    continue
                aligned_tokens = token_ids[:num_to_check]
                req_hash = _hash_token_ids(aligned_tokens)
                # Only store when we didn't match ANY prefix length (avoids
                # redundant stores when a shorter prefix already hits).
                if not self._any_prefix_in_catalog(aligned_tokens):
                    slot = _slot_mapping_from_blocks(
                        block_ids, self._block_size, len(aligned_tokens),
                    )
                    meta.requests.append(_ReqSpec(
                        request_hash=req_hash,
                        token_ids=aligned_tokens,
                        slot_mapping=slot,
                        is_store=True,
                        user_entities=_REQUEST_ENTITIES.get(new_req.req_id, frozenset()),
                    ))
                    total_store += 1

        # Handle preempt-resumed requests (see ExampleConnector.build_connector_meta).
        cached = getattr(scheduler_output, 'scheduled_cached_reqs', None)
        if cached is not None:
            for i, req_id in enumerate(cached.req_ids):
                if req_id not in self._requests_need_load:
                    continue
                resumed = getattr(cached, 'resumed_req_ids', set())
                if req_id not in resumed:
                    continue
                num_computed = cached.num_computed_tokens[i]
                num_new = scheduler_output.num_scheduled_tokens[req_id]
                new_block_ids = cached.new_block_ids[i]
                if not new_block_ids or not new_block_ids[0]:
                    continue
                request = self._requests_need_load[req_id]
                total_tokens = num_computed + num_new
                token_ids = list(request.all_token_ids)[:total_tokens]
                slot = _slot_mapping_from_blocks(
                    new_block_ids[0], self._block_size, len(token_ids),
                )
                meta.requests.append(_ReqSpec(
                    request_hash=_hash_token_ids(token_ids),
                    token_ids=token_ids,
                    slot_mapping=slot,
                    is_store=False,
                ))
                total_load += 1

        self._requests_need_load.clear()
        self._hash_cache.clear()
        self._matched_len.clear()
        self._entity_hit_uuid.clear()
        if total_load or total_store:
            logger.info('EdgeServe build_connector_meta: load=%d store=%d',
                        total_load, total_store)
        return meta

    def request_finished(
        self, request: "Request", block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        return False, None

    def take_events(self) -> Iterable["KVCacheEvent"]:
        return []


# ---------------------------------------------------------------------------
# Worker side: gathers / scatters actual bytes between paged buffer and
# EdgeServe.

class _Worker:
    def __init__(self, vllm_config: "VllmConfig",
                 kv_cache_config: "KVCacheConfig | None",
                 client: "EdgeServeBackendClient",
                 block_size: int) -> None:
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config
        self._client = client
        self._block_size = block_size
        self._provenance = _engine_provenance(vllm_config, block_size)
        self._connector_metadata: Optional[EdgeServeKVMetadata] = None
        # request_hash -> {layer_name: cpu_tensor}. Accumulated across
        # save_kv_layer calls; flushed in wait_for_save.
        self._pending_saves: dict[str, dict[str, torch.Tensor]] = {}

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        pass  # No-op; gather uses per-call `kv_layer` tensors.

    def bind_connector_metadata(self, metadata: KVConnectorMetadata) -> None:
        assert isinstance(metadata, EdgeServeKVMetadata)
        self._connector_metadata = metadata

    def clear_connector_metadata(self) -> None:
        self._connector_metadata = None

    def start_load_kv(
        self, forward_context: "ForwardContext", **kwargs: Any,
    ) -> None:
        if self._connector_metadata is None:
            return
        attn_metadata = getattr(forward_context, 'attn_metadata', None)
        if attn_metadata is None:
            return

        for req in self._connector_metadata.requests:
            if req.is_store:
                continue
            # Entity hit: scheduler found a header by entity tags; fetch by UUID
            # directly so we don't need to re-run the bloom query.
            if req.hit_block_uuid is not None:
                hit = self._client.client.resolve_by_uuid(
                    uuid.UUID(req.hit_block_uuid), timeout=5.0,
                )
                if hit is not None:
                    logger.info(
                        'EdgeServe entity load: block %s (%d tokens)',
                        req.hit_block_uuid, req.slot_mapping.shape[0],
                    )
            else:
                hit = self._client.client.resolve({req.request_hash}, timeout=5.0)
            if hit is None:
                logger.warning(
                    'EdgeServe: scheduler said cache HIT but resolve returned None '
                    '(request_hash=%s, entity_uuid=%s)',
                    req.request_hash, req.hit_block_uuid,
                )
                continue

            blob, _header = hit
            try:
                from safetensors.torch import load as st_load
                tensors = st_load(blob)
            except Exception as e:
                logger.exception('EdgeServe: failed to decode KV blob: %s', e)
                continue

            # forward_context.no_compile_layers is {name: module}; each
            # attention module has .kv_cache attribute holding the paged buffer.
            want_n = req.slot_mapping.shape[0]
            for layer_name in forward_context.no_compile_layers:
                layer = forward_context.no_compile_layers[layer_name]
                kv_cache_layer = getattr(layer, 'kv_cache', None)
                if kv_cache_layer is None:
                    continue
                if layer_name not in tensors:
                    continue
                src = tensors[layer_name]
                # attn_metadata can be a dict keyed by layer_name (Triton backend).
                layer_attn = attn_metadata.get(layer_name) \
                    if isinstance(attn_metadata, dict) else attn_metadata
                # Saved tensor may have MORE tokens than we want (publisher
                # had a longer prompt; this consumer matched a shorter
                # prefix). Slice to want_n along the correct axis.
                src = _slice_first_n_tokens(src, want_n, layer_attn)
                _inject_kv_into_layer(
                    kv_cache_layer, src, req.slot_mapping,
                    layer_attn, self._block_size,
                )

    def wait_for_layer_load(self, layer_name: str) -> None:
        pass  # synchronous load today

    def save_kv_layer(
        self, layer_name: str, kv_layer: torch.Tensor,
        attn_metadata: "AttentionMetadata", **kwargs: Any,
    ) -> None:
        if self._connector_metadata is None:
            logger.debug('EdgeServe save_kv_layer: no metadata bound; skip %s',
                         layer_name)
            return
        store_reqs = [r for r in self._connector_metadata.requests if r.is_store]
        if not store_reqs:
            return
        logger.debug('EdgeServe save_kv_layer: %s <- %d store requests',
                     layer_name, len(store_reqs))
        for req in store_reqs:
            layer_attn = attn_metadata.get(layer_name) \
                if isinstance(attn_metadata, dict) else attn_metadata
            try:
                kv_slice = _extract_kv_from_layer(
                    kv_layer, req.slot_mapping, layer_attn, self._block_size,
                )
            except Exception as e:
                logger.exception('EdgeServe extract_kv failed on %s: %s',
                                 layer_name, e)
                continue
            entry = self._pending_saves.setdefault(
                req.request_hash, {
                    'layers': {},
                    'token_ids': req.token_ids,
                    'user_entities': req.user_entities,
                },
            )
            entry['layers'][layer_name] = \
                kv_slice.detach().cpu().contiguous()

    def wait_for_save(self) -> None:
        if not self._pending_saves:
            return
        try:
            from safetensors.torch import save as st_save
        except ImportError:
            self._pending_saves.clear()
            return

        # Publish each cached entry under ALL its block-boundary prefix
        # hashes as bloom entities on a single header. This lets a consumer
        # with a shorter / differently-suffixed prompt still hit the cache
        # via the LONGEST SHARED prefix.
        for req_hash, entry in self._pending_saves.items():
            layers = entry.get('layers', {}) if isinstance(entry, dict) else entry
            token_ids = entry.get('token_ids', []) if isinstance(entry, dict) else []
            user_ents = entry.get('user_entities', frozenset()) if isinstance(entry, dict) else frozenset()
            if not layers:
                continue
            try:
                blob = st_save(layers)
                # Split the bloom entities into two buckets so the header can
                # advertise explicit exact-match lists (Phase 7.0).  Prefix
                # hashes are deterministic from the tokens; user_entities are
                # whatever tags the caller attached via set_request_entities.
                prefix_hashes = self._multi_boundary_hashes(token_ids, req_hash)
                self._client.client.publish(
                    prefix_hashes,
                    blob,
                    num_tokens=len(token_ids),
                    user_entities=set(user_ents),
                    model_id=self._provenance.get('model_id'),
                    model_version=self._provenance.get('model_version'),
                    tokenizer_hash=self._provenance.get('tokenizer_hash'),
                    block_size=self._provenance.get('block_size', 0),
                )
                logger.info(
                    'EdgeServe: published KV for hash=%s (%d layers, %.2f MB, '
                    '%d prefix tags, %d entity tags, model=%s)',
                    req_hash, len(layers), len(blob) / 1e6,
                    len(prefix_hashes), len(user_ents),
                    self._provenance.get('model_id'),
                )
            except Exception as e:
                logger.exception('EdgeServe publish failed: %s', e)
        self._pending_saves.clear()

    def _multi_boundary_hashes(
        self, token_ids: list, full_hash: str,
    ) -> set:
        """Return hashes at every block-boundary prefix so that any consumer
        whose prompt shares a block-aligned prefix with us can find this
        entry in the catalog."""
        entities = {full_hash}
        if not token_ids:
            return entities
        bs = self._block_size
        for n in range(bs, len(token_ids) + 1, bs):
            if n == len(token_ids):
                continue  # already included as full_hash
            entities.add(_hash_token_ids(list(token_ids[:n])))
        return entities

    def get_finished(
        self, finished_req_ids: set[str],
    ) -> tuple[set[str] | None, set[str] | None]:
        return None, None


# ---------------------------------------------------------------------------
# Backend client.

# Process-wide cache. vLLM instantiates the connector TWICE (SCHEDULER role
# + WORKER role) per engine. Both need to talk to the same SemanticCacheClient
# -- and Pulsar subscriptions are Exclusive, so creating two clients with the
# same node_id conflicts on the catalog subscription. Share the instance.
_BACKEND_CACHE: dict[tuple, "EdgeServeBackendClient"] = {}


class EdgeServeBackendClient:
    def __init__(self, client) -> None:
        self.client = client
        self._close_me = False

    def close(self) -> None:
        if not self._close_me:
            return
        try:
            self.client.close()
        except Exception:
            pass

    @classmethod
    def get_or_create(cls, extra_config: dict) -> "EdgeServeBackendClient":
        key = (
            extra_config.get('pulsar_url', 'pulsar://localhost:6650'),
            extra_config.get('topic', 'kvcache-headers'),
            extra_config.get('node_id', 'vllm-rank-0'),
        )
        if key in _BACKEND_CACHE:
            return _BACKEND_CACHE[key]
        from edgeserve.semantic_cache import SemanticCacheClient
        sc = SemanticCacheClient(
            pulsar_node=key[0],
            node_id=key[2],
            local_cache_path=extra_config.get('local_cache_path', '/tmp/edgeserve-kv'),
            http_port=int(extra_config.get('http_port', 0)),
            http_host=extra_config.get('http_host', '0.0.0.0'),
            topic=key[1],
        )
        handle = cls(sc)
        handle._close_me = True
        _BACKEND_CACHE[key] = handle
        return handle


# ---------------------------------------------------------------------------
# The top-level connector class.

class EdgeServeKVConnector(KVConnectorBase_V1):
    """vLLM KV connector backed by EdgeServe Semantic Cache Routing."""

    def __init__(
        self,
        vllm_config: "VllmConfig",
        role: "KVConnectorRole",
        kv_cache_config: "KVCacheConfig | None" = None,
    ) -> None:
        if _HAVE_VLLM:
            super().__init__(vllm_config, role, kv_cache_config)

        extra_config: dict = {}
        block_size = 16
        if _HAVE_VLLM:
            if vllm_config.kv_transfer_config is not None:
                extra_config = (
                    vllm_config.kv_transfer_config.kv_connector_extra_config or {}
                )
            block_size = vllm_config.cache_config.block_size

        self._client = EdgeServeBackendClient.get_or_create(extra_config)
        self._scheduler: Optional[_Scheduler] = None
        self._worker: Optional[_Worker] = None

        if role == KVConnectorRole.SCHEDULER:
            self._scheduler = _Scheduler(
                vllm_config, kv_cache_config, self._client, block_size,
            )
        elif role == KVConnectorRole.WORKER:
            self._worker = _Worker(
                vllm_config, kv_cache_config, self._client, block_size,
            )

    # Worker-side ----------------------------------------------------------
    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        if self._worker is not None:
            self._worker.register_kv_caches(kv_caches)

    def bind_connector_metadata(self, connector_metadata: KVConnectorMetadata) -> None:
        if _HAVE_VLLM:
            super().bind_connector_metadata(connector_metadata)
        if self._worker is not None:
            self._worker.bind_connector_metadata(connector_metadata)

    def clear_connector_metadata(self) -> None:
        if _HAVE_VLLM:
            super().clear_connector_metadata()
        if self._worker is not None:
            self._worker.clear_connector_metadata()

    def save_kv_layer(self, layer_name: str, kv_layer: torch.Tensor,
                      attn_metadata: "AttentionMetadata", **kwargs: Any) -> None:
        if self._worker is not None:
            self._worker.save_kv_layer(layer_name, kv_layer, attn_metadata, **kwargs)

    def start_load_kv(self, forward_context: "ForwardContext",
                      **kwargs: Any) -> None:
        if self._worker is not None:
            self._worker.start_load_kv(forward_context, **kwargs)

    def wait_for_layer_load(self, layer_name: str) -> None:
        if self._worker is not None:
            self._worker.wait_for_layer_load(layer_name)

    def wait_for_save(self) -> None:
        if self._worker is not None:
            self._worker.wait_for_save()

    def get_finished(self, finished_req_ids: set[str]):
        if self._worker is not None:
            return self._worker.get_finished(finished_req_ids)
        return None, None

    # Scheduler-side -------------------------------------------------------
    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int,
    ) -> tuple[Optional[int], bool]:
        if self._scheduler is not None:
            return self._scheduler.get_num_new_matched_tokens(
                request, num_computed_tokens,
            )
        return 0, False

    def update_state_after_alloc(
        self, request: "Request", blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        if self._scheduler is not None:
            self._scheduler.update_state_after_alloc(
                request, blocks, num_external_tokens,
            )

    def build_connector_meta(
        self, scheduler_output: "SchedulerOutput",
    ) -> "KVConnectorMetadata":
        if self._scheduler is not None:
            return self._scheduler.build_connector_meta(scheduler_output)
        return EdgeServeKVMetadata()

    def request_finished(
        self, request: "Request", block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        if self._scheduler is not None:
            return self._scheduler.request_finished(request, block_ids)
        return False, None

    def take_events(self) -> Iterable["KVCacheEvent"]:
        if self._scheduler is not None:
            return list(self._scheduler.take_events())
        return []


_REGISTERED = False


def register() -> None:
    """Register EdgeServeKVConnector with vLLM's connector factory."""
    global _REGISTERED
    if _REGISTERED:
        return
    if not _HAVE_VLLM:
        raise RuntimeError('vLLM not importable; cannot register KV connector.')
    from vllm.distributed.kv_transfer.kv_connector.factory import (
        KVConnectorFactory,
    )
    KVConnectorFactory.register_connector(
        name='EdgeServeKVConnector',
        module_path='edgeserve.inference.vllm_kv_connector',
        class_name='EdgeServeKVConnector',
    )
    _REGISTERED = True
