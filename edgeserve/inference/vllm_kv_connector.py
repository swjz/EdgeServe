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


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metadata shuttled from scheduler to worker per step.

@dataclass
class _ReqSpec:
    """What the worker needs to know for one scheduled request this step."""
    request_hash: str           # hex digest; catalog key
    token_ids: list[int]        # aligned to block_size
    slot_mapping: torch.Tensor  # flat [num_aligned_tokens]
    is_store: bool


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

class _Scheduler:
    def __init__(self, vllm_config: "VllmConfig",
                 kv_cache_config: "KVCacheConfig | None",
                 client: "EdgeServeBackendClient",
                 block_size: int) -> None:
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config
        self._client = client
        self._block_size = block_size
        # request_id -> Request (we've decided to load external KV on this req)
        self._requests_need_load: dict[str, Any] = {}
        # Cache to avoid re-hashing in this step
        self._hash_cache: dict[str, str] = {}

    def _req_hash(self, request: "Request", upto: Optional[int] = None) -> str:
        token_ids = list(request.prompt_token_ids or [])
        if upto is not None:
            token_ids = token_ids[:upto]
        key = (request.request_id, upto)
        if key not in self._hash_cache:
            self._hash_cache[str(key)] = _hash_token_ids(token_ids)
        return self._hash_cache[str(key)]

    def _catalog_has(self, request_hash: str) -> bool:
        hits = self._client.client.catalog.lookup({request_hash})
        return bool(hits)

    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int,
    ) -> tuple[Optional[int], bool]:
        token_ids = list(request.prompt_token_ids or [])
        num_to_check = _align_to_block(len(token_ids) - 1, self._block_size)
        if num_to_check <= num_computed_tokens:
            return 0, False

        prefix_hash = _hash_token_ids(token_ids[:num_to_check])
        if not self._catalog_has(prefix_hash):
            return 0, False

        logger.info('EdgeServe cache HIT for request %s (%d tokens matched)',
                    request.request_id, num_to_check)
        return num_to_check - num_computed_tokens, False

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

        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])
            # block_ids[0]: blocks for this sequence in the default KV group
            if not new_req.block_ids or not new_req.block_ids[0]:
                continue
            block_ids = new_req.block_ids[0]

            if new_req.req_id in self._requests_need_load:
                num_to_check = _align_to_block(len(token_ids) - 1, self._block_size)
                aligned_tokens = token_ids[:num_to_check]
                req_hash = _hash_token_ids(aligned_tokens)
                slot = _slot_mapping_from_blocks(
                    block_ids, self._block_size, len(aligned_tokens),
                )
                meta.requests.append(_ReqSpec(
                    request_hash=req_hash,
                    token_ids=aligned_tokens,
                    slot_mapping=slot,
                    is_store=False,
                ))
                total_load += 1
            else:
                num_to_check = _align_to_block(len(token_ids) - 1, self._block_size)
                aligned_tokens = token_ids[:num_to_check]
                req_hash = _hash_token_ids(aligned_tokens)
                if not self._catalog_has(req_hash):
                    slot = _slot_mapping_from_blocks(
                        block_ids, self._block_size, len(aligned_tokens),
                    )
                    meta.requests.append(_ReqSpec(
                        request_hash=req_hash,
                        token_ids=aligned_tokens,
                        slot_mapping=slot,
                        is_store=True,
                    ))

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
            hit = self._client.client.resolve({req.request_hash}, timeout=5.0)
            if hit is None:
                logger.warning(
                    'EdgeServe: scheduler said cache HIT but resolve returned None '
                    '(request_hash=%s)', req.request_hash,
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
            return
        for req in self._connector_metadata.requests:
            if not req.is_store:
                continue
            layer_attn = attn_metadata.get(layer_name) \
                if isinstance(attn_metadata, dict) else attn_metadata
            kv_slice = _extract_kv_from_layer(
                kv_layer, req.slot_mapping, layer_attn, self._block_size,
            )
            self._pending_saves.setdefault(req.request_hash, {})[layer_name] = \
                kv_slice.detach().cpu().contiguous()

    def wait_for_save(self) -> None:
        if not self._pending_saves:
            return
        try:
            from safetensors.torch import save as st_save
        except ImportError:
            self._pending_saves.clear()
            return

        for req_hash, layers in self._pending_saves.items():
            if not layers:
                continue
            try:
                blob = st_save(layers)
                self._client.client.publish({req_hash}, blob)
                logger.info(
                    'EdgeServe: published KV for hash=%s (%d layers, %.2f MB)',
                    req_hash, len(layers), len(blob) / 1e6,
                )
            except Exception as e:
                logger.exception('EdgeServe publish failed: %s', e)
        self._pending_saves.clear()

    def get_finished(
        self, finished_req_ids: set[str],
    ) -> tuple[set[str] | None, set[str] | None]:
        return None, None


# ---------------------------------------------------------------------------
# Backend client.

class EdgeServeBackendClient:
    def __init__(self, extra_config: dict) -> None:
        from edgeserve.semantic_cache import SemanticCacheClient
        self.client = SemanticCacheClient(
            pulsar_node=extra_config.get('pulsar_url', 'pulsar://localhost:6650'),
            node_id=extra_config.get('node_id', 'vllm-rank-0'),
            local_cache_path=extra_config.get('local_cache_path', '/tmp/edgeserve-kv'),
            http_port=int(extra_config.get('http_port', 0)),
            http_host=extra_config.get('http_host', '0.0.0.0'),
            topic=extra_config.get('topic', 'kvcache-headers'),
        )

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


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

        self._client = EdgeServeBackendClient(extra_config)
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
