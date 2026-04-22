"""EdgeServeKVConnector: a vllm.KVConnectorBase_V1 that routes KV cache
blocks through EdgeServe's SemanticCacheClient (Pulsar-backed discovery +
HTTP / same-host transport).

Status
------
SKELETON. The class shape is complete and registers via vLLM's connector
factory. The heavy lifting -- copying KV between vLLM's PagedAttention
block pool and EdgeServe bytes -- is stubbed with `NotImplementedError`
and `TODO(EDGESERVE)` markers pointing at the exact vLLM surfaces
involved. End-to-end routed inference with vLLM requires implementing
those paths against the specific vLLM release in use; see
`vllm.distributed.kv_transfer.kv_connector.v1.simple_cpu_offload_connector`
for the closest reference (it delegates to SimpleCPUOffloadScheduler +
SimpleCPUOffloadWorker, which we mirror below as `_Scheduler` / `_Worker`).

Architecture mapping
--------------------
vLLM KVConnector role    |  EdgeServe equivalent
-------------------------+---------------------------------------------
SCHEDULER: lookup cache  |  SemanticCacheClient.catalog.lookup(entities)
SCHEDULER: allocate      |  vllm BlockPool (not ours)
SCHEDULER: tell worker   |  KVConnectorMetadata with block_uuid + layers
WORKER: save layer       |  SemanticCacheClient.publish(entities, bytes)
WORKER: load layer       |  SemanticCacheClient.resolve_into(...)
                         |    -> safetensors safe_open + .to(cuda) per layer

Registering with vLLM
---------------------
    from edgeserve.inference.vllm_kv_connector import register

    register()  # idempotent; adds EdgeServeKVConnector to vLLM's factory

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

Outstanding work
----------------
1. `_Worker.save_kv_layer` — given a per-layer KV tensor + attn_metadata,
   extract the per-sequence slice from PagedAttention block indices and
   emit bytes. vLLM's `kv_cache_manager` is the source of truth for the
   layout (block_size, num_kv_heads, head_size). The tricky bit is that
   one layer's KV for one sequence is scattered across `block_size`-sized
   fragments in the physical block pool. We need to gather them in order
   (see `vllm.v1.core.kv_cache_manager` for helpers).

2. `_Worker.start_load_kv` — go the other way. SemanticCacheClient fetches
   the layer bytes (path or HTTP), deserialize into a contiguous tensor,
   then scatter into the blocks vLLM pre-allocated for this request.

3. `_Scheduler.get_num_new_matched_tokens` — right now returns (0, False).
   Real implementation queries the bloom-filter catalog and returns how
   many tokens at the start of this request's prompt are already cached
   remotely. EdgeServe headers carry `prefix_hash` (SHA-256 of prompt
   tokens) which is the natural join key.

4. Serialization per layer vs per block. Our `kv_io.save_past_key_values`
   currently packs ALL layers as one safetensors blob. vLLM invokes
   `save_kv_layer` per layer, so we need a per-layer format. Option:
   keep the header's `block_uuid` and write one safetensors file with
   key `k.{layer_idx}` + `v.{layer_idx}` that grows as layers complete.
"""

from __future__ import annotations

from collections.abc import Iterable
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
    # Stub bases so this module at least imports without vLLM. Useful for
    # static analysis and docs; actual use requires vLLM installed.
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
# Metadata type carried from scheduler to worker.

class EdgeServeKVMetadata(KVConnectorMetadata):
    """Per-forward-pass blob describing what the worker must load/save.

    Populated by the scheduler in `build_connector_meta`, consumed by the
    worker in `start_load_kv` / `save_kv_layer`.
    """
    def __init__(self) -> None:
        super().__init__() if _HAVE_VLLM else None
        # request_id -> dict with:
        #   'load_block_uuid': Optional[UUID]   (fetch from EdgeServe)
        #   'load_block_ids':  list[int]        (where to write in vLLM's pool)
        #   'save_entities':   Optional[set[str]]  (publish after compute)
        #   'save_block_ids':  list[int]
        self.per_request: dict = {}


# ---------------------------------------------------------------------------
# The scheduler-side and worker-side helpers. Both are stubs with clear
# TODOs documenting what vLLM expects.

class _Scheduler:
    def __init__(self, vllm_config: "VllmConfig",
                 kv_cache_config: "KVCacheConfig | None",
                 client: "EdgeServeBackendClient") -> None:
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config
        self._client = client

    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int,
    ) -> tuple[Optional[int], bool]:
        # TODO(EDGESERVE): query self._client.catalog.lookup(...) for a
        # header whose prefix_hash matches a prefix of request.prompt_token_ids.
        # Return (num_matched_tokens, load_async=True) if a hit; else (0, False).
        return 0, False

    def update_state_after_alloc(
        self, request: "Request", blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        # TODO(EDGESERVE): record the mapping request -> block_uuid + block_ids
        # so build_connector_meta can emit it.
        pass

    def build_connector_meta(
        self, scheduler_output: "SchedulerOutput",
    ) -> KVConnectorMetadata:
        meta = EdgeServeKVMetadata()
        # TODO(EDGESERVE): populate meta.per_request from recorded state.
        return meta

    def request_finished(
        self, request: "Request", block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        # TODO(EDGESERVE): if we want this request's KV to be published on
        # completion, flag it here and return save_kv=True.
        return False, None

    def take_events(self) -> Iterable["KVCacheEvent"]:
        return []


class _Worker:
    def __init__(self, vllm_config: "VllmConfig",
                 kv_cache_config: "KVCacheConfig | None",
                 client: "EdgeServeBackendClient") -> None:
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config
        self._client = client
        self._kv_caches: dict[str, torch.Tensor] = {}

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        # vLLM hands us the per-layer KV block-pool tensors. These are what
        # we write into (on load) and read from (on save).
        self._kv_caches = kv_caches

    def save_kv_layer(
        self, layer_name: str, kv_layer: torch.Tensor,
        attn_metadata: "AttentionMetadata", **kwargs: Any,
    ) -> None:
        raise NotImplementedError(
            'EdgeServeKVConnector: save_kv_layer not yet implemented. See '
            '"Outstanding work" in the module docstring.'
        )

    def start_load_kv(
        self, forward_context: "ForwardContext", **kwargs: Any,
    ) -> None:
        raise NotImplementedError(
            'EdgeServeKVConnector: start_load_kv not yet implemented. See '
            '"Outstanding work" in the module docstring.'
        )

    def wait_for_layer_load(self, layer_name: str) -> None:
        pass  # Synchronous load today; revisit when async is wired.

    def wait_for_save(self) -> None:
        pass

    def get_finished(
        self, finished_req_ids: set[str],
    ) -> tuple[set[str] | None, set[str] | None]:
        return None, None


# ---------------------------------------------------------------------------
# Thin adapter around SemanticCacheClient. Keeps the connector free of
# SemanticCacheClient construction details (which differ per vLLM rank).

class EdgeServeBackendClient:
    """Owns the SemanticCacheClient instance for this vLLM rank."""

    def __init__(self, extra_config: dict) -> None:
        # Import lazily so the connector module imports cleanly without
        # edgeserve.semantic_cache available (e.g., during static tests).
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
        self.client.close()


# ---------------------------------------------------------------------------
# The connector class vLLM instantiates (once per role, per rank).

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

        extra_config = {}
        if _HAVE_VLLM and vllm_config.kv_transfer_config is not None:
            extra_config = (
                vllm_config.kv_transfer_config.kv_connector_extra_config or {}
            )

        self._client = EdgeServeBackendClient(extra_config)
        self._scheduler: Optional[_Scheduler] = None
        self._worker: Optional[_Worker] = None

        if role == KVConnectorRole.SCHEDULER:
            self._scheduler = _Scheduler(vllm_config, kv_cache_config, self._client)
        elif role == KVConnectorRole.WORKER:
            self._worker = _Worker(vllm_config, kv_cache_config, self._client)

    # ---- Worker-side ------------------------------------------------------
    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        if self._worker is not None:
            self._worker.register_kv_caches(kv_caches)

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

    # ---- Scheduler-side ---------------------------------------------------
    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int,
    ) -> tuple[Optional[int], bool]:
        if self._scheduler is not None:
            return self._scheduler.get_num_new_matched_tokens(request, num_computed_tokens)
        return 0, False

    def update_state_after_alloc(
        self, request: "Request", blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        if self._scheduler is not None:
            self._scheduler.update_state_after_alloc(request, blocks, num_external_tokens)

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
    """Register EdgeServeKVConnector with vLLM's connector factory.

    Idempotent; safe to call multiple times. Requires vLLM to be installed.
    """
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
