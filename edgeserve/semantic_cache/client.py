import hashlib
import os
import socket
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple

from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.catalog import HeaderCatalog
from edgeserve.semantic_cache.header import CacheHeader
from edgeserve.semantic_cache.http_client import http_fetch
from edgeserve.semantic_cache.http_server import CacheHttpServer
from edgeserve.semantic_cache.publisher import HeaderPublisher

if TYPE_CHECKING:
    from edgeserve.semantic_cache.tiered_store import TieredStore


class SemanticCacheClient:
    """Per-node handle that ties together publishing, discovery, and retrieval.

    One instance per worker node. Manages:
      - a local HTTP server serving cache blocks under ``local_cache_path``
      - a Pulsar-backed header publisher for new cache blocks
      - a Pulsar-backed header catalog indexing peers' recent blocks
      - optionally a TieredStore (L2 RAM + L3 NVMe) with tombstone propagation
    """

    def __init__(
        self,
        pulsar_node: str,
        node_id: str,
        local_cache_path: str,
        http_port: int = 0,
        http_host: str = '0.0.0.0',
        topic: str = 'kvcache-headers',
        ttl_ms: float = 10 * 60 * 1000,
        bloom_capacity: int = 1024,
        tiered_store: 'Optional[TieredStore]' = None,
    ) -> None:
        self.node_id = node_id
        self.bloom_capacity = bloom_capacity
        self._tiered_store = tiered_store

        self.http = CacheHttpServer(
            local_cache_path, port=http_port, host=http_host,
            tiered_store=tiered_store,
        )
        self.http.start()
        self.publisher = HeaderPublisher(pulsar_node, topic=topic)
        self.catalog = HeaderCatalog(
            pulsar_node, node_id=node_id, topic=topic, ttl_ms=ttl_ms
        )

        # Wire tombstone callback: when tiered store evicts a block below L3,
        # publish a deleted header so all catalog subscribers purge the entry.
        if tiered_store is not None and tiered_store.on_tombstone is None:
            tiered_store.on_tombstone = self._publish_tombstone

    def publish(
        self,
        entities: Iterable[str],
        data: bytes,
        prefix_tokens: Optional[bytes] = None,
        num_tokens: int = 0,
    ) -> uuid.UUID:
        """Store ``data`` locally, then broadcast a header describing it.

        Routes storage through the TieredStore if one is attached (L2 hot
        layer + L3 NVMe), otherwise writes directly to ``local_cache_path``.
        """
        block_uuid = uuid.uuid4()

        if self._tiered_store is not None:
            self._tiered_store.put(block_uuid, data)
            local_path = self._tiered_store.get_l3_path(block_uuid)
        else:
            local_path = self.http.write_block(str(block_uuid), data)

        bloom = SemanticBloomFilter.for_capacity(self.bloom_capacity)
        for e in entities:
            bloom.add(e)

        prefix_hash = hashlib.sha256(prefix_tokens).digest() if prefix_tokens else b''
        header = CacheHeader(
            block_uuid=block_uuid,
            node_uri=self.http.uri,
            prefix_hash=prefix_hash,
            bloom=bloom,
            hostname=socket.gethostname(),
            local_path=os.path.abspath(local_path) if local_path else None,
            num_tokens=num_tokens,
        )
        self.publisher.publish(header)
        # Seed local catalog immediately so same-node resolve() works without
        # waiting for the subscription round trip.
        self.catalog.insert(header)
        return block_uuid

    def _publish_tombstone(self, block_uuid: uuid.UUID) -> None:
        """Broadcast a deleted=True header so all catalog subscribers purge the entry."""
        header = CacheHeader(
            block_uuid=block_uuid,
            node_uri=self.http.uri,
            prefix_hash=b'',
            bloom=SemanticBloomFilter.for_capacity(1),
            hostname=socket.gethostname(),
            deleted=True,
        )
        self.publisher.publish(header)

    def resolve_by_uuid(
        self, block_uuid: uuid.UUID, timeout: float = 5.0,
    ) -> Optional[Tuple[bytes, CacheHeader]]:
        """Fetch a specific block by its UUID, bypassing bloom lookup.

        Used when the scheduler has already identified the matching header via
        entity-based lookup and passes the block UUID directly to the worker.
        """
        with self.catalog._lock:
            header = self.catalog._headers.get(block_uuid)
        if header is None:
            return None
        try:
            if self._is_local_readable(header):
                if self._tiered_store is not None:
                    data, _ = self._tiered_store.get(block_uuid)
                    if data is not None:
                        return data, header
                with open(header.local_path, 'rb') as f:
                    return f.read(), header
            data = http_fetch(header.node_uri, header.block_uuid, timeout=timeout)
            return data, header
        except Exception:
            return None

    def resolve(
        self, entities: Iterable[str], timeout: float = 5.0
    ) -> Optional[Tuple[bytes, CacheHeader]]:
        """Find a peer whose bloom filter matches every entity, then fetch.

        Returns `(bytes, header)` on success, or `None` if no candidate
        satisfies the query or all fetches fail. Legacy API -- pays the
        bytes round trip even when same-host.
        """
        for header in self.catalog.lookup(entities):
            try:
                if self._is_local_readable(header):
                    with open(header.local_path, 'rb') as f:
                        return f.read(), header
                data = http_fetch(header.node_uri, header.block_uuid, timeout=timeout)
                return data, header
            except Exception:
                continue
        return None

    def resolve_into(
        self, entities: Iterable[str], engine: Any, timeout: float = 5.0,
    ) -> Optional[Tuple[Any, CacheHeader, Dict[str, Any]]]:
        """Find a peer and materialize the KV cache into `engine`'s format.

        Uses the fastest available transport:
          - same-host path: `engine.deserialize_cache_from_path(local_path)` --
            lets the engine mmap the safetensors file and load directly
            onto its device, skipping the bytes round trip.
          - cross-host: HTTP fetch + `engine.deserialize_cache(bytes)`.

        Returns `(cache_handle, header, info_dict)` or None. `info_dict`
        carries `transport` ('local_path' | 'http') and transport-specific
        fields ('bytes' or 'path') for telemetry.

        `engine` only needs to quack -- `.deserialize_cache(bytes)` and
        `.deserialize_cache_from_path(path)`. Matches what `HFEngine`
        exposes; vLLM / SGLang adapters will do the same.
        """
        for header in self.catalog.lookup(entities):
            try:
                if (self._is_local_readable(header)
                        and hasattr(engine, 'deserialize_cache_from_path')):
                    cache = engine.deserialize_cache_from_path(header.local_path)
                    return cache, header, {'transport': 'local_path',
                                           'path': header.local_path}
                data = http_fetch(header.node_uri, header.block_uuid, timeout=timeout)
                cache = engine.deserialize_cache(data)
                return cache, header, {'transport': 'http', 'bytes': len(data)}
            except Exception:
                continue
        return None

    def _is_local_readable(self, header: CacheHeader) -> bool:
        return (
            header.hostname is not None
            and header.hostname == socket.gethostname()
            and header.local_path is not None
            and os.path.isfile(header.local_path)
        )

    def close(self) -> None:
        self.publisher.close()
        self.catalog.close()
        self.http.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
