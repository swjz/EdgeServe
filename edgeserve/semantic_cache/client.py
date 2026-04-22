import hashlib
import os
import socket
import uuid
from typing import Any, Dict, Iterable, Optional, Tuple

from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.catalog import HeaderCatalog
from edgeserve.semantic_cache.header import CacheHeader
from edgeserve.semantic_cache.http_client import http_fetch
from edgeserve.semantic_cache.http_server import CacheHttpServer
from edgeserve.semantic_cache.publisher import HeaderPublisher


class SemanticCacheClient:
    """Per-node handle that ties together publishing, discovery, and retrieval.

    One instance per worker node. Manages:
      - a local HTTP server serving cache blocks under `local_cache_path`
      - a Pulsar-backed header publisher for new cache blocks
      - a Pulsar-backed header catalog indexing peers' recent blocks
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
    ) -> None:
        self.node_id = node_id
        self.bloom_capacity = bloom_capacity

        self.http = CacheHttpServer(local_cache_path, port=http_port, host=http_host)
        self.http.start()
        self.publisher = HeaderPublisher(pulsar_node, topic=topic)
        self.catalog = HeaderCatalog(
            pulsar_node, node_id=node_id, topic=topic, ttl_ms=ttl_ms
        )

    def publish(
        self,
        entities: Iterable[str],
        data: bytes,
        prefix_tokens: Optional[bytes] = None,
    ) -> uuid.UUID:
        """Store `data` locally, then broadcast a header describing it.

        `entities` are semantic tags (doc IDs, function names, file paths, etc.)
        that agents will later query against. `prefix_tokens`, if given, is
        SHA-256'd for the exact-match fallback hash.
        """
        block_uuid = uuid.uuid4()
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
            local_path=os.path.abspath(local_path),
        )
        self.publisher.publish(header)
        # Seed local catalog immediately so same-node resolve() works without
        # waiting for the subscription round trip.
        self.catalog.insert(header)
        return block_uuid

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
