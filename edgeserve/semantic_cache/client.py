import hashlib
import uuid
from typing import Iterable, Optional, Tuple

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
        self.http.write_block(str(block_uuid), data)

        bloom = SemanticBloomFilter.for_capacity(self.bloom_capacity)
        for e in entities:
            bloom.add(e)

        prefix_hash = hashlib.sha256(prefix_tokens).digest() if prefix_tokens else b''
        header = CacheHeader(
            block_uuid=block_uuid,
            node_uri=self.http.uri,
            prefix_hash=prefix_hash,
            bloom=bloom,
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
        satisfies the query or all fetches fail.
        """
        for header in self.catalog.lookup(entities):
            try:
                data = http_fetch(header.node_uri, header.block_uuid, timeout=timeout)
                return data, header
            except Exception:
                continue
        return None

    def close(self) -> None:
        self.publisher.close()
        self.catalog.close()
        self.http.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
