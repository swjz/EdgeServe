import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import msgpack

from edgeserve.semantic_cache.bloom import SemanticBloomFilter


@dataclass
class CacheHeader:
    block_uuid: uuid.UUID
    node_uri: str
    prefix_hash: bytes
    bloom: SemanticBloomFilter
    created_ms: float = field(default_factory=lambda: time.time() * 1000)
    # Same-host fast path: when the publisher and consumer share a filesystem
    # (e.g. multiple workers on one GPU box), the consumer can open the
    # block file directly and skip the HTTP round trip. Populated by
    # SemanticCacheClient.publish(). Legacy headers without these fields are
    # still usable over HTTP.
    hostname: Optional[str] = None
    local_path: Optional[str] = None
    # Number of prompt tokens whose KV this blob covers (block-aligned).
    # Required for entity-based hits where the consumer can't infer coverage
    # from its own prompt structure. Zero means unknown (legacy header).
    num_tokens: int = 0
    # Tombstone flag: when True, consumers must remove this UUID from their
    # catalog rather than indexing it. Set by the tiered store on L3 eviction.
    deleted: bool = False

    def to_bytes(self) -> bytes:
        d = {
            'block_uuid': self.block_uuid.bytes,
            'node_uri': self.node_uri,
            'prefix_hash': self.prefix_hash,
            'bloom': self.bloom.to_bytes(),
            'created_ms': self.created_ms,
            'num_tokens': self.num_tokens,
        }
        if self.hostname is not None:
            d['hostname'] = self.hostname
        if self.local_path is not None:
            d['local_path'] = self.local_path
        if self.deleted:
            d['deleted'] = True
        return msgpack.packb(d, use_bin_type=True)

    @classmethod
    def from_bytes(cls, blob: bytes) -> 'CacheHeader':
        d = msgpack.unpackb(blob, raw=False)
        return cls(
            block_uuid=uuid.UUID(bytes=d['block_uuid']),
            node_uri=d['node_uri'],
            prefix_hash=d['prefix_hash'],
            bloom=SemanticBloomFilter.from_bytes(d['bloom']),
            created_ms=d['created_ms'],
            hostname=d.get('hostname'),
            local_path=d.get('local_path'),
            num_tokens=d.get('num_tokens', 0),
            deleted=d.get('deleted', False),
        )
