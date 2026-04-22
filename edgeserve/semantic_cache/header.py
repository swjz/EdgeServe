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

    def to_bytes(self) -> bytes:
        d = {
            'block_uuid': self.block_uuid.bytes,
            'node_uri': self.node_uri,
            'prefix_hash': self.prefix_hash,
            'bloom': self.bloom.to_bytes(),
            'created_ms': self.created_ms,
        }
        if self.hostname is not None:
            d['hostname'] = self.hostname
        if self.local_path is not None:
            d['local_path'] = self.local_path
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
        )
