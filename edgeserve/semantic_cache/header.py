import time
import uuid
from dataclasses import dataclass, field

import msgpack

from edgeserve.semantic_cache.bloom import SemanticBloomFilter


@dataclass
class CacheHeader:
    block_uuid: uuid.UUID
    node_uri: str
    prefix_hash: bytes
    bloom: SemanticBloomFilter
    created_ms: float = field(default_factory=lambda: time.time() * 1000)

    def to_bytes(self) -> bytes:
        return msgpack.packb({
            'block_uuid': self.block_uuid.bytes,
            'node_uri': self.node_uri,
            'prefix_hash': self.prefix_hash,
            'bloom': self.bloom.to_bytes(),
            'created_ms': self.created_ms,
        }, use_bin_type=True)

    @classmethod
    def from_bytes(cls, blob: bytes) -> 'CacheHeader':
        d = msgpack.unpackb(blob, raw=False)
        return cls(
            block_uuid=uuid.UUID(bytes=d['block_uuid']),
            node_uri=d['node_uri'],
            prefix_hash=d['prefix_hash'],
            bloom=SemanticBloomFilter.from_bytes(d['bloom']),
            created_ms=d['created_ms'],
        )
