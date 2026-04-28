import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

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

    # ── Phase 7.0 exact-match metadata ──────────────────────────────────────
    # Bloom filters have false positives by design.  These fields let the
    # consumer verify EXACTLY what entities / prefix hashes this block covers
    # BEFORE loading its KV into the model; without this check, a Bloom false
    # positive would silently scatter the wrong attention state.
    #
    # Keep Bloom as the scalable prefilter (catalog.lookup can evaluate O(N)
    # headers via cheap bit ops); use the explicit lists as the correctness
    # gate on any bloom-positive candidate.
    #
    # Engine provenance: reject cross-model / cross-tokenizer / wrong
    # block-size hits even when a hash collision would otherwise match.
    model_id: Optional[str] = None          # e.g. "Qwen/Qwen2.5-1.5B"
    model_version: Optional[str] = None     # HF commit / checkpoint hash
    tokenizer_hash: Optional[str] = None    # stable hash of tokenizer vocab+config
    block_size: int = 0                     # 0 = unknown (legacy)

    # Explicit sets of what the bloom filter asserts membership of.
    # prefix_hashes: every block-aligned prefix hash the publisher embedded
    # entity_keys:   every user-declared entity tag (e.g. "doc_id:wiki42")
    # Both stored as lists for msgpack stability; treated as sets at lookup time.
    prefix_hashes: List[str] = field(default_factory=list)
    entity_keys: List[str] = field(default_factory=list)

    # ── Exact-match predicates (the correctness gate) ────────────────────────

    def matches_engine(
        self,
        model_id: Optional[str],
        model_version: Optional[str] = None,
        tokenizer_hash: Optional[str] = None,
        block_size: int = 0,
    ) -> bool:
        """Return True iff engine provenance is compatible with this header.

        A mismatch on any NON-EMPTY field is a hard reject.  Empty fields on
        either side degrade gracefully (legacy headers pre-7.0 have empty
        provenance and are accepted when the caller also supplies None).
        """
        if model_id is not None and self.model_id is not None:
            if model_id != self.model_id:
                return False
        if model_version is not None and self.model_version is not None:
            if model_version != self.model_version:
                return False
        if tokenizer_hash is not None and self.tokenizer_hash is not None:
            if tokenizer_hash != self.tokenizer_hash:
                return False
        if block_size and self.block_size and block_size != self.block_size:
            return False
        return True

    def covers_prefix_hash(self, prefix_hash: str) -> bool:
        """Return True iff the publisher EXPLICITLY listed this prefix hash.

        The bloom filter might say "maybe" due to a false positive; this check
        is the ground truth.  Legacy headers with empty prefix_hashes fall
        back to bloom-only (strict_exact=False in the caller).
        """
        return prefix_hash in self.prefix_hashes

    def covers_entities(self, entities) -> bool:
        """Return True iff every entity tag is in the publisher's explicit set.

        `entities` is any iterable of str.  Legacy headers (empty entity_keys)
        fall back to bloom-only.
        """
        return all(e in self.entity_keys for e in entities)

    def has_exact_metadata(self) -> bool:
        """True when this header carries Phase 7.0 exact-validation fields.

        Used by the scheduler to decide whether a bloom-positive result is
        admissible as a cache hit (exact metadata present → exact check is the
        gate) or whether to fall back to legacy bloom-only behavior.
        """
        return bool(self.prefix_hashes or self.entity_keys or self.model_id)

    # ── msgpack round trip ──────────────────────────────────────────────────

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
        # Exact-match metadata (Phase 7.0) — only packed when non-empty so the
        # wire format stays backward compatible with pre-7.0 consumers.
        if self.model_id is not None:
            d['model_id'] = self.model_id
        if self.model_version is not None:
            d['model_version'] = self.model_version
        if self.tokenizer_hash is not None:
            d['tokenizer_hash'] = self.tokenizer_hash
        if self.block_size:
            d['block_size'] = self.block_size
        if self.prefix_hashes:
            d['prefix_hashes'] = list(self.prefix_hashes)
        if self.entity_keys:
            d['entity_keys'] = list(self.entity_keys)
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
            model_id=d.get('model_id'),
            model_version=d.get('model_version'),
            tokenizer_hash=d.get('tokenizer_hash'),
            block_size=d.get('block_size', 0),
            prefix_hashes=list(d.get('prefix_hashes', []) or []),
            entity_keys=list(d.get('entity_keys', []) or []),
        )
