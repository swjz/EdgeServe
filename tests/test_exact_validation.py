"""Phase 7.0: exact-match validation after Bloom-positive lookup.

Bloom filters have false positives by design.  A false-positive KV load would
silently inject the wrong attention state.  These tests verify that the
catalog's post-filter rejects Bloom false positives and engine-provenance
mismatches, so Bloom stays a discovery prefilter rather than the correctness
decision.

All tests use the in-memory HeaderCatalog via `insert()` + `lookup()`; no
Pulsar required.
"""
from __future__ import annotations

import hashlib
import uuid

import pytest

from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.catalog import HeaderCatalog, _entities_covered_exact
from edgeserve.semantic_cache.header import CacheHeader


# ── headless catalog helper ───────────────────────────────────────────────────
# HeaderCatalog normally opens a Pulsar connection in __init__; the tests
# skip that by instantiating with object.__new__ and wiring state manually.

def _make_headless_catalog():
    cat = object.__new__(HeaderCatalog)
    import threading
    cat._headers = {}
    cat._lock = threading.Lock()
    cat.rank_fn = lambda h: h.created_ms
    return cat


def _h(entity: str) -> str:
    return hashlib.sha256(entity.encode()).hexdigest()


def _make_header(
    *, prefix_hashes=None, entity_keys=None,
    model_id='Qwen/Qwen2.5-1.5B', model_version='abc123+dtype=bfloat16',
    block_size=16, bloom_capacity=1024,
):
    ph = list(prefix_hashes or [])
    ek = list(entity_keys or [])
    bloom = SemanticBloomFilter.for_capacity(bloom_capacity)
    for e in ph + ek:
        bloom.add(e)
    return CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://localhost:0',
        prefix_hash=b'',
        bloom=bloom,
        num_tokens=128,
        model_id=model_id,
        model_version=model_version,
        block_size=block_size,
        prefix_hashes=ph,
        entity_keys=ek,
    )


# ── header predicates ─────────────────────────────────────────────────────────

def test_matches_engine_accepts_same_model():
    h = _make_header(prefix_hashes=['abc'])
    assert h.matches_engine('Qwen/Qwen2.5-1.5B', 'abc123+dtype=bfloat16', None, 16)


def test_matches_engine_rejects_different_model_id():
    h = _make_header(prefix_hashes=['abc'])
    assert not h.matches_engine('meta-llama/Llama-3-8B')


def test_matches_engine_rejects_different_block_size():
    h = _make_header(prefix_hashes=['abc'], block_size=16)
    assert not h.matches_engine('Qwen/Qwen2.5-1.5B', block_size=32)


def test_matches_engine_rejects_different_dtype():
    h = _make_header(prefix_hashes=['abc'], model_version='v1+dtype=bfloat16')
    assert not h.matches_engine(
        'Qwen/Qwen2.5-1.5B', model_version='v1+dtype=float16',
    )


def test_matches_engine_tolerates_missing_fields():
    """Legacy headers (pre-7.0 with no provenance) shouldn't be auto-rejected
    when a modern consumer asks."""
    h = _make_header(prefix_hashes=['abc'])
    h.model_id = None
    h.model_version = None
    h.block_size = 0
    # Consumer specifies its own engine — the header has no opinion, accept.
    assert h.matches_engine('Qwen/Qwen2.5-1.5B', 'v1', None, 16)


def test_covers_prefix_hash_and_entities():
    h = _make_header(prefix_hashes=['h_64', 'h_32', 'h_16'],
                     entity_keys=['doc_id:wiki42'])
    assert h.covers_prefix_hash('h_64')
    assert not h.covers_prefix_hash('h_48')
    assert h.covers_entities(['doc_id:wiki42'])
    assert not h.covers_entities(['doc_id:other'])


def test_has_exact_metadata_detects_legacy():
    legacy = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://x',
        prefix_hash=b'',
        bloom=SemanticBloomFilter.for_capacity(16),
    )
    assert not legacy.has_exact_metadata()
    exact = _make_header(prefix_hashes=['h1'])
    assert exact.has_exact_metadata()


# ── _entities_covered_exact helper ────────────────────────────────────────────

def test_entities_covered_accepts_prefix_or_entity_bucket():
    h = _make_header(prefix_hashes=['hash_A'], entity_keys=['tag_X'])
    assert _entities_covered_exact(h, ['hash_A'])      # from prefix bucket
    assert _entities_covered_exact(h, ['tag_X'])       # from entity bucket
    assert _entities_covered_exact(h, ['hash_A', 'tag_X'])  # mixed
    assert not _entities_covered_exact(h, ['hash_B'])  # not listed


# ── catalog lookup: bloom false-positive rejection ────────────────────────────

def _force_bloom_false_positive_header(true_ent: str, false_ent: str):
    """Construct a header whose bloom flags true_ent AND (by collision or by
    adding it directly to the bloom but NOT to the exact list) false_ent.

    We simulate the real-world false positive by adding false_ent to the bloom
    but leaving it out of prefix_hashes / entity_keys.  This mirrors what a
    bloom-FPR collision produces: bloom says "maybe", explicit list says "no".
    """
    bloom = SemanticBloomFilter.for_capacity(1024)
    bloom.add(true_ent)
    bloom.add(false_ent)  # simulate collision — flagged but not in exact list
    return CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://localhost:0',
        prefix_hash=b'',
        bloom=bloom,
        num_tokens=128,
        model_id='Qwen/Qwen2.5-1.5B',
        model_version='v1+dtype=bfloat16',
        block_size=16,
        prefix_hashes=[true_ent],   # ← only the true entity is explicitly listed
        entity_keys=[],
    )


def test_bloom_false_positive_is_rejected_by_exact_validation():
    """Bloom says yes; explicit list says no → lookup must return []."""
    cat = _make_headless_catalog()
    true_hash = _h('real-document-prefix')
    colliding_hash = _h('totally-different-prompt')

    hdr = _force_bloom_false_positive_header(true_hash, colliding_hash)
    cat._headers[hdr.block_uuid] = hdr

    # Sanity: bloom would return this header on either query.
    assert true_hash in hdr.bloom
    assert colliding_hash in hdr.bloom

    # Exact validation catches the false positive.
    hits_true = cat.lookup({true_hash})
    hits_false = cat.lookup({colliding_hash})
    assert len(hits_true) == 1, 'true prefix must still match'
    assert len(hits_false) == 0, 'false positive must be rejected'


def test_legacy_header_without_exact_metadata_still_matches_via_bloom():
    """Headers written by pre-7.0 publishers have empty prefix_hashes /
    entity_keys.  The catalog must not break backward compat — those headers
    fall back to bloom-only matching."""
    cat = _make_headless_catalog()
    bloom = SemanticBloomFilter.for_capacity(128)
    bloom.add('legacy-tag')
    legacy = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://x', prefix_hash=b'', bloom=bloom,
        num_tokens=64,
    )
    assert not legacy.has_exact_metadata()
    cat._headers[legacy.block_uuid] = legacy

    hits = cat.lookup({'legacy-tag'})
    assert len(hits) == 1, 'legacy header should pass through bloom-only'


def test_cross_model_rejection():
    """A header published by a different model must not satisfy our query."""
    cat = _make_headless_catalog()
    h = _h('doc-hash-shared')
    hdr = _make_header(prefix_hashes=[h], model_id='meta-llama/Llama-3-8B')
    cat._headers[hdr.block_uuid] = hdr

    # Same model → hit.
    hits_same = cat.lookup(
        {h}, engine_model_id='meta-llama/Llama-3-8B',
        engine_block_size=16,
    )
    assert len(hits_same) == 1

    # Different model → miss, even though bloom + prefix_hashes match.
    hits_diff = cat.lookup(
        {h}, engine_model_id='Qwen/Qwen2.5-1.5B',
        engine_block_size=16,
    )
    assert len(hits_diff) == 0


def test_cross_dtype_rejection():
    """bf16-seeded KV must not silently serve an fp16 consumer."""
    cat = _make_headless_catalog()
    h = _h('tokens-abcdef')
    hdr = _make_header(prefix_hashes=[h], model_version='abc+dtype=bfloat16')
    cat._headers[hdr.block_uuid] = hdr

    hits_same = cat.lookup(
        {h}, engine_model_id='Qwen/Qwen2.5-1.5B',
        engine_model_version='abc+dtype=bfloat16', engine_block_size=16,
    )
    assert len(hits_same) == 1

    hits_diff_dtype = cat.lookup(
        {h}, engine_model_id='Qwen/Qwen2.5-1.5B',
        engine_model_version='abc+dtype=float16', engine_block_size=16,
    )
    assert len(hits_diff_dtype) == 0


def test_cross_block_size_rejection():
    """vLLM's paged buffer layout depends on block_size; mismatched blocks
    would scatter into the wrong slots."""
    cat = _make_headless_catalog()
    h = _h('doc-block16')
    hdr = _make_header(prefix_hashes=[h], block_size=16)
    cat._headers[hdr.block_uuid] = hdr

    assert len(cat.lookup({h}, engine_block_size=16)) == 1
    assert len(cat.lookup({h}, engine_block_size=32)) == 0


def test_exact_validate_false_bypasses_gate():
    """Callers that already verified exactness elsewhere (e.g. resolve-by-uuid
    on the same node) can disable the gate."""
    cat = _make_headless_catalog()
    hdr = _force_bloom_false_positive_header(_h('real'), _h('collision'))
    cat._headers[hdr.block_uuid] = hdr
    hits = cat.lookup({_h('collision')}, exact_validate=False)
    assert len(hits) == 1, 'exact_validate=False restores legacy behavior'


# ── high-FPR stress test: tiny bloom guarantees real collisions ──────────────

def test_tight_bloom_stress_many_false_positives_rejected():
    """Stuff a tiny bloom with many tags so FPR climbs; then query with a tag
    that was NEVER inserted.  Without exact validation, many of these queries
    would return the header.  With exact validation, none should."""
    # m_bits=64, k=7 → ~8 bytes.  After 50 inserts this is almost saturated;
    # FPR on unseen tags will be very high.
    bloom = SemanticBloomFilter(m_bits=64, k=7)
    inserted_tags = [f'real_tag_{i}' for i in range(50)]
    for t in inserted_tags:
        bloom.add(t)
    hdr = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://x',
        prefix_hash=b'',
        bloom=bloom,
        num_tokens=64,
        model_id='Qwen/Qwen2.5-1.5B',
        model_version='v1+dtype=bfloat16',
        block_size=16,
        prefix_hashes=list(inserted_tags),
        entity_keys=[],
    )

    cat = _make_headless_catalog()
    cat._headers[hdr.block_uuid] = hdr

    # Probe with tags that were NEVER inserted.  Count how many the bloom
    # returns a false positive on; the catalog must still return [] for all.
    unseen = [f'unseen_tag_{i}' for i in range(500)]
    bloom_fps = sum(1 for t in unseen if t in hdr.bloom)
    # Not strictly required for the test, but assert the stress actually
    # produces false positives so the test is meaningful.
    assert bloom_fps > 5, (
        f'Tight bloom should produce false positives but got only {bloom_fps}'
    )

    for t in unseen:
        hits = cat.lookup({t}, engine_model_id='Qwen/Qwen2.5-1.5B',
                          engine_block_size=16)
        assert hits == [], (
            f'Tag {t!r} was never inserted but catalog returned a hit — '
            f'exact validation failed to reject a bloom false positive'
        )

    # Sanity: real tags still match.
    for t in inserted_tags[:10]:
        hits = cat.lookup({t}, engine_model_id='Qwen/Qwen2.5-1.5B',
                          engine_block_size=16)
        assert len(hits) == 1


# ── header round trip: exact metadata survives msgpack ──────────────────────

def test_header_roundtrip_preserves_exact_metadata():
    h = _make_header(
        prefix_hashes=['h1', 'h2', 'h3'],
        entity_keys=['doc_id:wiki42', 'codebase:myrepo'],
    )
    blob = h.to_bytes()
    h2 = CacheHeader.from_bytes(blob)
    assert h2.model_id == h.model_id
    assert h2.model_version == h.model_version
    assert h2.block_size == h.block_size
    assert h2.prefix_hashes == h.prefix_hashes
    assert h2.entity_keys == h.entity_keys
    assert h2.has_exact_metadata()


def test_header_roundtrip_legacy_has_empty_exact_fields():
    """A pre-7.0 header (no exact metadata) round-trips to empty fields."""
    legacy = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://x', prefix_hash=b'',
        bloom=SemanticBloomFilter.for_capacity(32),
    )
    h2 = CacheHeader.from_bytes(legacy.to_bytes())
    assert h2.model_id is None
    assert h2.prefix_hashes == []
    assert h2.entity_keys == []
    assert not h2.has_exact_metadata()
