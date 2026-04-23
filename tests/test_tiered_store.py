"""Tests for TieredStore L2/L3 tiers, eviction, promotion, and tombstones.

No GPU, no Pulsar, no vLLM required — pure unit tests.
"""
import os
import tempfile
import uuid

import pytest

from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.header import CacheHeader
from edgeserve.semantic_cache.tiered_store import TieredStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_store(l2_mb=10, l3_mb=None, on_tombstone=None, tmpdir=None):
    path = tmpdir or tempfile.mkdtemp(prefix='ts-test-')
    return TieredStore(
        l3_path=path,
        l2_max_bytes=l2_mb * 1024 * 1024,
        l3_max_bytes=l3_mb * 1024 * 1024 if l3_mb else None,
        on_tombstone=on_tombstone,
        promote_async=False,
    ), path


def blob(mb: float) -> bytes:
    return b'\xab' * int(mb * 1024 * 1024)


# ---------------------------------------------------------------------------
# CacheHeader tombstone serialization
# ---------------------------------------------------------------------------

class TestCacheHeaderTombstone:
    def _minimal_header(self, deleted=False):
        return CacheHeader(
            block_uuid=uuid.uuid4(),
            node_uri='http://localhost:1',
            prefix_hash=b'',
            bloom=SemanticBloomFilter.for_capacity(1),
            deleted=deleted,
        )

    def test_deleted_false_roundtrip(self):
        h = self._minimal_header(deleted=False)
        h2 = CacheHeader.from_bytes(h.to_bytes())
        assert h2.deleted is False

    def test_deleted_true_roundtrip(self):
        h = self._minimal_header(deleted=True)
        h2 = CacheHeader.from_bytes(h.to_bytes())
        assert h2.deleted is True

    def test_legacy_header_no_deleted_field(self):
        # Simulate a header serialized without 'deleted' key (old format)
        import msgpack
        d = {
            'block_uuid': uuid.uuid4().bytes,
            'node_uri': 'http://x:1',
            'prefix_hash': b'',
            'bloom': SemanticBloomFilter.for_capacity(1).to_bytes(),
            'created_ms': 0.0,
            'num_tokens': 0,
        }
        h = CacheHeader.from_bytes(msgpack.packb(d, use_bin_type=True))
        assert h.deleted is False


# ---------------------------------------------------------------------------
# TieredStore basics
# ---------------------------------------------------------------------------

class TestTieredStorePutGet:
    def test_put_lands_in_l2_when_capacity(self):
        store, _ = make_store(l2_mb=10)
        uid = uuid.uuid4()
        tier = store.put(uid, blob(1))
        assert tier == 'l2'

    def test_put_writes_l3_file(self):
        store, path = make_store(l2_mb=10)
        uid = uuid.uuid4()
        store.put(uid, blob(1))
        assert os.path.isfile(os.path.join(path, str(uid) + '.bin'))

    def test_get_l2_hit(self):
        store, _ = make_store(l2_mb=10)
        uid = uuid.uuid4()
        data = blob(1)
        store.put(uid, data)
        got, tier = store.get(uid)
        assert tier == 'l2'
        assert got == data

    def test_get_l3_hit_after_l2_eviction(self):
        store, _ = make_store(l2_mb=5)  # 5 MB cap
        uids = []
        for _ in range(6):  # 6 × 1 MB → first entry evicted from L2
            uid = uuid.uuid4()
            store.put(uid, blob(1))
            uids.append(uid)
        # First blob was evicted from L2
        _, tier = store.get(uids[0])
        assert tier == 'l3'

    def test_get_miss(self):
        store, _ = make_store(l2_mb=10)
        got, tier = store.get(uuid.uuid4())
        assert got is None and tier is None

    def test_peek_l2_hit(self):
        store, _ = make_store(l2_mb=10)
        uid = uuid.uuid4()
        data = blob(1)
        store.put(uid, data)
        assert store.peek_l2(uid) is not None

    def test_peek_l2_miss_for_l3_only(self):
        store, _ = make_store(l2_mb=1)  # tiny cap
        uid = uuid.uuid4()
        store.put(uid, blob(2))  # 2 MB > 1 MB cap → L3 only
        assert store.peek_l2(uid) is None

    def test_get_l3_path(self):
        store, path = make_store(l2_mb=10)
        uid = uuid.uuid4()
        store.put(uid, blob(1))
        assert store.get_l3_path(uid) == os.path.join(path, str(uid) + '.bin')

    def test_get_l3_path_missing(self):
        store, _ = make_store(l2_mb=10)
        assert store.get_l3_path(uuid.uuid4()) is None


# ---------------------------------------------------------------------------
# LRU eviction behavior
# ---------------------------------------------------------------------------

class TestLRUEviction:
    def test_l2_lru_order(self):
        store, _ = make_store(l2_mb=3)  # 3 MB
        u1, u2, u3, u4 = [uuid.uuid4() for _ in range(4)]
        store.put(u1, blob(1))  # L2: [u1]
        store.put(u2, blob(1))  # L2: [u1, u2]
        store.put(u3, blob(1))  # L2: [u1, u2, u3], full
        # Access u1 to make it MRU
        store.peek_l2(u1)       # L2: [u2, u3, u1]
        store.put(u4, blob(1))  # Evict LRU = u2; L2: [u3, u1, u4]
        assert store.peek_l2(u2) is None  # evicted
        assert store.peek_l2(u1) is not None  # still in L2
        assert store.peek_l2(u4) is not None  # just inserted

    def test_l3_hit_promotes_to_l2(self):
        store, _ = make_store(l2_mb=2)
        u1, u2, u3 = [uuid.uuid4() for _ in range(3)]
        store.put(u1, blob(1))
        store.put(u2, blob(1))  # L2 full: [u1, u2]
        store.put(u3, blob(1))  # Evicts u1 from L2; L2: [u2, u3]
        # u1 is in L3 only
        assert store.peek_l2(u1) is None
        store.get(u1)           # L3 hit → promotes u1 (evicts u2)
        assert store.peek_l2(u1) is not None  # now in L2
        assert store.peek_l2(u2) is None      # evicted to make room

    def test_stats_accurate(self):
        store, _ = make_store(l2_mb=5)
        for _ in range(3):
            store.put(uuid.uuid4(), blob(1))
        s = store.stats
        assert s['l2_entries'] == 3
        assert s['l2_bytes'] == 3 * 1024 * 1024
        assert s['l3_entries'] == 3


# ---------------------------------------------------------------------------
# Tombstone / eviction below L3
# ---------------------------------------------------------------------------

class TestTombstones:
    def test_l3_eviction_fires_tombstone(self):
        fired = []
        store, _ = make_store(l2_mb=2, l3_mb=3, on_tombstone=fired.append)
        uids = []
        for _ in range(4):  # 4 × 1 MB; L3 cap 3 MB → one eviction
            uid = uuid.uuid4()
            store.put(uid, blob(1))
            uids.append(uid)
        assert len(fired) == 1
        # Evicted block's file must be gone
        evicted_uid = fired[0]
        assert store.get_l3_path(evicted_uid) is None

    def test_tombstone_uuid_matches_lru_entry(self):
        fired = []
        store, _ = make_store(l2_mb=1, l3_mb=2, on_tombstone=fired.append)
        u1 = uuid.uuid4()
        store.put(u1, blob(1))  # L3: [u1], L2 evicted (1>1? no, 1=1)
        u2 = uuid.uuid4()
        store.put(u2, blob(1))  # L3: [u1, u2]=2 MB, at cap
        u3 = uuid.uuid4()
        store.put(u3, blob(1))  # L3 over cap; evict u1
        assert u1 in fired

    def test_manual_evict_fires_tombstone(self):
        fired = []
        store, _ = make_store(l2_mb=10, on_tombstone=fired.append)
        uid = uuid.uuid4()
        store.put(uid, blob(1))
        assert store.evict(uid)
        assert uid in fired
        assert store.get(uid) == (None, None)

    def test_evict_removes_l3_file(self):
        store, path = make_store(l2_mb=10)
        uid = uuid.uuid4()
        store.put(uid, blob(1))
        store.evict(uid)
        assert not os.path.isfile(os.path.join(path, str(uid) + '.bin'))

    def test_evict_nonexistent_returns_false(self):
        store, _ = make_store(l2_mb=10)
        assert not store.evict(uuid.uuid4())

    def test_no_tombstone_on_l2_only_eviction(self):
        fired = []
        store, _ = make_store(l2_mb=2, on_tombstone=fired.append)
        # Put 3 × 1 MB with 2 MB L2 cap and no L3 cap → L2 eviction but no tombstone
        for _ in range(3):
            store.put(uuid.uuid4(), blob(1))
        assert fired == []


# ---------------------------------------------------------------------------
# Startup scan of existing L3 files
# ---------------------------------------------------------------------------

class TestStartupScan:
    def test_pre_existing_files_tracked(self):
        with tempfile.TemporaryDirectory() as d:
            # Write two files before creating the store
            u1, u2 = uuid.uuid4(), uuid.uuid4()
            for uid in (u1, u2):
                with open(os.path.join(d, str(uid) + '.bin'), 'wb') as f:
                    f.write(blob(1))
            store = TieredStore(d, l2_max_bytes=10 * 1024 * 1024, promote_async=False)
            assert store.stats['l3_entries'] == 2
            _, tier = store.get(u1)
            assert tier == 'l3'

    def test_non_bin_files_ignored(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'readme.txt'), 'w') as f:
                f.write('hello')
            store = TieredStore(d, promote_async=False)
            assert store.stats['l3_entries'] == 0
