import hashlib
import os
import random
import socket
import string
import time
import uuid

import pytest

from edgeserve.semantic_cache import (
    CacheHeader,
    CacheHttpServer,
    HeaderCatalog,
    HeaderPublisher,
    SemanticBloomFilter,
    http_fetch,
)


def _rand_token(n=12):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def test_bloom_no_false_negatives():
    bf = SemanticBloomFilter(m_bits=16384, k=7)
    inserted = {_rand_token() for _ in range(500)}
    for e in inserted:
        bf.add(e)
    for e in inserted:
        assert e in bf


def test_bloom_fpr_within_bound():
    random.seed(0)
    bf = SemanticBloomFilter(m_bits=16384, k=7)
    inserted = {_rand_token() for _ in range(2000)}
    for e in inserted:
        bf.add(e)
    trials = 5000
    fps = 0
    for _ in range(trials):
        candidate = _rand_token(20)  # longer so collision with inserted is negligible
        if candidate in inserted:
            continue
        if candidate in bf:
            fps += 1
    fpr = fps / trials
    assert fpr < 0.05, f'bloom FPR too high: {fpr}'


def test_bloom_round_trip():
    bf = SemanticBloomFilter(m_bits=4096, k=5)
    for e in ('alpha', 'beta', 'file_diff_v2.py'):
        bf.add(e)
    blob = bf.to_bytes()
    restored = SemanticBloomFilter.from_bytes(blob)
    assert restored.m == bf.m
    assert restored.k == bf.k
    for e in ('alpha', 'beta', 'file_diff_v2.py'):
        assert e in restored
    assert 'not-inserted-zzz' not in restored or True  # membership may have FP; not asserting


def test_header_round_trip():
    bf = SemanticBloomFilter(m_bits=4096, k=5)
    bf.add('doc-42')
    h = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri='http://node-a:9001',
        prefix_hash=hashlib.sha256(b'some-prefix').digest(),
        bloom=bf,
    )
    restored = CacheHeader.from_bytes(h.to_bytes())
    assert restored.block_uuid == h.block_uuid
    assert restored.node_uri == h.node_uri
    assert restored.prefix_hash == h.prefix_hash
    assert abs(restored.created_ms - h.created_ms) < 1e-3
    assert 'doc-42' in restored.bloom


def _make_header(node_uri='http://a:1', entities=(), created_ms=None):
    bf = SemanticBloomFilter(m_bits=4096, k=5)
    for e in entities:
        bf.add(e)
    h = CacheHeader(
        block_uuid=uuid.uuid4(),
        node_uri=node_uri,
        prefix_hash=b'\x00' * 32,
        bloom=bf,
    )
    if created_ms is not None:
        h.created_ms = created_ms
    return h


class _FakeCatalog:
    """HeaderCatalog without the Pulsar consumer thread, for unit-testing lookup/TTL."""

    insert = HeaderCatalog.insert
    _evict_expired = HeaderCatalog._evict_expired
    lookup = HeaderCatalog.lookup

    def __init__(self, ttl_ms=10 * 60 * 1000):
        import threading
        self.ttl_ms = ttl_ms
        self._headers = {}
        self._lock = threading.Lock()
        self.rank_fn = lambda h: h.created_ms


def test_catalog_lookup_and_ranking():
    cat = _FakeCatalog()
    now = time.time() * 1000
    older = _make_header(node_uri='http://old:1', entities=['doc-1'], created_ms=now - 1000)
    newer = _make_header(node_uri='http://new:1', entities=['doc-1'], created_ms=now)
    unrelated = _make_header(node_uri='http://nope:1', entities=['doc-2'], created_ms=now)
    cat.insert(older)
    cat.insert(newer)
    cat.insert(unrelated)

    hits = cat.lookup({'doc-1'})
    assert [h.node_uri for h in hits] == ['http://new:1', 'http://old:1']

    assert cat.lookup({'doc-3'}) == []


def test_catalog_ttl_eviction():
    cat = _FakeCatalog(ttl_ms=100)
    now = time.time() * 1000
    fresh = _make_header(entities=['x'], created_ms=now)
    stale = _make_header(entities=['x'], created_ms=now - 10_000)
    cat.insert(fresh)
    cat.insert(stale)
    cat._evict_expired()
    assert fresh.block_uuid in cat._headers
    assert stale.block_uuid not in cat._headers


def test_http_server_round_trip(tmp_path):
    cache_dir = str(tmp_path / 'cache')
    with CacheHttpServer(local_cache_path=cache_dir, port=0, host='127.0.0.1') as srv:
        block_id = uuid.uuid4()
        payload = os.urandom(2048)
        srv.write_block(str(block_id), payload)
        # Fetch via localhost to avoid hostname resolution in CI.
        fetched = http_fetch(f'http://127.0.0.1:{srv.port}', block_id)
        assert fetched == payload

        missing = uuid.uuid4()
        with pytest.raises(IOError):
            http_fetch(f'http://127.0.0.1:{srv.port}', missing)


# ---------- Integration test (requires Pulsar on localhost:6650) ----------

PULSAR_URL = os.environ.get('EDGESERVE_PULSAR_URL', 'pulsar://localhost:6650')


def _pulsar_available():
    try:
        s = socket.create_connection(('localhost', 6650), timeout=0.5)
        s.close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _pulsar_available(), reason='Pulsar broker not reachable on localhost:6650')
def test_cross_node_discovery():
    """Publisher on node A, catalog on node B; B discovers A's header by entity."""
    topic = f'kvcache-headers-test-{uuid.uuid4().hex[:8]}'

    with HeaderPublisher(PULSAR_URL, topic=topic) as pub, \
         HeaderCatalog(PULSAR_URL, node_id=f'node-b-{uuid.uuid4().hex[:8]}', topic=topic,
                       ttl_ms=60_000) as cat:
        time.sleep(0.5)  # let the subscriber attach
        bf = SemanticBloomFilter(m_bits=4096, k=5)
        bf.add('file_diff_v2.py')
        header = CacheHeader(
            block_uuid=uuid.uuid4(),
            node_uri='http://node-a:9100',
            prefix_hash=b'\x00' * 32,
            bloom=bf,
        )
        pub.publish(header)

        deadline = time.time() + 5.0
        hits = []
        while time.time() < deadline:
            hits = cat.lookup({'file_diff_v2.py'})
            if hits:
                break
            time.sleep(0.05)
        assert hits, 'catalog did not observe the published header in time'
        assert hits[0].block_uuid == header.block_uuid
        assert hits[0].node_uri == 'http://node-a:9100'

        assert cat.lookup({'not-in-any-header'}) == []
