"""Three-node Semantic Cache Routing demo against an in-process mock broker.

Run directly with `python3 tests/demo_semantic_cache.py`. No Pulsar needed.

Scenario:
  Node A caches the KV-block for a 50k-token codebase fragment tagged with
  entity `file_diff_v2.py`, writes the bytes under its HTTP server, and
  broadcasts a header.

  Node B, running a Compute task with `semantic_cache` injected, is asked to
  "analyze file_diff_v2.py". Its local token-prefix cache misses, so it
  queries the catalog by entity, finds A's bloom-filter match, fetches over
  HTTP, and proceeds with inference.

  Node C queries an unrelated entity. Bloom filter returns negative on every
  cached header so C short-circuits with zero HTTP traffic and recomputes.
"""

# IMPORTANT: install the mock BEFORE any edgeserve.semantic_cache import,
# because catalog.py / publisher.py import `pulsar` at module load time.
from edgeserve.semantic_cache import mock_pulsar
mock_pulsar.install()

import tempfile
import time

from edgeserve.semantic_cache import SemanticCacheClient


MOCK_URL = 'pulsar://mock'
TOPIC = 'kvcache-headers-demo'


def run_node_a(cache_dir):
    """Producer node: caches a big KV block and broadcasts its header."""
    client = SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id='node-a',
        local_cache_path=cache_dir, http_host='127.0.0.1',
        topic=TOPIC, bloom_capacity=64,
    )
    # Pretend this is real KV-cache tensor bytes for a 50k-token codebase block.
    kv_bytes = b'KV-TENSOR-PAYLOAD-for-file_diff_v2.py-' + b'\x00' * 4096
    entities = {'file_diff_v2.py', 'compute_diff', 'apply_patch'}
    block_uuid = client.publish(entities, kv_bytes, prefix_tokens=b'codebase-v2')
    print(f'[node-a] published block {block_uuid} serving at {client.http.uri}')
    return client, kv_bytes


def run_node_b(cache_dir, expected_bytes):
    """Consumer node: persona-prefixed prompt, exact-match misses, semantic hit."""
    client = SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id='node-b',
        local_cache_path=cache_dir, http_host='127.0.0.1',
        topic=TOPIC, bloom_capacity=64,
    )
    # Give the catalog's background consumer a moment to drain the broker.
    deadline = time.time() + 2.0
    hit = None
    while time.time() < deadline:
        hit = client.resolve({'file_diff_v2.py'})
        if hit is not None:
            break
        time.sleep(0.05)
    if hit is None:
        raise AssertionError('[node-b] expected to discover node-a\'s block')
    data, header = hit
    assert data == expected_bytes, '[node-b] bytes mismatch'
    print(f'[node-b] resolved file_diff_v2.py -> {header.node_uri} '
          f'(block {header.block_uuid}, {len(data)} bytes)')
    return client


def run_node_c(cache_dir):
    """Consumer node: queries an entity nobody has. Bloom short-circuits."""
    client = SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id='node-c',
        local_cache_path=cache_dir, http_host='127.0.0.1',
        topic=TOPIC, bloom_capacity=64,
    )
    # Let the subscription observe any headers before asking.
    time.sleep(0.2)
    hit = client.resolve({'some_unrelated_module.rs'})
    assert hit is None, '[node-c] expected no bloom hit'
    print('[node-c] no cached peer for some_unrelated_module.rs (as expected)')
    return client


def main():
    with tempfile.TemporaryDirectory() as tmp:
        import os
        a_dir = os.path.join(tmp, 'a')
        b_dir = os.path.join(tmp, 'b')
        c_dir = os.path.join(tmp, 'c')
        for d in (a_dir, b_dir, c_dir):
            os.makedirs(d, exist_ok=True)

        a, kv_bytes = run_node_a(a_dir)
        try:
            b = run_node_b(b_dir, kv_bytes)
            try:
                c = run_node_c(c_dir)
                c.close()
            finally:
                b.close()
        finally:
            a.close()

    print('demo ok.')


if __name__ == '__main__':
    main()
