"""Integration tests for Semantic Cache Routing against a mock Pulsar broker.

Exercises the full stack (publisher, catalog, HTTP retrieval, Compute task
injection) without requiring a live Pulsar server.
"""

# Install the mock before any pulsar imports. The other test file imports
# `HeaderCatalog` eagerly via edgeserve.semantic_cache.__init__, so if that
# file ran first it would have pulled in the real `pulsar`. pytest collects
# one module at a time, but the module-level `import pulsar` happens once per
# process. Safest to install here too -- install() is idempotent re: behavior.
from edgeserve.semantic_cache import mock_pulsar
mock_pulsar.install()

import tempfile
import time
import uuid

import pytest

from edgeserve.compute import Compute
from edgeserve.message_format import GraphCodec
from edgeserve.semantic_cache import SemanticCacheClient


MOCK_URL = 'pulsar://mock'


@pytest.fixture(autouse=True)
def _reset_mock_broker():
    """Each test gets a clean broker so subscriptions don't leak."""
    mock_pulsar.reset_broker()
    yield
    mock_pulsar.reset_broker()


def _fresh_client(node_id, tmp_path, topic, bloom_capacity=64):
    cache_dir = str(tmp_path / node_id)
    import os
    os.makedirs(cache_dir, exist_ok=True)
    return SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id=node_id,
        local_cache_path=cache_dir, http_host='127.0.0.1',
        topic=topic, bloom_capacity=bloom_capacity,
    )


def test_cross_node_discovery_via_mock(tmp_path):
    topic = f'kvcache-{uuid.uuid4().hex[:8]}'
    producer = _fresh_client('node-a', tmp_path, topic)
    consumer = _fresh_client('node-b', tmp_path, topic)
    try:
        kv = b'KV-BYTES-' + b'\x00' * 1024
        producer.publish({'doc-alpha', 'doc-beta'}, kv)

        deadline = time.time() + 2.0
        hit = None
        while time.time() < deadline:
            hit = consumer.resolve({'doc-alpha'})
            if hit is not None:
                break
            time.sleep(0.02)
        assert hit is not None
        data, _ = hit
        assert data == kv

        # Bloom filter short-circuits on missing entity: no HTTP traffic needed.
        assert consumer.resolve({'not-cached-anywhere'}) is None
    finally:
        consumer.close()
        producer.close()


def test_compute_injects_semantic_cache_kwarg(tmp_path):
    """A task declaring `semantic_cache` in its signature gets the client injected."""
    topic = f'kvcache-{uuid.uuid4().hex[:8]}'

    # Seed cache on node-a.
    node_a = _fresh_client('node-a', tmp_path, topic)
    try:
        kv = b'CACHED-ANSWER-bytes'
        node_a.publish({'file_diff_v2.py'}, kv)
    finally:
        # Keep node-a's HTTP server up during the compute run.
        pass

    # Node B runs a Compute task that pulls from its semantic cache client.
    node_b = _fresh_client('node-b', tmp_path, topic)
    # Let node-b's catalog observe the header.
    time.sleep(0.3)

    captured = {}

    def inference_task(prompt, semantic_cache):
        entities = {prompt.decode('utf-8')}
        hit = semantic_cache.resolve(entities)
        if hit is not None:
            data, header = hit
            captured['data'] = data
            captured['uri'] = header.node_uri
            return b'cached:' + data
        captured['data'] = None
        return b'miss'

    input_topic = f'prompt-{uuid.uuid4().hex[:8]}'
    output_topic = f'resp-{uuid.uuid4().hex[:8]}'

    compute = Compute(
        task=inference_task,
        pulsar_node=MOCK_URL,
        worker_id='agent-b',
        topic_in=input_topic,
        topic_out=output_topic,
        semantic_cache=node_b,
    )

    # Inject a prompt directly into the mock broker by using a Pulsar producer
    # through the same mock, wrapped in the same GraphCodec.
    import pulsar
    codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)
    producer_client = pulsar.Client(MOCK_URL)
    producer = producer_client.create_producer(input_topic, schema=pulsar.schema.BytesSchema())
    producer.send(codec.encode(uuid.uuid4(), 'prompt', b'file_diff_v2.py'))

    # Subscribe to the output topic first (before Compute runs) so the message
    # is queued for our subscription.
    verifier = pulsar.Client(MOCK_URL)
    verifier_consumer = verifier.subscribe(
        output_topic, subscription_name='verifier',
        schema=pulsar.schema.BytesSchema(),
    )
    # Drive one tick.
    result = next(compute)
    assert result is not None
    assert captured.get('data') == b'CACHED-ANSWER-bytes'
    assert result == b'cached:CACHED-ANSWER-bytes'

    # And the wrapped output was actually sent on the output topic.
    out_msg = verifier_consumer.receive(timeout_millis=500)
    _, op_from, _, payload = codec.decode(out_msg.value())
    assert op_from == 'agent-b'
    assert payload == b'cached:CACHED-ANSWER-bytes'

    compute.client.close()
    producer_client.close()
    verifier.close()
    node_b.close()
    node_a.close()


def test_compute_without_semantic_cache_backwards_compat(tmp_path):
    """Tasks that don't declare `semantic_cache` run exactly as before."""
    topic_in = f'in-{uuid.uuid4().hex[:8]}'
    topic_out = f'out-{uuid.uuid4().hex[:8]}'

    def echo(src):
        return b'echoed:' + src

    compute = Compute(
        task=echo,
        pulsar_node=MOCK_URL,
        worker_id='agent',
        topic_in=topic_in,
        topic_out=topic_out,
    )

    import pulsar
    codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)
    pc = pulsar.Client(MOCK_URL)
    p = pc.create_producer(topic_in, schema=pulsar.schema.BytesSchema())
    p.send(codec.encode(uuid.uuid4(), 'src', b'hello'))

    result = next(compute)
    assert result == b'echoed:hello'

    compute.client.close()
    pc.close()
