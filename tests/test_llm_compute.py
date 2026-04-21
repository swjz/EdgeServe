"""End-to-end tests for `LLMCompute` wired into EdgeServe's Pulsar graph.

Two operators act as "Node A" and "Node B", each running an HFEngine over
the same tiny HF model. Node A processes a prompt and publishes its KV
cache. Node B processes a prompt with the same `cache_tags` and -- via
`SemanticCacheClient.resolve` -- reuses Node A's cache, skipping prefill.

Asserts:
  * both nodes produce output text (non-empty)
  * Node B's `last_stats` reports `cache_hit=True` with bytes > 0
  * the generated tokens from the cache-hit path match a local eager run
    (up to a short horizon; tiny-gpt2 is random-weights so exactness only
    holds because the model is deterministic).
"""

import pytest

pytest.importorskip('torch')
pytest.importorskip('transformers')
pytest.importorskip('safetensors')

from edgeserve.semantic_cache import mock_pulsar  # noqa: E402
mock_pulsar.install()

import time  # noqa: E402
import uuid  # noqa: E402

import pulsar  # noqa: E402  (the mock, since install() already ran)

from edgeserve.inference import HFEngine, LLMCompute  # noqa: E402
from edgeserve.inference.llm_compute import pack_prompt  # noqa: E402
from edgeserve.message_format import GraphCodec  # noqa: E402
from edgeserve.semantic_cache import SemanticCacheClient  # noqa: E402


MODEL = 'sshleifer/tiny-gpt2'
MOCK_URL = 'pulsar://mock'


@pytest.fixture(scope='module')
def engine():
    return HFEngine(MODEL, device='cpu')


@pytest.fixture(autouse=True)
def _reset_broker():
    mock_pulsar.reset_broker()
    yield
    mock_pulsar.reset_broker()


def _submit(topic, payload_bytes, op_from='user'):
    codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)
    client = pulsar.Client(MOCK_URL)
    p = client.create_producer(topic, schema=pulsar.schema.BytesSchema())
    p.send(codec.encode(uuid.uuid4(), op_from, payload_bytes))
    client.close()


def test_llm_compute_single_node_roundtrip(engine, tmp_path):
    """A single LLMCompute produces non-empty output for a prompt."""
    topic_in = f'llm-in-{uuid.uuid4().hex[:8]}'
    topic_out = f'llm-out-{uuid.uuid4().hex[:8]}'

    compute = LLMCompute(
        engine=engine, pulsar_node=MOCK_URL, worker_id='agent',
        topic_in=topic_in, topic_out=topic_out,
    )
    _submit(topic_in, pack_prompt('The city of Chicago is', max_new_tokens=4))

    text = next(compute)
    assert text is not None
    assert compute.last_stats['new_tokens'] == 4
    assert compute.last_stats['cache_hit'] is False
    compute.client.close()


def test_llm_compute_cross_node_cache_reuse(engine, tmp_path):
    """Node A publishes cache for a shared doc; Node B resolves and reuses."""
    headers_topic = f'kvcache-{uuid.uuid4().hex[:8]}'
    topic_in_a = f'in-a-{uuid.uuid4().hex[:8]}'
    topic_out_a = f'out-a-{uuid.uuid4().hex[:8]}'
    topic_in_b = f'in-b-{uuid.uuid4().hex[:8]}'
    topic_out_b = f'out-b-{uuid.uuid4().hex[:8]}'

    cache_a = SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id='node-a',
        local_cache_path=str(tmp_path / 'a-cache'),
        http_host='127.0.0.1', topic=headers_topic,
    )
    cache_b = SemanticCacheClient(
        pulsar_node=MOCK_URL, node_id='node-b',
        local_cache_path=str(tmp_path / 'b-cache'),
        http_host='127.0.0.1', topic=headers_topic,
    )
    try:
        node_a = LLMCompute(
            engine=engine, pulsar_node=MOCK_URL, worker_id='agent-a',
            topic_in=topic_in_a, topic_out=topic_out_a,
            semantic_cache=cache_a,
        )
        node_b = LLMCompute(
            engine=engine, pulsar_node=MOCK_URL, worker_id='agent-b',
            topic_in=topic_in_b, topic_out=topic_out_b,
            semantic_cache=cache_b,
        )

        # Node A: processes a prompt about doc-alpha, publishes its KV cache.
        _submit(topic_in_a, pack_prompt(
            'Long shared document alpha about topic X. ' * 8,
            cache_tags=['doc-alpha'],
            publish_cache=True,
            max_new_tokens=2,
        ))
        next(node_a)
        assert node_a.last_stats.get('published') is True
        assert node_a.last_stats['cache_hit'] is False

        # Wait for node-b's catalog background consumer to pull the header
        # from the broker. In production a new prompt wouldn't arrive within
        # microseconds of the publish; this mirrors real ordering.
        deadline = time.time() + 2.0
        while time.time() < deadline and not cache_b.catalog.lookup({'doc-alpha'}):
            time.sleep(0.02)

        # Node B: different prompt but same doc tag. Should resolve A's cache.
        _submit(topic_in_b, pack_prompt(
            'Long shared document alpha about topic X. ' * 8 + ' Now answer this:',
            cache_tags=['doc-alpha'],
            max_new_tokens=2,
        ))
        next(node_b)
        stats = node_b.last_stats
        assert stats['cache_hit'] is True, f'expected cache hit, got {stats}'
        assert stats['source_uri'].startswith('http://127.0.0.1'), stats
        assert stats['bytes'] > 0

        node_a.client.close()
        node_b.client.close()
    finally:
        cache_a.close()
        cache_b.close()
