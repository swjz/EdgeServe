"""LLM correctness test for Semantic Cache Routing.

Scenario: two "nodes" sharing a small HF model. Node A prefills a shared
document, publishes a cache header, and serves the `past_key_values` bytes
from its HTTP server. Node B discovers A's block via bloom filter, fetches
the KV tensors, and continues generation with a unique agent-specific
suffix appended.

Assertion: the continuation logits from the routed path are bit-identical
(within fp tolerance) to the eager path that reprocesses the full prompt
from scratch. If this holds, the speedup path is correctness-preserving.

Auto-skips if transformers / torch aren't installed, so this file is safe
to commit without making them mandatory deps.
"""

import pytest

pytest.importorskip('torch')
pytest.importorskip('transformers')
pytest.importorskip('safetensors')

# Install the mock Pulsar broker BEFORE any pulsar imports. See
# tests/test_semantic_cache_integration.py for the rationale.
from edgeserve.semantic_cache import mock_pulsar  # noqa: E402
mock_pulsar.install()

import time  # noqa: E402
import uuid  # noqa: E402

import torch  # noqa: E402
from transformers import AutoModelForCausalLM, AutoTokenizer  # noqa: E402

from edgeserve.semantic_cache import SemanticCacheClient  # noqa: E402
from edgeserve.semantic_cache.kv_io import (  # noqa: E402
    _as_legacy, load_past_key_values, save_past_key_values,
)


# `sshleifer/tiny-gpt2` is a ~5MB random-weights GPT-2 commonly used in HF CI.
# We pick a tiny model so the test runs in seconds and without heavy downloads.
TINY_MODEL = 'sshleifer/tiny-gpt2'


@pytest.fixture(scope='module')
def model_and_tokenizer():
    tok = AutoTokenizer.from_pretrained(TINY_MODEL)
    if tok.pad_token_id is None:
        tok.pad_token = tok.eos_token
    model = AutoModelForCausalLM.from_pretrained(TINY_MODEL)
    model.eval()
    return model, tok


@pytest.fixture(autouse=True)
def _reset_broker():
    mock_pulsar.reset_broker()
    yield
    mock_pulsar.reset_broker()


def _prefill(model, input_ids):
    """Run a forward pass on `input_ids` and return (logits_last, past_key_values)."""
    with torch.no_grad():
        out = model(input_ids=input_ids, use_cache=True)
    return out.logits[:, -1, :], out.past_key_values


def _continue_with_cache(model, suffix_ids, past_key_values):
    """Run a forward pass appending `suffix_ids` on top of an existing KV cache."""
    with torch.no_grad():
        out = model(input_ids=suffix_ids, past_key_values=past_key_values, use_cache=True)
    return out.logits[:, -1, :]


def test_kv_round_trip_is_lossless(model_and_tokenizer):
    """Serialize past_key_values -> bytes -> back, verify tensors equal."""
    model, tok = model_and_tokenizer
    doc = 'The city of Chicago is on Lake Michigan. ' * 8
    ids = tok(doc, return_tensors='pt').input_ids
    _, pkv = _prefill(model, ids)

    blob = save_past_key_values(pkv)
    restored_legacy = load_past_key_values(blob, device='cpu', as_cache=False)

    legacy = _as_legacy(pkv)
    assert len(restored_legacy) == len(legacy)
    for orig, kv2 in zip(legacy, restored_legacy):
        k1, v1 = orig[0], orig[1]
        k2, v2 = kv2[0], kv2[1]
        assert torch.allclose(k1.cpu(), k2, atol=0, rtol=0)
        assert torch.allclose(v1.cpu(), v2, atol=0, rtol=0)


def test_cross_node_kv_reuse_matches_eager(tmp_path, model_and_tokenizer):
    """Routed-cache path produces the same next-token logits as eager."""
    model, tok = model_and_tokenizer

    doc = 'Chicago was founded in 1833 and incorporated as a city in 1837. ' * 16
    agent_b_suffix = ' As an SRE, summarize what infrastructure this implies.'

    doc_ids = tok(doc, return_tensors='pt').input_ids
    suffix_ids = tok(agent_b_suffix, return_tensors='pt', add_special_tokens=False).input_ids
    full_ids = torch.cat([doc_ids, suffix_ids], dim=1)

    # --- Eager baseline: run the full prompt from scratch on node B.
    eager_logits, _ = _prefill(model, full_ids)

    # --- Routed path: node A prefills the doc and publishes; node B fetches.
    topic = f'kvcache-{uuid.uuid4().hex[:8]}'
    node_a = SemanticCacheClient(
        pulsar_node='pulsar://mock', node_id='node-a',
        local_cache_path=str(tmp_path / 'a'), http_host='127.0.0.1',
        topic=topic, bloom_capacity=32,
    )
    node_b = SemanticCacheClient(
        pulsar_node='pulsar://mock', node_id='node-b',
        local_cache_path=str(tmp_path / 'b'), http_host='127.0.0.1',
        topic=topic, bloom_capacity=32,
    )
    try:
        _, pkv = _prefill(model, doc_ids)
        kv_bytes = save_past_key_values(pkv)
        node_a.publish({'chicago-doc-v1'}, kv_bytes)

        # Give node-b's catalog consumer a moment.
        deadline = time.time() + 2.0
        hit = None
        while time.time() < deadline:
            hit = node_b.resolve({'chicago-doc-v1'})
            if hit is not None:
                break
            time.sleep(0.02)
        assert hit is not None, 'node-b failed to discover node-a\'s KV cache'
        fetched_bytes, _header = hit
        routed_pkv = load_past_key_values(fetched_bytes, device='cpu')

        routed_logits = _continue_with_cache(model, suffix_ids, routed_pkv)

        assert torch.allclose(eager_logits, routed_logits, atol=1e-5, rtol=1e-4), \
            'routed-cache logits diverge from eager; the KV transport is not ' \
            'correctness-preserving for this model'
    finally:
        node_b.close()
        node_a.close()
