"""Shape tests for EdgeServeKVConnector.

Does NOT exercise the KV save/load paths -- those raise NotImplementedError
until the worker/scheduler stubs are filled in against a specific vLLM
release. This suite pins the import + factory-registration contract so
refactors can't silently break integration.

Skips entirely if vLLM isn't installed.
"""

import pytest

pytest.importorskip('vllm')


def test_connector_module_imports():
    from edgeserve.inference.vllm_kv_connector import (
        EdgeServeKVConnector, EdgeServeKVMetadata, register,
    )
    assert EdgeServeKVConnector is not None
    assert EdgeServeKVMetadata is not None
    assert callable(register)


def test_metadata_instantiates_with_empty_requests():
    from edgeserve.inference.vllm_kv_connector import EdgeServeKVMetadata
    m = EdgeServeKVMetadata()
    assert isinstance(m.requests, list)
    assert m.requests == []


def test_helpers_shape():
    from edgeserve.inference.vllm_kv_connector import (
        _align_to_block, _hash_token_ids, _slot_mapping_from_blocks,
    )
    # alignment drops the partial tail block
    assert _align_to_block(127, 16) == 112
    assert _align_to_block(16, 16) == 0
    assert _align_to_block(17, 16) == 16
    # deterministic hashing
    assert _hash_token_ids([1, 2, 3]) == _hash_token_ids([1, 2, 3])
    assert _hash_token_ids([1, 2, 3]) != _hash_token_ids([1, 2, 4])
    # slot mapping: blocks [3, 7] at block_size=4 for 7 tokens
    #   -> tokens 12,13,14,15 (block 3) and 28,29,30 (block 7 trimmed)
    sm = _slot_mapping_from_blocks([3, 7], 4, 7)
    assert sm.tolist() == [12, 13, 14, 15, 28, 29, 30]


def test_register_is_idempotent():
    from edgeserve.inference.vllm_kv_connector import register
    register()
    register()  # second call must not explode


def test_factory_knows_edgeserve_connector_after_register():
    from edgeserve.inference.vllm_kv_connector import register
    register()
    from vllm.distributed.kv_transfer.kv_connector.factory import (
        KVConnectorFactory,
    )
    registry = getattr(KVConnectorFactory, '_registry', None)
    assert registry is not None, 'vLLM changed factory internals; update this test'
    assert 'EdgeServeKVConnector' in registry


def test_abstract_interface_covered():
    """Spot-check that we implement everything vLLM declares abstract,
    so an `EdgeServeKVConnector(...)` construction wouldn't fail with
    `Can't instantiate abstract class`.
    """
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
    )
    from edgeserve.inference.vllm_kv_connector import EdgeServeKVConnector

    for name in KVConnectorBase_V1.__abstractmethods__:
        assert name not in EdgeServeKVConnector.__abstractmethods__, \
            f'abstract method {name!r} is not implemented on EdgeServeKVConnector'


def test_slice_first_n_tokens_handles_flash_and_triton_layouts():
    """_slice_first_n_tokens has to dispatch on whether the saved tensor
    uses Flash (axis 1 is tokens) or anything else (axis 0 is tokens)."""
    import torch
    from edgeserve.inference.vllm_kv_connector import _slice_first_n_tokens
    # Flash-style: (2, L, hidden)
    flash = torch.arange(2 * 10 * 4).reshape(2, 10, 4)
    sliced = _slice_first_n_tokens(flash, 3, None)
    assert sliced.shape == (2, 3, 4)
    # MLA / Triton-style: (L, hidden) or (L, heads, dim)
    mla = torch.arange(10 * 8).reshape(10, 8)
    sliced = _slice_first_n_tokens(mla, 3, None)
    assert sliced.shape == (3, 8)
    triton = torch.arange(10 * 4 * 8).reshape(10, 4, 8)
    sliced = _slice_first_n_tokens(triton, 3, None)
    assert sliced.shape == (3, 4, 8)


def test_scheduler_prefix_match_finds_longest():
    """Scheduler should return the LONGEST block-boundary prefix in the catalog."""
    from edgeserve.inference.vllm_kv_connector import (
        _Scheduler, _hash_token_ids,
    )

    class FakeRequest:
        def __init__(self, token_ids, req_id='r0'):
            self.prompt_token_ids = token_ids
            self.request_id = req_id

    class FakeCatalog:
        def __init__(self, known_hashes):
            self._known = set(known_hashes)
        def lookup(self, entities):
            for e in entities:
                if e in self._known:
                    return [object()]  # any truthy list
            return []

    class FakeClient:
        def __init__(self, known_hashes):
            self.catalog = FakeCatalog(known_hashes)

    class FakeBackend:
        def __init__(self, known_hashes):
            self.client = FakeClient(known_hashes)

    # Seed catalog with two prefix hashes: first 16 tokens and first 48 tokens.
    tokens = list(range(80))
    known = {
        _hash_token_ids(tokens[:16]),
        _hash_token_ids(tokens[:48]),
    }
    sched = object.__new__(_Scheduler)
    sched._vllm_config = None
    sched._kv_cache_config = None
    sched._client = FakeBackend(known)
    sched._block_size = 16
    sched._requests_need_load = {}
    sched._matched_len = {}
    sched._hash_cache = {}

    # Request is 80 tokens. Longest aligned = 64. The catalog doesn't have
    # hash(tokens[:64]); it has hash(tokens[:48]) and hash(tokens[:16]).
    # Scheduler should return 48 (longest hit).
    req = FakeRequest(tokens)
    n_matched, load_async = sched.get_num_new_matched_tokens(req, 0)
    assert n_matched == 48
    assert load_async is False
    assert sched._matched_len[req.request_id] == 48


def test_multi_boundary_hashes_produces_every_block_boundary():
    """Publisher's wait_for_save emits entity tags at every block boundary
    so a consumer with a shorter prefix still finds the entry."""
    from edgeserve.inference.vllm_kv_connector import (
        EdgeServeKVConnector, _hash_token_ids,
    )
    # Make a bogus connector instance just to reach _Worker (we only need
    # the `_multi_boundary_hashes` method; inline a minimal worker.)
    from edgeserve.inference.vllm_kv_connector import _Worker
    class FakeClient:
        pass
    worker = object.__new__(_Worker)
    worker._block_size = 4
    tokens = list(range(10))  # 10 tokens; block_size 4 -> boundaries at 4, 8
    entities = worker._multi_boundary_hashes(tokens, _hash_token_ids(tokens))
    # full hash + boundary at 4 + boundary at 8 = 3 entities
    assert _hash_token_ids(tokens) in entities
    assert _hash_token_ids(tokens[:4]) in entities
    assert _hash_token_ids(tokens[:8]) in entities
    assert len(entities) == 3
