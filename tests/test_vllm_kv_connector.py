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
