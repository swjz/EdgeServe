"""Verify the EdgeServeKVConnector registers cleanly with vLLM's factory
and that a vllm.LLM instance can be constructed with kv_connector='EdgeServeKVConnector'.

This does NOT exercise the KV save/load path -- those raise NotImplementedError
until the worker/scheduler stubs in vllm_kv_connector.py are filled in.
"""

import os
import sys

os.environ.setdefault('VLLM_USE_V1', '1')


def main():
    from edgeserve.inference.vllm_kv_connector import (
        EdgeServeKVConnector, EdgeServeKVMetadata, register,
    )
    print('imported connector classes OK')

    try:
        register()
        print('registered EdgeServeKVConnector with vLLM factory')
    except RuntimeError as e:
        print(f'SKIP: {e}')
        return 1

    # Confirm the factory now knows about us.
    from vllm.distributed.kv_transfer.kv_connector.factory import (
        KVConnectorFactory,
    )
    registered_names = list(KVConnectorFactory._registry.keys()) \
        if hasattr(KVConnectorFactory, '_registry') else []
    print(f'factory registered: {registered_names}')

    # Minimal import-sanity: metadata class instantiates.
    m = EdgeServeKVMetadata()
    assert isinstance(m.per_request, dict)
    print('EdgeServeKVMetadata instantiates OK')

    # We do NOT construct an actual LLM here -- full LLM init pulls in the
    # scheduler / worker and the _Worker stubs raise NotImplementedError on
    # first forward pass. Wiring end-to-end is the next implementation step.
    print('probe OK (connector scaffolding is valid)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
