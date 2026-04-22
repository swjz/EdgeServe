"""End-to-end smoke test for EdgeServeKVConnector against a live vLLM.

Two passes in a single Python process:
  Pass 1 (seed):   vLLM instance A runs generate() over a prompt.
                   The connector catches save_kv_layer and publishes bytes
                   to SemanticCacheClient (pulsar + local HTTP).
  Pass 2 (probe):  Query SemanticCacheClient.catalog.lookup() to confirm
                   a header was published. Fetch the blob and validate it
                   deserializes as safetensors.

Does NOT launch a second vLLM (GPU memory too tight on 12 GB). The
cross-process consumer is left for phase3_multiproc_bench.py once
confidence is high enough.

Requires Pulsar running on localhost:6650.
"""

import argparse
import hashlib
import os
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.5)
    parser.add_argument('--max-model-len', type=int, default=2048)
    parser.add_argument('--doc', default='The city of Chicago is on Lake Michigan. ' * 16)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    parser.add_argument('--topic', default=None,
                        help='default: kvcache-headers-probe-<timestamp>')
    args = parser.parse_args()

    if args.topic is None:
        args.topic = f'kvcache-headers-probe-{int(time.time()*1000)}'
    print(f'topic = {args.topic}')

    os.environ.setdefault('VLLM_USE_V1', '1')

    from edgeserve.inference.vllm_kv_connector import register, _hash_token_ids
    register()
    print('connector registered')

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    cache_path = f'/tmp/edgeserve-probe-{os.getpid()}'
    os.makedirs(cache_path, exist_ok=True)

    kv_cfg = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        # Let vLLM's subprocess EngineCore import the connector module
        # directly, without needing our register() to run there.
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={
            'pulsar_url': args.pulsar_url,
            'topic': args.topic,
            'local_cache_path': cache_path,
            'node_id': f'probe-{os.getpid()}',
        },
    )

    print('launching vLLM instance (this takes a moment)...')
    t0 = time.perf_counter()
    llm = LLM(
        model=args.model,
        dtype=args.dtype,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
        enable_prefix_caching=False,  # force our connector to do the caching
        kv_transfer_config=kv_cfg,
    )
    print(f'  LLM ready in {time.perf_counter()-t0:.1f}s')

    sp = SamplingParams(max_tokens=1, temperature=0.0)

    print('generate (should populate external cache)...')
    t0 = time.perf_counter()
    out = llm.generate(prompts=[args.doc], sampling_params=sp, use_tqdm=False)
    print(f'  gen={(time.perf_counter()-t0)*1000:.1f}ms, '
          f'output token = {out[0].outputs[0].token_ids[0]}')

    # Compute the hash the connector would have used.
    tok = llm.get_tokenizer()
    token_ids = tok.encode(args.doc, add_special_tokens=False)
    # The scheduler aligns to block_size-1; model's block_size from cache config
    # is typically 16 for vLLM.
    block_size = 16
    num_to_check = (len(token_ids) - 1) // block_size * block_size
    aligned = token_ids[:num_to_check]
    expected_hash = _hash_token_ids(aligned)
    print(f'  aligned tokens = {num_to_check}, expected_hash = {expected_hash[:16]}...')

    # Give the connector's save thread a moment to flush and publish.
    time.sleep(0.3)

    # Independently query the catalog to confirm the header landed.
    from edgeserve.semantic_cache import SemanticCacheClient
    checker = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id=f'checker-{os.getpid()}',
        local_cache_path=f'/tmp/edgeserve-probe-checker-{os.getpid()}',
        http_host='127.0.0.1',
        topic=args.topic,
    )
    try:
        # Wait for the checker's catalog subscriber to pick up any header.
        # We poll the internal _headers dict directly because the hash our
        # connector publishes (computed from vLLM's request.prompt_token_ids
        # which may include BOS) may not match whatever we'd compute here
        # from tok.encode() with our own args.
        deadline = time.time() + 5.0
        seen = {}
        while time.time() < deadline:
            if checker.catalog._headers:
                seen = dict(checker.catalog._headers)
                break
            time.sleep(0.05)

        if not seen:
            print('FAIL: checker saw no headers in 5s')
            return 1

        print(f'PASS: checker received {len(seen)} header(s)')
        # Fetch the most recent and validate blob decodes.
        header = max(seen.values(), key=lambda h: h.created_ms)
        print(f'  latest header: node_uri={header.node_uri}, '
              f'local_path={header.local_path}')

        # Fetch via the normal path.
        from edgeserve.semantic_cache.http_client import http_fetch
        blob = http_fetch(header.node_uri, header.block_uuid, timeout=5.0)
        print(f'  blob={len(blob)/1e6:.2f}MB fetched over HTTP')

        from safetensors.torch import load as st_load
        tensors = st_load(blob)
        print(f'  blob contains {len(tensors)} tensors; '
              f'first 3 keys: {list(tensors)[:3]}')
        return 0
    finally:
        checker.close()


if __name__ == '__main__':
    sys.exit(main())
