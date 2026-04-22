"""Single-request vLLM worker used by demo_kvconnector_two_stage.py."""

import argparse
import json
import os
import sys
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', required=True)  # seeder | consumer
    parser.add_argument('--prompt', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.6)
    parser.add_argument('--max-model-len', type=int, default=2048)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    os.environ.setdefault('VLLM_USE_V1', '1')

    # Registration isn't strictly necessary if we pass kv_connector_module_path,
    # but call it so logs are friendlier.
    from edgeserve.inference.vllm_kv_connector import register
    try:
        register()
    except Exception:
        pass

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    cache_path = f'/tmp/edgeserve-demo-{args.role}-{os.getpid()}'
    os.makedirs(cache_path, exist_ok=True)

    kv_cfg = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={
            'pulsar_url': args.pulsar_url,
            'topic': args.topic,
            'local_cache_path': cache_path,
            'node_id': f'{args.role}-{uuid.uuid4().hex[:6]}',
        },
    )

    llm = LLM(
        model=args.model,
        dtype=args.dtype,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
        enable_prefix_caching=False,  # force our connector to do the caching
        kv_transfer_config=kv_cfg,
    )

    sp = SamplingParams(max_tokens=1, temperature=0.0)

    # If consumer, give its catalog a moment to ingest any prior headers
    # broadcast on this topic before the generate call (matters for cache-hit
    # path; without this, the consumer's first call races against catalog
    # backlog drain).
    if args.role == 'consumer':
        time.sleep(1.0)

    t0 = time.perf_counter()
    out = llm.generate(prompts=[args.prompt], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000

    # Extra wait after seeder to let wait_for_save publish land.
    if args.role == 'seeder':
        time.sleep(0.3)

    result = {
        'role': args.role,
        'gen_ms': gen_ms,
        'output_token': int(out[0].outputs[0].token_ids[0]),
        'cache_path': cache_path,
    }
    print(f'RESULT {json.dumps(result)}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
