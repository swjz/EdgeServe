"""Worker that runs the same prompt twice and reports cold/warm timings.

Mode=internal: vLLM with enable_prefix_caching=True, no connector.
Mode=connector: vLLM with enable_prefix_caching=False + EdgeServeKVConnector.
"""

import argparse
import json
import os
import sys
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', required=True, choices=['internal', 'connector'])
    parser.add_argument('--prompt', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=4096)
    args = parser.parse_args()

    os.environ.setdefault('VLLM_USE_V1', '1')

    from vllm import LLM, SamplingParams

    llm_kwargs = dict(
        model=args.model, dtype=args.dtype,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
    )

    if args.mode == 'internal':
        llm_kwargs['enable_prefix_caching'] = True
    else:
        from vllm.config import KVTransferConfig
        from edgeserve.inference.vllm_kv_connector import register
        try:
            register()
        except Exception:
            pass
        cache_path = f'/tmp/edgeserve-ceiling-{os.getpid()}'
        os.makedirs(cache_path, exist_ok=True)
        llm_kwargs['enable_prefix_caching'] = False
        llm_kwargs['kv_transfer_config'] = KVTransferConfig(
            kv_connector='EdgeServeKVConnector',
            kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
            kv_role='kv_both',
            kv_connector_extra_config={
                'pulsar_url': 'pulsar://localhost:6650',
                'topic': args.topic,
                'local_cache_path': cache_path,
                'node_id': f'ceiling-{uuid.uuid4().hex[:6]}',
            },
        )

    llm = LLM(**llm_kwargs)
    sp = SamplingParams(max_tokens=1, temperature=0.0)

    # Cold call: full prefill / MISS
    t0 = time.perf_counter()
    cold = llm.generate(prompts=[args.prompt], sampling_params=sp, use_tqdm=False)
    cold_ms = (time.perf_counter() - t0) * 1000
    cold_tok = int(cold[0].outputs[0].token_ids[0])

    # Let the connector's save flush publish.
    if args.mode == 'connector':
        time.sleep(0.3)

    # Warm call: same prompt; expect cache hit
    t0 = time.perf_counter()
    warm = llm.generate(prompts=[args.prompt], sampling_params=sp, use_tqdm=False)
    warm_ms = (time.perf_counter() - t0) * 1000
    warm_tok = int(warm[0].outputs[0].token_ids[0])

    result = {
        'mode': args.mode,
        'cold_ms': cold_ms,
        'warm_ms': warm_ms,
        'cold_token': cold_tok,
        'warm_token': warm_tok,
    }
    print(f'RESULT {json.dumps(result)}', flush=True)


if __name__ == '__main__':
    sys.exit(main())
