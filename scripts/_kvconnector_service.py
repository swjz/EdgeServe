"""Long-lived vLLM worker with EdgeServeKVConnector.

Pattern mirrors phase3_worker.py but the engine stays alive across multiple
requests. Coordinator sends JSON commands on stdin, receives JSON on stdout.

Protocol:
    worker -> coord: READY <json meta>
    coord -> worker: {"cmd":"run", "prompt":..., "max_new_tokens":N}
    worker -> coord: RESULT <json metrics>
    coord -> worker: {"cmd":"shutdown"}
"""

import argparse
import json
import os
import sys
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker-id', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.3)
    parser.add_argument('--max-model-len', type=int, default=4096)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    os.environ.setdefault('VLLM_USE_V1', '1')

    from edgeserve.inference.vllm_kv_connector import register
    try:
        register()
    except Exception:
        pass

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    cache_path = f'/tmp/edgeserve-svc-{args.worker_id}-{os.getpid()}'
    os.makedirs(cache_path, exist_ok=True)

    kv_cfg = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={
            'pulsar_url': args.pulsar_url,
            'topic': args.topic,
            'local_cache_path': cache_path,
            'node_id': f'{args.worker_id}-{uuid.uuid4().hex[:6]}',
        },
    )

    llm = LLM(
        model=args.model,
        dtype=args.dtype,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
        enable_prefix_caching=False,
        kv_transfer_config=kv_cfg,
    )

    ready = {'worker_id': args.worker_id, 'pid': os.getpid()}
    print(f'READY {json.dumps(ready)}', flush=True)

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            cmd = json.loads(line)
            op = cmd.get('cmd', 'run')
            if op == 'shutdown':
                break
            if op != 'run':
                print(f'RESULT {json.dumps({"error": f"unknown cmd {op}"})}', flush=True)
                continue

            prompt = cmd['prompt']
            max_new = int(cmd.get('max_new_tokens', 1))
            sp = SamplingParams(max_tokens=max_new, temperature=0.0)

            t0 = time.perf_counter()
            out = llm.generate(prompts=[prompt], sampling_params=sp, use_tqdm=False)
            gen_ms = (time.perf_counter() - t0) * 1000
            result = {
                'worker_id': args.worker_id,
                'gen_ms': gen_ms,
                'output_token': int(out[0].outputs[0].token_ids[0]),
            }
            print(f'RESULT {json.dumps(result)}', flush=True)
    finally:
        pass


if __name__ == '__main__':
    sys.exit(main())
