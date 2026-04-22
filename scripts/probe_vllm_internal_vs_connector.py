"""Ceiling comparison: vLLM's own internal prefix cache vs EdgeServeKVConnector.

Both paths avoid recomputing KV for a repeated prompt. vLLM's internal
prefix cache only works within ONE process. Our connector works across
processes. This script measures the gap on a fair workload.

Setup:
  - Run 1: vLLM instance A with `enable_prefix_caching=True`, no connector.
           First request computes doc prefill. Second request (same
           prompt) hits vLLM's internal radix cache.
  - Run 2: vLLM instance B with `enable_prefix_caching=False` and
           EdgeServeKVConnector. First request computes doc prefill and
           publishes via connector. Second request (same prompt, same
           process) hits our connector's cache path.

Difference = overhead of the cross-process transport machinery even
when the consumer happens to be the same process.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid


HERE = os.path.dirname(os.path.abspath(__file__))


def run_once(mode, prompt, topic, args):
    """Launch a subprocess that runs the same prompt twice and reports both
    timings. `mode` is 'internal' (vLLM prefix cache) or 'connector'."""
    cmd = [
        sys.executable,
        os.path.join(HERE, '_ceiling_worker.py'),
        '--mode', mode, '--prompt', prompt, '--topic', topic,
        '--model', args.model, '--dtype', args.dtype,
        '--gpu-mem', str(args.gpu_mem),
        '--max-model-len', str(args.max_model_len),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    result = None
    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            result = json.loads(line[len('RESULT '):])
    if result is None:
        print(proc.stderr[-2000:])
        raise RuntimeError(f'{mode} worker produced no RESULT')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=4096)
    parser.add_argument('--doc-repeats', type=int, default=128)
    args = parser.parse_args()

    topic = f'ceiling-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    prompt = 'The city of Chicago is on Lake Michigan. ' * args.doc_repeats

    print(f'model: {args.model}')
    print(f'prompt: {len(prompt)} chars')
    print()

    print('=== Run 1: vLLM internal prefix cache')
    r_internal = run_once('internal', prompt, topic, args)
    print(f'  first  (cold): gen={r_internal["cold_ms"]:.1f}ms  token={r_internal["cold_token"]}')
    print(f'  second (warm): gen={r_internal["warm_ms"]:.1f}ms  token={r_internal["warm_token"]}')
    speedup_i = r_internal['cold_ms'] / max(r_internal['warm_ms'], 1e-3)
    print(f'  internal speedup: {speedup_i:.2f}x')

    print()
    print('=== Run 2: EdgeServeKVConnector (external cache, same process)')
    r_conn = run_once('connector', prompt, topic, args)
    print(f'  first  (cold): gen={r_conn["cold_ms"]:.1f}ms  token={r_conn["cold_token"]}')
    print(f'  second (warm): gen={r_conn["warm_ms"]:.1f}ms  token={r_conn["warm_token"]}')
    speedup_c = r_conn['cold_ms'] / max(r_conn['warm_ms'], 1e-3)
    print(f'  connector speedup: {speedup_c:.2f}x')

    print()
    print('=== Comparison')
    print(f'warm internal : {r_internal["warm_ms"]:.1f}ms')
    print(f'warm connector: {r_conn["warm_ms"]:.1f}ms')
    overhead = r_conn['warm_ms'] - r_internal['warm_ms']
    print(f'connector overhead vs internal: +{overhead:.1f}ms')
    if r_internal['warm_token'] == r_conn['warm_token']:
        print('correctness: both paths produce the same warm token')
    else:
        print(f'WARN: internal warm token={r_internal["warm_token"]} '
              f'but connector warm token={r_conn["warm_token"]}')


if __name__ == '__main__':
    sys.exit(main())
