"""Two-stage demo: vLLM-A publishes KV for a prompt; vLLM-B consumes it.

Launches two subprocesses sequentially:
  Stage 1 (seeder): vLLM with EdgeServeKVConnector, generates the prompt,
                    publishes KV. Prints timing, then exits.
  Stage 2 (consumer): fresh vLLM with same KVConnector config + same
                      prompt. Should see "cache HIT" in the scheduler and
                      skip the prefill. Prints timing.

Compares wall-clock time for (2) against (1) at the same doc length.
If the routing layer + HTTP round trip save time on the shared doc
prefill, you'll see stage 2 noticeably faster than stage 1.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid


HERE = os.path.dirname(os.path.abspath(__file__))


def run_single_vllm(prompt: str, topic: str, role: str, args) -> dict:
    """Start a fresh vLLM subprocess, have it generate one prompt, report stats."""
    env = os.environ.copy()
    cmd = [
        sys.executable,
        os.path.join(HERE, '_kvconnector_worker.py'),
        '--role', role,
        '--prompt', prompt,
        '--topic', topic,
        '--model', args.model,
        '--dtype', args.dtype,
        '--gpu-mem', str(args.gpu_mem),
        '--max-model-len', str(args.max_model_len),
        '--pulsar-url', args.pulsar_url,
    ]
    proc = subprocess.run(
        cmd, env=env, capture_output=True, text=True, timeout=600,
    )
    # Look for a single RESULT json line in stdout.
    result = None
    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            result = json.loads(line[len('RESULT '):])
    if result is None:
        print(f'--- {role} stdout tail ---')
        print('\n'.join(proc.stdout.splitlines()[-30:]))
        print(f'--- {role} stderr tail ---')
        print('\n'.join(proc.stderr.splitlines()[-30:]))
        raise RuntimeError(f'{role} worker produced no RESULT line')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.6)
    parser.add_argument('--max-model-len', type=int, default=2048)
    parser.add_argument('--doc-repeats', type=int, default=64,
                        help='how many copies of the filler sentence to tile')
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    prompt = 'The city of Chicago is on Lake Michigan. ' * args.doc_repeats
    topic = f'kvcache-demo-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}'
    print(f'prompt len (chars) = {len(prompt)}')
    print(f'topic = {topic}')
    print()

    print('=== Stage 1: seeder (expected cache MISS -> store)')
    t0 = time.perf_counter()
    seed = run_single_vllm(prompt, topic, 'seeder', args)
    wall_a = time.perf_counter() - t0
    print(f'seeder wall = {wall_a*1000:.0f}ms; '
          f'gen = {seed["gen_ms"]:.1f}ms; output_token = {seed["output_token"]}')
    print()

    print('=== Stage 2: consumer (expected cache HIT -> load)')
    t0 = time.perf_counter()
    consumer = run_single_vllm(prompt, topic, 'consumer', args)
    wall_b = time.perf_counter() - t0
    print(f'consumer wall = {wall_b*1000:.0f}ms; '
          f'gen = {consumer["gen_ms"]:.1f}ms; output_token = {consumer["output_token"]}')
    print()

    print('=== Summary')
    print(f'{"stage":<12} {"wall":>8} {"gen":>8}')
    print(f'{"seeder":<12} {wall_a*1000:>7.0f}ms {seed["gen_ms"]:>7.1f}ms')
    print(f'{"consumer":<12} {wall_b*1000:>7.0f}ms {consumer["gen_ms"]:>7.1f}ms')
    if seed['gen_ms'] > 0:
        print(f'gen speedup consumer/seeder: {seed["gen_ms"]/max(consumer["gen_ms"], 1e-3):.2f}x')
    if seed['output_token'] == consumer['output_token']:
        print(f'output tokens MATCH ({seed["output_token"]}): correctness preserved')
    else:
        print(f'!! output tokens DIFFER: seeder={seed["output_token"]} '
              f'consumer={consumer["output_token"]} (KV reconstruction bug)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
