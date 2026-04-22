"""Multi-entry test: two different prompts both seeded on the same topic,
consumer for prompt A should still correctly hit A (not B).

Stage 1: seeder for prompt A
Stage 2: seeder for prompt B
Stage 3: consumer for prompt A -- should cache-hit A and produce the
         same output token as Stage 1's seeder.

If cross-contamination happens (consumer produces B's token, or
produces neither), the connector has a bug in how it disambiguates
entries.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid


HERE = os.path.dirname(os.path.abspath(__file__))


def run(prompt, topic, role, args):
    cmd = [
        sys.executable,
        os.path.join(HERE, '_kvconnector_worker.py'),
        '--role', role, '--prompt', prompt, '--topic', topic,
        '--model', args.model, '--dtype', args.dtype,
        '--gpu-mem', str(args.gpu_mem),
        '--max-model-len', str(args.max_model_len),
        '--pulsar-url', args.pulsar_url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    result = None
    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            result = json.loads(line[len('RESULT '):])
    if result is None:
        raise RuntimeError(f'{role} produced no RESULT:\n{proc.stderr[-2000:]}')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=2048)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    topic = f'kvcache-multi-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    print(f'topic = {topic}')

    prompt_a = (
        'The scientific method involves observation, hypothesis, experiment, and conclusion. '
        'Consider the following chemistry problem step by step. '
    ) * 64
    prompt_b = (
        'Once upon a time there was a brave knight who traveled to a faraway castle. '
        'Describe what the knight would do next in the story. '
    ) * 64

    print('\n=== seeder A')
    seed_a = run(prompt_a, topic, 'seeder', args)
    print(f'  A gen={seed_a["gen_ms"]:.1f}ms  token={seed_a["output_token"]}')

    print('\n=== seeder B')
    seed_b = run(prompt_b, topic, 'seeder', args)
    print(f'  B gen={seed_b["gen_ms"]:.1f}ms  token={seed_b["output_token"]}')

    print('\n=== consumer A (should hit A, not B)')
    cons_a = run(prompt_a, topic, 'consumer', args)
    print(f'  A\' gen={cons_a["gen_ms"]:.1f}ms  token={cons_a["output_token"]}')

    speedup = seed_a['gen_ms'] / max(cons_a['gen_ms'], 1e-3)
    print()
    if cons_a['output_token'] == seed_a['output_token']:
        print(f'PASS: consumer correctly hit A (same token {cons_a["output_token"]})')
        print(f'      gen speedup {speedup:.2f}x over seeder A')
        return 0
    if cons_a['output_token'] == seed_b['output_token']:
        print(f'FAIL: consumer incorrectly loaded B\'s cache for A\'s prompt')
        print(f'      got token {cons_a["output_token"]} expected {seed_a["output_token"]}')
        return 1
    print(f'FAIL: consumer produced an unexpected token {cons_a["output_token"]}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
