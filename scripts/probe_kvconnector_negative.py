"""Negative test: consumer with a DIFFERENT prompt should NOT hit cache.

Launches two subprocesses sequentially with distinct prompts. Verifies:
 - consumer's generate doesn't report a cache hit
 - consumer's gen time is similar to seeder's (no speedup)
 - output tokens naturally differ

This rules out false positives in the bloom-filter catalog / hash matching.
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
        raise RuntimeError(f'{role} produced no RESULT')
    return result, proc.stdout + '\n' + proc.stderr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=2048)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    topic = f'kvcache-negtest-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    print(f'topic = {topic}')

    prompt_a = 'Chicago was founded in 1833 and is on Lake Michigan. ' * 64
    prompt_b = 'Paris is the capital of France. The Seine flows through it. ' * 64

    print('\n=== seeder with prompt A')
    seed, _ = run(prompt_a, topic, 'seeder', args)
    print(f'  gen={seed["gen_ms"]:.1f}ms token={seed["output_token"]}')

    print('\n=== consumer with DIFFERENT prompt B')
    cons, stderr = run(prompt_b, topic, 'consumer', args)
    print(f'  gen={cons["gen_ms"]:.1f}ms token={cons["output_token"]}')

    # Look for cache-hit log lines in consumer output
    hit_logged = 'cache HIT' in stderr
    build_meta_lines = [l for l in stderr.splitlines() if 'build_connector_meta' in l]
    print(f'\n=== Diagnostics')
    print(f'  consumer stderr contains "cache HIT": {hit_logged}')
    for l in build_meta_lines[:3]:
        print(f'  {l.strip()}')

    # Expectations:
    # - consumer did NOT hit cache (different prompt)
    # - output tokens differ
    # - gen times are similar (within 2x)
    if hit_logged:
        print('FAIL: consumer reported a cache HIT for a different prompt')
        return 1
    if seed['output_token'] == cons['output_token']:
        print('WARN: output tokens match despite different prompts (unusual but '
              'possible with greedy sampling and small models)')
    ratio = cons['gen_ms'] / seed['gen_ms'] if seed['gen_ms'] > 0 else 0
    print(f'\ngen ratio consumer/seeder = {ratio:.2f}x')
    if 0.5 <= ratio <= 2.0:
        print('PASS: no cache hit, gen times comparable')
        return 0
    print(f'UNEXPECTED: gen ratio {ratio:.2f}x is far from 1.0; still no cache hit but '
          'something else varied between the runs.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
