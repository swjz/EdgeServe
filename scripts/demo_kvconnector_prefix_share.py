"""Demo: two prompts share a document prefix but differ in the suffix.
Consumer should cache-hit at the LONGEST SHARED PREFIX (the doc), then
process its own suffix on top.

This is the scenario from the Semantic Cache Routing paper: multiple
agents ask different questions about the same document. With
prefix-boundary publishing (see `_Worker.wait_for_save`) in
EdgeServeKVConnector, the consumer's catalog lookup finds the right
cached entry via one of the prefix hashes.

Compares:
  - cold consumer (no cache hit) — baseline
  - warm consumer (via connector, hits doc prefix)
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
        print(proc.stderr[-2000:])
        raise RuntimeError(f'{role} produced no RESULT')
    return result, proc.stderr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=4096)
    parser.add_argument('--doc-repeats', type=int, default=128)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    topic = f'kvcache-prefix-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    print(f'topic = {topic}')

    doc = 'Chicago is on Lake Michigan and was founded in 1833. ' * args.doc_repeats
    suffix_a = ' As a scientist, discuss geology.'
    suffix_b = ' As a historian, discuss 19th-century America.'
    prompt_a = doc + suffix_a
    prompt_b = doc + suffix_b

    # Cold baseline: fresh topic, consumer B with NO seeder. Tells us the
    # uncached gen time for B.
    cold_topic = f'{topic}-cold'
    print('\n--- cold baseline: consumer B with empty catalog')
    cold_b, _ = run(prompt_b, cold_topic, 'consumer', args)
    print(f'  cold B gen={cold_b["gen_ms"]:.1f}ms token={cold_b["output_token"]}')

    # Warm path: seeder A populates the catalog, then consumer B
    # runs with its DIFFERENT suffix. Scheduler should hit on the doc
    # prefix.
    print('\n--- seeder A (doc + suffix_A)')
    seed_a, _ = run(prompt_a, topic, 'seeder', args)
    print(f'  seed A gen={seed_a["gen_ms"]:.1f}ms token={seed_a["output_token"]}')

    print('\n--- consumer B (doc + suffix_B) — expected PREFIX HIT')
    warm_b, stderr = run(prompt_b, topic, 'consumer', args)
    print(f'  warm B gen={warm_b["gen_ms"]:.1f}ms token={warm_b["output_token"]}')

    hit_lines = [l.strip() for l in stderr.splitlines() if 'cache HIT' in l]
    for l in hit_lines[:3]:
        print(f'  [log] {l}')

    # Report
    print()
    print(f'cold consumer B gen:  {cold_b["gen_ms"]:.1f}ms')
    print(f'warm consumer B gen:  {warm_b["gen_ms"]:.1f}ms')
    if warm_b['gen_ms'] > 0:
        print(f'prefix-hit speedup:   {cold_b["gen_ms"]/warm_b["gen_ms"]:.2f}x')

    # B's output on warm path must equal B's cold output (proves the
    # prefix-load is bit-exact for the shared portion; the suffix compute
    # is identical).
    if cold_b['output_token'] == warm_b['output_token']:
        print('correctness (warm B token == cold B token): OK')
        return 0
    print(f'CORRECTNESS FAIL: cold token={cold_b["output_token"]}, '
          f'warm token={warm_b["output_token"]}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
