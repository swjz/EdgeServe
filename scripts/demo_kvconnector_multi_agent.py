"""Multi-agent realistic demo: 1 seeder + N consumers, all with different
persona/query suffixes on the same shared document.

Mirrors the Semantic Cache Routing paper's motivating scenario. Each
consumer's prompt is `doc + unique_persona_and_query`. Seeder populates
the cache; each consumer hits the doc prefix via the connector's
multi-boundary matching, then only pays the prefill cost for its own
(short) suffix.

Sequential -- a single GPU can't host many vLLM instances simultaneously.
Each consumer is a fresh vLLM subprocess that fails into the cache via
Pulsar on the shared topic.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import uuid


HERE = os.path.dirname(os.path.abspath(__file__))

DEFAULT_SUFFIXES = [
    ' As an SRE, list three infra concerns.',
    ' As a historian, highlight key dates.',
    ' As a tourist, suggest two activities.',
    ' As a biologist, discuss the ecosystem.',
    ' As a journalist, summarize the article.',
    ' As a poet, write a short verse.',
    ' As a lawyer, flag risk language.',
    ' As a student, outline the material.',
]


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
    parser.add_argument('--num-consumers', type=int, default=5)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    topic = f'kvcache-multi-agent-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    print(f'topic = {topic}')

    doc = 'Chicago is on Lake Michigan and was founded in 1833. ' * args.doc_repeats
    suffixes = DEFAULT_SUFFIXES[:args.num_consumers + 1]

    # Cold baselines: run each consumer WITHOUT a seeder to get the cold
    # prefill time for each suffix.
    cold_topic = f'{topic}-cold'
    cold_times = []
    cold_tokens = []
    print(f'\n--- cold baselines ({len(suffixes)-1} consumers, no cache)')
    for i, suf in enumerate(suffixes[1:], start=1):
        r, _ = run(doc + suf, cold_topic, 'consumer', args)
        cold_times.append(r['gen_ms'])
        cold_tokens.append(r['output_token'])
        print(f'  consumer {i} (suffix={suf[:30]!r}...): '
              f'gen={r["gen_ms"]:.1f}ms token={r["output_token"]}')

    # Seeder uses suffix 0 to pre-populate the doc prefix in the catalog.
    print(f'\n--- seeder (suffix 0)')
    seed_r, _ = run(doc + suffixes[0], topic, 'seeder', args)
    print(f'  seeder gen={seed_r["gen_ms"]:.1f}ms token={seed_r["output_token"]}')

    # Warm consumers: each with a different suffix, each should prefix-hit
    # on the doc.
    print(f'\n--- warm consumers ({len(suffixes)-1}) sharing seeder\'s doc cache')
    warm_times = []
    warm_tokens = []
    for i, suf in enumerate(suffixes[1:], start=1):
        r, stderr = run(doc + suf, topic, 'consumer', args)
        warm_times.append(r['gen_ms'])
        warm_tokens.append(r['output_token'])
        hit_lines = [l for l in stderr.splitlines() if 'cache HIT' in l]
        matched = 'no'
        for l in hit_lines:
            if 'of' in l and 'tokens matched' in l:
                import re
                m = re.search(r'(\d+) of (\d+) tokens matched', l)
                if m:
                    matched = f'{m.group(1)}/{m.group(2)}'
                    break
        print(f'  consumer {i}: gen={r["gen_ms"]:.1f}ms token={r["output_token"]} '
              f'matched={matched}')

    # Summary
    print()
    print('=== summary ===')
    cold_median = statistics.median(cold_times)
    warm_median = statistics.median(warm_times)
    print(f'cold gen median:   {cold_median:.1f}ms  '
          f'(range {min(cold_times):.1f}..{max(cold_times):.1f})')
    print(f'warm gen median:   {warm_median:.1f}ms  '
          f'(range {min(warm_times):.1f}..{max(warm_times):.1f})')
    if warm_median > 0:
        print(f'multi-agent prefix-hit speedup: {cold_median/warm_median:.2f}x')
    correctness = all(c == w for c, w in zip(cold_tokens, warm_tokens))
    print(f'correctness (every consumer warm == cold): {correctness}')
    return 0 if correctness else 1


if __name__ == '__main__':
    sys.exit(main())
