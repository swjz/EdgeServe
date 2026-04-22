"""Concurrent demo: N live vLLM workers with EdgeServeKVConnector.

All workers share a Pulsar topic. The first request for a given prompt
goes to worker 0 (cache MISS -> publishes). Subsequent requests for the
SAME prompt go to other workers, which should hit cache via the
connector and run faster.

The workers are PERSISTENT (not re-launched per request), so the
vLLM-init cost is paid once per worker, not per request.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid


HERE = os.path.dirname(os.path.abspath(__file__))


def _spawn(cmd):
    return subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=sys.stderr, text=True, bufsize=1,
    )


def _send(proc, payload):
    proc.stdin.write(json.dumps(payload) + '\n')
    proc.stdin.flush()


def _await(proc, expect, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f'worker exited (code {proc.returncode})')
            continue
        line = line.rstrip('\n')
        if line.startswith(expect + ' '):
            return json.loads(line[len(expect)+1:])
    raise TimeoutError(f'no {expect} within {timeout}s')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--num-workers', type=int, default=2)
    parser.add_argument('--doc-repeats', type=int, default=128)
    parser.add_argument('--gpu-mem', type=float, default=0.3,
                        help='per-worker gpu_memory_utilization; total '
                             'should be <= 0.95 for N workers')
    parser.add_argument('--max-model-len', type=int, default=4096)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = parser.parse_args()

    topic = f'kvcache-conc-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}'
    doc = 'The city of Chicago is on Lake Michigan. ' * args.doc_repeats
    # Each worker uses a slightly different suffix so the demo exercises
    # EdgeServeKVConnector's prefix-boundary matching (shared doc, unique
    # suffix), not just exact-match cache hits.
    personas = [
        ' As an SRE, list infra concerns.',
        ' As a historian, highlight key dates.',
        ' As a tourist, suggest activities.',
        ' As a biologist, discuss ecosystem.',
    ]
    prompts = [doc + personas[i % len(personas)] for i in range(args.num_workers)]
    print(f'topic = {topic}')
    print(f'doc len = {len(doc)} chars; per-worker suffix differs')
    print(f'spawning {args.num_workers} workers (each ~{args.gpu_mem*12:.1f} GB)...')

    procs = []
    for i in range(args.num_workers):
        cmd = [
            sys.executable, os.path.join(HERE, '_kvconnector_service.py'),
            '--worker-id', f'w{i}', '--topic', topic, '--model', args.model,
            '--gpu-mem', str(args.gpu_mem),
            '--max-model-len', str(args.max_model_len),
            '--pulsar-url', args.pulsar_url,
        ]
        procs.append(_spawn(cmd))
        if i > 0:
            # Serialize spawns so each vLLM finishes init before the next
            # claims GPU memory (same race as in phase3_multiproc_bench).
            _await(procs[-1], 'READY')
            print(f'  worker {i} ready')
        else:
            _await(procs[-1], 'READY')
            print(f'  worker {i} ready')

    try:
        print('\n--- round 1: worker 0 seeds with its unique suffix (MISS + publish)')
        t0 = time.perf_counter()
        _send(procs[0], {'cmd': 'run', 'prompt': prompts[0], 'max_new_tokens': 1})
        r0 = _await(procs[0], 'RESULT')
        wall0 = (time.perf_counter() - t0) * 1000
        print(f'  worker 0: gen={r0["gen_ms"]:.1f}ms token={r0["output_token"]} '
              f'wall={wall0:.0f}ms')

        time.sleep(0.5)  # let publish propagate

        print('\n--- round 2: workers 1..N run with DIFFERENT suffixes '
              '(expect prefix HIT on shared doc)')
        round2 = []
        for i in range(1, args.num_workers):
            t0 = time.perf_counter()
            _send(procs[i], {'cmd': 'run', 'prompt': prompts[i], 'max_new_tokens': 1})
            r = _await(procs[i], 'RESULT')
            wall = (time.perf_counter() - t0) * 1000
            print(f'  worker {i}: gen={r["gen_ms"]:.1f}ms token={r["output_token"]} '
                  f'wall={wall:.0f}ms')
            round2.append(r)

        print()
        print(f'seeder gen (full prefill, worker 0): {r0["gen_ms"]:.1f}ms')
        if round2:
            mean_r2 = sum(r['gen_ms'] for r in round2) / len(round2)
            print(f'consumer gen mean (prefix-hit, workers 1..{args.num_workers-1}): '
                  f'{mean_r2:.1f}ms')
            print(f'seeder / consumer speedup: {r0["gen_ms"]/max(mean_r2, 1e-3):.2f}x')
        print(f'distinct tokens produced (different suffixes): '
              f'{len(set(r["output_token"] for r in [r0] + round2))}')
    finally:
        for p in procs:
            try:
                _send(p, {'cmd': 'shutdown'})
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=10)
            except Exception:
                p.kill()

    return 0


if __name__ == '__main__':
    sys.exit(main())
