"""Phase-3 benchmark: N independent worker processes, shared cache routing.

This is the setup that actually validates the Semantic Cache Routing claim.
Each worker is its own Python subprocess with its own model instance and
its own SemanticCacheClient. Cache discovery + retrieval happens through
Pulsar headers + per-node HTTP, exactly as it would cross-host.

Scenario per trial:
  - All N workers receive the SAME document as prompt context + a unique
    per-worker suffix (persona + query).
  - Mode "eager":  each worker prefills the full (doc + suffix) from scratch.
  - Mode "routed": worker 0 ("seed") prefills doc + suffix and publishes
    the KV header tagged `doc-X`. Workers 1..N-1 resolve the tag, fetch the
    KV bytes over HTTP, and continue with their own suffix.

Wall-clock end-to-end per trial is the max worker total_ms. The benchmark
reports per-agent medians and the grand total.

Requires a running Pulsar broker (docker run apachepulsar/pulsar:3.1.0
bin/pulsar standalone) and torch+transformers+safetensors installed.
"""

import argparse
import itertools
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from typing import Dict, List


def _workload(doc_tokens: int, num_agents: int, suffix_tokens: int = 32):
    """Return (doc_text, [suffix_text, ...], cache_tag)."""
    filler = 'Chicago is on Lake Michigan and was founded in 1833. '
    doc = (filler * (doc_tokens // 10 + 2)).strip()
    personas = [
        'As an SRE, list three infra concerns.',
        'As a historian, highlight key dates.',
        'As a tourist, suggest two activities.',
        'As a biologist, discuss the ecosystem.',
        'As a journalist, summarize the article.',
        'As a poet, write a short verse.',
        'As a lawyer, flag risk language.',
        'As a student, outline the material.',
    ]
    suffixes = []
    for i in range(num_agents):
        s = personas[i % len(personas)] + ' Reply ' + str(i) + '. '
        suffixes.append(s * max(1, suffix_tokens // 10))
    tag = f'doc-bench-{uuid.uuid4().hex[:8]}'
    return doc, suffixes, tag


def _spawn(worker_cmd: List[str], env_override: Dict[str, str]):
    env = os.environ.copy()
    env.update(env_override)
    return subprocess.Popen(
        worker_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=sys.stderr, env=env, text=True, bufsize=1,
    )


def _await_ready(proc, worker_id: str, timeout: float = 180.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f'worker {worker_id} exited before READY (code {proc.returncode})')
            continue
        line = line.rstrip('\n')
        if line.startswith('READY '):
            return json.loads(line[len('READY '):])
    raise TimeoutError(f'worker {worker_id} did not become READY within {timeout}s')


def _send(proc, payload: dict):
    proc.stdin.write(json.dumps(payload) + '\n')
    proc.stdin.flush()


def _await_result(proc, worker_id: str, timeout: float = 300.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f'worker {worker_id} died mid-run (code {proc.returncode})')
            continue
        line = line.rstrip('\n')
        if line.startswith('RESULT '):
            return json.loads(line[len('RESULT '):])
        # ignore anything else (progress noise)
    raise TimeoutError(f'worker {worker_id} result timeout')


def _shutdown(procs):
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


def run_trial(
    mode: str,
    workers: List[subprocess.Popen],
    doc: str,
    suffixes: List[str],
    tag: str,
    max_new_tokens: int,
    warm_cache: bool = False,
):
    """Dispatch one request to every worker, collect metrics.

    mode = 'eager': every worker prefills the full prompt from scratch.
    mode = 'routed': worker 0 publishes KV for the doc; workers 1..N resolve.
      With warm_cache=True, the publish runs OUTSIDE the timed region to
      simulate steady-state where the cache already exists on a peer. This
      is the deployment-relevant metric -- the first request pays the
      publish once, every subsequent one just fetches.
    """
    assert len(workers) == len(suffixes)
    results = [None] * len(workers)

    if mode == 'routed' and warm_cache:
        # Pre-warm: seed publishes, we wait for propagation, then START timing.
        _send(workers[0], {
            'cmd': 'run',
            'prompt': doc + ' ' + suffixes[0],
            'cache_tags': [tag],
            'publish': True,
            'max_new_tokens': max_new_tokens,
        })
        seed_result = _await_result(workers[0], 'worker-0')
        time.sleep(0.25)  # catalog propagation

        # Timed region: N-1 consumers resolve + continue concurrently.
        t_trial_start = time.perf_counter()
        for i in range(1, len(workers)):
            _send(workers[i], {
                'cmd': 'run',
                'prompt': suffixes[i],
                'cache_tags': [tag],
                'publish': False,
                'max_new_tokens': max_new_tokens,
            })
        for i in range(1, len(workers)):
            results[i] = _await_result(workers[i], f'worker-{i}')
        results[0] = seed_result  # for reporting, not timing
        total_wall = (time.perf_counter() - t_trial_start) * 1000
        return {'total_wall_ms': total_wall, 'per_worker': results,
                'timed_workers': len(workers) - 1, 'warm_cache': True}

    t_trial_start = time.perf_counter()
    if mode == 'routed':
        _send(workers[0], {
            'cmd': 'run',
            'prompt': doc + ' ' + suffixes[0],
            'cache_tags': [tag],
            'publish': True,
            'max_new_tokens': max_new_tokens,
        })
        results[0] = _await_result(workers[0], 'worker-0')
        time.sleep(0.2)
        for i in range(1, len(workers)):
            _send(workers[i], {
                'cmd': 'run',
                'prompt': suffixes[i],
                'cache_tags': [tag],
                'publish': False,
                'max_new_tokens': max_new_tokens,
            })
        for i in range(1, len(workers)):
            results[i] = _await_result(workers[i], f'worker-{i}')
    else:
        if warm_cache:
            # Apples-to-apples with routed warm_cache: worker 0 is assumed to
            # have already done its work in a past trial, so skip it entirely.
            # Only time workers 1..N-1 doing fresh prefills in parallel.
            t_trial_start = time.perf_counter()
            for i in range(1, len(workers)):
                _send(workers[i], {
                    'cmd': 'run',
                    'prompt': doc + ' ' + suffixes[i],
                    'cache_tags': [],
                    'publish': False,
                    'max_new_tokens': max_new_tokens,
                })
            for i in range(1, len(workers)):
                results[i] = _await_result(workers[i], f'worker-{i}')
            total_wall = (time.perf_counter() - t_trial_start) * 1000
            return {'total_wall_ms': total_wall, 'per_worker': results,
                    'timed_workers': len(workers) - 1, 'warm_cache': True}
        for i, w in enumerate(workers):
            _send(w, {
                'cmd': 'run',
                'prompt': doc + ' ' + suffixes[i],
                'cache_tags': [],
                'publish': False,
                'max_new_tokens': max_new_tokens,
            })
        for i, w in enumerate(workers):
            results[i] = _await_result(w, f'worker-{i}')

    total_wall = (time.perf_counter() - t_trial_start) * 1000
    return {'total_wall_ms': total_wall, 'per_worker': results,
            'timed_workers': len(workers), 'warm_cache': False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--num-agents', type=int, default=3)
    parser.add_argument('--doc-tokens', type=int, default=2048)
    parser.add_argument('--suffix-tokens', type=int, default=32)
    parser.add_argument('--max-new-tokens', type=int, default=16)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    parser.add_argument('--modes', default='eager,routed')
    parser.add_argument('--device', default='cuda')
    parser.add_argument('--dtype', default='bf16', choices=['fp32', 'fp16', 'bf16'])
    parser.add_argument('--cache-root', default='/tmp/edgeserve-phase3')
    parser.add_argument('--warm-cache', action='store_true',
                        help='Measure only N-1 consumer requests after the seed '
                             'has already published. Steady-state deployment metric.')
    args = parser.parse_args()

    os.makedirs(args.cache_root, exist_ok=True)
    modes = [m.strip() for m in args.modes.split(',') if m.strip()]

    summary = {}

    for mode in modes:
        print(f'\n=== mode={mode} agents={args.num_agents} '
              f'doc~={args.doc_tokens} model={args.model} ===', flush=True)
        workers: List[subprocess.Popen] = []
        # One shared topic per mode-run so every worker's catalog sees
        # every worker's publish.
        headers_topic = f'kvcache-bench-{mode}-{uuid.uuid4().hex[:8]}'
        try:
            for i in range(args.num_agents):
                wid = f'worker-{i}'
                cache_dir = os.path.join(args.cache_root, f'{mode}-{wid}')
                cmd = [
                    sys.executable,
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phase3_worker.py'),
                    '--worker-id', wid,
                    '--model', args.model,
                    '--pulsar-url', args.pulsar_url,
                    '--headers-topic', headers_topic,
                    '--cache-dir', cache_dir,
                    '--mode', mode,
                ]
                proc = _spawn(cmd, {
                    'EDGESERVE_DEVICE': args.device,
                    'EDGESERVE_DTYPE': args.dtype,
                })
                workers.append(proc)

            ready_infos = []
            for i, w in enumerate(workers):
                ready_infos.append(_await_ready(w, f'worker-{i}'))
            print(f'  all {len(workers)} workers ready', flush=True)

            trials = []
            for r in range(args.repeats):
                doc, suffixes, tag = _workload(args.doc_tokens, args.num_agents, args.suffix_tokens)
                trial = run_trial(mode, workers, doc, suffixes, tag,
                                  args.max_new_tokens, warm_cache=args.warm_cache)
                trials.append(trial)
                print(f'  repeat {r+1}: wall={trial["total_wall_ms"]:.1f}ms', flush=True)
        finally:
            _shutdown(workers)

        wall_ms = [t['total_wall_ms'] for t in trials]
        summary[mode] = {
            'wall_ms_median': statistics.median(wall_ms),
            'wall_ms_min': min(wall_ms),
            'wall_ms_max': max(wall_ms),
            'trials': trials,
        }

    # Final table.
    print('\n=== summary ===', flush=True)
    print(f'{"mode":<10} {"wall median(ms)":>16} {"min":>8} {"max":>8}')
    base = summary.get('eager', {}).get('wall_ms_median')
    for mode, s in summary.items():
        suffix = ''
        if base and mode != 'eager':
            suffix = f'   x{base / s["wall_ms_median"]:.2f}'
        print(f'{mode:<10} {s["wall_ms_median"]:>16.1f} {s["wall_ms_min"]:>8.1f} '
              f'{s["wall_ms_max"]:>8.1f}{suffix}')

    # Routed breakdown: show the first trial's per-worker metrics.
    if 'routed' in summary and summary['routed']['trials']:
        first = summary['routed']['trials'][0]
        print('\nrouted trial 1 per-worker metrics (ms):')
        for m in first['per_worker']:
            if m.get('cache_hit'):
                print(f'  {m["worker_id"]}: resolve={m.get("resolve_ms", 0):.1f} '
                      f'deserialize={m.get("deserialize_ms", 0):.1f} '
                      f'generate={m["generate_ms"]:.1f} '
                      f'fetched={m.get("fetched_bytes", 0)/1e6:.2f}MB')
            else:
                extra = ''
                if m.get('published'):
                    extra = (f' serialize={m.get("serialize_ms", 0):.1f} '
                             f'publish={m.get("publish_ms", 0):.1f} '
                             f'blob={m.get("published_bytes", 0)/1e6:.2f}MB')
                print(f'  {m["worker_id"]}: generate={m["generate_ms"]:.1f}{extra}  (seed/miss)')


if __name__ == '__main__':
    main()
