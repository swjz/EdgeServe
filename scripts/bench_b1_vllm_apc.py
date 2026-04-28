"""bench_b1_vllm_apc.py — Phase 7.1: the B1 same-host ceiling.

Measures the "hard baseline" against which EdgeServe's cross-process
sharing should be compared honestly: a SINGLE vLLM instance with
`enable_prefix_caching=True`, serving N agents via continuous batching
in one `llm.generate(prompts=[...])` call.

This is what a production multi-agent deployment on one machine would
use by default.  EdgeServe's claim is NOT that it beats this on the
same host (it doesn't — vLLM's internal APC is essentially free).
EdgeServe's niche is:
  - cross-host: N agents on N separate machines (APC can't share)
  - edge decode: when the consumer can't run vLLM at all (Mac / CPU)
  - process restart: GPU cache evicted, need durable KV (see Phase 3.6)

Workload identical to Phase 2.3 (bench_multiagent_fanout.py):
  - shared prefix of `doc_repeats` chunks
  - N agent queries, each a distinct suffix on the same prefix

Output: per-agent TTFT for B1, comparable to the B2/EdgeServe rows
already in RESULTS.md §Phase 2.3.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society. "
)

AGENT_SUFFIXES = [
    ' As a historian, discuss the 19th-century origins.',
    ' As a scientist, summarise the key research themes.',
    ' As a journalist, write a short news article.',
    ' As a poet, compose a brief reflection.',
    ' As a sceptic, list the open problems.',
    ' As a biologist, draw analogies to evolution.',
    ' As a tourist, describe what stood out.',
    ' As an engineer, identify the practical levers.',
]


def _find_python() -> str:
    if sys.prefix != sys.base_prefix:
        return sys.executable
    venv = os.path.join(os.path.dirname(HERE), '.venv', 'bin', 'python')
    return venv if os.path.isfile(venv) else sys.executable


def _run_worker(script_src: str, timeout: int = 600) -> dict:
    PYTHON = _find_python()
    with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as f:
        f.write(script_src)
        path = f.name
    proc = subprocess.run([PYTHON, path], capture_output=True, text=True, timeout=timeout)
    result = None
    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            try:
                result = json.loads(line[len('RESULT '):])
            except Exception:
                pass
    if result is None:
        print('--- worker stdout tail ---')
        print('\n'.join(proc.stdout.splitlines()[-40:]))
        print('--- worker stderr tail ---')
        print('\n'.join(proc.stderr.splitlines()[-20:]))
        raise RuntimeError('Worker produced no RESULT line')
    os.unlink(path)
    return result


def run_b1(prompts: list, args) -> dict:
    """Single vLLM instance with APC.  Issues all N prompts as ONE generate()
    call so vLLM's continuous batching + radix prefix cache can fire."""
    src = f"""\
import time, json, os
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from vllm import LLM, SamplingParams

    llm = LLM(
        model={repr(args.model)},
        gpu_memory_utilization={args.gpu_mem},
        max_model_len={args.max_model_len},
        enable_prefix_caching=True,   # ← the whole point of this baseline
    )
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    prompts = {prompts!r}

    # First request: cold path — vLLM has to prefill from scratch.
    # We time the first prompt alone to get an apples-to-apples "cold"
    # number, then issue the rest as a batched generate() to measure
    # warm per-agent cost with continuous batching + APC reuse.
    t_cold = time.perf_counter()
    out = llm.generate(prompts[:1], sampling_params=sp, use_tqdm=False)
    cold_ms = (time.perf_counter() - t_cold) * 1000
    tokens = [int(out[0].outputs[0].token_ids[0])]

    # Warm batch: remaining prompts in one call.  Continuous batching
    # means vLLM interleaves them; APC reuses the shared prefix KV.
    t_warm = time.perf_counter()
    out = llm.generate(prompts[1:], sampling_params=sp, use_tqdm=False)
    warm_batch_ms = (time.perf_counter() - t_warm) * 1000
    for o in out:
        tokens.append(int(o.outputs[0].token_ids[0]))

    # Also run each warm prompt SEPARATELY to get per-agent latency
    # (not just batch total) for comparison with B2 sequential numbers.
    warm_solo = []
    for p in prompts[1:]:
        t0 = time.perf_counter()
        out = llm.generate([p], sampling_params=sp, use_tqdm=False)
        warm_solo.append((time.perf_counter() - t0) * 1000)
        tokens.append(int(out[0].outputs[0].token_ids[0]))

    print('RESULT ' + json.dumps({{
        'cold_first_ms':  cold_ms,
        'warm_batch_ms':  warm_batch_ms,
        'warm_solo_ms':   warm_solo,
        'tokens':         tokens,
        'n_agents':       len(prompts),
    }}))
"""
    return _run_worker(src)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--doc-repeats', type=int, default=128)
    ap.add_argument('--num-agents', type=int, default=4)
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=16384)
    args = ap.parse_args()

    doc = DOC_CHUNK * args.doc_repeats
    N = args.num_agents
    prompts = [doc + AGENT_SUFFIXES[i % len(AGENT_SUFFIXES)] for i in range(N)]

    n_tokens_approx = len(doc) // 4
    print(f'Model:        {args.model}')
    print(f'Doc repeats:  {args.doc_repeats}  (~{n_tokens_approx} tokens shared prefix)')
    print(f'Agents (N):   {N}')
    print()
    print('Running B1: single vLLM, enable_prefix_caching=True ...')
    res = run_b1(prompts, args)

    cold = res['cold_first_ms']
    warm_batch = res['warm_batch_ms']
    warm_solo = res['warm_solo_ms']
    per_agent_batch = warm_batch / (N - 1) if N > 1 else float('nan')
    per_agent_solo = statistics.mean(warm_solo) if warm_solo else float('nan')

    print()
    print('=' * 60)
    print('B1 results (single vLLM + APC)')
    print('=' * 60)
    print(f'Cold first agent (no cache):          {cold:.0f} ms')
    print(f'Warm batched ({N-1} agents, one call): {warm_batch:.0f} ms total '
          f'({per_agent_batch:.0f} ms/agent amortised)')
    print(f'Warm sequential (one call per agent): {per_agent_solo:.0f} ms/agent median')
    print()
    print('Compared to B2 and EdgeServe at the same workload (doc_repeats='
          f'{args.doc_repeats}, N={N}):')
    print('  B2 baseline (from RESULTS §2.3):   ~240 ms/agent  (fresh vLLM each)')
    print('  EdgeServe (from RESULTS §2.3):     ~176 ms/agent  (1.36× vs B2)')
    print(f'  B1 (this run, warm solo):         {per_agent_solo:>4.0f} ms/agent')
    print(f'  B1 (this run, warm batch avg):    {per_agent_batch:>4.0f} ms/agent')
    print()
    print('Markdown row:')
    print(f'| B1 (single vLLM + APC) | {N-1} warm agents | {per_agent_solo:.0f} '
          f'| {per_agent_batch:.0f} | cold first: {cold:.0f} |')


if __name__ == '__main__':
    main()
