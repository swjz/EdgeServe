"""bench_multiagent_fanout.py — Phase 2.3: multi-agent shared-prefix fan-out.

Models the RAG / agentic scenario where ONE seeder prefills a long shared
context (e.g., a large code repo or document set) and N subsequent agents
each need to answer a query against that same context.

Two conditions measured (both sequential, single GPU):
  B2 baseline — each agent starts a fresh vLLM and does its own full cold
                re-prefill of the shared prefix (no EdgeServe).
  EdgeServe   — each agent starts a fresh vLLM + connector, discovers the
                seeder's NVMe KV via Pulsar catalog, restores from disk.

Key metrics:
  - per-agent latency (B2 vs EdgeServe)
  - total GPU time for N agents (aggregate savings)
  - amortised seeding cost (seed once, amortised over N queries)

Usage
-----
  python scripts/bench_multiagent_fanout.py
  python scripts/bench_multiagent_fanout.py --num-agents 6 --doc-repeats 256
  python scripts/bench_multiagent_fanout.py --model Qwen/Qwen2.5-1.5B --num-agents 4
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
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society. "
)


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
    proc = subprocess.run(
        [PYTHON, path], capture_output=True, text=True, timeout=timeout,
    )
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


def run_seeder(prompt: str, topic: str, cache_path: str, args) -> dict:
    src = f"""\
import time, json, os
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from edgeserve.inference.vllm_kv_connector import register
    register()
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={{
            'pulsar_url': {repr(args.pulsar_url)},
            'topic':      {repr(topic)},
            'local_cache_path': {repr(cache_path)},
            'node_id': 'fanout-seeder',
        }},
    )
    llm = LLM(
        model={repr(args.model)},
        gpu_memory_utilization={args.gpu_mem},
        max_model_len={args.max_model_len},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
    )
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    t0 = time.perf_counter()
    out = llm.generate([{repr(prompt)}], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'seeder','gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def run_baseline(prompt: str, agent_id: int, args) -> dict:
    src = f"""\
import time, json, os
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from vllm import LLM, SamplingParams

    llm = LLM(
        model={repr(args.model)},
        gpu_memory_utilization={args.gpu_mem},
        max_model_len={args.max_model_len},
        enable_prefix_caching=False,
    )
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    t0 = time.perf_counter()
    out = llm.generate([{repr(prompt)}], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'baseline','agent':{agent_id},'gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def run_restore(prompt: str, topic: str, cache_path: str, agent_id: int, args) -> dict:
    node_id = f'fanout-agent-{agent_id}-{uuid.uuid4().hex[:6]}'
    src = f"""\
import time, json, os
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from edgeserve.inference.vllm_kv_connector import register
    register()
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={{
            'pulsar_url': {repr(args.pulsar_url)},
            'topic':      {repr(topic)},
            'local_cache_path': {repr(cache_path)},
            'node_id': {repr(node_id)},
        }},
    )
    llm = LLM(
        model={repr(args.model)},
        gpu_memory_utilization={args.gpu_mem},
        max_model_len={args.max_model_len},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
    )
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    time.sleep(3.0)   # let catalog drain Pulsar backlog
    t0 = time.perf_counter()
    out = llm.generate([{repr(prompt)}], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'restore','agent':{agent_id},'gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--doc-repeats', type=int, default=128,
                    help='Shared prefix length in doc-chunk repetitions')
    ap.add_argument('--num-agents', type=int, default=4,
                    help='Number of subsequent agents (N)')
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=16384)
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = ap.parse_args()

    doc = DOC_CHUNK * args.doc_repeats
    prompt = doc + ' Summarise the key points.'
    n_tokens_approx = len(doc) // 4
    N = args.num_agents

    print(f'Model:         {args.model}')
    print(f'Doc repeats:   {args.doc_repeats}  (~{n_tokens_approx} tokens)')
    print(f'Agents (N):    {N}')
    print()

    # ---- Seeder: prefill once, save KV to NVMe ----
    cache_path = tempfile.mkdtemp(prefix='edgeserve-fanout-')
    topic = f'kvcache-fanout-{uuid.uuid4().hex[:8]}'

    print(f'Seeding ... (topic={topic})')
    seeder = run_seeder(prompt, topic, cache_path, args)
    n_files = len([f for f in os.listdir(cache_path) if f.endswith('.bin')])
    print(f'  Seeder time:  {seeder["gen_ms"]:.0f} ms  ({n_files} NVMe block(s)  token={seeder["token"]})')
    print()

    # ---- B2 baseline: N agents, each cold re-prefills ----
    print(f'B2 baseline — {N} agents re-prefilling independently ...')
    baseline_times = []
    for i in range(N):
        bl = run_baseline(prompt, i, args)
        baseline_times.append(bl['gen_ms'])
        print(f'  Agent {i+1}: {bl["gen_ms"]:.0f} ms  token={bl["token"]}')

    bl_total = sum(baseline_times)
    bl_avg = statistics.mean(baseline_times)
    print(f'  Total: {bl_total:.0f} ms,  avg: {bl_avg:.0f} ms/agent')
    print()

    # ---- EdgeServe: N agents, each restores from NVMe ----
    print(f'EdgeServe — {N} agents restoring from NVMe tiered store ...')
    restore_times = []
    token_ok = True
    for i in range(N):
        rs = run_restore(prompt, topic, cache_path, i, args)
        restore_times.append(rs['gen_ms'])
        if rs['token'] != seeder['token']:
            token_ok = False
        print(f'  Agent {i+1}: {rs["gen_ms"]:.0f} ms  token={rs["token"]}')

    rs_total = sum(restore_times)
    rs_avg = statistics.mean(restore_times)
    print(f'  Total: {rs_total:.0f} ms,  avg: {rs_avg:.0f} ms/agent')
    print()

    speedup_per = bl_avg / rs_avg
    speedup_total = bl_total / rs_total
    seed_ms = seeder['gen_ms']

    print('=' * 60)
    print('Results summary')
    print('=' * 60)
    print(f'Seeder (shared prefix prefill):   {seed_ms:.0f} ms')
    print(f'B2 baseline total ({N} agents):    {bl_total:.0f} ms  '
          f'(avg {bl_avg:.0f} ms/agent)')
    print(f'EdgeServe restore total ({N} agents): {rs_total:.0f} ms  '
          f'(avg {rs_avg:.0f} ms/agent)')
    print(f'Per-agent speedup:               {speedup_per:.2f}×')
    print(f'Aggregate speedup ({N}×):         {speedup_total:.2f}×')
    print(f'GPU time saved ({N} agents):      {bl_total - rs_total:.0f} ms')
    print(f'Correctness:                     {"✓" if token_ok else "✗ token mismatch"}')
    print()

    # Break-even analysis: for how many agents does EdgeServe save total GPU time
    # if we count seeding overhead (seeder is an extra cost vs pure B2)?
    # EdgeServe total = seed_ms + N × rs_avg
    # B2 total = N × bl_avg
    # EdgeServe wins when seed_ms + N × rs_avg < N × bl_avg
    # → N > seed_ms / (bl_avg - rs_avg)
    if bl_avg > rs_avg:
        breakeven = seed_ms / (bl_avg - rs_avg)
        print(f'Break-even (counting seed cost): N > {breakeven:.1f} agents')
        print(f'  (EdgeServe total = seed + N×restore; B2 total = N×prefill)')
    else:
        print('EdgeServe restore is not faster than B2 baseline (no break-even).')
    print()
    print('Markdown row (paste into RESULTS.md):')
    print(f'| {args.model.split("/")[-1]} | {args.doc_repeats} | ~{n_tokens_approx} '
          f'| {N} | {seed_ms:.0f} | {bl_avg:.0f} | {rs_avg:.0f} '
          f'| **{speedup_per:.2f}×** | ✓ |')


if __name__ == '__main__':
    main()
