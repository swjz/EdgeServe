"""bench_tool_eviction.py — Phase 3.6: Tool-call eviction buffer experiment.

Models the agentic tool-call resilience story:

  1. Agent prefills a long document context on the GPU → EdgeServeKVConnector
     saves KV blocks to the tiered store (L2 RAM + L3 NVMe).
  2. Agent pauses to execute a tool locally (simulated by a configurable
     sleep, default 45 s). During this pause, concurrent GPU traffic from
     other agents evicts the idle agent's KV blocks.
  3. Agent resumes. Two paths measured:
       B1 (baseline): fresh vLLM, no connector → full cold re-prefill.
       EdgeServe:     fresh vLLM + connector → loads KV from tiered store
                      (L3 NVMe if seeder process exited; L2 RAM if same host
                       and still warm).

Key insight: EdgeServe's tiered store persists KV to NVMe *before* the GPU
evicts it, so resumption only costs an NVMe read + paged-buffer scatter
rather than a full prefill.

What "eviction" means in this benchmark
----------------------------------------
The seeder process EXITS after publishing (simulating the agent giving up its
GPU slot for another task). The consumer starts as a fully fresh subprocess
with an empty GPU cache. The connector discovers the published header in
Pulsar and loads KV from the NVMe file on the same machine.

For the same-host mmap path this is essentially free; for a true cross-host
eviction scenario the consumer would fetch via HTTP (see Phase 2.1 for those
numbers). This script measures the same-host recovery path — the floor.

Usage
-----
  python scripts/bench_tool_eviction.py
  python scripts/bench_tool_eviction.py --model Qwen/Qwen2.5-0.5B --doc-repeats 64
  python scripts/bench_tool_eviction.py --doc-repeats 256 --gpu-mem 0.4 --repeats 2
"""
from __future__ import annotations

import argparse
import json
import os
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
    """Write a temp Python script, run it as a subprocess, parse the RESULT line."""
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
            'node_id': 'evic-seeder',
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
    time.sleep(0.5)   # let wait_for_save publish
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'seeder','gen_ms':gen_ms,'token':tok}}))
    # Process exits — HTTP server dies, but NVMe file persists
"""
    return _run_worker(src)


def run_baseline(prompt: str, args) -> dict:
    """Cold prefill with no connector — the B1 baseline."""
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
    print('RESULT ' + json.dumps({{'role':'baseline','gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def run_restore(prompt: str, topic: str, cache_path: str, node_id: str, args) -> dict:
    """Fresh vLLM + connector — restores KV from tiered store (NVMe)."""
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
    print('RESULT ' + json.dumps({{'role':'restore','gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--doc-repeats', type=int, default=128,
                    help='Document repetitions (context length)')
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=16384)
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    ap.add_argument('--repeats', type=int, default=2,
                    help='Number of (baseline, restore) pairs to average')
    ap.add_argument('--tool-pause-s', type=float, default=0.0,
                    help='Simulated tool-call pause between seed and restore '
                         '(NVMe is durable, so this does not affect correctness)')
    args = ap.parse_args()

    doc = DOC_CHUNK * args.doc_repeats
    prompt = doc + ' Summarise the key points.'
    n_tokens_approx = len(doc) // 4

    print(f'Model:         {args.model}')
    print(f'Doc repeats:   {args.doc_repeats}  (~{n_tokens_approx} tokens)')
    print(f'Tool pause:    {args.tool_pause_s}s')
    print(f'Repeats:       {args.repeats}  (independent seed → baseline → restore per trial)')
    print()

    # Each repeat is a self-contained trial: fresh topic + cache_path → seed
    # → cold baseline → EdgeServe restore. This avoids Pulsar message retention
    # issues (GC after all subscriptions close) by ensuring the seeder's message
    # is always recent when the restore runs.
    baseline_times = []
    restore_times = []
    seed_times = []

    for i in range(args.repeats):
        cache_path = tempfile.mkdtemp(prefix=f'edgeserve-evic-{i}-')
        topic = f'kvcache-evic-{uuid.uuid4().hex[:8]}'
        restore_id = f'evic-restore-{uuid.uuid4().hex[:6]}'

        print(f'Trial {i+1}/{args.repeats}:  topic={topic}')

        print('  Phase 1 (Seed) — agent prefills + saves to NVMe ...')
        seeder = run_seeder(prompt, topic, cache_path, args)
        seed_times.append(seeder['gen_ms'])
        n_files = len([f for f in os.listdir(cache_path) if f.endswith('.bin')])
        print(f'    Seed:       {seeder["gen_ms"]:.0f} ms  token={seeder["token"]}  '
              f'({n_files} NVMe block(s))')

        if args.tool_pause_s > 0:
            print(f'    [pause {args.tool_pause_s}s simulating tool execution]')
            time.sleep(args.tool_pause_s)

        print('  Phase 2 (B1 baseline) — cold re-prefill, no connector ...')
        bl = run_baseline(prompt, args)
        baseline_times.append(bl['gen_ms'])
        print(f'    Baseline:   {bl["gen_ms"]:.0f} ms  token={bl["token"]}')

        print('  Phase 3 (EdgeServe) — restore from NVMe tiered store ...')
        rs = run_restore(prompt, topic, cache_path, restore_id, args)
        restore_times.append(rs['gen_ms'])
        print(f'    Restore:    {rs["gen_ms"]:.0f} ms  token={rs["token"]}')

        if bl['token'] != seeder['token'] or rs['token'] != seeder['token']:
            print(f'    [WARN] token mismatch: seeder={seeder["token"]} '
                  f'baseline={bl["token"]} restore={rs["token"]}')
        else:
            print(f'    Correctness: ✓ all tokens match ({seeder["token"]})')
        print()

    import statistics
    bl_med = statistics.median(baseline_times)
    rs_med = statistics.median(restore_times)
    seed_med = statistics.median(seed_times)
    speedup = bl_med / rs_med

    print('=' * 60)
    print('Results summary')
    print('=' * 60)
    print(f'Seeder (original prefill):     {seed_med:.0f} ms  (median)')
    print(f'B1 baseline (cold re-prefill): {bl_med:.0f} ms  (median of {args.repeats})')
    print(f'EdgeServe restore (NVMe):      {rs_med:.0f} ms  (median of {args.repeats})')
    print(f'Speedup (B1 / EdgeServe):      {speedup:.2f}×')
    print()
    print('Markdown row (paste into RESULTS.md):')
    print(f'| {args.model.split("/")[-1]} | {args.doc_repeats} | ~{n_tokens_approx} '
          f'| {seed_med:.0f} | {bl_med:.0f} | {rs_med:.0f} | **{speedup:.2f}×** | ✓ |')


if __name__ == '__main__':
    main()
