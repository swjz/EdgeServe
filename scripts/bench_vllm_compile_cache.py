"""bench_vllm_compile_cache.py — Phase E3: share vLLM compile cache fleet-wide.

Every vLLM cold start pays ~8.6 s on torch.compile + CUDA graph capture
(observed `INFO core.py:283 init engine took 8.59 seconds` in our logs).
vLLM writes deterministic artifacts to ``~/.cache/vllm/torch_compile_cache/
<cache_config_sha>/``; identical engine configurations produce bit-identical
cache directories.  EdgeServe publishes those directories through the same
bloom-catalog + exact-validation gate used for KV cache; any fleet member
with matching engine provenance can fetch the ~7 MB tarball over LAN in
<100 ms and skip compile entirely.

This bench simulates a 4-member fleet on one machine by alternating:

  1. SEED:      run vLLM once with a warm disk cache (normal vLLM behavior);
                publish the resulting compile-cache directory to EdgeServe.
  2. Cold B0:   wipe ``~/.cache/vllm/torch_compile_cache/<sha>/`` and
                run vLLM again — simulates a fresh fleet member with no
                shared cache.  Measures full cold-start time.
  3. Cold ES:   wipe the same dir, fetch the EdgeServe-published tarball,
                unpack it to the vLLM cache root, then run vLLM.  Measures
                fetch+unpack+start time.
  4. Negative:  re-fetch with a deliberately wrong gpu_arch stamp; the
                exact-validation gate must reject (correctness demo).

Usage
-----
    python scripts/bench_vllm_compile_cache.py \\
        --model Qwen/Qwen2.5-1.5B --repeats 3

    # To exercise the correctness gate alone (no live vLLM), pass
    # --correctness-only.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent


def _find_python() -> str:
    if sys.prefix != sys.base_prefix:
        return sys.executable
    venv = REPO_ROOT / '.venv' / 'bin' / 'python'
    return str(venv) if venv.is_file() else sys.executable


def _run_vllm_init(model: str, gpu_mem: float, max_len: int) -> dict:
    """Start a vLLM engine in a subprocess, run one 1-token generate(), exit.

    Returns ``{init_ms, first_gen_ms, compile_dir_name, compile_dir_size}``.

    ``init_ms`` is the wall-clock from LLM() entry to the first successful
    generate call.  We write it ourselves (from t_init) instead of parsing
    vLLM's log, because that line has moved across versions.  vLLM writes
    its compile cache during this window regardless of whether it's hot or
    cold on disk — on a cold start you see the full 8-9 s, on a warm start
    ~1-2 s.
    """
    PYTHON = _find_python()
    src = f'''\
import json, time, os, glob
from pathlib import Path
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from vllm import LLM, SamplingParams

    t0 = time.perf_counter()
    llm = LLM(
        model={model!r},
        enable_prefix_caching=False,
        gpu_memory_utilization={gpu_mem},
        max_model_len={max_len},
    )
    init_ms = (time.perf_counter() - t0) * 1000

    sp = SamplingParams(max_tokens=1, temperature=0.0)
    t1 = time.perf_counter()
    out = llm.generate(['hello world'], sampling_params=sp, use_tqdm=False)
    first_gen_ms = (time.perf_counter() - t1) * 1000

    # Find the compile-cache dir vLLM just populated.  It's the newest
    # mtime under the cache root that is NOT torch_aot_compile.
    root = Path.home() / '.cache' / 'vllm' / 'torch_compile_cache'
    newest = None
    newest_mtime = -1
    if root.is_dir():
        for d in root.iterdir():
            if d.name == 'torch_aot_compile' or not d.is_dir():
                continue
            m = d.stat().st_mtime
            if m > newest_mtime:
                newest_mtime, newest = m, d
    compile_dir_name = newest.name if newest else None
    compile_dir_size = 0
    if newest is not None:
        for p in newest.rglob('*'):
            try:
                if p.is_file():
                    compile_dir_size += p.stat().st_size
            except OSError:
                pass

    print('RESULT ' + json.dumps({{
        'init_ms': init_ms,
        'first_gen_ms': first_gen_ms,
        'compile_dir_name': compile_dir_name,
        'compile_dir_size': compile_dir_size,
    }}))
'''
    with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as f:
        f.write(src)
        path = f.name
    try:
        proc = subprocess.run(
            [PYTHON, path],
            capture_output=True, text=True, timeout=300,
        )
    finally:
        os.unlink(path)

    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            return json.loads(line[len('RESULT '):])
    tail = '\n'.join(proc.stdout.splitlines()[-30:])
    err  = '\n'.join(proc.stderr.splitlines()[-20:])
    raise RuntimeError(f'vLLM subprocess produced no RESULT.\nSTDOUT tail:\n{tail}\n\nSTDERR tail:\n{err}')


def _dir_size_mb(p: Path) -> float:
    if not p.is_dir():
        return 0.0
    total = 0
    for f in p.rglob('*'):
        try:
            if f.is_file():
                total += f.stat().st_size
        except OSError:
            pass
    return total / 1e6


def _wipe_full_compile_state() -> float:
    """Remove all vLLM compile-state that a cold-start rebuilds:
    ``torch_compile_cache/`` (all config dirs + torch_aot_compile/) and
    ``modelinfos/``.  Returns total MB freed.

    Original version of this script wiped only one per-config dir and saw
    1.01× speedup — vLLM happily reused the AOT-compiled function cache
    and the model-info JSON on cold starts.  Bundle-wipe reveals the
    full ~8 s saving that the EdgeServe bundle share actually buys.
    """
    from edgeserve.artifacts.vllm_compile import (
        DEFAULT_CACHE_ROOT, DEFAULT_MODELINFOS,
    )
    freed = 0.0
    for target in (DEFAULT_CACHE_ROOT, DEFAULT_MODELINFOS):
        if target.is_dir():
            freed += _dir_size_mb(target)
            shutil.rmtree(target)
    return freed


def _wait_gpu_free(timeout_s: int = 30) -> None:
    """Wait for any orphan vLLM EngineCore subprocesses to free GPU memory."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            out = subprocess.check_output(
                ['nvidia-smi', '--query-gpu=memory.used',
                 '--format=csv,noheader,nounits'],
                text=True,
            )
            used = int(out.strip().split('\n')[0])
            if used < 500:
                return
        except Exception:
            return
        time.sleep(1.0)
    # Best effort: kill orphan EngineCores if they're stuck.
    subprocess.run(['pkill', '-f', 'VLLM::EngineCore'], check=False)
    time.sleep(3)


# ── Experiment driver ───────────────────────────────────────────────────────

def experiment(args) -> None:
    from edgeserve.artifacts import vllm_compile as vc
    from edgeserve.semantic_cache.client import SemanticCacheClient

    client_cache = tempfile.mkdtemp(prefix='edgeserve-compile-')
    topic = f'vllm-compile-{uuid.uuid4().hex[:8]}'

    print('=' * 66)
    print('Phase E3 — vLLM compile-state fleet sharing')
    print('=' * 66)
    print(f'Model:        {args.model}')
    print(f'Repeats:      {args.repeats}')
    print(f'Topic:        {topic}')
    print(f'Client cache: {client_cache}')
    print()

    # ── Step 1 (seed): run vLLM once to populate all three cache
    # components on disk (per-config dir, AOT-compile cache, modelinfos),
    # then publish the full bundle.
    print('── Step 1: seed (run vLLM, then publish full compile-state bundle)')
    _wait_gpu_free()
    seed = _run_vllm_init(args.model, args.gpu_mem, args.max_model_len)
    print(f'   init={seed["init_ms"]:.0f} ms  '
          f'compile_dir={seed["compile_dir_name"]}  '
          f'per-config={seed["compile_dir_size"]/1e6:.1f} MB')
    if seed['compile_dir_name'] is None:
        raise RuntimeError('vLLM did not produce a compile cache dir')

    compile_sha = seed['compile_dir_name']
    tcc_mb  = _dir_size_mb(vc.DEFAULT_CACHE_ROOT)
    mi_mb   = _dir_size_mb(vc.DEFAULT_MODELINFOS)
    print(f'   on-disk: torch_compile_cache={tcc_mb:.1f} MB  '
          f'modelinfos={mi_mb:.1f} MB')
    _wait_gpu_free()

    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id='compile-seed',
        local_cache_path=client_cache,
        topic=topic,
    )
    block_uuid, blob_size, prov = vc.publish_bundle(
        client, model_id=args.model, cache_config_sha=compile_sha,
    )
    print(f'   published block_uuid={block_uuid}  blob={blob_size/1e6:.1f} MB')
    print(f'   provenance: {prov}')
    print()

    # ── Step 2 (cold B0): wipe the full compile state, run vLLM → full
    # compile path including AOT compile + modelinfos rebuild.
    b0_times = []
    print('── Step 2: cold B0 (wipe FULL compile state; vLLM rebuilds everything)')
    for i in range(args.repeats):
        freed = _wipe_full_compile_state()
        _wait_gpu_free()
        t0 = time.perf_counter()
        run = _run_vllm_init(args.model, args.gpu_mem, args.max_model_len)
        total_ms = (time.perf_counter() - t0) * 1000
        b0_times.append(run['init_ms'])
        print(f'   trial {i+1}/{args.repeats}: init={run["init_ms"]:.0f} ms  '
              f'(wipe freed {freed:.1f} MB, total subprocess {total_ms:.0f} ms)')
    print()

    # ── Step 3 (EdgeServe): wipe, fetch + unpack bundle, then run vLLM.
    es_total_times = []
    es_fetch_times = []
    es_init_times = []
    print('── Step 3: EdgeServe (wipe + fetch full bundle + unpack, then vLLM)')
    for i in range(args.repeats):
        freed = _wipe_full_compile_state()
        _wait_gpu_free()

        t_fetch = time.perf_counter()
        ok, reason, n_files = vc.fetch_bundle(
            client, model_id=args.model, cache_config_sha=compile_sha,
        )
        fetch_ms = (time.perf_counter() - t_fetch) * 1000
        if not ok:
            raise RuntimeError(f'EdgeServe fetch failed: {reason}')

        _wait_gpu_free()
        run = _run_vllm_init(args.model, args.gpu_mem, args.max_model_len)
        es_fetch_times.append(fetch_ms)
        es_init_times.append(run['init_ms'])
        es_total_times.append(fetch_ms + run['init_ms'])
        print(f'   trial {i+1}/{args.repeats}: '
              f'fetch={fetch_ms:.0f} ms ({n_files} files)  '
              f'init={run["init_ms"]:.0f} ms  '
              f'total={fetch_ms + run["init_ms"]:.0f} ms')
    print()

    # ── Step 4 (correctness demo): wipe, try to fetch with the wrong
    # gpu_arch stamp — must be rejected.
    print('── Step 4: correctness — heterogeneous fleet (wrong gpu_arch)')
    _wipe_full_compile_state()
    wrong_arch = 'sm89' if prov['model_version'].endswith('sm86') else 'sm86'
    ok_wrong, reason_wrong, _ = vc.fetch_bundle(
        client, model_id=args.model, cache_config_sha=compile_sha,
        gpu_arch=wrong_arch,
    )
    wrong_gate_ok = (not ok_wrong)
    print(f'   wrong-arch probe (gpu_arch={wrong_arch}): '
          f'{"REJECTED ✓" if wrong_gate_ok else "ADMITTED ✗ (BUG)"}  '
          f'reason={reason_wrong}')

    ok_model, reason_model, _ = vc.fetch_bundle(
        client, model_id='meta-llama/Llama-3-8B',
        cache_config_sha=compile_sha,
    )
    model_gate_ok = (not ok_model)
    print(f'   wrong-model probe (Llama-3-8B): '
          f'{"REJECTED ✓" if model_gate_ok else "ADMITTED ✗ (BUG)"}  '
          f'reason={reason_model}')
    print()

    # Cleanup final: one successful fetch so the box exits with a warm cache.
    vc.fetch_bundle(
        client, model_id=args.model, cache_config_sha=compile_sha,
    )

    import statistics
    b0_median = statistics.median(b0_times)
    es_total_median = statistics.median(es_total_times)
    es_fetch_median = statistics.median(es_fetch_times)
    es_init_median  = statistics.median(es_init_times)

    print('=' * 66)
    print('Results summary')
    print('=' * 66)
    print(f'Blob size (compile cache tarball):    {blob_size/1e6:>6.1f} MB')
    print(f'B0 cold vLLM init (median of {args.repeats}):     {b0_median:>7.0f} ms')
    print(f'EdgeServe fetch (median):             {es_fetch_median:>7.0f} ms')
    print(f'EdgeServe vLLM init after fetch:      {es_init_median:>7.0f} ms')
    print(f'EdgeServe total (fetch + init):       {es_total_median:>7.0f} ms')
    print(f'Init speedup (B0 init / ES init):     {b0_median/es_init_median:>7.2f}×')
    print(f'Total speedup (B0 init / ES total):   {b0_median/es_total_median:>7.2f}×')
    print()
    print(f'Correctness (wrong gpu_arch rejected): '
          f'{"✓" if wrong_gate_ok else "✗"}')
    print(f'Correctness (wrong model rejected):    '
          f'{"✓" if model_gate_ok else "✗"}')
    print()
    print('Markdown row (paste into RESULTS.md §E3):')
    print(f'| {args.model.split("/")[-1]} '
          f'| {blob_size/1e6:.1f} MB '
          f'| {b0_median:.0f} | {es_fetch_median:.0f} | {es_init_median:.0f} '
          f'| {es_total_median:.0f} '
          f'| **{b0_median/es_total_median:.2f}×** '
          f'| {"✓" if wrong_gate_ok and model_gate_ok else "✗"} |')

    client.close()
    shutil.rmtree(client_cache, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=4096)
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    ap.add_argument('--repeats', type=int, default=3,
                    help='Number of independent trials per condition')
    args = ap.parse_args()
    experiment(args)


if __name__ == '__main__':
    main()
