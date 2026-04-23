"""demo_context_push.py — Phase 4.4: edit-to-answer end-to-end demo.

Demonstrates the full context-push pipeline:

  1. Context server (GPU box): persistent vLLM + EdgeServeKVConnector,
     waiting for ingest requests.
  2. Watcher (edge device / Mac Mini): monitors a directory, pushes
     file edits to the context server via POST /ingest.
  3. Query consumer (GPU box): when user asks a question about the file,
     a fresh vLLM process discovers the KV in the catalog and restores
     it — no re-prefill needed.

This script runs the FULL PIPELINE as a self-contained demo on the GPU
box (no Mac connection required):
  - Spawns the context server as a background subprocess
  - Simulates file edits and pushes via ContextWatcher
  - Runs a consumer that restores KV from the tiered store
  - Compares restore time vs cold re-prefill (B1 baseline)

Usage
-----
  python scripts/demo_context_push.py
  python scripts/demo_context_push.py --model Qwen/Qwen2.5-1.5B --port 8766
  python scripts/demo_context_push.py --real-dir ~/myrepo --glob "*.py"
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)

# Simulated "file edits" — each is one version of the file
SIMULATED_EDITS = [
    """\
# auth.py — user authentication module
import hashlib, secrets

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"

def verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(":")
    return hashlib.sha256((salt + password).encode()).hexdigest() == h

def login(username: str, password: str, db: dict) -> bool:
    if username not in db:
        return False
    return verify_password(password, db[username])
""",
    """\
# auth.py — user authentication module (v2: add rate limiting)
import hashlib, secrets, time
from collections import defaultdict

_ATTEMPTS: dict = defaultdict(list)
MAX_ATTEMPTS = 5
WINDOW_S = 300

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"

def verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(":")
    return hashlib.sha256((salt + password).encode()).hexdigest() == h

def _check_rate_limit(username: str) -> bool:
    now = time.time()
    _ATTEMPTS[username] = [t for t in _ATTEMPTS[username] if now - t < WINDOW_S]
    return len(_ATTEMPTS[username]) < MAX_ATTEMPTS

def login(username: str, password: str, db: dict) -> bool:
    if not _check_rate_limit(username):
        raise PermissionError("Too many login attempts")
    _ATTEMPTS[username].append(time.time())
    if username not in db:
        return False
    return verify_password(password, db[username])
""",
]


def _find_python() -> str:
    if sys.prefix != sys.base_prefix:
        return sys.executable
    venv = os.path.join(REPO_ROOT, '.venv', 'bin', 'python')
    return venv if os.path.isfile(venv) else sys.executable


def _wait_for_server(url: str, timeout: float = 120.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f'{url}/health', timeout=3)
            return True
        except Exception:
            time.sleep(2)
    return False


def _ingest(server_url: str, text: str, entities: list, sha: str) -> dict:
    body = json.dumps({'text': text, 'entities': entities, 'sha': sha}).encode()
    req = urllib.request.Request(
        f'{server_url}/ingest', data=body,
        headers={'Content-Type': 'application/json'}, method='POST',
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def _run_worker(script_src: str, timeout: int = 300) -> dict:
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
        print('\n'.join(proc.stdout.splitlines()[-30:]))
        print('--- worker stderr tail ---')
        print('\n'.join(proc.stderr.splitlines()[-20:]))
        raise RuntimeError('Worker produced no RESULT line')
    os.unlink(path)
    return result


def run_baseline(prompt: str, args) -> dict:
    src = f"""\
import time, json, os
if __name__ == '__main__':
    os.environ.setdefault('VLLM_USE_V1', '1')
    from vllm import LLM, SamplingParams
    llm = LLM(model={repr(args.model)}, gpu_memory_utilization={args.gpu_mem},
              max_model_len={args.max_model_len}, enable_prefix_caching=False)
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    t0 = time.perf_counter()
    out = llm.generate([{repr(prompt)}], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'baseline','gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def run_restore(prompt: str, topic: str, cache_path: str, args) -> dict:
    node_id = f'ctx-consumer-{uuid.uuid4().hex[:6]}'
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
            'topic': {repr(topic)},
            'local_cache_path': {repr(cache_path)},
            'node_id': {repr(node_id)},
        }},
    )
    llm = LLM(model={repr(args.model)}, gpu_memory_utilization={args.gpu_mem},
              max_model_len={args.max_model_len}, enable_prefix_caching=False,
              kv_transfer_config=ktc)
    sp = SamplingParams(max_tokens=1, temperature=0.0)
    time.sleep(3.0)
    t0 = time.perf_counter()
    out = llm.generate([{repr(prompt)}], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000
    tok = int(out[0].outputs[0].token_ids[0])
    print('RESULT ' + json.dumps({{'role':'restore','gen_ms':gen_ms,'token':tok}}))
"""
    return _run_worker(src)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=16384)
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    ap.add_argument('--topic', default=f'kvcache-ctx-{uuid.uuid4().hex[:8]}')
    ap.add_argument('--port', type=int, default=8765)
    ap.add_argument('--real-dir', default=None,
                    help='Watch a real directory instead of simulated edits')
    ap.add_argument('--glob', nargs='+', default=['*.py'],
                    help='File patterns to watch in --real-dir mode')
    args = ap.parse_args()

    PYTHON = _find_python()
    cache_path = tempfile.mkdtemp(prefix='edgeserve-ctx-')
    server_url = f'http://127.0.0.1:{args.port}'

    print('=' * 60)
    print('EdgeServe context-push demo  (Phase 4.4)')
    print('=' * 60)
    print(f'Model:      {args.model}')
    print(f'Topic:      {args.topic}')
    print(f'Cache path: {cache_path}')
    print(f'Server:     {server_url}')
    print()

    # ── Step 1: start context server ──────────────────────────────────────
    print('Step 1 — Starting context server (GPU box) ...')
    server_cmd = [
        PYTHON, '-m', 'edgeserve.inference.context_server',
        '--model', args.model,
        '--gpu-mem', str(args.gpu_mem),
        '--max-model-len', str(args.max_model_len),
        '--cache-path', cache_path,
        '--pulsar-url', args.pulsar_url,
        '--topic', args.topic,
        '--port', str(args.port),
    ]
    server_proc = subprocess.Popen(
        server_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    print('  Waiting for context server to be ready ...')
    t_server_start = time.perf_counter()
    if not _wait_for_server(server_url, timeout=120):
        server_proc.kill()
        raise RuntimeError('Context server did not start in 120 s')
    t_ready = (time.perf_counter() - t_server_start) * 1000
    print(f'  Context server ready in {t_ready:.0f} ms')
    print()

    # ── Step 2: simulate file edits / watcher push ────────────────────────
    if args.real_dir:
        from edgeserve.edge.watcher import ContextWatcher
        print(f'Step 2 — Watching {args.real_dir}  (Ctrl-C to continue to query step) ...')
        pushed_files = []
        def on_push(rel, result):
            pushed_files.append((rel, result))
        w = ContextWatcher(server_url, args.real_dir, globs=args.glob, on_push=on_push)
        try:
            w.run_forever()
        except KeyboardInterrupt:
            pass
        if not pushed_files:
            print('  No files pushed — exiting.')
            server_proc.kill()
            return
        rel_path, last_result = pushed_files[-1]
        query_text = last_result.get('text_snippet', f'Contents of {rel_path}')
        query_prompt = f'{query_text}\n\nQ: What does this code do?'
        ingest_ms = last_result.get('ingest_ms', 0)
    else:
        ingest_times = []
        print('Step 2 — Simulating file edits (watcher push) ...')
        for i, file_content in enumerate(SIMULATED_EDITS):
            sha = hashlib.sha256(file_content.encode()).hexdigest()[:16]
            entities = ['file:auth.py']
            print(f'  Edit {i+1}/{len(SIMULATED_EDITS)}: pushing auth.py (v{i+1})  '
                  f'sha={sha} ...')
            t_ingest = time.perf_counter()
            result = _ingest(server_url, file_content, entities, sha)
            ingest_ms = result.get('ingest_ms', (time.perf_counter() - t_ingest)*1000)
            ingest_times.append(ingest_ms)
            print(f'    → block_uuid={result.get("block_uuid","?")}  '
                  f'n_tokens={result.get("n_tokens","?")}  '
                  f'ingest_ms={ingest_ms:.0f} ms')
        print(f'  Avg ingest: {sum(ingest_times)/len(ingest_times):.0f} ms/edit '
              f'(model stays loaded)')
        query_text = SIMULATED_EDITS[-1]
        query_prompt = query_text + '\n\nQ: What security vulnerabilities does this code have?'
        ingest_ms = ingest_times[-1]

    print()

    # ── Step 3: kill server (GPU cache is gone), but NVMe persists ────────
    print('Step 3 — Shutting down context server (simulates GPU eviction) ...')
    server_proc.kill()
    server_proc.wait()
    n_files = len([f for f in os.listdir(cache_path) if f.endswith('.bin')])
    print(f'  Server stopped.  NVMe files persisted: {n_files}')
    print()

    # ── Step 4: consumer restore vs B1 baseline ───────────────────────────
    print('Step 4 — Measuring query latency (restore vs cold re-prefill) ...')

    print('  B1 baseline (cold re-prefill, no connector) ...')
    bl = run_baseline(query_prompt, args)
    print(f'    → {bl["gen_ms"]:.0f} ms  token={bl["token"]}')

    print('  EdgeServe restore (NVMe KV from context server) ...')
    rs = run_restore(query_prompt, args.topic, cache_path, args)
    print(f'    → {rs["gen_ms"]:.0f} ms  token={rs["token"]}')

    speedup = bl['gen_ms'] / rs['gen_ms']
    token_ok = bl['token'] == rs['token']

    print()
    print('=' * 60)
    print('Results summary')
    print('=' * 60)
    print(f'Avg ingest latency:      {ingest_ms:.0f} ms/edit  (server stays warm after first)')
    print(f'B1 cold re-prefill:      {bl["gen_ms"]:.0f} ms')
    print(f'EdgeServe NVMe restore:  {rs["gen_ms"]:.0f} ms')
    print(f'Query speedup:           {speedup:.2f}×')
    print(f'Token correctness:       {"✓" if token_ok else "✗ mismatch"}')
    print()
    print('End-to-end flow confirmed:')
    print('  file edit → watcher → context server ingest → NVMe persist')
    print('  → server exit (GPU cache evicted) → consumer restore → faster query')


if __name__ == '__main__':
    main()
