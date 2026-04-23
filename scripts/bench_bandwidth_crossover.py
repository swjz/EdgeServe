"""bench_bandwidth_crossover.py — Phase 2.2: bandwidth-vs-recompute crossover sweep.

Measures how LAN HTTP fetch time scales with blob size (KV cache size) compared
to GPU prefill time, to find the crossover point where fetching beats recomputing.

Mathematical crossover for Qwen2.5-1.5B on 3080 Ti:
  prefill throughput ≈ 6122 tok/s  (from Phase 2.1 measurements: 1.38 s for 8448 tok)
  blob size ≈ 27.8 KB/token (234.9 MB / 8448 tokens)
  crossover network speed = 27.8 KB/tok × 6122 tok/s ≈ 170 MB/s ≈ 1.36 Gbps

Usage
-----
Run on GPU box (seeder), one doc-repeats value at a time:

  # Sweep all sizes (takes ~30 min total):
  for R in 16 32 64 128 256; do
    python scripts/bench_bandwidth_crossover.py seed --doc-repeats $R --gpu-mem 0.4
  done

Then on Mac Mini (consumer), for each seed run:

  python scripts/bench_bandwidth_crossover.py consume \\
      --pulsar-url pulsar://192.168.1.214:6650 \\
      --topic kvcache-xover-XXXX \\           # printed by seeder
      --model Qwen/Qwen2.5-1.5B \\
      --doc-repeats 256 \\                    # match seeder
      --repeats 5

Or run the full sweep on a single machine (same-host, mmap path) with --same-host:

  python scripts/bench_bandwidth_crossover.py sweep \\
      --model Qwen/Qwen2.5-1.5B \\
      --doc-repeats 16 32 64 128 256 \\
      --gpu-mem 0.4

Output
------
Prints a markdown table row for each (doc-repeats, tokens, blob_mb, prefill_s,
fetch_s, crossover_ratio). Append with >> to build up RESULTS.md.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import statistics
import sys
import tempfile
import time
import uuid

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society. "
)


def _find_python() -> str:
    if sys.prefix != sys.base_prefix:
        return sys.executable
    venv = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        '.venv', 'bin', 'python')
    return venv if os.path.isfile(venv) else sys.executable


def _compute_prefix_hashes(model: str, doc: str, block_size: int = 16) -> list[str]:
    import torch
    from transformers import AutoTokenizer
    tok = AutoTokenizer.from_pretrained(model)
    token_ids = tok.encode(doc, add_special_tokens=False)
    n = len(token_ids)
    boundaries = list(range(block_size, n + 1, block_size))
    hashes = []
    for b in reversed(boundaries):
        arr = torch.tensor(token_ids[:b], dtype=torch.long).numpy().tobytes()
        hashes.append(hashlib.sha256(arr).hexdigest())
    return hashes, n


def cmd_seed(args):
    """Run on GPU box: seed one doc-repeats value, print topic + timing."""
    import subprocess

    PYTHON = _find_python()
    doc = DOC_CHUNK * args.doc_repeats
    topic = f"kvcache-xover-{uuid.uuid4().hex[:8]}"
    cache_path = tempfile.mkdtemp(prefix="edgeserve-xover-seed-")

    prompt = doc + " Summarise the key points."
    print(f"doc-repeats: {args.doc_repeats}")
    print(f"topic:       {topic}")
    print(f"cache:       {cache_path}")
    print()

    seed_script = f"""\
import time, sys, os
from vllm import LLM, SamplingParams
from vllm.config import KVTransferConfig
from edgeserve.inference.vllm_kv_connector import register
register()

if __name__ == '__main__':
    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={{
            'pulsar_url':       {repr(args.pulsar_url)},
            'topic':            {repr(topic)},
            'local_cache_path': {repr(cache_path)},
            'node_id':          'xover-seeder',
        }},
    )
    llm = LLM(
        model={repr(args.model)},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
        gpu_memory_utilization={args.gpu_mem},
        max_model_len=16384,
    )
    prompt = {repr(prompt)}
    t0 = time.perf_counter()
    out = llm.generate([prompt], SamplingParams(max_tokens=1, temperature=0))
    prefill_ms = (time.perf_counter() - t0) * 1000
    tok_id = out[0].outputs[0].token_ids[0]
    print(f'[seeder] prefill={{prefill_ms:.1f}}ms  token={{tok_id}}  doc_repeats={args.doc_repeats}', flush=True)
    print(f'[seeder] topic={repr(topic)}', flush=True)
    # Read back block_uuid + node_uri
    import pulsar as _pulsar, uuid as _uuid, msgpack as _mp
    _pc = _pulsar.Client({repr(args.pulsar_url)})
    _c = _pc.subscribe(
        {repr(topic)}, 'xover-rb-' + _uuid.uuid4().hex[:8],
        consumer_type=_pulsar.ConsumerType.Exclusive,
        initial_position=_pulsar.InitialPosition.Earliest,
    )
    try:
        _m = _c.receive(timeout_millis=5000)
        _d = _mp.unpackb(_m.data(), raw=False)
        _buuid = str(_uuid.UUID(bytes=_d['block_uuid']))
        _nuri = _d.get('node_uri', '?')
        print(f'[seeder] block_uuid={{_buuid}}', flush=True)
        print(f'[seeder] node_uri={{_nuri}}', flush=True)
    except Exception as _e:
        print(f'[seeder] readback error: {{_e}}', flush=True)
    finally:
        _c.close(); _pc.close()
    print('[seeder] KV published — HTTP server alive, ^C to stop', flush=True)
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
"""
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(seed_script)
        path = f.name

    subprocess.run([PYTHON, path])


def cmd_consume(args):
    """Run on consumer: discover KV via prefix hash, measure HTTP fetch times."""
    from edgeserve.semantic_cache.client import SemanticCacheClient

    node_id = f"xover-consumer-{uuid.uuid4().hex[:12]}"
    cache_path = tempfile.mkdtemp(prefix="edgeserve-xover-consume-")

    doc = DOC_CHUNK * args.doc_repeats
    prompt = doc + " Summarise the key points."

    print(f"Connecting to {args.pulsar_url} ...")
    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id=node_id,
        local_cache_path=cache_path,
        topic=args.topic,
        ttl_ms=30 * 60 * 1000,
    )

    if args.block_uuid and args.node_uri:
        from edgeserve.semantic_cache.header import CacheHeader
        import uuid as _uuid
        header = CacheHeader(
            block_uuid=_uuid.UUID(args.block_uuid),
            node_uri=args.node_uri,
            prefix_hash=b'',
            bloom=None,  # type: ignore[arg-type]
            num_tokens=0,
        )
        client.close()
        print(f"Direct fetch: {args.node_uri}/cache/{args.block_uuid}")
    else:
        print(f"Computing prefix hashes for doc-repeats={args.doc_repeats} ...")
        probe_hashes, n_tokens = _compute_prefix_hashes(args.model, prompt)
        print(f"  {n_tokens} tokens → {len(probe_hashes)} boundary hashes to probe")

        print(f"Waiting for header on topic {args.topic!r} (up to {args.wait}s) ...")
        deadline = time.time() + args.wait
        header = None
        while time.time() < deadline:
            for h in probe_hashes:
                hits = list(client.catalog.lookup([h]))
                if hits:
                    header = hits[0]
                    break
            if header:
                break
            time.sleep(1.0)
        client.close()

        if header is None:
            print(f"[ERROR] no header found within {args.wait}s")
            return

    print(f"Header: block_uuid={header.block_uuid}  node_uri={header.node_uri}")

    from edgeserve.semantic_cache.http_client import http_fetch

    print("Warmup fetch ...")
    try:
        data = http_fetch(header.node_uri, header.block_uuid, timeout=120.0)
        blob_mb = len(data) / 1e6
        print(f"  warmup: {blob_mb:.1f} MB")
    except Exception as e:
        print(f"[WARN] warmup failed: {e}")
        blob_mb = 0.0

    times = []
    for i in range(args.repeats):
        t0 = time.perf_counter()
        try:
            data = http_fetch(header.node_uri, header.block_uuid, timeout=120.0)
            elapsed = (time.perf_counter() - t0) * 1000
            blob_mb = len(data) / 1e6
            bps = blob_mb * 8000 / elapsed
            times.append(elapsed)
            print(f"  fetch {i+1}/{args.repeats}: {elapsed:.1f}ms  {blob_mb:.1f}MB  {bps:.0f}Mbps")
        except Exception as e:
            print(f"  fetch {i+1}/{args.repeats}: FAILED — {e}")

    if times:
        med = statistics.median(times)
        print(f"\n--- doc-repeats={args.doc_repeats} ---")
        print(f"Blob:       {blob_mb:.1f} MB")
        print(f"Median:     {med:.1f} ms")
        print(f"Throughput: {blob_mb * 8000 / med:.0f} Mbps (median)")
        print()
        print("Markdown row (paste into RESULTS.md):")
        print(f"| {args.doc_repeats} | ~{n_tokens if 'n_tokens' in dir() else '?'} | "
              f"{blob_mb:.1f} | {med/1000:.2f} | ? (seeder) | "
              f"{blob_mb * 8000 / med:.0f} |")


def cmd_sweep(args):
    """Same-host sweep: run seeder + consumer in the same process to measure both sides."""
    import subprocess

    PYTHON = _find_python()
    results = []

    for doc_repeats in args.doc_repeats:
        print(f"\n{'='*60}")
        print(f"doc-repeats={doc_repeats}")
        print(f"{'='*60}")

        doc = DOC_CHUNK * doc_repeats
        prompt = doc + " Summarise the key points."
        topic = f"kvcache-sweep-{uuid.uuid4().hex[:8]}"
        cache_path = tempfile.mkdtemp(prefix=f"edgeserve-sweep-{doc_repeats}-")

        sweep_script = f"""\
import time, sys, os, statistics, tempfile, uuid
from vllm import LLM, SamplingParams
from vllm.config import KVTransferConfig
from edgeserve.inference.vllm_kv_connector import register
register()

if __name__ == '__main__':
    cache = {repr(cache_path)}
    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={{
            'pulsar_url':       {repr(args.pulsar_url)},
            'topic':            {repr(topic)},
            'local_cache_path': cache,
            'node_id':          'sweep-seeder',
        }},
    )
    llm = LLM(
        model={repr(args.model)},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
        gpu_memory_utilization={args.gpu_mem},
        max_model_len=16384,
    )
    prompt = {repr(prompt)}

    # Seeder run
    t0 = time.perf_counter()
    out = llm.generate([prompt], SamplingParams(max_tokens=1, temperature=0))
    prefill_ms = (time.perf_counter() - t0) * 1000
    print(f'PREFILL_MS={{prefill_ms:.1f}}', flush=True)

    # Read back block_uuid + node_uri
    import pulsar as _pulsar, uuid as _uuid, msgpack as _mp
    _pc = _pulsar.Client({repr(args.pulsar_url)})
    _c = _pc.subscribe(
        {repr(topic)}, 'sweep-rb-' + _uuid.uuid4().hex[:8],
        consumer_type=_pulsar.ConsumerType.Exclusive,
        initial_position=_pulsar.InitialPosition.Earliest,
    )
    try:
        _m = _c.receive(timeout_millis=5000)
        _d = _mp.unpackb(_m.data(), raw=False)
        _buuid = str(_uuid.UUID(bytes=_d['block_uuid']))
        _nuri = _d.get('node_uri', '?')
        print(f'BLOCK_UUID={{_buuid}}', flush=True)
        print(f'NODE_URI={{_nuri}}', flush=True)
    finally:
        _c.close(); _pc.close()

    # Measure same-host mmap fetch (via HTTP server)
    from edgeserve.semantic_cache.http_client import http_fetch
    import uuid as _uuid2
    _bu = _uuid2.UUID(_buuid)

    # warmup
    try:
        _data = http_fetch(_nuri, _bu, timeout=30.0)
    except Exception as _e:
        print(f'WARMUP_FAIL={{_e}}', flush=True)

    _times = []
    for _ in range({args.repeats}):
        _t0 = time.perf_counter()
        _data = http_fetch(_nuri, _bu, timeout=60.0)
        _times.append((time.perf_counter() - _t0) * 1000)

    _med = statistics.median(_times)
    _blob_mb = len(_data) / 1e6
    print(f'FETCH_MS={{_med:.1f}}', flush=True)
    print(f'BLOB_MB={{_blob_mb:.2f}}', flush=True)
    print(f'THROUGHPUT_MBPS={{_blob_mb * 8000 / _med:.0f}}', flush=True)
"""
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            f.write(sweep_script)
            path = f.name

        result = subprocess.run([PYTHON, path], capture_output=True, text=True)
        output = result.stdout + result.stderr

        prefill_ms = fetch_ms = blob_mb = None
        for line in output.splitlines():
            if line.startswith("PREFILL_MS="):
                prefill_ms = float(line.split("=")[1])
            elif line.startswith("FETCH_MS="):
                fetch_ms = float(line.split("=")[1])
            elif line.startswith("BLOB_MB="):
                blob_mb = float(line.split("=")[1])

        if prefill_ms and fetch_ms and blob_mb:
            ratio = fetch_ms / prefill_ms
            n_approx = int(blob_mb * 1e6 / 27800)  # ~27.8 KB/token for Qwen2.5-1.5B bf16
            tput = blob_mb * 8000 / fetch_ms
            results.append((doc_repeats, n_approx, blob_mb, prefill_ms, fetch_ms, ratio, tput))
            print(f"  prefill={prefill_ms:.0f}ms  fetch={fetch_ms:.0f}ms  "
                  f"ratio={ratio:.2f}x  blob={blob_mb:.1f}MB  throughput={tput:.0f}Mbps")
        else:
            print(f"  [ERROR] could not parse output")
            print(output[-2000:])

    print()
    print("## Phase 2.2 — bandwidth-vs-recompute crossover (same-host mmap)")
    print()
    print("| doc-repeats | ~tokens | blob MB | prefill s | fetch s | ratio | throughput Mbps |")
    print("|---|---|---|---|---|---|---|")
    for r in results:
        dr, n, mb, pm, fm, ratio, tput = r
        print(f"| {dr} | ~{n} | {mb:.1f} | {pm/1000:.2f} | {fm/1000:.2f} | {ratio:.2f}× | {tput:.0f} |")


def main():
    ap = argparse.ArgumentParser(description="Phase 2.2 crossover benchmark")
    sub = ap.add_subparsers(dest='cmd', required=True)

    sp = sub.add_parser('seed', help='Run seeder on GPU box')
    sp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    sp.add_argument('--doc-repeats', type=int, default=128)
    sp.add_argument('--gpu-mem', type=float, default=0.4)
    sp.add_argument('--pulsar-url', default='pulsar://localhost:6650')

    cp = sub.add_parser('consume', help='Run consumer (remote or same host)')
    cp.add_argument('--pulsar-url', required=True)
    cp.add_argument('--topic', required=True)
    cp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    cp.add_argument('--doc-repeats', type=int, default=128)
    cp.add_argument('--block-uuid', default='')
    cp.add_argument('--node-uri', default='')
    cp.add_argument('--wait', type=float, default=120.0)
    cp.add_argument('--repeats', type=int, default=5)

    sw = sub.add_parser('sweep', help='Same-host sweep across doc-repeats values')
    sw.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    sw.add_argument('--doc-repeats', type=int, nargs='+', default=[16, 32, 64, 128, 256])
    sw.add_argument('--gpu-mem', type=float, default=0.4)
    sw.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    sw.add_argument('--repeats', type=int, default=3)

    args = ap.parse_args()
    {'seed': cmd_seed, 'consume': cmd_consume, 'sweep': cmd_sweep}[args.cmd](args)


if __name__ == '__main__':
    main()
