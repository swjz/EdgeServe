"""bench_tier_hit_rates.py — Phase 3.5: Zipfian tier hit-rate benchmark.

Simulates a realistic KV-cache workload with Zipfian document popularity
(most requests hit a small fraction of popular documents) and measures
L2/L3 hit rates at various L2 capacity settings.

The Zipfian distribution models real RAG / agentic workloads well: a few
"hot" documents (API reference, codebase root, long system prompt) get
hit repeatedly while a long tail of cold documents are rarely reused.

Experiment design
-----------------
- N_DOCS distinct documents, blob_mb MB each.
- Zipfian access sequence of N_ACCESSES requests drawn from Zipf(s=1.0).
- Three L2 capacity configurations:
    tiny  (≈ 5% of working set)
    small (≈ 20%)
    large (≈ 50%)
- L3 unbounded (no eviction).
- Baseline: single-tier L3-only (l2_max_bytes=0) — every get hits disk.

Metrics recorded per config:
  L2 hit rate, L3 hit rate, miss rate (=0 when L3 unbounded),
  median get latency for L2 hits vs L3 hits.

Usage
-----
  python scripts/bench_tier_hit_rates.py
  python scripts/bench_tier_hit_rates.py --docs 200 --accesses 5000 --blob-kb 512
"""
from __future__ import annotations

import argparse
import os
import random
import statistics
import tempfile
import time
import uuid

from edgeserve.semantic_cache.tiered_store import TieredStore


# ---------------------------------------------------------------------------
# Zipfian sampler
# ---------------------------------------------------------------------------

def zipf_samples(n_docs: int, n_samples: int, s: float = 1.0, seed: int = 42) -> list[int]:
    """Return n_samples document indices drawn from Zipf(s) over [0, n_docs)."""
    rng = random.Random(seed)
    # Precompute harmonic weights
    weights = [1.0 / (i + 1) ** s for i in range(n_docs)]
    total = sum(weights)
    probs = [w / total for w in weights]
    # CDF for inverse-transform sampling
    cdf = []
    acc = 0.0
    for p in probs:
        acc += p
        cdf.append(acc)

    samples = []
    for _ in range(n_samples):
        r = rng.random()
        lo, hi = 0, n_docs - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cdf[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        samples.append(lo)
    return samples


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def run_config(
    name: str,
    l3_path: str,
    doc_uuids: list[uuid.UUID],
    access_seq: list[int],
    blob_mb: float,
    l2_max_bytes: int,
) -> dict:
    blob_bytes = int(blob_mb * 1024 * 1024)

    store = TieredStore(
        l3_path=l3_path,
        l2_max_bytes=l2_max_bytes,
        l3_max_bytes=None,
        promote_async=False,
    )

    # Pre-populate: all documents go into the store (cold publish)
    for uid in doc_uuids:
        # Deterministic content per doc so reads are verifiable
        data = bytes([uid.bytes[0]] * blob_bytes)
        store.put(uid, data)

    # Reset counters so publish ops don't pollute get statistics
    store.reset_counters()

    # Access phase: simulate read workload
    l2_times: list[float] = []
    l3_times: list[float] = []

    for doc_idx in access_seq:
        uid = doc_uuids[doc_idx]
        t0 = time.perf_counter()
        data, tier = store.get(uid)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if tier == 'l2':
            l2_times.append(elapsed_ms)
        elif tier == 'l3':
            l3_times.append(elapsed_ms)

    s = store.stats
    n = s['total_gets']

    return {
        'name': name,
        'l2_max_mb': l2_max_bytes / 1024 / 1024,
        'l2_entries': s['l2_entries'],
        'hits_l2': s['hits_l2'],
        'hits_l3': s['hits_l3'],
        'misses': s['misses'],
        'hit_rate_l2': s['hit_rate_l2'],
        'hit_rate_l3': s['hit_rate_l3'],
        'miss_rate': s['miss_rate'],
        'median_l2_ms': statistics.median(l2_times) if l2_times else None,
        'median_l3_ms': statistics.median(l3_times) if l3_times else None,
        'evictions_l2': s['hits_l2'],  # same as l2 evictions proxy
        'total_gets': n,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--docs', type=int, default=100, help='Distinct documents')
    ap.add_argument('--accesses', type=int, default=2000, help='Total access requests')
    ap.add_argument('--blob-kb', type=float, default=256,
                    help='Blob size per document in KB (default 256 KB)')
    ap.add_argument('--zipf-s', type=float, default=1.0, help='Zipf exponent (default 1.0)')
    ap.add_argument('--seed', type=int, default=42)
    args = ap.parse_args()

    blob_mb = args.blob_kb / 1024
    working_set_mb = args.docs * blob_mb

    print(f'Documents:     {args.docs}')
    print(f'Blob size:     {args.blob_kb:.0f} KB each')
    print(f'Working set:   {working_set_mb:.1f} MB total')
    print(f'Accesses:      {args.accesses}  (Zipf s={args.zipf_s})')
    print()

    # Generate document UUIDs (stable across configs)
    rng = random.Random(args.seed)
    doc_uuids = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(args.docs)]

    # Generate access sequence once (shared across configs for fairness)
    access_seq = zipf_samples(args.docs, args.accesses, s=args.zipf_s, seed=args.seed)

    # Popularity distribution stats
    from collections import Counter
    freq = Counter(access_seq)
    top5_pct = sum(v for _, v in freq.most_common(5)) / args.accesses * 100
    top20_pct = sum(v for _, v in freq.most_common(20)) / args.accesses * 100
    print(f'Top-5 docs account for  {top5_pct:.1f}% of accesses')
    print(f'Top-20 docs account for {top20_pct:.1f}% of accesses')
    print()

    # L2 configs: baseline (0), tiny (5%), small (20%), large (50%)
    wsmb = working_set_mb
    configs = [
        ('L3-only (baseline)', 0),
        (f'L2=5% WS  ({wsmb*0.05:.0f} MB)',  int(wsmb * 0.05  * 1024 * 1024)),
        (f'L2=10% WS ({wsmb*0.10:.0f} MB)',  int(wsmb * 0.10  * 1024 * 1024)),
        (f'L2=20% WS ({wsmb*0.20:.0f} MB)',  int(wsmb * 0.20  * 1024 * 1024)),
        (f'L2=50% WS ({wsmb*0.50:.0f} MB)',  int(wsmb * 0.50  * 1024 * 1024)),
        (f'L2=100%WS ({wsmb:.0f} MB)',        int(wsmb         * 1024 * 1024)),
    ]

    results = []
    for name, l2_bytes in configs:
        l3_path = tempfile.mkdtemp(prefix='ts-bench-')
        print(f'Running: {name} ...', flush=True)
        r = run_config(name, l3_path, doc_uuids, access_seq, blob_mb, l2_bytes)
        results.append(r)

        l2_ms = f'{r["median_l2_ms"]:.2f}ms' if r['median_l2_ms'] else '—'
        l3_ms = f'{r["median_l3_ms"]:.2f}ms' if r['median_l3_ms'] else '—'
        print(f'  L2 hit: {r["hit_rate_l2"]*100:5.1f}%  '
              f'L3 hit: {r["hit_rate_l3"]*100:5.1f}%  '
              f'miss: {r["miss_rate"]*100:4.1f}%  '
              f'latency L2={l2_ms} L3={l3_ms}')

    # Markdown table
    print()
    print('## Phase 3.5 — Tier hit rates under Zipfian workload')
    print()
    print(f'Model: synthetic blobs ({args.blob_kb:.0f} KB each), '
          f'{args.docs} documents, {args.accesses} accesses, Zipf s={args.zipf_s}.')
    print(f'Top-5 docs = {top5_pct:.0f}% of traffic; top-20 = {top20_pct:.0f}%.')
    print()
    print('| L2 capacity | L2 hit rate | L3 hit rate | miss rate | median L2 get | median L3 get |')
    print('|---|---|---|---|---|---|')
    for r in results:
        l2_ms = f'{r["median_l2_ms"]:.2f} ms' if r['median_l2_ms'] else '—'
        l3_ms = f'{r["median_l3_ms"]:.2f} ms' if r['median_l3_ms'] else '—'
        print(f'| {r["name"]} '
              f'| {r["hit_rate_l2"]*100:.1f}% '
              f'| {r["hit_rate_l3"]*100:.1f}% '
              f'| {r["miss_rate"]*100:.1f}% '
              f'| {l2_ms} '
              f'| {l3_ms} |')

    # Speedup of L2 over L3
    print()
    baseline_l3 = next((r['median_l3_ms'] for r in results if r['median_l3_ms']), None)
    for r in results:
        if r['median_l2_ms'] and baseline_l3:
            speedup = baseline_l3 / r['median_l2_ms']
            print(f'{r["name"]}: L2 get is {speedup:.1f}× faster than L3 get '
                  f'({r["median_l2_ms"]:.2f} ms vs {baseline_l3:.2f} ms)')


if __name__ == '__main__':
    main()
