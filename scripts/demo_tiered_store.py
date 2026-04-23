"""demo_tiered_store.py — Phase 3 tiered-storage smoke test.

Exercises the TieredStore L2/L3 tiers and tombstone propagation without
requiring a GPU or a running vLLM instance.  Uses synthetic KV blobs (random
bytes) to fill L2, force L2 eviction, trigger L3 promotion, and optionally
fire tombstones on L3 eviction.

Usage
-----
  python scripts/demo_tiered_store.py
  python scripts/demo_tiered_store.py --l2-mb 64 --blob-mb 10 --blobs 12
  python scripts/demo_tiered_store.py --test-tombstone  # also tests L3 eviction
  python scripts/demo_tiered_store.py --with-pulsar     # tombstones via Pulsar
"""
from __future__ import annotations

import argparse
import os
import tempfile
import time
import uuid

from edgeserve.semantic_cache.tiered_store import TieredStore


def make_blob(size_bytes: int) -> bytes:
    return os.urandom(size_bytes)


def fmt_bytes(n: int) -> str:
    if n >= 1024 ** 3:
        return f'{n/1024**3:.2f} GB'
    if n >= 1024 ** 2:
        return f'{n/1024**2:.1f} MB'
    return f'{n/1024:.1f} KB'


def run_demo(args):
    l2_max = args.l2_mb * 1024 * 1024
    l3_max = (args.l3_mb * 1024 * 1024) if args.test_tombstone else None
    blob_size = args.blob_mb * 1024 * 1024
    n_blobs = args.blobs

    l3_path = tempfile.mkdtemp(prefix='edgeserve-tier-demo-')
    print(f'L3 path: {l3_path}')
    print(f'L2 cap:  {fmt_bytes(l2_max)}')
    print(f'L3 cap:  {fmt_bytes(l3_max) if l3_max else "unbounded"}')
    print(f'Blob:    {fmt_bytes(blob_size)} × {n_blobs}')
    print()

    tombstones_fired = []

    if args.with_pulsar:
        import pulsar as _pulsar
        from edgeserve.semantic_cache.bloom import SemanticBloomFilter
        from edgeserve.semantic_cache.header import CacheHeader
        from edgeserve.semantic_cache.publisher import HeaderPublisher

        _pub = HeaderPublisher(args.pulsar_url)

        def on_tombstone(block_uuid: uuid.UUID) -> None:
            tombstones_fired.append(block_uuid)
            header = CacheHeader(
                block_uuid=block_uuid,
                node_uri='http://localhost:0',
                prefix_hash=b'',
                bloom=SemanticBloomFilter.for_capacity(1),
                deleted=True,
            )
            _pub.publish(header)
            print(f'  [tombstone → Pulsar] {block_uuid}')
    else:
        def on_tombstone(block_uuid: uuid.UUID) -> None:
            tombstones_fired.append(block_uuid)
            print(f'  [tombstone] {block_uuid}')

    store = TieredStore(
        l3_path=l3_path,
        l2_max_bytes=l2_max,
        l3_max_bytes=l3_max,
        on_tombstone=on_tombstone,
        promote_async=False,  # synchronous for predictable demo output
    )

    uuids = []

    # --- Phase A: fill until L2 overflows ---
    print('=== Phase A: publishing blobs ===')
    for i in range(n_blobs):
        uid = uuid.uuid4()
        data = make_blob(blob_size)
        t0 = time.perf_counter()
        tier = store.put(uid, data)
        elapsed = (time.perf_counter() - t0) * 1000
        uuids.append(uid)
        s = store.stats
        print(f'  blob {i+1:2d}/{n_blobs}  tier={tier}  '
              f'l2={fmt_bytes(s["l2_bytes"])}/{fmt_bytes(l2_max)}  '
              f'l3={fmt_bytes(s["l3_bytes"])}  '
              f'put={elapsed:.1f}ms')

    print()
    print('=== Phase B: read back all blobs (hit rate by tier) ===')
    l2_hits = l3_hits = misses = 0
    for i, uid in enumerate(uuids):
        t0 = time.perf_counter()
        data, tier = store.get(uid)
        elapsed = (time.perf_counter() - t0) * 1000
        if tier == 'l2':
            l2_hits += 1
        elif tier == 'l3':
            l3_hits += 1
        else:
            misses += 1
        if i < 5 or i >= len(uuids) - 3:
            print(f'  blob {i+1:2d}  tier={tier or "MISS"}  get={elapsed:.1f}ms')
        elif i == 5:
            print(f'  ... ({len(uuids) - 8} blobs omitted) ...')

    print()
    print(f'Hit summary: L2={l2_hits}  L3={l3_hits}  miss={misses}')

    # --- Phase C: re-read the L3 hits → they should promote to L2 ---
    if l3_hits > 0:
        print()
        print('=== Phase C: re-read an L3 hit → should be promoted to L2 ===')
        # Find first L3 entry (it was the one written earliest, so evicted from L2 first)
        for uid in uuids:
            data, tier = store.get(uid)
            if tier == 'l3':
                l3_uuid = uid
                break
        else:
            l3_uuid = None

        if l3_uuid:
            print(f'  First L3 blob: {l3_uuid}')
            # Second read should be L2 now (promotion happened sync)
            _, tier2 = store.get(l3_uuid)
            print(f'  Re-read tier: {tier2}  (expected l2)')

    # --- Phase D: tombstone test ---
    if args.test_tombstone and tombstones_fired:
        print()
        print(f'=== Phase D: {len(tombstones_fired)} tombstones fired on L3 eviction ===')
        for uid in tombstones_fired:
            path_exists = store.get_l3_path(uid) is not None
            print(f'  {uid}  L3 file exists: {path_exists}  (expected False)')

    # --- Phase E: manual evict ---
    print()
    print('=== Phase E: manual evict of one L2 entry ===')
    l2_target = next((uid for uid in reversed(uuids) if store.peek_l2(uid) is not None), None)
    if l2_target:
        print(f'  evicting {l2_target} from L2...')
        store.evict(l2_target)
        data, tier = store.get(l2_target)
        print(f'  after evict: tier={tier or "MISS"}  (expected MISS)')
    else:
        print('  (no L2 entries to evict)')

    print()
    final = store.stats
    print('Final stats:')
    for k, v in final.items():
        print(f'  {k}: {fmt_bytes(v) if "bytes" in k else v}')

    if args.with_pulsar:
        _pub.close()

    print()
    print('Done.')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--l2-mb', type=int, default=50,
                    help='L2 RAM cap in MB (default 50 → overflows at ~5 blobs × 10 MB)')
    ap.add_argument('--l3-mb', type=int, default=80,
                    help='L3 NVMe cap in MB (only used with --test-tombstone)')
    ap.add_argument('--blob-mb', type=int, default=10,
                    help='Synthetic blob size in MB (default 10)')
    ap.add_argument('--blobs', type=int, default=10,
                    help='Number of blobs to publish (default 10)')
    ap.add_argument('--test-tombstone', action='store_true',
                    help='Enable L3 capacity cap so tombstones fire')
    ap.add_argument('--with-pulsar', action='store_true',
                    help='Publish tombstones to Pulsar (requires broker)')
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    run_demo(ap.parse_args())


if __name__ == '__main__':
    main()
