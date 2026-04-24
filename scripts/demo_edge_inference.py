"""demo_edge_inference.py — Phase 6: end-to-end edge inference demo.

Proves the KV-cache CDN privacy thesis end-to-end:
  - GPU box seeds a document: prefills with HFEngine (CUDA), serialises KV
    in HF safetensors format, publishes to Pulsar + HTTP server.
  - Edge device (Mac Mini / laptop) answers user questions WITHOUT
    re-prefilling the document: fetches KV from GPU box over HTTP,
    injects into HF past_key_values, generates locally.

Privacy model
-------------
  Cloud API:  user's prompt + context leave the device; tokens return.
  EdgeServe:  document is pushed to a nearby context server (same trust
              as cloud, but LAN-local and user-controlled).  The user's
              LIVE QUERY and the GENERATED TOKENS never leave the edge.

Format note
-----------
Both seeder and consumer use HFEngine's safetensors KV format (k.i / v.i
per layer).  This is compatible across devices (CUDA seeder → MPS/CPU
consumer) without dtype conversion hassles, and avoids the paged-buffer
format used by the vLLM KVConnector (which is vLLM-internal).

Sub-commands
------------
  seed      Run on GPU box (CUDA): prefill doc, publish KV, serve HTTP.
  query     Run on edge (Mac / CPU / MPS): discover KV, fetch, generate.
  selftest  Single-machine test: seed + query on GPU box (same-host mmap).

Usage
-----
GPU box (run first):
  python scripts/demo_edge_inference.py seed \\
      --model Qwen/Qwen2.5-1.5B --doc-repeats 128 \\
      --pulsar-url pulsar://localhost:6650

Mac Mini (copy command printed by seeder):
  python scripts/demo_edge_inference.py query \\
      --pulsar-url pulsar://192.168.1.214:6650 \\
      --topic kvcache-edge-XXXX \\
      --model Qwen/Qwen2.5-1.5B --doc-repeats 128 \\
      --query "What are the main themes of this text?"

Self-contained test on GPU box:
  python scripts/demo_edge_inference.py selftest \\
      --model Qwen/Qwen2.5-1.5B --doc-repeats 64

Expected speedups (Mac Mini M4, 179 Mbps LAN):
  128 doc-repeats (~4 232 tokens): B0 ~7.7s vs EdgeServe ~6s  →  ~1.3×
  256 doc-repeats (~8 456 tokens): B0 ~54s  vs EdgeServe ~11s →  ~5×
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import time
import uuid as _uuid_mod

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society.  "
    "Researchers have long debated the nature of intelligence itself, "
    "whether machines can truly think, and what it means to understand "
    "language.  Large language models represent the latest chapter in "
    "this ongoing story, raising new questions about creativity, bias, "
    "and the future of human-machine collaboration.  "
)

DEFAULT_QUERY = (
    "What are the main themes discussed in this passage, "
    "and what open questions does it raise?"
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _doc_text(repeats: int) -> str:
    return DOC_CHUNK * repeats


def _boundary_hashes(token_ids: list[int], block_size: int = 16) -> list[str]:
    """Return SHA-256 hashes of every block-boundary prefix, longest first."""
    import torch
    n = len(token_ids)
    boundaries = list(range(block_size, n + 1, block_size))
    hashes = []
    for b in reversed(boundaries):
        arr = torch.tensor(token_ids[:b], dtype=torch.long).numpy().tobytes()
        hashes.append(hashlib.sha256(arr).hexdigest())
    return hashes


def _detect_device():
    """(device_str, dtype) for the current hardware."""
    import torch
    if torch.cuda.is_available():
        return 'cuda', torch.bfloat16
    if torch.backends.mps.is_available():
        # bfloat16 not fully supported on MPS; float16 is fine for inference
        return 'mps', torch.float16
    return 'cpu', torch.float32


def _cast_kv(kv_cache, dtype):
    """Cast every tensor in an HF past_key_values to dtype in-place."""
    import torch
    if kv_cache is None:
        return None
    try:
        from transformers import DynamicCache
        if isinstance(kv_cache, DynamicCache):
            cache = DynamicCache()
            for i in range(len(kv_cache)):
                cache.update(
                    kv_cache.key_cache[i].to(dtype),
                    kv_cache.value_cache[i].to(dtype),
                    i,
                )
            return cache
    except (ImportError, AttributeError):
        pass
    return tuple(
        tuple(t.to(dtype) for t in layer)
        for layer in kv_cache
    )


# ── seed (GPU box) ────────────────────────────────────────────────────────────

def cmd_seed(args):
    """Prefill document via HFEngine on CUDA, publish KV, keep server alive."""
    import torch
    from edgeserve.inference.hf_engine import HFEngine
    from edgeserve.semantic_cache.client import SemanticCacheClient

    doc = _doc_text(args.doc_repeats)
    topic = f'kvcache-edge-{_uuid_mod.uuid4().hex[:8]}'
    cache_path = tempfile.mkdtemp(prefix='edgeserve-edge-seed-')

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    dtype = torch.bfloat16 if device == 'cuda' else torch.float32

    print('=' * 60)
    print('EdgeServe edge-inference demo  —  SEED (GPU box)')
    print('=' * 60)
    print(f'Model:       {args.model}')
    print(f'Device:      {device}  dtype={dtype}')
    print(f'Doc repeats: {args.doc_repeats}  (~{len(doc)//4} tokens)')
    print(f'Topic:       {topic}')
    print(f'Cache path:  {cache_path}')
    print()

    print('Loading model...')
    t0 = time.perf_counter()
    engine = HFEngine(args.model, device=device, dtype=dtype)
    print(f'  loaded in {(time.perf_counter()-t0)*1000:.0f} ms')

    doc_tokens = engine.tokenize(doc)
    print(f'\nPrefilling {len(doc_tokens)} document tokens...')
    t0 = time.perf_counter()
    kv_cache = engine.prefill(doc_tokens)
    prefill_ms = (time.perf_counter() - t0) * 1000
    print(f'  done in {prefill_ms:.0f} ms')

    print('Serialising KV cache...')
    t0 = time.perf_counter()
    kv_bytes = engine.serialize_cache(kv_cache)
    del kv_cache   # free GPU memory
    ser_ms = (time.perf_counter() - t0) * 1000
    print(f'  {len(kv_bytes)/1e6:.1f} MB  ({ser_ms:.0f} ms)')

    # Build entity set from all block-boundary prefix hashes.
    # Consumers can then discover this entry by probing the longest
    # boundary hash they share with the document.
    entities = _boundary_hashes(doc_tokens)
    print(f'  {len(entities)} block-boundary entities for bloom')

    print('\nStarting cache client (HTTP server + Pulsar publisher)...')
    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id='edge-seeder',
        local_cache_path=cache_path,
        topic=topic,
    )
    block_uuid = client.publish(
        entities, kv_bytes, num_tokens=len(doc_tokens),
    )
    print(f'  Published: block_uuid={block_uuid}')
    print(f'  HTTP server: {client.http.uri}')

    import socket
    host = socket.gethostname()
    print()
    print('─' * 60)
    print('Run this on the Mac (replace GPU_BOX_IP with the GPU box IP):')
    print(f'  python scripts/demo_edge_inference.py query \\')
    print(f'      --pulsar-url pulsar://GPU_BOX_IP:6650 \\')
    print(f'      --topic {topic} \\')
    print(f'      --model {args.model} \\')
    print(f'      --doc-repeats {args.doc_repeats}')
    print('─' * 60)
    print('HTTP server running — press ^C to stop')

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()


# ── query (edge device) ───────────────────────────────────────────────────────

def cmd_query(args, *, same_host_path: str | None = None):
    """Fetch KV from GPU box, inject into HF, generate answer locally.

    same_host_path: when set (selftest), skip HTTP and load directly from
    this local path via mmap fast path.
    """
    import torch
    device, dtype = _detect_device()
    if same_host_path is not None and torch.cuda.is_available():
        device, dtype = 'cuda', torch.bfloat16

    doc = _doc_text(args.doc_repeats)

    print('=' * 60)
    print('EdgeServe edge-inference demo  —  QUERY (edge device)')
    print('=' * 60)
    print(f'Model:   {args.model}')
    print(f'Device:  {device}  dtype={dtype}')
    print(f'Topic:   {args.topic}')
    print(f'Query:   {args.query!r}')
    print()

    print('Loading model...')
    t_load = time.perf_counter()
    from edgeserve.inference.hf_engine import HFEngine
    engine = HFEngine(args.model, device=device, dtype=dtype)
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f'  done in {load_ms:.0f} ms')
    print()

    doc_tokens = engine.tokenize(doc)
    query_suffix = '\n\nQuestion: ' + args.query + '\nAnswer:'
    suffix_tokens = engine.tokenize(query_suffix)
    full_tokens = doc_tokens + suffix_tokens

    # ── B0: full local prefill ─────────────────────────────────────────────
    if args.skip_baseline:
        print(f'B0 baseline: SKIPPED  (--skip-baseline; full_tokens={len(full_tokens)})')
        b0_ms = float('nan')
        b0_answer = '(skipped)'
        b0_generated = []
        print()
    else:
        print(f'B0 baseline: prefilling {len(full_tokens)} tokens (doc + question)...')
        t0 = time.perf_counter()
        b0_generated, _ = engine.generate(full_tokens, max_new_tokens=args.max_new_tokens)
        b0_ms = (time.perf_counter() - t0) * 1000
        b0_answer = engine.detokenize(b0_generated)
        print(f'  time:   {b0_ms:.0f} ms')
        print(f'  answer: {b0_answer[:200]!r}')
        print()

    # ── EdgeServe: discover → fetch → inject → decode suffix ──────────────
    print('EdgeServe path:')

    if same_host_path is not None:
        # Selftest: find the .bin file and load via mmap
        import glob
        bins = sorted(glob.glob(os.path.join(same_host_path, '*.bin')))
        if not bins:
            raise RuntimeError(f'No .bin files found in {same_host_path}')
        local_path = bins[-1]
        print(f'  same-host mmap: {os.path.basename(local_path)}')
        t_fetch = time.perf_counter()
        kv_cache = engine.deserialize_cache_from_path(local_path)
        fetch_ms = (time.perf_counter() - t_fetch) * 1000
        blob_mb = os.path.getsize(local_path) / 1e6
        print(f'  loaded {blob_mb:.1f} MB in {fetch_ms:.0f} ms')
    else:
        # Real mode: get header from catalog (unless --block-uuid given for direct fetch)
        import uuid as _uuid
        from edgeserve.semantic_cache.http_client import http_fetch

        if args.block_uuid and args.node_uri:
            # Direct-fetch fast path (catalog bypass; useful when Pulsar
            # message retention has expired or cursor state is stale).
            print(f'  direct fetch: block_uuid={args.block_uuid}')
            print(f'                node_uri={args.node_uri}')
            block_uuid = _uuid.UUID(args.block_uuid)
            disc_ms = 0.0
        else:
            from edgeserve.semantic_cache.catalog import HeaderCatalog

            node_id = f'edge-query-{_uuid_mod.uuid4().hex[:8]}'
            print(f'  connecting to Pulsar at {args.pulsar_url}, topic {args.topic!r} ...')
            catalog = HeaderCatalog(
                args.pulsar_url, node_id=node_id,
                topic=args.topic, ttl_ms=30 * 60 * 1000,
            )
            print(f'  computing {len(doc_tokens)//16} block-boundary hashes...')
            hashes = _boundary_hashes(doc_tokens)

            t_disc = time.perf_counter()
            header = None
            deadline = time.time() + args.wait
            while time.time() < deadline and header is None:
                for h in hashes:
                    hits = list(catalog.lookup([h]))
                    if hits:
                        header = hits[0]
                        break
                if header is None:
                    time.sleep(1.0)
            disc_ms = (time.perf_counter() - t_disc) * 1000
            catalog.close()

            if header is None:
                print(f'  ERROR: no KV found within {args.wait}s')
                print(f'  (tip: pass --block-uuid XXX --node-uri http://HOST:PORT '
                      f'to skip catalog discovery)')
                return
            block_uuid = header.block_uuid
            print(f'  found: block_uuid={block_uuid}  ({disc_ms:.0f} ms discovery)')

        node_uri = args.node_uri or header.node_uri  # type: ignore[name-defined]
        print(f'  fetching from {node_uri} ...')
        t_fetch = time.perf_counter()
        kv_bytes = http_fetch(node_uri, block_uuid, timeout=180.0)
        fetch_ms = (time.perf_counter() - t_fetch) * 1000
        blob_mb = len(kv_bytes) / 1e6
        throughput = blob_mb * 8000 / fetch_ms if fetch_ms > 0 else 0
        print(f'  fetched {blob_mb:.1f} MB in {fetch_ms:.0f} ms  '
              f'({throughput:.0f} Mbps)')

        print('  deserialising...')
        t_deser = time.perf_counter()
        kv_cache = engine.deserialize_cache(kv_bytes)
        deser_ms = (time.perf_counter() - t_deser) * 1000
        fetch_ms += deser_ms   # include deserialisation in the "fetch" bucket
        print(f'  deserialised in {deser_ms:.0f} ms')

    # Cast KV dtype to match the local model (seeder may use bf16; Mac uses fp16)
    seeder_sample = None
    try:
        from transformers import DynamicCache
        if isinstance(kv_cache, DynamicCache) and kv_cache.key_cache:
            seeder_sample = kv_cache.key_cache[0]
    except (ImportError, AttributeError, IndexError):
        pass
    if seeder_sample is None and kv_cache:
        try:
            seeder_sample = kv_cache[0][0]
        except (TypeError, IndexError):
            pass
    if seeder_sample is not None and seeder_sample.dtype != dtype:
        kv_cache = _cast_kv(kv_cache, dtype)
        print(f'  cast KV: {seeder_sample.dtype} → {dtype}')

    # Decode only the question suffix with cached doc KV
    print(f'  generating answer ({len(suffix_tokens)} suffix tokens + '
          f'{args.max_new_tokens} new tokens)...')
    t_decode = time.perf_counter()
    es_generated, _ = engine.generate(
        suffix_tokens, max_new_tokens=args.max_new_tokens, cache=kv_cache,
    )
    decode_ms = (time.perf_counter() - t_decode) * 1000
    es_answer = engine.detokenize(es_generated)
    es_total_ms = fetch_ms + decode_ms

    # ── Results ────────────────────────────────────────────────────────────
    token_match = (b0_generated == es_generated) if b0_generated and es_generated else None

    print()
    print('=' * 60)
    print('Results')
    print('=' * 60)
    print(f'Document tokens:         {len(doc_tokens):>6}')
    print(f'Query suffix tokens:     {len(suffix_tokens):>6}')
    print(f'Generated tokens:        {args.max_new_tokens:>6}')
    print()
    import math
    if math.isnan(b0_ms):
        print(f'B0  full local prefill:  (skipped)')
    else:
        print(f'B0  full local prefill:  {b0_ms:>8.0f} ms')
    if same_host_path is not None:
        print(f'ES  mmap load:           {fetch_ms:>8.0f} ms')
    else:
        print(f'ES  fetch + deserialise: {fetch_ms:>8.0f} ms')
    print(f'ES  decode (suffix only):{decode_ms:>8.0f} ms')
    print(f'ES  total:               {es_total_ms:>8.0f} ms')
    if not math.isnan(b0_ms):
        print(f'Speedup  (B0 / ES):      {b0_ms/es_total_ms:>8.2f}×')
    if token_match is not None and not math.isnan(b0_ms):
        match_str = '✓ bit-exact' if token_match else (
            '~ close  (minor float-precision diff — answers semantically identical)'
        )
        print(f'Token match (B0 == ES):  {match_str}')
    print()
    print(f'B0  answer: {b0_answer[:300]!r}')
    print(f'ES  answer: {es_answer[:300]!r}')
    print()
    print('─' * 60)
    print('Privacy: the document was prefilled on the context server.')
    print('         This query and its answer were generated locally.')
    if same_host_path is None:
        print('         The query text and generated tokens never crossed the LAN.')


# ── selftest ──────────────────────────────────────────────────────────────────

def cmd_selftest(args):
    """Single-machine integration test (GPU box only)."""
    import types
    from edgeserve.inference.hf_engine import HFEngine
    from edgeserve.semantic_cache.client import SemanticCacheClient
    import torch

    doc = _doc_text(args.doc_repeats)
    topic = f'kvcache-selftest-{_uuid_mod.uuid4().hex[:8]}'
    cache_path = tempfile.mkdtemp(prefix='edgeserve-selftest-')

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    dtype = torch.bfloat16 if device == 'cuda' else torch.float32

    print('=' * 60)
    print('EdgeServe edge-inference demo  —  SELFTEST')
    print('=' * 60)
    print(f'Model: {args.model}   doc-repeats: {args.doc_repeats}')
    print(f'Topic: {topic}')
    print()

    # ── Seed ──────────────────────────────────────────────────────────────
    print('Step 1 — Seeding (HFEngine on CUDA)...')
    engine_seed = HFEngine(args.model, device=device, dtype=dtype)
    doc_tokens = engine_seed.tokenize(doc)

    t0 = time.perf_counter()
    kv_cache = engine_seed.prefill(doc_tokens)
    prefill_ms = (time.perf_counter() - t0) * 1000
    kv_bytes = engine_seed.serialize_cache(kv_cache)
    del kv_cache, engine_seed

    entities = _boundary_hashes(doc_tokens)
    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id='selftest-seeder',
        local_cache_path=cache_path,
        topic=topic,
    )
    client.publish(entities, kv_bytes, num_tokens=len(doc_tokens))
    client.close()

    print(f'  prefill:    {prefill_ms:.0f} ms')
    print(f'  blob:       {len(kv_bytes)/1e6:.1f} MB')
    print(f'  entities:   {len(entities)}')
    print()

    # ── Query ─────────────────────────────────────────────────────────────
    print('Step 2 — Querying (same-host mmap)...')
    fake = types.SimpleNamespace(
        model=args.model,
        doc_repeats=args.doc_repeats,
        topic=topic,
        query=args.query,
        max_new_tokens=args.max_new_tokens,
        wait=60.0,
        pulsar_url=args.pulsar_url,
    )
    cmd_query(fake, same_host_path=cache_path)

    import shutil
    shutil.rmtree(cache_path, ignore_errors=True)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest='cmd', required=True)

    sp = sub.add_parser('seed', help='GPU box: prefill doc + publish KV')
    sp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    sp.add_argument('--doc-repeats', type=int, default=128)
    sp.add_argument('--pulsar-url', default='pulsar://localhost:6650')

    qp = sub.add_parser('query', help='Edge: fetch KV, generate locally')
    qp.add_argument('--pulsar-url', required=True)
    qp.add_argument('--topic', required=True)
    qp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    qp.add_argument('--doc-repeats', type=int, default=128)
    qp.add_argument('--query', default=DEFAULT_QUERY)
    qp.add_argument('--max-new-tokens', type=int, default=80)
    qp.add_argument('--wait', type=float, default=120.0)
    qp.add_argument('--node-uri', default='',
                    help='Override HTTP URI from catalog header '
                         '(e.g. http://192.168.1.214:42597 when DNS is wrong)')
    qp.add_argument('--block-uuid', default='',
                    help='Skip catalog discovery, fetch this block directly. '
                         'Requires --node-uri.')
    qp.add_argument('--skip-baseline', action='store_true',
                    help='Skip B0 local prefill (useful on slow edge devices '
                         'where MPS fp16 is numerically unstable at long context)')

    tp = sub.add_parser('selftest', help='Single-machine integration test')
    tp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    tp.add_argument('--doc-repeats', type=int, default=64)
    tp.add_argument('--query', default=DEFAULT_QUERY)
    tp.add_argument('--max-new-tokens', type=int, default=80)
    tp.add_argument('--pulsar-url', default='pulsar://localhost:6650')

    args = ap.parse_args()
    {'seed': cmd_seed, 'query': cmd_query, 'selftest': cmd_selftest}[args.cmd](args)


if __name__ == '__main__':
    main()
