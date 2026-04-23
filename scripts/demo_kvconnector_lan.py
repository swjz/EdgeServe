"""demo_kvconnector_lan.py — cross-host LAN transport benchmark.

Measures the HTTP KV-cache transfer path between two machines on a LAN:

  seeder  (GPU box, 192.168.1.214): runs vLLM + EdgeServeKVConnector,
          publishes KV for a document to Pulsar + local HTTP server.
  consumer (Mac Mini, 192.168.1.185): uses SemanticCacheClient to
          discover the seeder's header via Pulsar and fetch the KV blob
          over HTTP.

This isolates the *transport layer* (catalog lookup + HTTP fetch +
safetensors parse on consumer) from the actual LLM decode.

Two sub-commands:
  seed    — run on the GPU box: generate + publish KV, then keep the
            HTTP server alive until ^C.
  consume — run on the Mac Mini (or any remote): connect to Pulsar,
            resolve the blob by prefix-hash (derived from the same
            document text), measure fetch time.

Discovery strategy
------------------
The seeder publishes multi-boundary prefix hashes in the bloom filter.
The consumer tokenizes the same document (same model + same doc-repeats)
and probes each block-boundary prefix hash, descending from longest.
If transformers / torch are not available on the consumer, pass
--prefix-hash XXXX (printed by seeder with --print-hash) to skip
tokenization.

Usage
-----
GPU box (seeder, run first):
  python scripts/demo_kvconnector_lan.py seed \\
      --model Qwen/Qwen2.5-1.5B \\
      --doc-repeats 256 \\
      --pulsar-url pulsar://localhost:6650 \\
      --gpu-mem 0.5

Mac Mini (consumer, after seeder prints "KV published"):
  python scripts/demo_kvconnector_lan.py consume \\
      --pulsar-url pulsar://192.168.1.214:6650 \\
      --topic kvcache-lan-XXXX \\
      --model Qwen/Qwen2.5-1.5B \\
      --doc-repeats 256
"""
from __future__ import annotations

import argparse
import os
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
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    venv_python = os.path.join(repo_root, '.venv', 'bin', 'python')
    if os.path.isfile(venv_python):
        return venv_python
    return sys.executable


def cmd_seed(args):
    """Run on GPU box: publish KV and keep HTTP server alive."""
    import subprocess

    PYTHON = _find_python()
    doc = DOC_CHUNK * args.doc_repeats
    doc_id = f"doc_id:{uuid.uuid4().hex[:8]}"
    topic = f"kvcache-lan-{uuid.uuid4().hex[:8]}"
    cache_path = tempfile.mkdtemp(prefix="edgeserve-lan-seed-")

    print(f"doc_id:    {doc_id}")
    print(f"topic:     {topic}")
    print(f"doc len:   ~{len(doc)//4} tokens")
    print(f"cache:     {cache_path}")
    print()

    seed_script = f"""\
import time, sys, os
import torch
from vllm import LLM, SamplingParams
from vllm.config import KVTransferConfig
from edgeserve.inference.vllm_kv_connector import register, set_next_request_entities
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
            'node_id':          'lan-seeder',
        }},
    )
    llm = LLM(
        model={repr(args.model)},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
        gpu_memory_utilization={args.gpu_mem},
        max_model_len=16384,
    )
    set_next_request_entities({{{repr(doc_id)}}})
    prompt = {repr(doc)} + ' Summarise the key points.'
    t0 = time.perf_counter()
    out = llm.generate([prompt], SamplingParams(max_tokens=1, temperature=0))
    elapsed = (time.perf_counter() - t0) * 1000
    tok = out[0].outputs[0].token_ids[0]
    print(f'[seeder] elapsed={{elapsed:.1f}}ms  token={{tok}}', flush=True)
    print('[seeder] doc_id=' + {repr(doc_id)}, flush=True)
    print('[seeder] topic=' + {repr(topic)}, flush=True)
    print('[seeder] KV published — HTTP server running, ^C to stop', flush=True)
    # Keep process alive so HTTP server stays up for consumer
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


def _compute_prefix_hashes(model: str, doc: str, block_size: int = 16) -> list[str]:
    """Tokenize doc and return all block-boundary prefix hashes, longest first."""
    import hashlib
    import torch
    from transformers import AutoTokenizer

    tok = AutoTokenizer.from_pretrained(model)
    token_ids = tok.encode(doc, add_special_tokens=False)
    n = len(token_ids)
    # Descend from longest aligned boundary to shortest
    boundaries = list(range(block_size, n + 1, block_size))
    hashes = []
    for b in reversed(boundaries):
        arr = torch.tensor(token_ids[:b], dtype=torch.long).numpy().tobytes()
        hashes.append(hashlib.sha256(arr).hexdigest())
    print(f"  tokenized doc → {n} tokens; "
          f"{len(hashes)} block-boundary hashes to probe (block_size={block_size})")
    return hashes


def cmd_consume(args):
    """Run on Mac Mini (or any remote): fetch KV from seeder over HTTP."""
    from edgeserve.semantic_cache.client import SemanticCacheClient

    node_id = f"lan-consumer-{uuid.uuid4().hex[:4]}"
    cache_path = tempfile.mkdtemp(prefix="edgeserve-lan-consume-")

    print(f"Connecting to Pulsar at {args.pulsar_url} ...")
    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id=node_id,
        local_cache_path=cache_path,
        topic=args.topic,
        ttl_ms=10 * 60 * 1000,
    )

    # Build the list of entities to probe.
    # 1. If caller supplied --prefix-hash, use that directly.
    # 2. Otherwise, tokenize the document and compute all block-boundary hashes.
    # (Entity-tag lookup is skipped: set_next_request_entities does not cross
    # the vLLM EngineCore subprocess boundary, so the bloom only has prefix hashes.)
    # Fast path: caller already knows block UUID + node URI (skip Pulsar discovery).
    # Useful when the catalog subscription cursor state is stale (Pulsar durable
    # subscriptions remember the cursor; a previous run's ack advances it past the
    # message, making a new connection with InitialPosition.Earliest see nothing).
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
        print(f"Direct fetch mode: {args.node_uri}/cache/{args.block_uuid}")
        client.close()
    else:
        if args.prefix_hash:
            probe_entities = [args.prefix_hash]
            print(f"Using supplied prefix hash: {args.prefix_hash}")
        else:
            doc = DOC_CHUNK * args.doc_repeats
            prompt = doc + ' Summarise the key points.'
            print(f"Tokenizing document ({len(prompt)} chars) to build prefix-hash probes ...")
            probe_entities = _compute_prefix_hashes(args.model, prompt)

        print(f"Waiting for a matching header (up to {args.wait}s) ...")
        deadline = time.time() + args.wait
        header = None
        while time.time() < deadline:
            for entity in probe_entities:
                hits = list(client.catalog.lookup([entity]))
                if hits:
                    header = hits[0]
                    print(f"  matched on entity={entity[:16]}...")
                    break
            if header is not None:
                break
            time.sleep(1.0)

        client.close()

        if header is None:
            print(f"[ERROR] no header found within {args.wait}s")
            return

    print(f"Header: block_uuid={header.block_uuid}  node_uri={header.node_uri}")
    print()

    # Warm up DNS / TCP
    from edgeserve.semantic_cache.http_client import http_fetch
    print("Warming up connection ...")
    try:
        data = http_fetch(header.node_uri, header.block_uuid, timeout=30.0)
        print(f"  warmup: {len(data)/1e6:.1f} MB")
    except Exception as e:
        print(f"[WARN] warmup fetch failed: {e}")

    # Measure fetch times
    times = []
    for i in range(args.repeats):
        t0 = time.perf_counter()
        try:
            data = http_fetch(header.node_uri, header.block_uuid, timeout=60.0)
            elapsed = (time.perf_counter() - t0) * 1000
            times.append(elapsed)
            blob_mb = len(data) / 1e6
            print(f"  fetch {i+1}/{args.repeats}: {elapsed:.1f}ms  "
                  f"{blob_mb:.1f}MB  "
                  f"{blob_mb * 8000 / elapsed:.0f}Mbps")
        except Exception as e:
            print(f"  fetch {i+1}/{args.repeats}: FAILED — {e}")

    if times:
        import statistics
        med = statistics.median(times)
        blob_mb = len(data) / 1e6
        print(f"\nBlob size:  {blob_mb:.1f} MB")
        print(f"Median:     {med:.1f} ms")
        print(f"Min/max:    {min(times):.1f} / {max(times):.1f} ms")
        print(f"Throughput: {blob_mb * 8000 / med:.0f} Mbps (median)")


def main():
    ap = argparse.ArgumentParser(description="LAN KV-cache transport demo")
    sub = ap.add_subparsers(dest='cmd', required=True)

    sp = sub.add_parser('seed', help='Run seeder on GPU box')
    sp.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    sp.add_argument('--doc-repeats', type=int, default=256)
    sp.add_argument('--gpu-mem', type=float, default=0.5)
    sp.add_argument('--pulsar-url', default='pulsar://localhost:6650')

    cp = sub.add_parser('consume', help='Run consumer on Mac Mini')
    cp.add_argument('--pulsar-url', required=True,
                    help='e.g. pulsar://192.168.1.214:6650')
    cp.add_argument('--topic', required=True,
                    help='Pulsar topic (printed by seeder)')
    cp.add_argument('--model', default='Qwen/Qwen2.5-1.5B',
                    help='Same model as seeder (for tokenization)')
    cp.add_argument('--doc-repeats', type=int, default=256,
                    help='Same doc-repeats as seeder')
    cp.add_argument('--prefix-hash', default='',
                    help='Exact prefix hash (skip tokenization if known)')
    cp.add_argument('--block-uuid', default='',
                    help='Block UUID for direct fetch (skip Pulsar discovery)')
    cp.add_argument('--node-uri', default='',
                    help='HTTP node URI for direct fetch (skip Pulsar discovery)')
    cp.add_argument('--wait', type=float, default=60.0,
                    help='Seconds to wait for header')
    cp.add_argument('--repeats', type=int, default=5,
                    help='Number of fetch measurements')

    args = ap.parse_args()
    if args.cmd == 'seed':
        cmd_seed(args)
    elif args.cmd == 'consume':
        cmd_consume(args)


if __name__ == '__main__':
    main()
