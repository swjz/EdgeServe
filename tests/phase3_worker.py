"""Per-agent worker process for the Phase-3 multi-process benchmark.

Each worker is its own Python process with its own model instance and its
own SemanticCacheClient. The coordinator in `phase3_multiproc_bench.py`
spawns N of these and drives them via a simple newline-delimited JSON
protocol over stdin/stdout.

Protocol:
  worker -> coord:  READY <json meta>
  coord  -> worker: {"cmd": "run", "prompt": "...", "cache_tags": [...],
                     "publish": bool, "max_new_tokens": int}
  worker -> coord:  RESULT <json metrics>
  coord  -> worker: {"cmd": "shutdown"}
"""

import argparse
import json
import os
import sys
import time
import uuid

# Must install mock pulsar BEFORE any semantic_cache submodule import only if
# we want mock; here we use real Pulsar, so nothing to do. But we still lazy
# import so the MOCK could be swapped in by the coordinator via an env var.
USE_MOCK = os.environ.get('EDGESERVE_MOCK_PULSAR') == '1'
if USE_MOCK:
    from edgeserve.semantic_cache import mock_pulsar
    mock_pulsar.install()


def _device_from_env():
    dev = os.environ.get('EDGESERVE_DEVICE', 'cuda')
    return dev


def _dtype_from_env():
    import torch
    name = os.environ.get('EDGESERVE_DTYPE', 'bf16')
    return {
        'fp32': torch.float32, 'fp16': torch.float16, 'bf16': torch.bfloat16,
    }[name]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker-id', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    parser.add_argument('--headers-topic', default='kvcache-headers-bench')
    parser.add_argument('--cache-dir', required=True)
    parser.add_argument('--http-port', type=int, default=0)
    parser.add_argument('--mode', choices=['eager', 'routed'], default='routed')
    args = parser.parse_args()

    import torch
    from edgeserve.inference.hf_engine import HFEngine

    device = _device_from_env()
    dtype = _dtype_from_env()
    engine = HFEngine(args.model, device=device, dtype=dtype)

    # Warmup: first forward pass on CUDA pays one-time kernel compile and
    # allocator growth costs. Without this, whichever trial runs first on
    # this worker eats those costs, skewing timings.
    warmup_tokens = engine.tokenize('hello world ' * 32)
    _ = engine.prefill(warmup_tokens)
    _, _ = engine.generate(warmup_tokens[:4], max_new_tokens=4)
    if device == 'cuda':
        torch.cuda.synchronize()
    torch.cuda.empty_cache() if device == 'cuda' else None

    cache_client = None
    if args.mode == 'routed':
        from edgeserve.semantic_cache import SemanticCacheClient
        os.makedirs(args.cache_dir, exist_ok=True)
        cache_client = SemanticCacheClient(
            pulsar_node=args.pulsar_url,
            node_id=args.worker_id,
            local_cache_path=args.cache_dir,
            http_host='127.0.0.1',
            http_port=args.http_port,
            topic=args.headers_topic,
            bloom_capacity=64,
        )

    ready = {
        'worker_id': args.worker_id,
        'device': device,
        'http_uri': cache_client.http.uri if cache_client else None,
        'pid': os.getpid(),
    }
    print(f'READY {json.dumps(ready)}', flush=True)

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            cmd = json.loads(line)
            op = cmd.get('cmd', 'run')
            if op == 'shutdown':
                break
            if op != 'run':
                print(f'RESULT {json.dumps({"error": f"unknown cmd {op}"})}', flush=True)
                continue

            prompt = cmd['prompt']
            cache_tags = cmd.get('cache_tags', [])
            publish = bool(cmd.get('publish', False))
            max_new = int(cmd.get('max_new_tokens', 16))

            m = {'worker_id': args.worker_id, 'cache_hit': False}
            t_start = time.perf_counter()

            reused_cache = None
            if args.mode == 'routed' and cache_tags:
                t0 = time.perf_counter()
                hit = cache_client.resolve_into(cache_tags, engine, timeout=5.0)
                # resolve_into combines fetch + deserialize; we report them
                # together as `fetch_deserialize_ms` rather than splitting.
                m['fetch_deserialize_ms'] = (time.perf_counter() - t0) * 1000
                if hit is not None:
                    reused_cache, header, info = hit
                    m['cache_hit'] = True
                    m['source_uri'] = header.node_uri
                    m['transport'] = info['transport']
                    if info['transport'] == 'http':
                        m['fetched_bytes'] = info['bytes']
                    else:
                        m['fetched_path'] = info['path']

            t0 = time.perf_counter()
            prompt_tokens = engine.tokenize(prompt)
            m['prompt_tokens'] = len(prompt_tokens)
            new_tokens, final_cache = engine.generate(
                prompt_tokens, max_new_tokens=max_new, cache=reused_cache,
            )
            # Block on any async CUDA work so the timing reflects actual runtime.
            if device == 'cuda':
                torch.cuda.synchronize()
            m['generate_ms'] = (time.perf_counter() - t0) * 1000
            m['new_tokens'] = len(new_tokens)

            if publish and args.mode == 'routed' and not m['cache_hit']:
                t0 = time.perf_counter()
                blob = engine.serialize_cache(final_cache)
                m['serialize_ms'] = (time.perf_counter() - t0) * 1000
                m['published_bytes'] = len(blob)
                t1 = time.perf_counter()
                cache_client.publish(set(cache_tags), blob)
                m['publish_ms'] = (time.perf_counter() - t1) * 1000
                m['published'] = True

            m['total_ms'] = (time.perf_counter() - t_start) * 1000
            print(f'RESULT {json.dumps(m)}', flush=True)
    finally:
        if cache_client is not None:
            try:
                cache_client.close()
            except Exception:
                pass


if __name__ == '__main__':
    main()
