"""context_server.py — Phase 4.1: persistent vLLM ingest HTTP server.

Keeps a vLLM + EdgeServeKVConnector instance alive across requests so
that repeated ingests pay model-load cost only once.  Exposes:

  POST /ingest
    Body: {"text": "...", "entities": ["tag1", "tag2"], "sha": "abc123"}
    Resp: {"block_uuid": "...", "n_tokens": N, "ingest_ms": T}

  GET  /health
    Resp: {"status": "ok", "model": "...", "n_ingested": N}

The server runs single-threaded (vLLM requests are sequential), with the
HTTP listener in a daemon thread and `llm.generate()` on the main thread.

Usage
-----
  python -m edgeserve.inference.context_server \\
      --model Qwen/Qwen2.5-1.5B \\
      --cache-path /tmp/edgeserve-ctx \\
      --pulsar-url pulsar://localhost:6650 \\
      --port 8765

  # Then POST to ingest:
  curl -s -X POST http://localhost:8765/ingest \\
      -H 'Content-Type: application/json' \\
      -d '{"text": "def foo(): pass", "entities": ["file:foo.py"]}'
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import queue
import threading
import time
import uuid
from typing import Optional


def _server_main(args) -> None:
    os.environ.setdefault('VLLM_USE_V1', '1')

    from edgeserve.inference.vllm_kv_connector import register, set_next_request_entities
    register()

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    os.makedirs(args.cache_path, exist_ok=True)
    node_id = f'ctx-server-{uuid.uuid4().hex[:8]}'

    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={
            'pulsar_url': args.pulsar_url,
            'topic': args.topic,
            'local_cache_path': args.cache_path,
            'node_id': node_id,
        },
    )

    print(f'[context_server] Loading {args.model} ...', flush=True)
    t_load = time.perf_counter()
    llm = LLM(
        model=args.model,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
    )
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f'[context_server] Ready in {load_ms:.0f} ms  node={node_id}', flush=True)

    sp = SamplingParams(max_tokens=1, temperature=0.0)

    # Request queue: main thread pulls and calls llm.generate()
    req_q: queue.Queue = queue.Queue()
    n_ingested = [0]

    def _serve_http():
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/health':
                    body = json.dumps({
                        'status': 'ok',
                        'model': args.model,
                        'topic': args.topic,
                        'n_ingested': n_ingested[0],
                    }).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def do_POST(self):
                if self.path != '/ingest':
                    self.send_error(404)
                    return
                length = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(length))
                result_q: queue.Queue = queue.Queue()
                req_q.put({'body': body, 'result_q': result_q})
                try:
                    result = result_q.get(timeout=300)
                except queue.Empty:
                    self.send_error(504, 'Ingest timeout')
                    return
                resp = json.dumps(result).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

            def log_message(self, fmt, *args_):
                pass  # silence per-request logs

        server = http.server.HTTPServer(('0.0.0.0', args.port), Handler)
        print(f'[context_server] HTTP listening on 0.0.0.0:{args.port}', flush=True)
        server.serve_forever()

    t = threading.Thread(target=_serve_http, daemon=True)
    t.start()

    # Main loop: pull ingest requests and process them with vLLM
    while True:
        try:
            req = req_q.get(timeout=1.0)
        except queue.Empty:
            continue

        body = req['body']
        result_q = req['result_q']
        text = body.get('text', '')
        entities = body.get('entities', [])
        sha = body.get('sha', '')

        # Build entity set: user tags + optional sha tag
        ent_set = set(entities)
        if sha:
            # Add a sha-qualified variant of each entity for exact-version lookup
            ent_set.update(f'{e}@sha={sha}' for e in entities)

        if ent_set:
            set_next_request_entities(ent_set)

        t0 = time.perf_counter()
        try:
            out = llm.generate([text], sampling_params=sp, use_tqdm=False)
            ingest_ms = (time.perf_counter() - t0) * 1000
            n_tok = len(out[0].prompt_token_ids or [])
            n_ingested[0] += 1
            # The connector's wait_for_save() already ran; retrieve published UUID
            # from the NVMe cache directory (most recent .bin file)
            bin_files = sorted(
                [f for f in os.listdir(args.cache_path) if f.endswith('.bin')],
                key=lambda f: os.path.getmtime(os.path.join(args.cache_path, f)),
            )
            block_uuid = bin_files[-1][:-4] if bin_files else None
            result = {
                'block_uuid': block_uuid,
                'n_tokens': n_tok,
                'ingest_ms': round(ingest_ms),
                'entities': list(ent_set),
            }
            print(f'[context_server] ingested {n_tok} tokens in {ingest_ms:.0f} ms  '
                  f'entities={list(entities)}  uuid={block_uuid}', flush=True)
        except Exception as e:
            result = {'error': str(e)}
            print(f'[context_server] ingest error: {e}', flush=True)

        result_q.put(result)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--gpu-mem', type=float, default=0.4)
    ap.add_argument('--max-model-len', type=int, default=16384)
    ap.add_argument('--cache-path', default='/tmp/edgeserve-ctx')
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    ap.add_argument('--topic', default='kvcache-ctx-push')
    ap.add_argument('--port', type=int, default=8765)
    args = ap.parse_args()
    _server_main(args)


if __name__ == '__main__':
    main()
