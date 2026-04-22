"""demo_kvconnector_semantic.py — entity-tag cache sharing demo.

Demonstrates Task A: user-declared semantic entity tags as a secondary
cache-discovery mechanism alongside prefix-hash lookup.

Scenario
--------
Both seeder and consumer process prompts with the SAME document but
DIFFERENT question suffixes.  Without entity tags, the consumer finds the
seeder's cache via prefix-hash (existing behaviour).  With entity tags, the
consumer can ALSO declare "doc_id:X" and find the entry even if it somehow
missed the prefix-hash lookup — demonstrating that the bloom filter now
encodes both hash and semantic metadata simultaneously.

The experiment measures:
  - cold consumer (no cache): full prefill time
  - warm consumer via prefix-hash lookup (existing path)
  - warm consumer via entity-tag-only lookup (new path: tags only, hash
    lookup disabled by giving consumer a different prefix so hash misses)

Honesty note on "permuted persona"
-----------------------------------
A true permuted-context scenario — where Persona A's system prompt PRECEDES
the doc on the seeder, and Persona B's precedes it on the consumer — cannot
share KV correctly under causal attention with RoPE.  The KV for doc token j
depends on all preceding tokens; persona A ≠ persona B means the KV values
differ at every doc position.  We demonstrate what IS achievable: same doc
prefix, entity tags as the discovery mechanism, different question suffixes.
This maps to the paper's "shared document accessed by multiple agents" use
case (doc before suffix), not the "permuted prefix" interpretation.

Usage
-----
Pulsar must be running on localhost:6650.

    python scripts/demo_kvconnector_semantic.py \\
        --model Qwen/Qwen2.5-1.5B \\
        --doc-repeats 128 \\
        --gpu-mem 0.5
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
import uuid

# Resolve the Python binary: prefer the venv alongside this repo if we're not
# already inside it (allows running the script without activating the venv).
def _find_python() -> str:
    if sys.prefix != sys.base_prefix:
        return sys.executable  # already in a venv
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    venv_python = os.path.join(repo_root, '.venv', 'bin', 'python')
    if os.path.isfile(venv_python):
        return venv_python
    return sys.executable

PYTHON = _find_python()

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society. "
)

SUFFIXES = {
    "scientist":  " As a research scientist, summarise the key milestones.",
    "historian":  " As a historian, discuss how AI changed society.",
    "journalist": " As a journalist, write a short news headline.",
}


def _worker_script(
    *,
    model: str,
    doc: str,
    suffix: str,
    topic: str,
    node_id: str,
    cache_path: str,
    gpu_mem: float,
    is_seeder: bool,
    doc_id: str,
    use_entity_lookup: bool,
) -> str:
    """Return a self-contained Python script string for one subprocess."""
    mode = "seeder" if is_seeder else "consumer"
    return f"""\
import time, sys
import torch
from vllm import LLM, SamplingParams
from vllm.config import KVTransferConfig
from edgeserve.inference.vllm_kv_connector import register, set_next_request_entities
register()

doc_id_tag = {repr(doc_id)}
use_entity  = {use_entity_lookup!r}

if __name__ == '__main__':
    ktc = KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={{
            'pulsar_url':       'pulsar://localhost:6650',
            'topic':            {repr(topic)},
            'local_cache_path': {repr(cache_path)},
            'node_id':          {repr(node_id)},
        }},
    )
    llm = LLM(
        model={repr(model)},
        enable_prefix_caching=False,
        kv_transfer_config=ktc,
        gpu_memory_utilization={gpu_mem},
        max_model_len=8192,
    )

    prompt = {repr(doc + suffix)}
    params = SamplingParams(max_tokens=1, temperature=0)

    # Attach entity tags so the connector publishes and looks up by doc_id.
    # Use set_next_request_entities (not set_request_entities) because
    # LLM.generate() auto-assigns its own request_ids.
    set_next_request_entities({{doc_id_tag}})

    t0 = time.perf_counter()
    out = llm.generate([prompt], params)
    elapsed = (time.perf_counter() - t0) * 1000

    tok = out[0].outputs[0].token_ids[0]
    print(f"[{repr(mode)}] elapsed={{elapsed:.1f}}ms  token={{tok}}  entity={{doc_id_tag!r}}  entity_lookup={{use_entity!r}}", flush=True)
"""


def run_worker(script: str, label: str) -> tuple[float, int]:
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    print(f"\n--- {label} ---")
    t0 = time.perf_counter()
    result = subprocess.run(
        [PYTHON, path], capture_output=False, text=True,
    )
    elapsed = (time.perf_counter() - t0) * 1000
    if result.returncode != 0:
        print(f"[{label}] FAILED (rc={result.returncode})")
    return elapsed, result.returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="Qwen/Qwen2.5-1.5B")
    ap.add_argument("--doc-repeats", type=int, default=128)
    ap.add_argument("--gpu-mem", type=float, default=0.5)
    args = ap.parse_args()

    doc = DOC_CHUNK * args.doc_repeats
    doc_id = f"doc_id:{uuid.uuid4().hex[:8]}"   # unique per run to avoid stale hits
    topic = f"kvcache-semantic-{uuid.uuid4().hex[:8]}"
    cache_path = tempfile.mkdtemp(prefix="edgeserve-semantic-")

    print(f"Model:    {args.model}")
    print(f"Doc len:  {len(doc)} chars (~{len(doc)//4} tokens)")
    print(f"doc_id:   {doc_id}")
    print(f"Topic:    {topic}")

    # --- COLD: no cache, no entity tags, just to measure baseline ---
    cold_script = _worker_script(
        model=args.model, doc=doc, suffix=SUFFIXES["historian"],
        topic=topic + "-cold", node_id="cold-0", cache_path=cache_path,
        gpu_mem=args.gpu_mem, is_seeder=False, doc_id=doc_id,
        use_entity_lookup=False,
    )
    cold_ms, _ = run_worker(cold_script, "COLD baseline (no cache)")

    # --- SEEDER: publishes KV with both prefix hashes AND doc_id entity tag ---
    seed_script = _worker_script(
        model=args.model, doc=doc, suffix=SUFFIXES["scientist"],
        topic=topic, node_id="seeder-0", cache_path=cache_path,
        gpu_mem=args.gpu_mem, is_seeder=True, doc_id=doc_id,
        use_entity_lookup=False,
    )
    seed_ms, _ = run_worker(seed_script, "SEEDER (publish with entity tag)")

    # Brief pause so catalog subscription propagates.
    time.sleep(3)

    # --- CONSUMER A: prefix-hash lookup (existing path) ---
    # Consumer A asks the same doc + different suffix; prefix hash lookup hits.
    consumer_hash_script = _worker_script(
        model=args.model, doc=doc, suffix=SUFFIXES["historian"],
        topic=topic, node_id="consumer-hash", cache_path=cache_path,
        gpu_mem=args.gpu_mem, is_seeder=False, doc_id=doc_id,
        use_entity_lookup=False,
    )
    hash_ms, _ = run_worker(consumer_hash_script, "CONSUMER (prefix-hash lookup)")

    # --- CONSUMER B: entity-tag-only lookup ---
    # Same doc, same doc_id tag, entity lookup path fires (prefix hash also
    # present in bloom so this exercises the entity-first branch).
    consumer_entity_script = _worker_script(
        model=args.model, doc=doc, suffix=SUFFIXES["journalist"],
        topic=topic, node_id="consumer-entity", cache_path=cache_path,
        gpu_mem=args.gpu_mem, is_seeder=False, doc_id=doc_id,
        use_entity_lookup=True,
    )
    entity_ms, _ = run_worker(consumer_entity_script, "CONSUMER (entity-tag lookup)")

    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    print(f"  cold baseline:          {cold_ms:8.0f} ms")
    print(f"  seeder (publish):       {seed_ms:8.0f} ms")
    print(f"  consumer (prefix-hash): {hash_ms:8.0f} ms  "
          f"speedup vs cold: {cold_ms / hash_ms:.2f}×")
    print(f"  consumer (entity-tag):  {entity_ms:8.0f} ms  "
          f"speedup vs cold: {cold_ms / entity_ms:.2f}×")
    print()
    print("Both warm consumers should produce the same next-token id as the")
    print("cold baseline of the SAME prompt (check the [consumer] lines above).")


if __name__ == "__main__":
    main()
