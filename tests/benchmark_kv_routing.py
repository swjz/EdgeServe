"""Benchmark for Semantic Cache Routing vs baselines.

Scenario:
  A long shared document (the "codebase chunk" from the paper) plus K agents,
  each with a unique short suffix (persona + task). The document is placed
  FIRST in the prompt so its KV cache is reusable -- this is the scenario
  where cache reuse actually yields a speedup; permuted-prefix scenarios
  require a different attention trick that's out of scope here.

Baselines:
  eager       Each of K "nodes" forward-passes (doc + suffix) independently.
              No cache sharing. Represents distributed deployment without
              any cross-node cache.

  prefix-oracle
              One process forward-passes the doc once, then reuses
              `past_key_values` for all K suffixes. Represents the best
              case a single-node prefix cache (vLLM prefix caching,
              SGLang RadixAttention) could achieve on this workload.

  ours        Node A prefills the doc and publishes a Semantic Cache
              Routing header. Nodes B..K discover via bloom filter, fetch
              KV bytes from A over HTTP, and resume generation. Distributed
              with cross-node reuse. Costs include safetensors serialize +
              HTTP round trip + deserialize.

  vllm        Real vLLM if importable, else noted as skipped.
  sglang      Real SGLang if importable, else noted as skipped.

Usage:
  python3 tests/benchmark_kv_routing.py \\
      [--model HF_MODEL_ID] \\
      [--doc-tokens N] [--suffix-tokens M] [--num-agents K] \\
      [--device auto|cpu|mps|cuda] \\
      [--baselines eager,prefix-oracle,ours]

Defaults are small enough to run on CPU in a few seconds; the numbers only
get interesting on a GPU with a real model. The code is portable, so the
same script is expected to produce meaningful deltas on the NVIDIA boxes.
"""

import argparse
import statistics
import sys
import time
from typing import List


# ---------------------------------------------------------------------------
# Mock Pulsar must be installed before any edgeserve.semantic_cache.* imports
# that pull in pulsar. Easy with lazy __init__.
from edgeserve.semantic_cache import mock_pulsar
mock_pulsar.install()


def _pick_device(requested: str) -> str:
    if requested != 'auto':
        return requested
    try:
        import torch
        if torch.cuda.is_available():
            return 'cuda'
        if getattr(torch.backends, 'mps', None) and torch.backends.mps.is_available():
            return 'mps'
    except ImportError:
        pass
    return 'cpu'


def _load_model(model_id: str, device: str, dtype_name: str = 'auto'):
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(model_id)
    if tok.pad_token_id is None:
        tok.pad_token = tok.eos_token
    dtype_map = {
        'fp32': torch.float32, 'float32': torch.float32,
        'fp16': torch.float16, 'float16': torch.float16, 'half': torch.float16,
        'bf16': torch.bfloat16, 'bfloat16': torch.bfloat16,
    }
    if dtype_name == 'auto':
        dtype = torch.bfloat16 if device == 'cuda' else torch.float32
    else:
        dtype = dtype_map[dtype_name]
    model = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=dtype)
    model.to(device)
    model.eval()
    return model, tok


def _build_workload(tok, doc_tokens: int, suffix_tokens: int, num_agents: int):
    """Return (doc_ids, [suffix_ids, ...]) sized to roughly the requested lengths."""
    import torch

    # A filler doc so we hit the requested prefix length. Content doesn't matter
    # for timing. Tokenize a phrase and tile it.
    filler = 'Chicago is on Lake Michigan and was founded in eighteen thirty three. '
    repeated = filler * max(1, doc_tokens // max(1, len(tok.encode(filler))) + 2)
    doc_ids = tok(repeated, return_tensors='pt').input_ids[:, :doc_tokens]

    suffixes = []
    personas = [
        ' As an SRE, list infra concerns.',
        ' As a historian, highlight three dates.',
        ' As a tourist, suggest two activities.',
        ' As a biologist, mention the lake ecosystem.',
        ' As a data engineer, estimate file sizes.',
    ]
    for i in range(num_agents):
        text = personas[i % len(personas)] + ' ' + (f'Reply {i}. ' * 20)
        ids = tok(text, return_tensors='pt', add_special_tokens=False).input_ids
        suffixes.append(ids[:, :suffix_tokens])
    return doc_ids, suffixes


# ---------------------------------------------------------------------------
# Baselines


def run_eager(model, doc_ids, suffix_ids_list) -> float:
    """Each agent forward-passes (doc + suffix) from scratch."""
    import torch
    start = time.perf_counter()
    with torch.no_grad():
        for suf in suffix_ids_list:
            full = torch.cat([doc_ids, suf], dim=1)
            _ = model(input_ids=full, use_cache=False)
    return time.perf_counter() - start


def run_prefix_oracle(model, doc_ids, suffix_ids_list) -> float:
    """Doc prefilled once; each agent continues with `past_key_values` reuse.

    Represents the best case a same-process prefix cache (vLLM / SGLang radix)
    could achieve on this workload: one doc prefill, K short continuations.
    The per-agent cost is a zero-copy tensor clone of the legacy tuple (KV
    updates are mutative, so we can't share one cache across agents safely).
    """
    import torch
    from edgeserve.semantic_cache.kv_io import _as_legacy

    def _clone_legacy(legacy):
        try:
            from transformers.cache_utils import DynamicCache
            return DynamicCache(tuple((k.clone(), v.clone()) for k, v in legacy))
        except ImportError:
            return tuple((k.clone(), v.clone()) for k, v in legacy)

    start = time.perf_counter()
    with torch.no_grad():
        doc_out = model(input_ids=doc_ids, use_cache=True)
        legacy_template = _as_legacy(doc_out.past_key_values)
        for suf in suffix_ids_list:
            pkv = _clone_legacy(legacy_template)
            _ = model(input_ids=suf, past_key_values=pkv, use_cache=True)
    return time.perf_counter() - start


def run_ours(model, tok_device, doc_ids, suffix_ids_list, tmp_dir) -> dict:
    """Node A prefills the doc; nodes B.. discover and fetch over HTTP."""
    import os
    import torch
    import uuid as uuid_mod

    from edgeserve.semantic_cache import SemanticCacheClient
    from edgeserve.semantic_cache.kv_io import (
        load_past_key_values, save_past_key_values,
    )

    topic = f'bench-{uuid_mod.uuid4().hex[:8]}'
    a_dir = os.path.join(tmp_dir, 'node-a')
    os.makedirs(a_dir, exist_ok=True)

    timings = {'publish': 0.0, 'resolve+fetch': 0.0, 'continue': 0.0, 'total': 0.0}

    node_a = SemanticCacheClient(
        pulsar_node='pulsar://mock', node_id='node-a',
        local_cache_path=a_dir, http_host='127.0.0.1',
        topic=topic, bloom_capacity=32,
    )

    consumers: List[SemanticCacheClient] = []
    for i in range(len(suffix_ids_list)):
        d = os.path.join(tmp_dir, f'node-{i+1}')
        os.makedirs(d, exist_ok=True)
        consumers.append(SemanticCacheClient(
            pulsar_node='pulsar://mock', node_id=f'node-{i+1}',
            local_cache_path=d, http_host='127.0.0.1',
            topic=topic, bloom_capacity=32,
        ))

    try:
        grand_start = time.perf_counter()
        # --- node-a publishes
        t0 = time.perf_counter()
        with torch.no_grad():
            pkv = model(input_ids=doc_ids, use_cache=True).past_key_values
        blob = save_past_key_values(pkv)
        doc_tag = f'doc-{uuid_mod.uuid4().hex[:8]}'
        node_a.publish({doc_tag}, blob)
        timings['publish'] = time.perf_counter() - t0

        # Give catalogs a beat to consume the header.
        for c in consumers:
            deadline = time.time() + 2.0
            while time.time() < deadline and not c.catalog.lookup({doc_tag}):
                time.sleep(0.01)

        for node_b, suf in zip(consumers, suffix_ids_list):
            t0 = time.perf_counter()
            hit = node_b.resolve({doc_tag})
            assert hit is not None, 'cache miss in ours-benchmark'
            fetched_bytes, _ = hit
            pkv_b = load_past_key_values(fetched_bytes, device=tok_device)
            timings['resolve+fetch'] += time.perf_counter() - t0

            t0 = time.perf_counter()
            with torch.no_grad():
                _ = model(input_ids=suf, past_key_values=pkv_b, use_cache=True)
            timings['continue'] += time.perf_counter() - t0

        timings['total'] = time.perf_counter() - grand_start
    finally:
        for c in consumers:
            c.close()
        node_a.close()

    return timings


def _try_real_vllm():
    try:
        import vllm  # noqa: F401
        return True
    except ImportError:
        return False


def _try_real_sglang():
    try:
        import sglang  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Harness


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='sshleifer/tiny-gpt2',
                        help='HF model id; for realistic numbers use e.g. '
                             'HuggingFaceTB/SmolLM2-135M or a 1B+ model on GPU')
    parser.add_argument('--doc-tokens', type=int, default=512)
    parser.add_argument('--suffix-tokens', type=int, default=64)
    parser.add_argument('--num-agents', type=int, default=3)
    parser.add_argument('--device', default='auto',
                        choices=['auto', 'cpu', 'mps', 'cuda'])
    parser.add_argument('--dtype', default='auto',
                        choices=['auto', 'fp32', 'fp16', 'bf16'],
                        help='auto=bf16 on cuda, fp32 elsewhere')
    parser.add_argument('--baselines', default='eager,prefix-oracle,ours',
                        help='comma-separated from eager, prefix-oracle, ours, vllm, sglang')
    parser.add_argument('--repeats', type=int, default=3,
                        help='number of timed iterations per baseline')
    args = parser.parse_args(argv)

    device = _pick_device(args.device)
    print(f'device = {device}')
    print(f'model  = {args.model}')

    try:
        import torch  # noqa: F401
    except ImportError:
        print('torch not available; install torch+transformers to run this benchmark')
        return 2

    model, tok = _load_model(args.model, device, args.dtype)
    doc_ids, suffixes = _build_workload(tok, args.doc_tokens, args.suffix_tokens, args.num_agents)
    doc_ids = doc_ids.to(device)
    suffixes = [s.to(device) for s in suffixes]
    print(f'workload: doc={doc_ids.shape[1]} tok, suffix={suffixes[0].shape[1]} tok, '
          f'agents={len(suffixes)}')

    selected = [b.strip() for b in args.baselines.split(',') if b.strip()]

    # Warmup: several forward passes to JIT / stabilize device allocators.
    # On MPS/CUDA the first few calls pay one-time kernel compile + memory
    # pool growth costs that would otherwise be charged to whichever baseline
    # ran first.
    import torch
    with torch.no_grad():
        for _ in range(3):
            _ = model(input_ids=doc_ids, use_cache=True)
            _ = model(input_ids=doc_ids, use_cache=False)
    if device == 'mps':
        torch.mps.synchronize()
    elif device == 'cuda':
        torch.cuda.synchronize()

    import tempfile
    results = {}

    for name in selected:
        times = []
        for _ in range(args.repeats):
            if name == 'eager':
                t = run_eager(model, doc_ids, suffixes)
                times.append(t)
            elif name == 'prefix-oracle':
                t = run_prefix_oracle(model, doc_ids, suffixes)
                times.append(t)
            elif name == 'ours':
                with tempfile.TemporaryDirectory() as tmp:
                    detail = run_ours(model, device, doc_ids, suffixes, tmp)
                times.append(detail['total'])
                results.setdefault(name, {}).update(detail_last=detail)
            elif name == 'vllm':
                if not _try_real_vllm():
                    print('[vllm] not installed; skipped. Install vllm and re-run on GPU.')
                    break
                print('[vllm] real path not implemented yet (TODO); skipped')
                break
            elif name == 'sglang':
                if not _try_real_sglang():
                    print('[sglang] not installed; skipped. Install sglang and re-run on GPU.')
                    break
                print('[sglang] real path not implemented yet (TODO); skipped')
                break
            else:
                print(f'unknown baseline: {name}')
                return 1
            mock_pulsar.reset_broker()  # between repeats for `ours`
        if times:
            results.setdefault(name, {})['times'] = times

    # Report.
    print()
    print(f'{"baseline":<18} {"median(s)":>10} {"min(s)":>8} {"max(s)":>8}')
    print('-' * 46)
    base_median = None
    for name, entry in results.items():
        times = entry.get('times', [])
        if not times:
            continue
        med = statistics.median(times)
        if name == 'eager':
            base_median = med
        suffix = ''
        if base_median and name != 'eager':
            suffix = f'   speedup x{base_median / med:.2f}'
        print(f'{name:<18} {med:>10.4f} {min(times):>8.4f} {max(times):>8.4f}{suffix}')

    if 'ours' in results and 'detail_last' in results['ours']:
        d = results['ours']['detail_last']
        print()
        print('ours breakdown (last repeat, seconds):')
        print(f'  publish (prefill + safetensors serialize + header pub): {d["publish"]:.4f}')
        print(f'  resolve + HTTP fetch + deserialize (sum across agents): {d["resolve+fetch"]:.4f}')
        print(f'  per-agent suffix continuation (sum):                    {d["continue"]:.4f}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
