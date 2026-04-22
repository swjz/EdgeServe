"""Single-process baseline comparison across engines.

Runs the same N-agent / shared-doc workload through three backends and
reports wall-clock wins vs the no-cache floor:

  * hf-eager         HFEngine, each agent runs full prefill from scratch
                     (no cache reuse). The honest floor.
  * hf-oracle        HFEngine with past_key_values reused across agents
                     inside one process. Approximates what a single-node
                     prefix cache could achieve on this workload.
  * vllm-prefix      vLLM with enable_prefix_caching=True running all
                     agents sequentially in one process. Exercises vLLM's
                     built-in radix prefix cache.
  * sglang-radix     (If sglang is installed.) Same idea, SGLang's
                     RadixAttention.

This is orthogonal to the Phase-3 multi-process benchmark. That one
tests our cross-process cache routing; this one tests what single-process
engines can already do with their own caches on the same workload -- the
ceiling we want to approach across processes.

Usage:
    python scripts/bench_engines.py \\
        --model Qwen/Qwen2.5-1.5B --doc-tokens 4096 \\
        --num-agents 4 --dtype bf16 \\
        --engines hf-eager,hf-oracle,vllm-prefix,sglang-radix

Each engine is loaded only if selected in --engines.
"""

import argparse
import importlib
import statistics
import sys
import time
from typing import List


def _workload(tok, doc_tokens: int, suffix_tokens: int, num_agents: int):
    """Return (doc_ids_list, [suffix_ids_list, ...]).

    We avoid tokenizing in every benchmark fn to keep timings clean.
    Returns plain python int lists so they're portable between HF and vLLM.
    """
    filler = 'Chicago is on Lake Michigan and was founded in 1833. '
    doc_tokens_ids = tok.encode(filler * (doc_tokens // 10 + 4),
                                add_special_tokens=False)[:doc_tokens]
    personas = [
        'As an SRE, list three infra concerns.',
        'As a historian, highlight key dates.',
        'As a tourist, suggest two activities.',
        'As a biologist, discuss the ecosystem.',
        'As a journalist, summarize the article.',
        'As a poet, write a short verse.',
        'As a lawyer, flag risk language.',
        'As a student, outline the material.',
    ]
    suffixes = []
    for i in range(num_agents):
        text = personas[i % len(personas)] + f' Reply {i}. ' * max(1, suffix_tokens // 10)
        ids = tok.encode(text, add_special_tokens=False)[:suffix_tokens]
        suffixes.append(ids)
    return doc_tokens_ids, suffixes


# ---------------------------------------------------------------------------
# HF baselines (duplicates what benchmark_kv_routing.py does; here for
# side-by-side comparison in one invocation)

def bench_hf_eager(model, tok, doc_ids, suffixes, max_new, device) -> float:
    import torch
    start = time.perf_counter()
    with torch.no_grad():
        for s in suffixes:
            ids = torch.tensor([doc_ids + s], dtype=torch.long, device=device)
            _ = model(input_ids=ids, use_cache=False)
    if device == 'cuda':
        torch.cuda.synchronize()
    return time.perf_counter() - start


def bench_hf_oracle(model, tok, doc_ids, suffixes, max_new, device) -> float:
    import torch
    from transformers.cache_utils import DynamicCache
    start = time.perf_counter()
    with torch.no_grad():
        doc_t = torch.tensor([doc_ids], dtype=torch.long, device=device)
        out = model(input_ids=doc_t, use_cache=True)
        pkv_legacy = tuple(
            (L.keys, L.values) for L in out.past_key_values.layers
        )
        for s in suffixes:
            pkv = DynamicCache(tuple((k.clone(), v.clone()) for k, v in pkv_legacy))
            suf_t = torch.tensor([s], dtype=torch.long, device=device)
            _ = model(input_ids=suf_t, past_key_values=pkv, use_cache=True)
    if device == 'cuda':
        torch.cuda.synchronize()
    return time.perf_counter() - start


# ---------------------------------------------------------------------------
# vLLM baseline: one LLM instance, sequential requests with prefix caching.

def bench_vllm_prefix(model_id, dtype, doc_ids, suffixes, max_new, gpu_mem) -> dict:
    from vllm import LLM, SamplingParams

    # Prefix caching ON: vLLM's internal radix cache will hit on the shared
    # doc_ids prefix from the second request onwards.
    llm_kwargs = dict(
        model=model_id,
        dtype=dtype,
        enable_prefix_caching=True,
        gpu_memory_utilization=gpu_mem,
    )
    # Keep the default max_model_len unless the doc is very long.
    total_len = len(doc_ids) + max(len(s) for s in suffixes) + max_new + 16
    if total_len > 2048:
        llm_kwargs['max_model_len'] = int(total_len * 1.25)
    llm = LLM(**llm_kwargs)

    sp = SamplingParams(max_tokens=max_new, temperature=0.0)
    prompts = [list(doc_ids) + list(s) for s in suffixes]

    # Warmup pass (prime CUDA kernels and fill prefix cache for the doc).
    # vLLM 0.19 accepts list[int] or list[list[int]] as `prompts`.
    llm.generate(prompts=[prompts[0]], sampling_params=sp, use_tqdm=False)

    # Timed pass: all N requests. First one is redundant with warmup (cache
    # is already populated), but we want to measure the steady state of
    # N requests sharing a prefix. Comparable to the eager/oracle loops above.
    start = time.perf_counter()
    llm.generate(prompts=prompts, sampling_params=sp, use_tqdm=False)
    wall = time.perf_counter() - start
    return {'time_s': wall}


# ---------------------------------------------------------------------------
# SGLang baseline (optional; installed separately).

def bench_sglang_radix(model_id, dtype, doc_ids, suffixes, max_new) -> dict:
    """Run the same workload through sglang's offline Engine.

    RadixAttention (sglang's prefix cache equivalent) is ON by default
    (`disable_radix_cache=False`); we leave it that way. Memory is kept
    modest via `mem_fraction_static` so sglang doesn't fight HF for VRAM
    when both engines are benched in the same invocation.
    """
    from sglang.srt.entrypoints.engine import Engine

    engine = Engine(
        model_path=model_id,
        dtype=dtype,
        mem_fraction_static=0.5,
        log_level='error',
        # Triton attention backend avoids sglang's flashinfer JIT which needs
        # a C++20 compiler not present on Ubuntu 20.04 (gcc 9). Slower than
        # flashinfer but installable on any box with CUDA + triton.
        attention_backend='triton',
        disable_cuda_graph=True,  # sidestep graph capture on older CUDA setups
    )
    try:
        sp = {'max_new_tokens': max_new, 'temperature': 0.0}
        # Warmup pass.
        engine.generate(input_ids=[list(doc_ids) + list(suffixes[0])],
                        sampling_params=sp)
        # Timed pass: all N requests at once, so radix cache hits on the
        # shared prefix across them.
        start = time.perf_counter()
        engine.generate(
            input_ids=[list(doc_ids) + list(s) for s in suffixes],
            sampling_params=sp,
        )
        wall = time.perf_counter() - start
        return {'time_s': wall}
    finally:
        try:
            engine.shutdown()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Harness

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    parser.add_argument('--num-agents', type=int, default=4)
    parser.add_argument('--doc-tokens', type=int, default=2048)
    parser.add_argument('--suffix-tokens', type=int, default=32)
    parser.add_argument('--max-new-tokens', type=int, default=1)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--dtype', default='bf16', choices=['fp32', 'fp16', 'bf16'])
    parser.add_argument('--device', default='cuda', choices=['cpu', 'mps', 'cuda'])
    parser.add_argument('--engines', default='hf-eager,hf-oracle,vllm-prefix',
                        help='comma list: hf-eager, hf-oracle, vllm-prefix, sglang-radix')
    parser.add_argument('--gpu-memory-utilization', type=float, default=0.5,
                        help='vLLM gpu_memory_utilization (leave headroom for HF models')
    args = parser.parse_args()

    selected = [e.strip() for e in args.engines.split(',') if e.strip()]
    results = {}

    # Load HF tokenizer once so all engines agree on token ids.
    from transformers import AutoTokenizer
    tok = AutoTokenizer.from_pretrained(args.model)
    if tok.pad_token_id is None:
        tok.pad_token = tok.eos_token
    doc_ids, suffixes = _workload(tok, args.doc_tokens, args.suffix_tokens, args.num_agents)
    print(f'workload: doc={len(doc_ids)} tok, suffix~={len(suffixes[0])} tok, '
          f'agents={len(suffixes)}, max_new={args.max_new_tokens}')

    # HF baselines share one model load.
    hf_loaded = False
    hf_model = None
    if {'hf-eager', 'hf-oracle'} & set(selected):
        print('loading HF model...')
        import torch
        from transformers import AutoModelForCausalLM
        dtype_map = {'fp32': torch.float32, 'fp16': torch.float16, 'bf16': torch.bfloat16}
        hf_model = AutoModelForCausalLM.from_pretrained(
            args.model, torch_dtype=dtype_map[args.dtype],
        ).to(args.device).eval()
        # Warmup.
        with torch.no_grad():
            for _ in range(2):
                _ = hf_model(
                    input_ids=__import__('torch').tensor(
                        [doc_ids], dtype=__import__('torch').long, device=args.device
                    ),
                    use_cache=True,
                )
        if args.device == 'cuda':
            torch.cuda.synchronize()
        hf_loaded = True

    # Run HF engines first while hf_model is loaded; free it BEFORE any vLLM /
    # SGLang engine spins up, otherwise they collide on GPU memory.
    hf_engines = [e for e in selected if e.startswith('hf-')]
    other_engines = [e for e in selected if not e.startswith('hf-')]

    for name in hf_engines:
        times = []
        print(f'\n[{name}] running {args.repeats} repeats...')
        try:
            for _ in range(args.repeats):
                if name == 'hf-eager':
                    t = bench_hf_eager(hf_model, tok, doc_ids, suffixes,
                                       args.max_new_tokens, args.device)
                elif name == 'hf-oracle':
                    t = bench_hf_oracle(hf_model, tok, doc_ids, suffixes,
                                        args.max_new_tokens, args.device)
                else:
                    print(f'  unknown hf engine: {name}')
                    break
                times.append(t)
        except Exception as e:
            print(f'  [{name}] failed: {e}')
            continue
        if times:
            results[name] = times

    # Free the HF model before vLLM / SGLang claim GPU memory.
    if hf_loaded:
        del hf_model
        import gc
        gc.collect()
        if args.device == 'cuda':
            import torch
            torch.cuda.empty_cache()
            torch.cuda.synchronize()

    for name in other_engines:
        times = []
        print(f'\n[{name}] running...')
        try:
            if name == 'vllm-prefix':
                out = bench_vllm_prefix(
                    args.model,
                    {'fp32': 'float32', 'fp16': 'float16', 'bf16': 'bfloat16'}[args.dtype],
                    doc_ids, suffixes, args.max_new_tokens,
                    args.gpu_memory_utilization,
                )
                times.append(out['time_s'])
            elif name == 'sglang-radix':
                out = bench_sglang_radix(
                    args.model,
                    {'fp32': 'float32', 'fp16': 'float16', 'bf16': 'bfloat16'}[args.dtype],
                    doc_ids, suffixes, args.max_new_tokens,
                )
                times.append(out['time_s'])
            else:
                print(f'  unknown engine: {name}')
                continue
        except Exception as e:
            print(f'  [{name}] failed: {e}')
            continue
        if times:
            results[name] = times

    # Report.
    print('\n=== summary ===')
    print(f'{"engine":<18} {"median(s)":>10} {"min":>8} {"max":>8}')
    base = None
    if 'hf-eager' in results:
        base = statistics.median(results['hf-eager'])
    for name, times in results.items():
        med = statistics.median(times)
        suffix = ''
        if base and name != 'hf-eager':
            suffix = f'   x{base / med:.2f}'
        print(f'{name:<18} {med:>10.4f} {min(times):>8.4f} {max(times):>8.4f}{suffix}')


if __name__ == '__main__':
    main()
