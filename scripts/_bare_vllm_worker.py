"""Bare-bones vLLM worker: one process, one prompt, report init + gen timings.
No connector, no prefix cache, no cache routing at all."""

import argparse
import json
import os
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prompt', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=4096)
    args = parser.parse_args()

    os.environ.setdefault('VLLM_USE_V1', '1')

    from vllm import LLM, SamplingParams

    t0 = time.perf_counter()
    llm = LLM(
        model=args.model, dtype=args.dtype,
        gpu_memory_utilization=args.gpu_mem,
        max_model_len=args.max_model_len,
        enable_prefix_caching=False,
    )
    init_ms = (time.perf_counter() - t0) * 1000

    sp = SamplingParams(max_tokens=1, temperature=0.0)

    t0 = time.perf_counter()
    out = llm.generate(prompts=[args.prompt], sampling_params=sp, use_tqdm=False)
    gen_ms = (time.perf_counter() - t0) * 1000

    result = {
        'init_ms': init_ms,
        'gen_ms': gen_ms,
        'output_token': int(out[0].outputs[0].token_ids[0]),
    }
    print(f'RESULT {json.dumps(result)}', flush=True)


if __name__ == '__main__':
    sys.exit(main())
