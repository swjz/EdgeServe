"""Isolate what makes the FIRST cold vLLM subprocess slower than subsequent
cold subprocesses (each with no catalog, no connector, fresh process).

Hypothesis pre-test: vLLM's torch.compile disk cache. Each fresh process
rebuilds compiled kernels the first time they're triggered; those
artifacts land in ~/.cache/vllm/torch_compile_cache/... The second
subprocess, even fresh, reuses them.

Test method: run N=4 subprocesses back-to-back with DIFFERENT prompts
(so prefix cache is irrelevant). No connector. Just vLLM with
enable_prefix_caching=False. Measure each subprocess's first generate().

If all 4 times are similar -> no compile-cache-per-subprocess effect.
If run 1 >> runs 2-4 -> yes, compile cache carries across processes.
"""

import argparse
import json
import os
import subprocess
import sys
import time


HERE = os.path.dirname(os.path.abspath(__file__))


def run(prompt, args):
    cmd = [
        sys.executable,
        os.path.join(HERE, '_bare_vllm_worker.py'),
        '--prompt', prompt, '--model', args.model, '--dtype', args.dtype,
        '--gpu-mem', str(args.gpu_mem),
        '--max-model-len', str(args.max_model_len),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    r = None
    for line in proc.stdout.splitlines():
        if line.startswith('RESULT '):
            r = json.loads(line[len('RESULT '):])
    return r


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='Qwen/Qwen2.5-0.5B')
    parser.add_argument('--dtype', default='bfloat16')
    parser.add_argument('--gpu-mem', type=float, default=0.55)
    parser.add_argument('--max-model-len', type=int, default=4096)
    parser.add_argument('--runs', type=int, default=4)
    parser.add_argument('--wipe-cache', action='store_true',
                        help='before each run, delete ~/.cache/vllm/torch_compile_cache/')
    args = parser.parse_args()

    doc = 'Chicago is on Lake Michigan and was founded in 1833. ' * 64

    # Optionally nuke vllm compile cache before run 1 only
    if args.wipe_cache:
        cache = os.path.expanduser('~/.cache/vllm/torch_compile_cache')
        if os.path.exists(cache):
            import shutil
            shutil.rmtree(cache)
            print(f'wiped {cache}')

    print(f'runs={args.runs}, each a fresh vLLM subprocess, different prompts, '
          'no connector, no prefix cache')
    print()
    for i in range(args.runs):
        # Different prompt per run so prefix cache never helps
        prompt = doc + f'\n\nUnique tail for run {i} {time.time_ns()}'
        r = run(prompt, args)
        if r is None:
            print(f'  run {i}: FAILED')
            continue
        print(f'  run {i}: init={r["init_ms"]:.0f}ms  '
              f'gen={r["gen_ms"]:.1f}ms  token={r["output_token"]}')


if __name__ == '__main__':
    main()
