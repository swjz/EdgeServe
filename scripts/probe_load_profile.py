"""Profile where time goes in load_past_key_values / load path.

Simulates the connector's load: serialize fake KV tensors, write them
to disk, load via safetensors + .to(cuda) per layer. Breaks out each
substep so we can see which optimizations would help.
"""

import argparse
import time
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-layers', type=int, default=24,
                        help='Qwen-0.5B = 24, Qwen-1.5B = 28')
    parser.add_argument('--tokens', type=int, default=4096)
    parser.add_argument('--head-dim', type=int, default=128)
    parser.add_argument('--num-kv-heads', type=int, default=2)
    parser.add_argument('--iters', type=int, default=3)
    parser.add_argument('--device', default='cuda')
    args = parser.parse_args()

    import torch
    from safetensors.torch import save as st_save, load as st_load
    from safetensors import safe_open

    # Build fake saved tensors in the Flash-attention layout:
    # each layer's saved tensor has shape (2, tokens, num_kv_heads * head_dim).
    hidden = args.num_kv_heads * args.head_dim
    layers = {}
    for i in range(args.num_layers):
        layers[f'model.layers.{i}.self_attn.attn'] = torch.randn(
            2, args.tokens, hidden, dtype=torch.bfloat16,
        )

    total_bytes = sum(v.numel() * v.element_size() for v in layers.values())
    print(f'workload: {args.num_layers} layers x (2, {args.tokens}, {hidden}) bf16 '
          f'= {total_bytes/1e6:.1f} MB')
    print(f'device: {args.device}')

    # Warmup cuda
    if args.device == 'cuda':
        x = torch.randn(1024, 1024, device='cuda')
        _ = x @ x.T
        torch.cuda.synchronize()

    # Reference timings:
    for it in range(args.iters):
        t0 = time.perf_counter()
        blob = st_save(layers)
        t_save = time.perf_counter() - t0

        t0 = time.perf_counter()
        tensors = st_load(blob)
        t_load = time.perf_counter() - t0

        t0 = time.perf_counter()
        migrated = {k: v.to(args.device, non_blocking=True) for k, v in tensors.items()}
        if args.device == 'cuda':
            torch.cuda.synchronize()
        t_h2d = time.perf_counter() - t0

        print(f'iter {it}: save={t_save*1000:.1f}ms '
              f'st.load(bytes)={t_load*1000:.1f}ms '
              f'.to(cuda)={t_h2d*1000:.1f}ms '
              f'total={(t_save+t_load+t_h2d)*1000:.1f}ms')

    # Alternative: safe_open from file on disk with device='cuda'
    tmp = '/tmp/probe_load_profile.safetensors'
    print(f'\n--- mmap path (safe_open from file with device={args.device})')
    for it in range(args.iters):
        with open(tmp, 'wb') as f:
            f.write(blob)

        t0 = time.perf_counter()
        with safe_open(tmp, framework='pt', device=args.device) as st:
            loaded = {k: st.get_tensor(k) for k in st.keys()}
        if args.device == 'cuda':
            torch.cuda.synchronize()
        t_mmap = time.perf_counter() - t0
        print(f'iter {it}: safe_open+get_tensor+sync = {t_mmap*1000:.1f}ms')

    os.unlink(tmp) if os.path.exists(tmp) else None


if __name__ == '__main__':
    main()
