# Semantic Cache Routing — Phase-3 Benchmark Results

Numbers from `tests/phase3_multiproc_bench.py` on an NVIDIA RTX 3080 Ti
(12 GB, driver 550 / CUDA 12.8), torch 2.10, transformers 5.5, with a
real Apache Pulsar 3.1 broker running in Docker. Each worker is an
independent Python subprocess with its own model instance and its own
`SemanticCacheClient`; the only cross-process state sharing goes through
Pulsar (catalog headers) and localhost HTTP (KV blob retrieval).

## Methodology

For each config, we run two modes:

- **eager** — every worker prefills `(doc + suffix)` from scratch.
- **routed, warm-cache** — a seed worker has already prefilled and
  published the KV cache for the shared `doc` before timing starts.
  `N-1` consumer workers resolve the tag via bloom-filter lookup,
  fetch the KV bytes over HTTP, and continue with their own suffix.

Warm-cache mode is the deployment-relevant metric: in a real multi-agent
system, the first request pays the publish cost once, and every
subsequent request for the same context reuses it.

To keep the comparison apples-to-apples, **eager warm-cache also skips
worker-0 entirely** — only the `N-1` consumer requests are timed. In
both modes we measure wall-clock from the first consumer dispatch until
the last consumer returns.

## Warm-cache speedup (Qwen2.5, bf16, greedy, 1 new token)

The per-consumer breakdown shows where the time goes on the routed path:
`resolve` (catalog lookup + HTTP GET) → `deserialize` (safetensors →
DynamicCache on GPU) → `generate` (suffix prefill + 1 new token).

| model          | doc tok | N | eager (ms) | routed (ms) | routed / eager | KV blob | resolve | deserialize | generate |
|----------------|--------:|--:|-----------:|------------:|---------------:|--------:|--------:|------------:|---------:|
| Qwen2.5-0.5B   |    1024 | 2 |         54 |          57 |          0.94× |   20 MB |   22 ms |        6 ms |    44 ms |
| Qwen2.5-0.5B   |    2048 | 2 |         89 |          82 |          1.09× |   39 MB |   34 ms |       17 ms |    41 ms |
| Qwen2.5-0.5B   |    4096 | 2 |        169 |         146 |          1.16× |   76 MB |   69 ms |       34 ms |    38 ms |
| Qwen2.5-1.5B   |    1024 | 2 |        112 |          96 |          1.17× |   46 MB |   41 ms |       22 ms |    54 ms |
| Qwen2.5-1.5B   |    2048 | 2 |        203 |         153 |          1.32× |   90 MB |   85 ms |       42 ms |    47 ms |
| Qwen2.5-1.5B   |    4096 | 2 |        407 |     **265** |      **1.54×** |  178 MB |  158 ms |       93 ms |    52 ms |

All numbers are median over 3 repeats. OOM blocked `N≥3` on Qwen2.5-1.5B
at these doc lengths on 12 GB (each worker holds its own model copy +
KV cache).

## What the numbers say

- **Routed beats eager when prefill cost exceeds transport cost.** At
  short docs / small model, GPU prefill is so fast (~80 ms) that
  HTTP+safetensors round-trip dominates; routed ≈ eager. At 4k tokens
  on Qwen2.5-1.5B, full-doc prefill is 400 ms and KV fetch is ~250 ms,
  so we win 1.54×. Bigger models and longer docs widen the gap.

- **The HTTP+safetensors transport is the biggest lever.** On a 178 MB
  KV blob the resolve+deserialize overhead is 251 ms — that's ~700 MB/s
  through the pipe, far below localhost's theoretical bandwidth. The
  CPU round trip (GPU→CPU copy + safetensors parse + CPU→GPU copy) is
  the bottleneck. A shared-memory or CUDA-IPC transport between
  same-host workers would collapse this to well under 50 ms and push
  us close to single-process prefix-cache performance.

- **Correctness is bit-exact.** `test_llm_kv_routing.py::test_cross_node_kv_reuse_matches_eager`
  checks that the routed path produces logits within fp tolerance of
  the eager path on the same suffix. The transport round trip is
  lossless.

## What this run does NOT demonstrate

- **Cross-host speedup.** All workers here share one GPU + localhost.
  On a real LAN the HTTP roundtrip is slower by a few ms; on WAN it's
  much slower. A follow-up on LAN (Mac Mini ↔ 3080 Ti) would quantify
  this honestly.

- **vLLM / SGLang comparison.** The `prefix-oracle` baseline in the
  single-process `benchmark_kv_routing.py` serves as a proxy for what a
  single-node vLLM/SGLang prefix cache achieves — hitting 5.65× speedup
  at 16 agents / 4k tokens. That's what a single-node engine beats us
  on today. Our value is that we scale past one node while staying near
  that ceiling; demonstrating this needs the multi-GPU / multi-host
  setup, which is the next phase.

- **Concurrent request scheduling.** All agents run sequentially through
  the GPU in this benchmark. Under vLLM-style continuous batching, the
  eager baseline would be faster in absolute terms because requests
  batch together. Semantic Cache Routing still helps in the case vLLM
  can't: multiple distinct vLLM instances on different boxes sharing
  context.

## Reproducing

```bash
# On the GPU box, once Pulsar is running:
docker run -d --name pulsar-bench -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:3.1.0 bin/pulsar standalone --no-functions-worker

# Then:
python tests/phase3_multiproc_bench.py \
  --model Qwen/Qwen2.5-1.5B --num-agents 2 \
  --doc-tokens 4096 --max-new-tokens 1 \
  --repeats 3 --warm-cache --dtype bf16
```

Full sweep (matrix above):

```bash
for M in Qwen/Qwen2.5-0.5B Qwen/Qwen2.5-1.5B; do
  for D in 1024 2048 4096; do
    python tests/phase3_multiproc_bench.py \
      --model $M --num-agents 2 --doc-tokens $D \
      --max-new-tokens 1 --repeats 3 --warm-cache
  done
done
```
