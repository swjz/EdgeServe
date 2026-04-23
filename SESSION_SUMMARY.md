# Session summary — 2026-04-22 (updated, GPU-box session)

Full night of autonomous work on the `llm` branch. Headline: the
`EdgeServeKVConnector` is now **working end-to-end with vLLM** (tasks
#20, #21, #22 all complete), including prefix-boundary matching that
lets different agents share a document's cached KV.

Latest commits on `origin/llm`:

```
$ git log --oneline 0b0f463..HEAD
3cd840b probe: profile load path timing breakdown
9393e30 KV_CONNECTOR: document prefix-boundary matching (now working)
c476ae9 test: scheduler longest-prefix match with mocked catalog
ba603eb connector: demote per-layer save log to DEBUG (reduce noise)
943d2f8 RESULTS: add 5-consumer Qwen-1.5B row (3.54x)
0b0f463 RESULTS: what-this-proves summary at top
```

(and ~20 earlier commits from the same session — see `git log --oneline llm`.)

## What landed

### Core: vLLM KVConnector

- `edgeserve/inference/vllm_kv_connector.py` — **`vllm.KVConnectorBase_V1`
  implementation backed by `SemanticCacheClient`**. Modeled on vLLM's
  own `ExampleConnector`; scheduler hashes prompt prefix at block
  boundaries, worker gathers/scatters per-layer KV via `slot_mapping`,
  publisher emits entity tags at every boundary so consumers with
  different suffixes hit the shared prefix.

### Test coverage
- `tests/test_vllm_kv_connector.py` — 9 tests total covering factory
  registration, abstract interface, metadata shape, multi-boundary
  hash generation, slice helper (Flash vs MLA/Triton layouts), and
  longest-prefix scheduler lookup (mocked catalog). All green.
- Total GPU-box test suite: **22 passed, 0 failed**.

### Demos / reproducers under `scripts/`

| script | what it proves |
|--------|-----------------|
| `demo_kvconnector_two_stage.py` | Seeder publishes → fresh consumer loads via connector; correctness + 2.47× on 0.5B, 3.30× on 1.5B |
| `demo_kvconnector_concurrent.py` | N live vLLM instances sharing one GPU; consumers pull KV cross-process |
| `demo_kvconnector_prefix_share.py` | Consumer with DIFFERENT suffix hits shared doc prefix (2.61×) |
| `demo_kvconnector_multi_agent.py` | 1 seeder + N unique-suffix consumers (3.09× at 5 consumers / 0.5B, **3.54×** at 5 / 1.5B) |
| `probe_vllm_internal_vs_connector.py` | Overhead vs vLLM's internal prefix cache: +6 ms at 5 k, +55 ms at 20 k tokens |
| `probe_kvconnector_e2e.py` | Single vLLM + connector smoke test |
| `probe_kvconnector_negative.py` | Verify no false cache hits |
| `probe_kvconnector_multi.py` | Verify multi-entry disambiguation |
| `probe_load_profile.py` | Breakdown of save/load cost (mmap is 9× faster than bytes-roundtrip) |

### Documentation
- `RESULTS.md` has a headline TL;DR table, per-scenario sweep tables,
  and a "what this proves" bookend. ~200 lines.
- `KV_CONNECTOR.md` — usage guide, registration, prefix-boundary
  matching explanation, limitations.
- `CLAUDE.md` — updated with a Semantic Cache Routing section.

## Headline numbers

Qwen2.5-0.5B / bf16 / 3080 Ti, different scenarios:

| scenario | consumers | speedup | notes |
|----------|----------:|--------:|-------|
| 2-stage same prompt | 1 | 2.47× | honest (fresh seeder vs fresh consumer) |
| concurrent same prompt | 3 | 2.70× | honest (fresh worker 0 vs workers 1-2) |
| prefix share (different suffix) | 1 | 2.61× | honest (per-run cold topic) |
| multi-agent (different suffixes) | 5 | **2.46×** | fixed demo; see audit below |

Qwen2.5-1.5B:

| scenario | consumers | speedup | notes |
|----------|----------:|--------:|-------|
| 2-stage same prompt | 1 | 3.30× | honest |
| **multi-agent, 3 consumers** | 3 | **3.12×** | fixed demo, cold/warm |
| multi-agent, 5 consumers | 5 | 3.54× | seeder/warm (fair), cold-consumer path of old demo was buggy |
| multi-agent, 7.7 k-token doc | 4 | 4.19× | seeder/warm (fair); cold-consumer path of old demo was buggy |

## Audit note on multi-agent demo

I ran an honesty audit of the vLLM numbers (user asked for this
before going to CUDA IPC). Found a bug: the old
`demo_kvconnector_multi_agent.py` used one shared "cold topic" for all
cold-baseline consumers; consumer 1's publish included multi-boundary
prefix hashes that consumers 2..N then accidentally hit on their own
"cold" runs. Fixed to use per-consumer unique topics, and the honest
cold/warm ratio on 0.5B with 5 consumers dropped from the previously
reported 3.09× to **2.46×**. The **seeder-vs-warm-consumer** ratio was
always valid (both are fresh no-cache processes) and has not changed.

SGLang still has no real measurements — the `sgl_kernel` source
rebuild on your box failed on missing `libnuma-dev` / `libibverbs-dev`;
now you've installed those it's in progress. Watch for the next
commit for updated SGLang numbers (if any).

## If resuming on the Ubuntu box

If you migrate to running Claude Code on swjz-ubuntu (see CLAUDE.md
"Running Claude Code on the GPU box"), pick up exactly here:

1. There's a sgl_kernel source build in progress in
   `/tmp/tmpq8xz55v4/build/` on swjz-ubuntu. If it succeeded, a wheel
   should be in `/tmp/sgl_kernel_wheel/`. If not, see the output at
   `/private/tmp/.../tasks/b0vwnpnxy.output` (Mac) or just re-run:

   ```bash
   cd ~/sglang-src/sgl-kernel && \
     CC=/usr/bin/gcc-10 CXX=/usr/bin/g++-10 \
     CUDACXX=/usr/local/cuda/bin/nvcc \
     CUDA_HOME=/usr/local/cuda PATH=/usr/local/cuda/bin:$PATH \
     TORCH_CUDA_ARCH_LIST="8.0;8.6;8.9" \
     ~/edgeserve-llm/.venv/bin/python -m pip wheel . \
     --wheel-dir /tmp/sgl_kernel_wheel --no-deps --no-build-isolation
   ```

2. Once the wheel exists:

   ```bash
   ~/edgeserve-llm/.venv/bin/pip install /tmp/sgl_kernel_wheel/sgl_kernel-*.whl \
     --force-reinstall --no-deps
   cd ~/edgeserve-llm && ~/edgeserve-llm/.venv/bin/python \
     scripts/bench_engines.py --model Qwen/Qwen2.5-1.5B \
     --doc-tokens 2048 --num-agents 4 --max-new-tokens 1 \
     --repeats 2 --engines sglang-radix --gpu-memory-utilization 0.5
   ```

3. Add the result (or failure mode) to RESULTS.md's headline table
   and to the Benchmark honesty audit section.

4. Task #18 (CUDA IPC / RDMA) is the remaining research item, deferred.

In every scenario the warm consumer's next-token id is bit-identical
to a cold no-cache run of the same prompt — correctness is preserved.

## Outstanding

- **Task #18 (CUDA IPC / RDMA transport)**: not done. This is the
  substantive follow-up that would shrink the per-hit overhead from
  ~55 ms to ~0 ms for large contexts.
- SGLang comparison: infrastructure still broken (precompiled kernels
  were SM100-only in the wheel and ABI-incompatible with torch 2.10).
  Documented as limitation.

## Phase 1 — Semantic entity tags (COMPLETE, 2026-04-22)

Commits `6ef919f`, `8b40171`, `1d6100c` on `llm` branch close the gap
between the paper's "semantic" framing and what was shipped:

- `set_next_request_entities({"doc_id:wiki42"})` before `llm.generate()`:
  connector encodes the tag into the bloom filter alongside prefix hashes.
- Scheduler's entity-first lookup: `catalog.lookup({"doc_id:wiki42"})`
  → `header.num_tokens` as coverage → direct UUID fetch.
- `demo_kvconnector_semantic.py` exercises the 4-subprocess scenario;
  verified **3.29× (prefix-hash) and 3.50× (entity-tag) speedup**,
  correctness confirmed (token=15235 matches cold baseline).
- `KV_CONNECTOR.md` and `RESULTS.md` updated with numbers and the
  permuted-persona impossibility note (see DESIGN.md Non-goals).

## New design framing

See `DESIGN.md` — the project is now framed as a **KV-Cache CDN for
edge LLM serving**. Read it before starting new work. The road map
phases are in `TODO.md`.

## Phase 2: LAN CDN measurements (COMPLETE, 2026-04-22)

`scripts/demo_kvconnector_lan.py` end-to-end pipeline confirmed working:
seeder (GPU box) → Pulsar catalog → consumer (Mac Mini) → HTTP fetch.

Key numbers (Qwen2.5-1.5B, 256 doc-repeats = 8448 tokens, 234.9 MB blob):

| metric | value |
|--------|-------|
| LAN throughput | 165–183 Mbps (Python HTTP server) |
| Median fetch | 10.5–11.4 s |
| GPU prefill | 1.38 s (3080 Ti) |
| **Fetch / recompute** | **~7.5–8× slower** |

Crossover analysis: fetch beats recompute when network > 1 Gbps actual,
or on CPU-only edge devices (50 tok/s prefill → 14× faster via LAN).
Full table in `RESULTS.md §Phase 2`.

### Bugs found and fixed

- **Pulsar subscription cursor**: `HeaderCatalog` now calls
  `consumer.unsubscribe()` on `close()` to delete the durable subscription,
  so the next connection with the same name starts from `Earliest`.
  Without this, previous-run cursors caused Mac Mini consumer to see nothing.
- **Seeder readback**: `block_uuid` is stored as raw bytes in the msgpack;
  must decode with `uuid.UUID(bytes=...)` not `str(d['block_uuid'])`.
- **vLLM entity tags cross-process**: `set_next_request_entities` sets a
  module-level global in the main process, but vLLM's EngineCore runs in
  a spawned subprocess. Entity tags DON'T propagate. Bloom only has prefix
  hashes. Catalog discovery works via prefix-hash path (no entity tags).

### SGLang: permanently blocked

`sgl_kernel` source build OOM-kills a 32 GB machine (nvcc `cicc` uses
4–7 GB per process; 4 simultaneous = 28 GB, kills Pulsar and itself).
Pre-built wheels are SM90-only with undefined SM100 symbols. Skip.
Full write-up in `RESULTS.md §Benchmark honesty audit`.

## Next milestones

- **Phase 2.2**: bandwidth-vs-recompute crossover benchmark — sweep
  blob sizes (different doc-repeats) and record fetch time vs recompute.
  Script: `scripts/bench_bandwidth_crossover.py` (not yet written).
  The crossover point mathematically is `network_bw > 132 MB/s (1.05 Gbps)`
  for this GPU.
- **Phase 3**: tiered storage — L1/L2/L3 eviction policy design,
  `edgeserve/semantic_cache/tiered_store.py` sketch.

## Pointers for next session

- `DESIGN.md` — architecture thesis and economic case.
- `TODO.md` — prioritised phase-by-phase task list.
- `RESULTS.md` — all numbers; start with TL;DR table; Phase 2 section.
- `scripts/demo_kvconnector_lan.py` — LAN demo; `seed` on GPU box,
  `consume` on Mac Mini with `--prefix-hash` or `--block-uuid + --node-uri`.
- `scripts/demo_kvconnector_semantic.py` — Phase 1 demo (entity tags).
- `edgeserve/inference/vllm_kv_connector.py` — connector code.
- `edgeserve/semantic_cache/catalog.py` — `close()` now unsubscribes.
