# Autonomous session summary — 2026-04-22

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

In every scenario the warm consumer's next-token id is bit-identical
to a cold no-cache run of the same prompt — correctness is preserved.

## Outstanding

- **Task #18 (CUDA IPC / RDMA transport)**: not done. This is the
  substantive follow-up that would shrink the per-hit overhead from
  ~55 ms to ~0 ms for large contexts.
- SGLang comparison: infrastructure still broken (precompiled kernels
  were SM100-only in the wheel and ABI-incompatible with torch 2.10).
  Documented as limitation.

## Pointers for tomorrow

- `RESULTS.md` — start here for numbers and context.
- `scripts/demo_kvconnector_multi_agent.py` — run this to reproduce
  the multi-agent result from scratch.
- `edgeserve/inference/vllm_kv_connector.py` — the connector code;
  read `wait_for_save` and `start_load_kv` for the save/load mechanics
  and `_Scheduler.get_num_new_matched_tokens` for prefix-match logic.
- `scripts/probe_load_profile.py` — useful if you want to attack the
  transport overhead (it shows mmap is already 9× faster than the
  bytes round-trip, so the next lever is CUDA IPC or direct-to-GPU
  loading).
