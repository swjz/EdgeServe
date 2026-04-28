# TODO — KV-Cache CDN build-out

Reorganized around the thesis in `DESIGN.md` (KV-cache CDN for edge
LLM serving, EdgeServe-v2). Read `DESIGN.md` first.

**Status snapshot (2026-04-23):** Phase 1 (entity-keyed discovery)
and Phase 2.1 (LAN CDN measurements) are complete. Publisher encodes
user-declared entity tags in the bloom alongside prefix hashes;
end-to-end LAN HTTP transport confirmed at 165–183 Mbps (7.5–8×
slower than GPU recompute). **The work pending now is Phase 2.2**
(bandwidth-vs-recompute crossover benchmark). SGLang is now measured
via `.venv-sglang` (sglang 0.5.10 + torch 2.9.1 separate venv):
sglang-radix 2.60× vs hf-eager on the 4-agent / 2048-token workload.

---

## Phase 1 — Entity-keyed discovery ✅ MOSTLY DONE

### 1.1 — Honesty note on prefix-bloom vs semantic-bloom ✅ done

Covered inline in `scripts/demo_kvconnector_semantic.py`'s "Honesty
note on permuted persona" docstring and in `KV_CONNECTOR.md`'s
"Semantic entity tags (Task A)" section. Also, see **DESIGN.md
non-goals** for the technical reason "permuted persona" can't work
under causal attention + RoPE.

### 1.2 — Publisher accepts entity tags ✅ done (`6ef919f`)

- `set_request_entities(request_id, entities)` side-channel on the
  publisher.
- Worker merges user entities into bloom alongside prefix hashes in
  `_Worker.wait_for_save`.
- `CacheHeader` gained `num_tokens` field so consumers know coverage
  on entity-keyed hits (where they don't know the original token
  sequence).

### 1.3 — Scheduler entity-intersection lookup ✅ done (`6ef919f`)

- `_Scheduler.get_num_new_matched_tokens` now tries the entity-first
  path when `_REQUEST_ENTITIES[request_id]` is non-empty, then falls
  through to prefix-hash longest-match.
- Entity hit returns `header.num_tokens` as coverage; matching
  `block_uuid` forwarded to `start_load_kv` via the metadata so the
  worker fetches directly without re-running the bloom query.
- `SemanticCacheClient.resolve_by_uuid()` added for the direct-UUID
  fetch path.

### 1.4 — Permuted-persona demo (originally scoped) ❌ infeasible

Under causal attention + RoPE, `persona_A + doc` and `persona_B + doc`
produce *different* KV at every doc position (different preceding
context + potentially different positions). You can't share KV across
persona-permuted prompts without architecture changes (PromptCache-
style position-agnostic KV, attention sinks, etc.).

**What the remote session did instead**, which is the right call:
demonstrate the achievable version — doc-as-prefix, same doc_id
entity tag, different suffix questions. See
`scripts/demo_kvconnector_semantic.py` for the 4-subprocess demo
(cold / seeder / consumer-via-hash / consumer-via-entity).

Recorded here so future-us doesn't resurrect this scope.

### 1.5 — RESULTS.md rewrite with entity-lookup numbers ✅ done

`scripts/demo_kvconnector_semantic.py` ran on the GPU box (4-subprocess
scenario): **3.29× via prefix-hash**, **3.50× via entity-tag**,
correctness confirmed (token matches cold baseline). Numbers added
to RESULTS.md and KV_CONNECTOR.md. Permuted-persona impossibility
noted in RESULTS.md audit section.

---

## Phase 2 — LAN CDN measurements

The paper's headline figures. Prove the architecture pays over LAN
for **doc-as-prefix** workloads (the honest scenario — see DESIGN.md
non-goals re: permuted persona, which is architecturally
infeasible under causal attention + RoPE).

Two experiments here. Experiment 3 (tool-call eviction buffer)
moves to Phase 3 because it requires tiered storage to be real.

### 2.1 — Two-host deployment on home LAN ✅ done (2026-04-22)

GPU box (3080 Ti) → Mac Mini over home gigabit LAN. Full pipeline
confirmed: seeder publishes KV via vLLM + connector → Pulsar catalog
→ Mac Mini consumer discovers via prefix-hash → HTTP fetch.

Key numbers (Qwen2.5-1.5B, 256 doc-repeats = 8448 tokens, 234.9 MB):

| metric | value |
|--------|-------|
| LAN throughput | 165–183 Mbps (Python HTTP server) |
| Median fetch | 10.5–11.4 s |
| GPU prefill | 1.38 s (3080 Ti) |
| Fetch / recompute | **~7.5–8× slower** |
| Crossover threshold | ≥1.05 Gbps actual throughput, or CPU edge |

Bugs fixed along the way: Pulsar subscription cursor (now calls
`unsubscribe()` on close), entity-tag cross-process gap (consumer uses
prefix-hash probe instead), seeder readback TypeError (UUID bytes).

See `RESULTS.md §Phase 2` for full tables and crossover analysis.
Script: `scripts/demo_kvconnector_lan.py`.

**Baseline framework used across 2.2 and 2.3 (defined here so later
sections can cite):**
- **B0 — Mac monolithic:** Mac runs vLLM/HF locally, no CDN. The
  "status quo edge developer" baseline. May be infeasible at long
  context — that itself is a result.
- **B1 — vLLM monolithic on 3080 Ti, HTTP decode-forwarding:** all
  compute on GPU, Mac is a dumb client. Honest comparison; avoids
  vLLM's experimental native P-D (fragile). SGLang could be a
  parallel baseline via `.venv-sglang` if desired (`scripts/
  bench_engines.py --engine sglang-radix` has numbers — 2.60× vs
  hf-eager on 4-agent/2048-token); not required for the CDN thesis.
- **B2 — N independent vLLM processes on 3080 Ti:** used in 2.3
  only. Each vLLM has its own APC, no cross-process share. This
  is the baseline EdgeServe clearly beats.

### 2.2 — Experiment 1: Bandwidth-vs-recompute crossover 🟡 partial

**The paper's money figure.** Same-host sweep landed in commit
`5ca0e45`; live-network sweep + B0 Mac prefill line still pending.

**Completed (2.2a ✅ done 2026-04-23):** Same-host mmap sweep for
Qwen2.5-1.5B on 3080 Ti across 16/32/64/128/256 doc-repeats:
- mmap fetch is **5–14× faster** than GPU prefill at every size.
- crossover bandwidth is flat at **1.2–1.6 Gbps** — blob size AND
  prefill both scale linearly with tokens in the measured range,
  so the ratio is model+GPU-specific, not context-length-specific.
- Home LAN at 179 Mbps is 7–8× below crossover → loses on GPU
  edge. 10 GbE breaks even. CPU edge at 50 tok/s drops crossover
  to 11 Mbps → any LAN wins.

Key implication for the story: the "crossover plot" is really a
**single-point** question (what bandwidth does your link have?),
not a curve across context lengths. The paper figure should be
bandwidth on the X-axis, ratio on the Y, with the GPU-edge
horizontal at 1.2–1.6 Gbps and the CPU-edge horizontal at 11 Mbps.
Annotate real-world links: typical home LAN, datacenter 10 GbE,
4G/5G WAN.

Physics sketch (confirmed by 2.2a):
- Prefill wall-time on 3080 Ti for Qwen2.5-1.5B scales linearly
  (not quadratically) over the measured range — flashattn + small
  model means attention isn't yet the dominant cost.
- KV bytes scale linearly. Linear / linear = constant ratio.
- For larger models or longer context, attention may eventually
  dominate and flip the scaling — worth re-checking at 50k+ on
  3B/7B models, but not a high priority.

**Pending:**
- ✅ **2.2b — Python-level bandwidth throttle sweep done (2026-04-23).**
  `scripts/bench_bandwidth_throttle.py` sweeps 50 Mbps → 10 Gbps using
  Python sleep-based rate limiting (no tc/root required). Empirical
  crossover: ~2–3 Gbps (vs analytic 1.54 Gbps; gap = Python HTTP overhead
  ~200ms). Results in RESULTS.md §Phase 2.2b.
- ✅ **2.2c — B0 Mac Mini (MPS) prefill baseline done (2026-04-23).**
  Qwen2.5-1.5B fp16 on Apple M4 MPS. Key: Mac crossover is 35–195 Mbps
  (vs GPU box 1.54 Gbps). Home LAN (179 Mbps) beats Mac prefill at 128+
  repeats (7.7s Mac vs 5.4s LAN fetch → 1.43× for EdgeServe). At 256
  repeats Mac is 40× slower than GPU box (attention quadratic on MPS). All
  tokens match. Results in RESULTS.md §Phase 2.2c.
- **2.2d — Warm-line measurement on live LAN.** Already captured in
  Phase 2.1 table (10.5s median via Python HTTP at 179 Mbps). Documented
  in RESULTS.md combined picture.

### 2.3 — Experiment 2: Multi-agent cross-process fan-out ✅ done (2026-04-23)

**The multi-agent CDN story.** N agents on N separate processes
(optionally N hosts) all analyzing the same ~50k-token git repo
with divergent per-agent task suffixes. This experiment is
largely independent of transport speed — the win comes from
avoiding N×prefill, not from beating local prefill.

**Prompt structure (load-bearing for honesty):**
- Prefix: `<50k tokens of a real repo dump>` — the shared context.
- Suffix: `[Task k: <per-agent task>]` — e.g., "summarize the auth
  module", "list all public APIs", "find the race conditions".
- **NOT** `[Agent k persona] + repo`. That's the permuted-persona
  case; DESIGN.md non-goals explains why it cannot share KV.

Declare `entity = codebase:{repo}@sha={content_sha}` on the
consumer; the repo prefix hits via the entity path even if the
consumer's prompt isn't byte-identical to the seeder's (suffix
differs).

Baselines (B0/B1/B2 defined in 2.1):
- **B0 (Mac monolithic, per agent):** N Mac processes, no share.
  Each must prefill the repo independently. Likely infeasible at
  50k on MPS — falls over, that's a result.
- **B1 (vLLM monolithic, 3080 Ti, N concurrent requests):** single
  vLLM server; APC shares the repo prefix via radix tree. The
  HARD baseline. EdgeServe's win over B1 is NOT cheaper prefill
  — B1 prefills once too. Win is decode on the edge (lower TPOT,
  offline resilience, no egress).
- **B2 (N separate vLLM processes, 3080 Ti, APC each):** N vLLMs
  share nothing with each other — each prefills repo from scratch.
  EdgeServe's catalog lets process N hit process 1's published KV.
  The honest cross-process-share win.

Metrics:
- Cache hit rate (tokens served from catalog vs prefilled).
- Total cluster compute time across all agents.
- Per-agent TTFT distribution (median, p95).

**Honest narrative:** EdgeServe beats B0 on feasibility at long
context, beats B2 on total compute (cross-process share), and
matches/trails B1 on compute but decouples decode onto the edge.
Do NOT frame this as "beats vLLM" — vLLM within a single process
is great. EdgeServe's niche is cross-process + edge-decode.

Deliverable: `scripts/bench_multiagent_repo.py` + numbers in
RESULTS.md.

### 2.4 — RESULTS.md "Cross-host" section 🔲 ongoing

Phase 2.1 table already in RESULTS.md (✅ done 2026-04-22). Extend
with 2.2 sweep table and 2.3 multi-agent numbers as they land.
Label each result with baseline set (B0/B1/B2) so readers can't
misread comparisons. Retire the simulated 1.54× "LAN number" in
favor of real measurements (already superseded by 2.1).

---

## Phase 3 — Tiered storage on the context server

Goal: context server holds far more KV than fits on its GPU.

### 3.1 — Design doc for eviction policy ✅ done (2026-04-23)

Policy is documented in DESIGN.md "Tiered storage" section. Key decisions:
- L1 = vLLM's paged buffer (we don't manage it).
- L2 = in-process pinned bytes (OrderedDict LRU), served zero-copy by HTTP server.
- L3 = NVMe files at `local_cache_path` (existing). Always written on publish.
- Tombstone = `CacheHeader.deleted=True` broadcast on Pulsar (reuses topic).

### 3.2 — L1/L2/L3 backing store ✅ done (2026-04-23)

`edgeserve/semantic_cache/tiered_store.py` — `TieredStore` class:
- `put(uuid, data)` → always writes L3; inserts L2 if capacity permits.
- `get(uuid)` → L2 hit (no I/O) or L3 hit (file read + async L2 promotion).
- `peek_l2(uuid)` → bytes or None, no disk I/O.
- `evict(uuid)` → removes all tiers, fires tombstone callback.
- `stats` property for tier occupancy monitoring.

`CacheHttpServer` updated: checks `store.peek_l2()` before disk; adds
`X-Cache-Tier` response header. `SemanticCacheClient` routes `publish()`
through `TieredStore.put()` when a store is attached.

### 3.3 — Hit promotion + miss-path fill ✅ done (2026-04-23)

L3 hits async-promote to L2 via background thread (50 ms poll interval);
`promote_async=False` for tests. New publishes land in both L2 and L3.

### 3.4 — Tombstone propagation ✅ done (2026-04-23)

`CacheHeader.deleted` field added (backward-compatible via `msgpack.get`).
`HeaderCatalog._run()` removes deleted UUIDs from `_headers` instead of
inserting. `SemanticCacheClient._publish_tombstone()` broadcasts a minimal
deleted header. `TieredStore.on_tombstone` callback wired automatically
when client instantiates with a store.

### 3.5 — Benchmark tier hit rates under realistic workload ✅ done (2026-04-23)

`scripts/bench_tier_hit_rates.py` — Zipf(s=1.0), 100 docs, 2000 accesses.
L2=5% WS → 23.6% hit rate; L2=20% WS → 55.5%; L2 reads 245–322× faster
than L3 NVMe. Results in RESULTS.md §Phase 3.5.

### 3.6 — Experiment 3: Tool-call eviction buffer ✅ done (2026-04-23)

**The agentic-workflow resilience story.** An agent generates,
pauses ~45 s to execute a tool locally (compile, web fetch,
test run), then resumes. During the pause, concurrent traffic on
the 3080 Ti forces vLLM to evict the idle agent's KV blocks.

Metric: resumption latency (time to next generated token after
the tool returns).

Baselines:
- **B1 (vLLM monolithic with default eviction):** evicted → full
  re-prefill on resume. Worst case.
- **B1' (vLLM with CPU-offload APC):** vLLM's experimental disk/
  CPU tier. If this exists and is stable in the pinned version,
  include it — a reviewer will ask. If not, pin the version and
  note that CPU offload wasn't available.
- **EdgeServe:** block was tiered-stored on the context server
  (L2/L3) before eviction; resume fetches it back via CDN.

Pressure simulation is finicky: vLLM evicts at block granularity
under LRU. Trial-and-error the concurrent traffic volume needed
to trigger eviction reliably. Pin vLLM + config + hardware state
so the result reproduces.

Story: EdgeServe insulates long-running agents from transient GPU
pressure. Complements 2.3 — same architecture, different axis of
resilience.

Deliverable: `scripts/bench_tool_eviction.py` + numbers in
RESULTS.md.

---

## Phase 4 — Context-push daemon

Goal: edge-side file watcher that keeps the context server's KV
fresh as the user's working set changes.

### 4.1 — Ingest endpoint on context server ✅ done (2026-04-23)

`edgeserve/inference/context_server.py` — persistent HTTP ingest server:
- `POST /ingest` accepts `{text, entities, sha}` → calls `llm.generate()`
  (1 token) → connector saves KV to NVMe → returns `{block_uuid, n_tokens, ingest_ms}`.
- `GET /health` → status, model, n_ingested.
- Model loads once (~22 s), stays hot. Subsequent ingests cost prefill only (~44ms).
- Single-threaded (HTTP daemon thread + main thread for vLLM).

### 4.2 — Edge-side watcher ✅ done (2026-04-23)

`edgeserve/edge/watcher.py` — `ContextWatcher` class + CLI:
- Polls a directory, computes sha256 per file, POSTs changed files to
  context server with entity tags `file:<rel>` and `file:<rel>@sha=<sha>`.
- `push_file(path)` for one-shot push; `run_forever()` for daemon mode.
- No dependencies beyond stdlib.

### 4.3 — Entity versioning semantics ✅ done (2026-04-23)

Implemented via entity-tag convention (no code change needed):
- `file:auth.py` → latest version (catalog sorts by `created_ms`).
- `file:auth.py@sha=882ec173` → exact content version.
Watcher automatically tags both forms on each push.

### 4.4 — End-to-end edit-to-answer demo ✅ done (2026-04-23)

`scripts/demo_context_push.py` — full pipeline demo:
- Starts context server, pushes two auth.py versions via watcher, kills
  server (simulates GPU eviction), measures restore vs cold re-prefill.
- Results: 44ms ingest (warm server), 38ms restore vs 44ms re-prefill
  (1.15× at 271 tokens). Token correctness ✓. See RESULTS.md §Phase 4.

---

## Phase 5 — Zero-copy transport (deferred)

Only after Phases 1–4.

### 5.1 — Same-host CUDA IPC

`torch.multiprocessing` style shared CUDA handles between processes
sharing a CUDA context. Drops the current ~6–55 ms overhead to near
zero for same-host hits.

### 5.2 — Cross-host RDMA / NCCL

For production clusters with real RDMA fabric, plumb NCCL / NIXL
behind `SemanticCacheClient.resolve_into` so transport is RDMA verbs
rather than HTTP. Big engineering lift, modest narrative payoff for
the edge-focused story. Mostly a "not slower than existing
in-datacenter solutions" checkbox.

---

## Phase 6 — End-to-end edge inference demo ✅ DONE (2026-04-24)

`scripts/demo_edge_inference.py` with seed/query/selftest subcommands.
Mac Mini (M4, MPS fp16) consumer fetches KV from GPU box (RTX 3080 Ti)
over wired gigabit LAN; query text and generated tokens never leave Mac.

**Headline results at 64 doc-repeats (5 632 tokens):**
- B0 (Mac local prefill + decode): **14.6 s**
- EdgeServe (fetch + cast + decode): **3.98 s**
- Speedup: **3.67×**  Token match: **✓ bit-exact**
- Wire throughput: **935 Mbps** (close to gigabit line rate)
- 161.5 MB KV blob fetched in 1.38 s

At 256 repeats (22.5 k tokens): MPS B0 produces numerically broken
output (fp16 overflow), while EdgeServe produces a coherent answer in
14.4 s (5.6 s fetch + 8.8 s quadratic MPS decode).  See RESULTS.md §6.

### Remaining Phase 6 sub-tasks



**Goal:** close the gap between the architecture thesis ("decode at the edge, prefill
near the data") and what's currently demonstrated. Every existing demo runs inference
on the GPU box. This phase builds the demo where a user on the Mac asks a question,
the Mac fetches cached KV from the GPU box, and generates the answer *locally* —
the user's prompt and the generated tokens never leave the Mac.

The Mac cannot run vLLM (no CUDA). Use `HFEngine` + `kv_io.load_past_key_values_from_path`
on the consumer side. This path already works (it's what `demo_kvconnector_lan.py`
uses for the Mac) but has not been wired into a full interactive inference demo.

### 6.1 — Mac consumer: KV-injected HF inference ✅ done (2026-04-24)

Pipeline:
1. GPU box (context server): ingests a large document. Connector publishes KV to
   catalog + NVMe.
2. Mac: user types a query suffix.
3. Mac: queries bloom-filter catalog (Pulsar) → discovers block UUID.
4. Mac: HTTP-fetches KV blob from GPU box.
5. Mac: injects into HF `past_key_values`; calls `model.generate(suffix_tokens)`.
6. Mac: prints answer. Document text, prompt, and generated tokens stay on Mac.

Measure end-to-end from "user hits enter" to "first token generated":

| path | latency breakdown | total |
|------|------------------|-------|
| B0 (Mac prefills doc+query locally) | Mac prefill ~7.7s (4k tok) | ~7.7s |
| EdgeServe (LAN fetch + Mac decode) | fetch ~3.4s + decode ~0.1s | ~3.5s |

Script: `scripts/demo_edge_inference.py`. Run seeder on GPU box, consumer CLI on Mac.

### 6.2 — Multi-turn conversation ✅ done (2026-04-27)

`scripts/bench_multiturn.py`.  Same-host run on GPU box (isolates
per-turn story from network cost).  Qwen2.5-1.5B.

Results: at 128 repeats (11 264 doc tokens) / 4 turns, per-turn
speedup is 2.13–2.39×; cumulative 1.99×.  At 64 repeats / 3 turns,
per-turn 1.56–1.62×, cumulative 1.38×.  Speedup grows with context
length because B0 re-prefills the entire doc every turn while
EdgeServe prefills only the 14–149-token conversation delta.

See RESULTS.md §Phase 6.2.

After Q1+A1, the conversation prefix grows. Measure second-query latency when the
new delta (Q1+A1, a few hundred tokens) is the only cold portion.
Shows that the KV CDN compounds benefits across turns.

### 6.3 — Privacy claim writeup ✅ done (in RESULTS.md §6 "Privacy narrative")

Document what crosses the wire in the EdgeServe model vs. the cloud-API model:
- Cloud API: prompt + context sent to remote server; generated tokens returned.
- EdgeServe: document pushed to nearby context server (same trust as cloud, but
  over LAN, controllable). Live prompt and generated tokens never leave the edge.
Quantify: under EdgeServe, what is the minimum information the context server must
see? (Answer: the document text at ingest time; not the query or response.)

---

## Phase 7 — Comparison baselines

These are the comparisons a systems conference reviewer will immediately ask for.

### 7.0 — Exact validation after Bloom-positive lookup 🔲

**Correctness blocker.** Today the catalog treats a Bloom-positive header as
the cache-match decision. That is unsafe: Bloom filters have false positives,
and a false-positive KV load can silently inject the wrong attention state.

Keep Bloom filters as the scalable discovery prefilter, but require exact
validation before the worker loads KV into the model:
- Header/manifest fields: `model_id`, `model_version` or checkpoint hash,
  tokenizer hash, block size, token-prefix hash for every published boundary,
  content/entity SHA, and `num_tokens`.
- Prefix-hash path: after Bloom says "maybe", verify the requested
  block-aligned token hash is explicitly present in the header/manifest before
  returning a hit to the scheduler.
- Entity-tag path: after Bloom says "maybe", verify the exact entity key
  `(entity, model_id, model_version, content_sha)` is present, and verify the
  cached `num_tokens` is compatible with the consumer's requested prefix.
- Negative test: force a tiny Bloom filter / high-FPR workload and prove the
  connector falls back to miss instead of loading wrong KV.

Deliverable: exact-match metadata in `CacheHeader` or a sidecar manifest,
unit tests for false-positive rejection, and one end-to-end vLLM negative test.

### 7.1 — B1 honest same-host framing ✅ done (2026-04-27)

`scripts/bench_b1_vllm_apc.py`.  Single vLLM with APC, same workload
as Phase 2.3 (4 agents, 6 272-token prefix).

  - B1 warm sequential:  24 ms/agent  (7× faster than EdgeServe 176 ms)
  - B1 warm batched:     15 ms/agent amortised  (12× faster)
  - B1 cold first:      237 ms  (comparable to B2)

Conclusion recorded in RESULTS §Phase 2.3: EdgeServe does NOT beat B1
on same host.  EdgeServe's niche is cross-host, process-restart, and
edge devices that can't run vLLM.  Frame the paper around those three.

### 7.2 — LMCache direct comparison 🔲

LMCache (MLSys'25) stores KV on CPU/disk per process, retrieves on cache hit.
Install: `pip install lmcache`. Configure as a vLLM KVConnector alongside ours.

Same workload (N=4 agents, 6k-token prefix, sequential processes):

| system | discovery mechanism | warm TTFT | cold-node setup cost |
|--------|--------------------:|----------:|---------------------:|
| LMCache | explicit URL/registry | TBD | must configure server address |
| EdgeServe | bloom-filter broadcast | 176 ms | zero — any node subscribes to topic |

**Key differentiator to measure:** cold-node discovery. Spin up a new agent on a
node that has never seen the document. With LMCache, the new node needs explicit
configuration pointing at the caching node. With EdgeServe, it subscribes to the
Pulsar topic and discovers the KV via the bloom catalog automatically. Measure
time-to-first-hit for a completely cold node in each system.

Script: `scripts/bench_lmcache_vs_edgeserve.py`.

### 7.3 — NIXL / NixlConnector comparison 🔲

`NixlConnector` ships with vLLM 0.19 and is registered alongside `EdgeServeKVConnector`
in the factory. Configure NIXL for the same cross-process same-host scenario.

| system | transport | warm overhead vs internal APC | cross-host capable |
|--------|-----------|-------------------------------:|:------------------:|
| NixlConnector | UCX/shared mem | TBD | only with RDMA fabric |
| EdgeServeKVConnector | mmap safetensors | +6–55 ms | yes (HTTP fallback) |

**NIXL's likely advantage:** lower same-host latency (UCX bypasses Python).
**EdgeServe's likely advantage:** works cross-host without RDMA; bloom-filter
discovery removes per-node configuration; entity tagging for semantic lookup.

Measure same-host warm hit latency for each connector at 5k and 20k tokens.
Script: `scripts/bench_nixl_vs_edgeserve.py`.

### 7.4 — Metadata-only discovery over huge remote context 🔲

**Core paper differentiator.** Show that EdgeServe can discover the correct
prefill node from a compact semantic handle without first downloading,
tokenizing, or trie-walking the raw context on the edge.

Correctness constraint: KV is still reused only when the exact token prefix is
compatible. Semantic metadata narrows the search to candidate cache blocks;
exact model/tokenizer/content/prefix validation gates the actual KV load.

Experiment A — metadata-only lookup:
- Context server has already prefetched and prefilled a large object
  (repo dump, 100 MB document collection, or remote object-store blob).
- Edge node receives only `{entity, model_id, model_version, content_sha}`
  plus the user's query, not the raw document bytes.
- EdgeServe path: Bloom/catalog lookup → exact metadata validation → KV fetch.
- Prefix-only baseline: edge must fetch raw data or token ids first to construct
  the prefix-trie/radix/APC lookup key, then either hit or recompute.
- Metrics: bytes downloaded before hit decision, time-to-hit decision,
  time-to-first-token, and edge CPU/tokenization cost.

Experiment B — cold-node join:
- Start a new edge node with empty local cache and no raw documents.
- Measure time to discover and use an existing prefill block via only the
  semantic entity header.
- Compare against LMCache/vLLM/NIXL configurations where the new node needs
  explicit cache-node configuration, raw-context materialization, or token
  sequence construction before lookup.

Experiment C — alias resolution:
- Register multiple semantic aliases for the same content hash:
  `file:<path>`, `repo:<name>@<commit>`, `url:<object>`, `vector_doc:<id>`.
- Show all aliases resolve to the same validated `content_sha` and therefore
  the same KV block when tokenization is identical.
- Negative case: same entity label but different `content_sha` or tokenizer
  version must miss.

Expected result: EdgeServe wins on **lookup bandwidth and cold-node setup**,
not necessarily on same-host warm-hit latency. This should be one of the main
paper figures because it isolates the value of semantic indexing from transport
engineering.

Deliverable: `scripts/bench_metadata_discovery.py` + RESULTS.md table:
`system`, raw bytes needed before lookup, lookup latency, TTFT, correctness.

---

## Phase 8 — Scale (deferred — needs A100 / H100 or multi-GPU)

Current hardware (RTX 3080 Ti, 12 GB) limits us to 1.5B models and ~8k tokens.
The motivating use case in DESIGN.md is 7B+ models and 50k-token codebases.
Revisit when better GPU access is available.

### 8.1 — 7B model evaluation 🔲

Model: Qwen2.5-7B or Llama-3-8B (requires ~24 GB VRAM minimum for BF16).

Key hypotheses:
- KV blob for 7B at 8k tokens ≈ 1.4 GB. At 1 Gbps LAN ≈ 11s; GPU prefill ≈ ?
  Expected: crossover shifts to lower bandwidth → CDN economics improve.
- Multi-agent fan-out at N=8–16 becomes the interesting operating point.
- Tool-eviction speedup at 7B should be substantially higher (larger blob
  serialization cost < quadratic attention cost for long contexts).

Run the full crossover analysis (Phase 2.2) and multi-agent fan-out (Phase 2.3)
at 7B / 32k tokens.

### 8.2 — 32k+ token context 🔲

7B model, 32k tokens: KV ≈ 5.6 GB. At 10 GbE (1 GB/s): 5.6s transfer.
Flash attention prefill on A100: roughly 15–30s (model-dependent).
Expected: crossover drops below 1 Gbps — home LAN starts to win even for GPU edge.

This is the regime where the paper's thesis is unambiguously correct. Measure it.

### 8.3 — Multi-host multi-GPU cluster 🔲

2–4 GPU nodes on a 10 GbE switch. One context server, N edge-inference nodes.
Measure: catalog discovery latency under multi-publisher traffic; throughput of the
Pulsar topic under N concurrent consumers; hit rate as fleet grows.

---

## Honesty threads to keep tracking

- ~~Phase 1 entity-tag numbers haven't been recorded in RESULTS.md~~
  ✅ Recorded (task 1.5 done): 3.29× prefix-hash, 3.50× entity-tag,
  correctness verified.
- ~~Cross-host section of RESULTS.md is empty~~ ✅ Filled (task 2.1
  done): real two-host LAN measurements, 165–183 Mbps, 7.5–8× slower
  than GPU recompute.
- ~~Phase 2.2 crossover benchmark is still pending.~~ ✅ Done (2026-04-23):
  Python-level throttle sweep confirms empirical crossover at ~2–3 Gbps
  (analytic: 1.54 Gbps; Python HTTP overhead accounts for the gap).
  Results in RESULTS.md §Phase 2.2b.
- ~~"KV-cache CDN" is aspirational until Phase 3 (tiered storage) +
  Phase 4 (context-push) land.~~ ✅ Both phases complete (2026-04-23).
  TieredStore (L2 RAM + L3 NVMe), tombstone propagation, ContextServer
  ingest, and ContextWatcher are all implemented and demo'd.
- **SGLang: unblocked via separate venv (2026-04-23).** Installed
  sglang 0.5.10 + torch 2.9.1 in `.venv-sglang` (isolated from the
  main vLLM venv). sglang-radix measures 2.60× vs hf-eager on the
  4-agent / 2048-token workload. Not required for the CDN thesis —
  vLLM numbers stand on their own — but available as a parallel
  comparison via `bench_engines.py --engine sglang-radix` when
  reviewers ask.

---

## Cross-ref: Claude Code TaskList on Mac (for traceability)

| section | Mac TaskList id | status |
|---|---|---|
| 1.1 | `#29` | completed |
| 1.2 | `#24` | completed |
| 1.3 | `#25` | completed |
| 1.4 | `#26` | retired (infeasible) — deleted |
| 1.5 | `#27` | completed (2026-04-22) |
| 2.1 | `#28` | completed (2026-04-22) |
| 2.2 | `#30` | ✅ done (2.2a mmap sweep, 2.2b Python throttle; 2.2c Mac skipped) |
| 2.3 | `#31` | ✅ done (2026-04-23) — bench_multiagent_fanout.py |
| 3.5 | — | ✅ done (2026-04-23) — bench_tier_hit_rates.py |
| 3.6 | `#32` | ✅ done (2026-04-23) — bench_tool_eviction.py |
| 5.1 | `#18` | pending (deferred) |
