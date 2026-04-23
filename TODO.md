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

### 4.1 — Ingest endpoint on context server 🔲 pending

Context server exposes `POST /ingest` with:
- Entity tag (including `content_sha`).
- Raw content (tokens OR text OR file bytes).
- Target model identifier.

Server tokenizes + prefills + publishes via the existing connector
path. Async; returns job id or streaming progress.

### 4.2 — Edge-side watcher 🔲 pending

`edgeserve/edge/watcher.py`: light daemon monitoring
`~/.edgeserve/context/`. On file change:
- Compute new `content_sha`.
- `POST` updated content to context server with entity
  `codebase:{repo}/{path}@sha={new_sha}`.
- Old entity (same entity, old sha) eventually tombstones via
  tiered-storage LRU eviction.

### 4.3 — Entity versioning semantics 🔲 pending

Entity tag includes `content_sha`.
- `get_by_entity("codebase:myrepo/file.py")` (no sha) → latest-sha
  entry from catalog (most recent `created_ms`).
- `get_by_entity("codebase:myrepo/file.py@sha=abc123")` (explicit
  sha) → that exact version or miss.

### 4.4 — End-to-end edit-to-answer demo 🔲 pending

Realistic flow:
- User edits `file_diff_v2.py` in their editor.
- Watcher pushes to context server, which prefills + publishes.
- User on edge device runs "analyze `file_diff_v2.py`" via local
  inference.
- Edge device declares entity
  `codebase:myrepo/file_diff_v2.py` (no sha → latest), catalog hits,
  pulls KV, decodes locally.
- No user prompt or generated token ever leaves edge.

Measure time from file save → first-token-generated, compared to
"no cache — edge prefills from scratch."

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
- "KV-cache CDN" is aspirational until Phase 3 (tiered storage) +
  Phase 4 (context-push) land. The current code is a single-tier
  distributed cache with read-on-demand; the CDN semantics arrive
  with tiering + origin-push.
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
