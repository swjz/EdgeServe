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
  vLLM's experimental native P-D (fragile; SGLang blocked).
- **B2 — N independent vLLM processes on 3080 Ti:** used in 2.3
  only. Each vLLM has its own APC, no cross-process share. This
  is the baseline EdgeServe clearly beats.

### 2.2 — Experiment 1: Bandwidth-vs-recompute crossover 🔲 next

**The paper's money figure.** At 8448 tokens, current Python HTTP
transport is **7.5–8× slower** than GPU prefill (see 2.1) — so
EdgeServe cold is a LOSS at this context size. Mathematical
crossover for Qwen2.5-1.5B on 3080 Ti: 1.05 Gbps effective
throughput. The sweep maps where the architecture flips to a win.

Physics sketch:
- Prefill on 3080 Ti grows ~O(n²) for attention at long n (vLLM's
  flashattn is linear in n for memory but still quadratic FLOPs
  for compute; prefill wall-time tracks FLOPs at long n).
- KV bytes grow O(n), so transfer time grows linearly.
- The quadratic-vs-linear gap means crossover moves toward
  "transport wins" as context grows.

Sweep:
- model size: Qwen2.5-0.5B, 1.5B, optionally 3B if fits on Mac.
- context length: vary `--doc-repeats` (16/32/64/128/256 repeats
  → ~530–8448 tokens; extend above 8k if Mac decode is feasible).
- effective link bandwidth: `tc qdisc netem rate Xmbit` to sim
  100/500/1000 Mbps; real LAN for the measured reference point.

**Three curves per `(model, context)` point — cold vs warm matters:**
- **B0 Mac prefill:** TTFT = Mac prefills from scratch (MPS/CPU).
  May OOM or drop below 5 tok/s at large context — that itself
  is a finding.
- **EdgeServe cold:** TTFT = 3080 Ti prefill + LAN KV transfer.
  Currently slower than B1's local prefill at 8k; sweep tells us
  where that flips.
- **EdgeServe warm:** TTFT = LAN KV transfer only; doc already
  cached on the context server. The CDN amortization story.

Metric: TTFT. Secondary: throughput to sanity-check decode isn't
the bottleneck.

Deliverable: `scripts/bench_bandwidth_crossover.py` that runs
seeder + consumer (over ssh if needed), emits a table, appends
rows to RESULTS.md §Phase 2.2.

### 2.3 — Experiment 2: Multi-agent cross-process fan-out 🔲 pending

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

### 3.1 — Design doc for eviction policy 🔲 pending

Short appendix (~150–250 lines, add to DESIGN.md or separate file).
Cover:
- Tier capacities (GPU / pinned CPU / NVMe).
- Promotion rules: on hit, move up one tier (L3→L2→L1) when space
  permits; stay-in-place otherwise.
- Eviction rules: LRU within tier; demote on pressure; tombstone on
  full-evict below L3.
- Tombstone broadcast: new `CacheHeader.deleted=True` flag vs.
  separate tombstone topic? Prefer the flag, reuse existing
  subscription.

### 3.2 — L1/L2/L3 backing store 🔲 pending

- L1 = GPU paged-buffer (already owned by vLLM; no change).
- L2 = pinned CPU tensors on the context server (new), mmap-backed
  so HTTP serving is zero-copy.
- L3 = NVMe file (already the current local_cache_path).

Write `edgeserve/semantic_cache/tiered_store.py` wrapping
`CacheHttpServer.write_block` with tier-aware routing.

### 3.3 — Hit promotion + miss-path fill 🔲 pending

On hit at L2/L3, schedule promotion to L1 (async, only if L1 has
room). On miss, new publish lands at L1; existing L1 entries demote
to L2 under pressure.

### 3.4 — Tombstone propagation 🔲 pending

On eviction below L3 (entry is gone for good), broadcast a tombstone
header so consumers don't chase dead pointers. Consumer-side catalog
removes the tombstoned block_uuid from local maps.

### 3.5 — Benchmark tier hit rates under realistic workload 🔲 pending

Zipfian workload: 100 distinct documents, skewed access pattern.
Measure L1 / L2 / L3 hit rates and miss rate; compare total
throughput vs. a single-tier (L1-only) baseline.

### 3.6 — Experiment 3: Tool-call eviction buffer 🔲 pending (needs 3.1–3.4)

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
- **Phase 2.2 crossover benchmark is still pending.** The 1.05 Gbps
  crossover threshold is calculated, not measured. The sweep needs to
  confirm the crossover curve matches the math.
- "KV-cache CDN" is aspirational until Phase 3 (tiered storage) +
  Phase 4 (context-push) land. The current code is a single-tier
  distributed cache with read-on-demand; the CDN semantics arrive
  with tiering + origin-push.
- **SGLang: permanently blocked on this machine (2026-04-22).**
  `cicc` (CUDA IR compiler) uses 4–7 GB RAM per `.cu` file; 4
  simultaneous = 28 GB, OOM-kills Pulsar and itself on the 32 GB box.
  Pre-built PyPI wheels are SM90/SM100-only and ABI-incompatible with
  torch 2.x. Unblocking paths: CUDA 12.8 toolkit upgrade (enables
  pre-built wheels with SM86 support), or Hopper (H100) machine.
  Do not attempt a source build on this machine without restricting
  to `THREADS=1` and closing all other processes.

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
| 2.2 | `#30` | pending (Experiment 1, next) |
| 2.3 | `#31` | pending (Experiment 2) |
| 3.6 | `#32` | pending (Experiment 3, blocked on 3.1–3.4) |
| 5.1 | `#18` | pending (deferred) |
