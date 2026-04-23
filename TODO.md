# TODO — KV-Cache CDN build-out

Reorganized around the thesis in `DESIGN.md` (KV-cache CDN for edge
LLM serving, EdgeServe-v2). Read `DESIGN.md` first.

**Status snapshot (2026-04-23):** Phase 1 (entity-keyed discovery)
and Phase 2.1 (LAN CDN measurements) are complete. Publisher encodes
user-declared entity tags in the bloom alongside prefix hashes;
end-to-end LAN HTTP transport confirmed at 165–183 Mbps (7.5–8×
slower than GPU recompute). **The work pending now is Phase 2.2**
(bandwidth-vs-recompute crossover benchmark). SGLang is permanently
blocked on this machine (OOM during compilation; see Honesty threads).

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

The paper's headline figures. Prove the architecture pays over LAN.

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

### 2.2 — Bandwidth-vs-recompute crossover benchmark 🔲 next

**← Active next task.** The paper's money figure. Mathematical crossover
for Qwen2.5-1.5B on 3080 Ti: 132 MB/s (1.05 Gbps). Script not yet
written.

Sweep plan:
- blob size: vary `--doc-repeats` (16 / 32 / 64 / 128 / 256 repeats
  → ~530–8448 tokens, ~7–235 MB blobs).
- optional bandwidth throttle: `tc qdisc netem rate Xmbit` to simulate
  100 / 500 / 1000 Mbps links.

For each blob size, record:
1. GPU prefill time (from `demo_kvconnector_lan.py seed` timings).
2. LAN fetch time (from `consume --repeats 5` median).
3. Crossover ratio = fetch / prefill.

Deliverable: `scripts/bench_bandwidth_crossover.py` that runs the
seeder side, then the consumer side (across ssh if needed), prints a
table, and appends rows to RESULTS.md §Phase 2.2.

### 2.3 — Update RESULTS.md "Cross-host" section ✅ done (2026-04-22)

Phase 2 section in RESULTS.md filled with 2.1 numbers (fetch table,
crossover analysis). Will expand with 2.2 sweep table once that runs.

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
| 1.4 | `#26` | ~~pending~~ retired (infeasible) |
| 1.5 | `#27` | completed (2026-04-22) |
| 2.1 | `#28` | completed (2026-04-22) |
| 2.2 | — | pending (next) |
| 5.1 | `#18` | pending (deferred) |
