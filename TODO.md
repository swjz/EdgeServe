# TODO — KV-Cache CDN build-out

Reorganized around the thesis in `DESIGN.md` (KV-cache CDN for edge
LLM serving, EdgeServe-v2). Read `DESIGN.md` first.

**Status snapshot:** Phase 1 (entity-keyed discovery) landed in commit
`6ef919f`. Publisher now encodes user-declared entity tags in the
bloom alongside prefix hashes; scheduler has an entity-first lookup
path with prefix-hash fallback; `scripts/demo_kvconnector_semantic.py`
exercises it end-to-end. **The work pending now is Phase 2+** (real
measurements + tiered storage + context-push).

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

### 1.5 — RESULTS.md rewrite with entity-lookup numbers 🔲 pending

Run `scripts/demo_kvconnector_semantic.py` on the GPU box, capture
the four timings (cold / seeder / consumer-hash / consumer-entity),
add a **"Semantic entity discovery"** section to RESULTS.md. Update
the headline TL;DR table with a new row. Retire the "prefix-bloom vs
semantic-bloom" note from the audit section now that both paths are
real.

Also: propagate the permuted-persona impossibility into RESULTS.md
(it lives in DESIGN.md non-goals today but the results doc should
mention it alongside the numbers — reviewers of the writeup will
ask).

---

## Phase 2 — LAN CDN measurements

The paper's headline figures. Prove the architecture pays over LAN.

### 2.1 — Two-host deployment on home LAN 🔲 pending

Mac Mini + 3080 Ti over home LAN. See
`SESSION_SUMMARY.md` migration section for the exact environment
setup. Key concrete steps:

- Pulsar broker on the GPU box (Docker, already running there).
- Seeder: GPU box (3080 Ti) running vLLM.
- Consumer: Mac Mini. Options:
  - Run vLLM CPU on Mac (slow; probably not worth the compute).
  - Run `HFEngine` on Mac MPS (works end-to-end, less realistic).
  - Measure the **transport only** (HTTP fetch + safetensors decode
    + paged-buffer scatter) on Mac, decouple from actual decode —
    this isolates the LAN question.
- Ensure consumer's `CacheHeader.hostname` differs from seeder's so
  `_is_local_readable()` returns False and the HTTP path fires, not
  the same-host mmap.

Deliverables:
- `scripts/demo_kvconnector_lan.py` or equivalent.
- LAN HTTP fetch wall-time for 50–1000 MB blobs, iperf comparison.
- End-to-end edge-decode wall-time (if running a real engine on Mac).

### 2.2 — Bandwidth-vs-recompute crossover benchmark 🔲 pending

The paper's money figure. Sweep:
- model size: Qwen2.5-0.5B, 1.5B, optionally 3B/7B if fits on edge.
- context length: 1k, 4k, 16k, 50k tokens.
- effective link bandwidth: sim with `tc qdisc` / `iproute2 netem`,
  sweep 100 Mbps / 500 Mbps / 1 Gbps / same-host.

Two curves per `(model, context)` point: "edge prefills locally" vs
"edge pulls KV over link." Output: single publication-quality plot
showing the regime where the architecture wins.

Deliverable: `scripts/bench_bandwidth_crossover.py` + numbers /
figure in RESULTS.md.

### 2.3 — Update RESULTS.md "Cross-host" section 🔲 pending

Today the section is empty placeholder. Fill with the 2.1/2.2
numbers.

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

- Phase 1 entity-tag numbers haven't been recorded in RESULTS.md
  yet. Until 1.5 lands, the existing RESULTS.md numbers are all
  prefix-hash-only. Don't rephrase them as "semantic routing"
  results without fresh measurements from the entity-tag demo.
- Cross-host section of RESULTS.md is empty. The 1.54× "LAN number"
  currently in RESULTS.md was measured by simulating HTTP on one
  machine (disabling the same-host fast path), not actual LAN.
  Until Phase 2.1 runs, treat that as simulation.
- "KV-cache CDN" is aspirational until Phase 3 (tiered storage) +
  Phase 4 (context-push) land. The current code is a single-tier
  distributed cache with read-on-demand; the CDN semantics arrive
  with tiering + origin-push.
- SGLang is still blocked on CUDA 12.8+ availability on the GPU box.
  Separate from all phases above; only unblocks on toolkit upgrade
  or moving to a Hopper machine.

---

## Cross-ref: Claude Code TaskList on Mac (for traceability)

| section | Mac TaskList id | status |
|---|---|---|
| 1.1 | `#29` | completed |
| 1.2 | `#24` | completed |
| 1.3 | `#25` | completed |
| 1.4 | `#26` | ~~pending~~ retired (infeasible) |
| 1.5 | `#27` | pending |
| 2.1 | `#28` | pending |
| 5.1 | `#18` | pending |
