# TODO — KV-Cache CDN build-out

Reorganized around the thesis in `DESIGN.md` (KV-cache CDN for edge
LLM serving, EdgeServe-v2). Read `DESIGN.md` first.

Claude Code TaskList IDs on the Mac side are listed for cross-reference
(e.g. `[#24]`) but won't map to the remote's TaskList — just use these
sections to create equivalent tasks via TaskCreate on the remote.

Phase ordering is important:
- **Phase 1** (entity-keyed discovery) closes the one big mechanism
  gap — today the bloom carries prefix hashes, the thesis needs
  semantic entities. Until this lands, the implementation doesn't match
  the design and the headline "permuted context hit" scenario can't be
  demonstrated.
- **Phase 2** (LAN measurement) is the paper's money graph. Blocked on
  nothing code-wise; can run in parallel with Phase 1, but the numbers
  are more compelling after entity discovery works.
- **Phases 3-5** are the production-shape story: tiered storage,
  context push, zero-copy transports. Do them in order.

---

## Phase 1 — Entity-keyed discovery

Goal: replace the prefix-hash bloom with a semantic-entity bloom
(prefix-hash retained as fallback), close the gap between what
RESULTS.md claims and what the code does.

### 1.1 — Honesty note in RESULTS.md `[#29, quick-win]`

~15 min, docs only. Before any code change, add a paragraph to
`RESULTS.md` under "Benchmark honesty audit" distinguishing:

- what the paper/DESIGN.md describes: bloom filter encoding semantic
  entity tags
- what's shipped in code today: bloom filter encoding block-aligned
  prefix hashes
- consequence: current speedups only apply to shared-token-prefix
  workloads (doc-first), not to permuted-context workloads (persona-
  first)

Do this first. Makes the repo internally consistent regardless of
whether Phase 1 completes.

### 1.2 — Publisher accepts entity tags `[#24]`

Extend the vLLM KVConnector publish path so each request can attach
user-declared entity tags to the bloom filter alongside the existing
prefix hashes.

**API proposal:** add `entities_fn` to `EdgeServeKVConnector`'s
`kv_connector_extra_config`:

```python
kv_connector_extra_config={
    ...
    'entities_fn_module_path': 'myapp.kv_entities',
    'entities_fn_name': 'extract_entities',
}
```

where `extract_entities(prompt_token_ids) -> set[str]` returns the
semantic tags for the prompt. Alternative if we find a way:
`request.metadata` field on vLLM.

**Implementation touches:**
- `edgeserve/inference/vllm_kv_connector.py` — plumb the fn through
  `_Worker` and call it in `save_kv_layer` /  `wait_for_save` to populate
  entities alongside `_hash_token_ids(...)`.
- `edgeserve/semantic_cache/bloom.py` — already supports arbitrary
  entity strings. No change.
- `KV_CONNECTOR.md` — document the API.

**Do NOT** modify the lookup path in this task. This task strictly
changes what gets published. Scheduler still uses prefix-hash matching.

### 1.3 — Scheduler entity-intersection lookup `[#25]`

After 1.2. Add an entity-based lookup path to
`_Scheduler.get_num_new_matched_tokens`.

**Flow on new request:**
1. If the request has declared entities (from the same `entities_fn`
   supplied at publish time), call
   `SemanticCacheClient.catalog.lookup(entities)`.
2. On positive hit, look up the header to determine how many tokens
   the cached KV covers (`header.covered_tokens` — new field on
   `CacheHeader`, populated at publish time).
3. If covered_tokens ≥ consumer's aligned prompt length, return
   `(covered_tokens - num_computed_tokens, False)` and flag the
   request as load path.
4. If not, or if no entity match, fall back to current prefix-hash
   longest-match logic.

**New field on CacheHeader:**
`covered_tokens: int` — number of tokens in the cached KV block.
Required because with entity lookup the consumer no longer
reconstructs the publisher's token sequence.

### 1.4 — Permuted-persona demo + correctness `[#26]`

Blocks on 1.2 + 1.3.

Write `scripts/demo_kvconnector_permuted.py`:
- Seeder prompt: `"Persona A: Reply as scientist. " + doc + " Summarize."`
- Consumer prompt: `"Persona B: Reply as historian. " + doc + " Summarize."`
- Both declare entity `{doc_id}` via `entities_fn`.

Verify:
- Consumer gets a cache HIT via entity match (not prefix — first
  tokens differ).
- Consumer's next-token id equals a cold no-cache run of the same
  consumer prompt.
- Speedup vs cold reported.

Add row to `RESULTS.md` headline TL;DR table.

### 1.5 — Rewrite RESULTS framing `[#27]`

Blocks on 1.4.

Replace the "prefix-bloom vs semantic-bloom" honesty note (from 1.1)
with a proper "Semantic entity discovery" section that includes the
permuted-persona numbers. Prefix-hash path remains documented as the
fallback for consumers without entity metadata.

---

## Phase 2 — LAN CDN measurements

Goal: the paper's headline figure. Prove the architecture pays for
real over LAN.

### 2.1 — Two-host deployment on home LAN `[#28]`

Mac Mini + 3080 Ti. Run the two-stage demo across them.

Concrete steps:
- Pulsar broker on one host (probably GPU box — easier Docker).
- Seeder = GPU box running vLLM.
- Consumer = Mac Mini running... something. Options:
  - vLLM on Mac CPU (available but slow; might not be worth it)
  - HFEngine on Mac (works, but non-production)
  - Just the "resolve + safetensors load + scatter" part, no actual
    inference, to isolate the transport measurement
- Ensure `CacheHeader.hostname` triggers the HTTP path, not
  `local_path` (the file system paths differ across hosts).

Deliverables:
- `scripts/demo_kvconnector_lan.py`
- LAN HTTP fetch wall-time for 50-1000 MB blobs (ballpark bandwidth
  under real conditions, not synthetic iperf).
- End-to-end edge-decode wall-time.

### 2.2 — Bandwidth-vs-recompute crossover benchmark

The paper's money figure. Pick 3 models (0.5B, 1.5B, 7B if it fits),
3-4 context lengths (1k, 4k, 16k, 50k tokens), plot:

- x-axis: effective link bandwidth (simulate with `tc qdisc` or
  `iproute2` netem; sweep 100 Mbps, 500 Mbps, 1 Gbps, local)
- y-axis: wall-time
- two curves per model×context: "edge prefill locally" vs "pull KV
  over link"

Output: publication-quality crossover plot showing the regime where
the architecture wins. Deliverable:
`scripts/bench_bandwidth_crossover.py` + `docs/bandwidth_crossover.png`
(if we allow non-gitignored image) or numbers table in RESULTS.md.

### 2.3 — Update RESULTS.md "Cross-host" section

Today the section is empty (just says "we haven't tested this"). Fill
with real numbers + the crossover plot reference.

---

## Phase 3 — Tiered storage on the context server

Goal: context server can hold far more KV than fits in GPU.

### 3.1 — Design doc for eviction policy

Short doc (~200 lines) covering:
- Tier capacities (GPU / pinned CPU / NVMe)
- Promotion rules (on hit, move up one tier)
- Eviction rules (LRU within tier, demote on pressure, tombstone on
  full-evict)
- Tombstone broadcast mechanic (new `CacheHeader.deleted=True` bit?
  Or a separate tombstone topic?)

### 3.2 — L1/L2/L3 backing store

- L1 = GPU paged-buffer (same as today, owned by vLLM)
- L2 = pinned CPU tensors on the context server, mmap-able
- L3 = NVMe file, mmap-able

Write an `edgeserve/semantic_cache/tiered_store.py` that wraps the
existing `CacheHttpServer.write_block` / read paths with tier-aware
routing.

### 3.3 — Hit promotion + miss path fill

On cache hit at L2/L3, promote the entry back to L1 (if there's room)
or stay-in-place (if not). On miss, new publish goes into L1, pushes
existing L1 entries down.

### 3.4 — Tombstone propagation

On eviction below L3 (i.e. entry is gone for good), broadcast a
tombstone header so consumers don't chase dead pointers. Add a
`tombstones` topic or reuse the header topic with a deletion flag.

### 3.5 — Benchmark tier traffic under realistic workload

Cold-working-set benchmark: 100 distinct documents, Zipf-distributed
access pattern. Measure:
- L1 hit rate, L2 hit rate, L3 hit rate
- Miss rate (recompute)
- Mean latency by tier

---

## Phase 4 — Context-push daemon

Goal: edge-side file watcher that keeps the context server's KV fresh.

### 4.1 — Context ingest endpoint on context server

Context server exposes `POST /ingest` with:
- Entity tag (including content-sha)
- The raw content (tokens, text, file bytes)
- Model target

Server tokenizes + prefills + publishes. Async; returns a job id or
streaming progress.

### 4.2 — Edge-side watcher

Lightweight daemon (`edgeserve.edge.watcher`) that monitors a
directory (e.g. `~/.edgeserve/context/`):
- On file change, compute new content-sha
- POST updated content to the context server with
  `entity=codebase:{repo}/{path}@sha={new_sha}`
- Old entity (same entity, old sha) eventually tombstones via tiered-
  storage LRU eviction

### 4.3 — Entity versioning semantics

- Entity tag includes content-sha.
- Consumer requesting `codebase:myrepo/file.py` without sha gets
  latest-sha'd entry from catalog (most recent `created_ms`).
- Consumer requesting `codebase:myrepo/file.py@sha=abc123` gets that
  specific version (or miss if evicted).

### 4.4 — End-to-end session demo

A realistic flow:
- User edits `file_diff_v2.py` in their editor.
- Watcher daemon pushes to context server, which prefills + publishes.
- User on edge runs "analyze `file_diff_v2.py`" via local inference.
- Edge discovers the fresh entity via catalog, pulls KV, decodes
  answer locally.
- No user tokens ever leave the edge.

Benchmark: time from file save to first-token-generated, compared to
"no cache — edge prefills from scratch" baseline.

---

## Phase 5 — Zero-copy transport

Defer until Phase 1-4 are done.

### 5.1 — Same-host CUDA IPC `[#18, existing task]`

For cases where context server and edge inference happen to share a
machine (single-GPU dev setup, or multi-process on a beefy
workstation), `torch.multiprocessing`-style CUDA IPC handles gives
zero-copy transfer between processes sharing a CUDA context.

Drops the current ~6-55 ms overhead to near zero for same-host hits.

### 5.2 — Cross-host RDMA / NCCL

For production clusters with real RDMA fabric, plumb NCCL / NIXL
behind `SemanticCacheClient.resolve_into` so the transport is RDMA
verbs rather than HTTP. Big engineering lift, modest narrative payoff
for the edge-focused story — mostly a "we're not slower than existing
in-datacenter solutions" check.

---

## Honesty threads to keep tracking

- The current numbers in RESULTS.md are prefix-hash, doc-first
  scenarios. Until Phase 1 lands, do NOT rephrase them as "semantic
  routing" results.
- Cross-host is empty. Until Phase 2.1 runs, the 1.54× "LAN number"
  in RESULTS.md is simulated, not measured.
- The paper's claim is CDN-for-LLM. Without tiered storage (Phase 3)
  and context push (Phase 4), the word "CDN" is aspirational. Be
  careful not to overclaim in writeups.
- SGLang is genuinely blocked on CUDA 12.8+. Separate from all
  phases above; only unblocks if the GPU box gets a toolkit upgrade
  or we move to a Hopper box.
