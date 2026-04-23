# DESIGN — KV-Cache CDN for Edge LLM Serving

*EdgeServe-v2.* A natural extension of EdgeServe (Shaowang & Krishnan,
[arxiv 2303.08028](https://arxiv.org/pdf/2303.08028.pdf)) from routing
streaming feature data and model outputs across edge nodes to routing
**KV-cache blocks** across edge nodes for large language model
serving.

---

## Thesis

**Decode wants to live at the edge; prefill wants to live next to the
data. KV cache is the transfer asset that bridges them, discovered
probabilistically by semantic entity.**

Small-to-medium LLMs running on local hardware (M-series Macs,
prosumer GPUs, eventually phones) are viable for interactive tasks but
are starved on long-context prefill. A 50 k-token codebase that a
developer wants to analyze takes seconds-to-minutes to prefill even on
a 3080 Ti; it takes a fraction of a second to *decode one more answer*
from a cached context.

This inverts the usual cloud-API pattern. The natural architecture is:

- **Edge inference node** near the user: runs decode, owns the live
  interactive loop, never sends user prompts or generated tokens over
  the wire.
- **Context/prefill server** on the nearby network: holds the user's
  working set (codebase, documents, long conversation history), runs
  prefill when the working set changes, serves KV-cache blocks on
  request.
- **Discovery fabric**: a lightweight catalog so the edge node can ask
  "does any nearby server already hold KV for `file_diff_v2.py` at
  revision `abc123`?" before paying for prefill itself.

Put together, it is a **content-distribution network for KV caches**:

| CDN concept | KV-cache CDN analogue |
|---|---|
| Origin server | Context server (holds durable KV over tiered storage) |
| Edge PoP | Edge inference node (decode, near user) |
| Cacheable asset | Per-entity KV-cache block |
| URL / content ID | Semantic entity tag `(entity, model, version, sha)` |
| Cache-control / TTL | Catalog header timestamp + TTL |
| DNS / edge selection | Bloom-filter catalog lookup |
| Origin-pull | Edge pulls KV on demand from context server |
| Origin write-through | Edge pushes changed working-set files to context server; context server re-prefills in background |

Why bloom filters specifically: in a many-entity, many-node fabric,
most lookups are misses (the agent asks about some doc that hasn't
been cached anywhere). A compact bloom per node lets the requester
filter locally — most queries short-circuit in microseconds, without
any RPC. Precedent: Coral (NSDI '04) used bloom filters over a DHT for
peer discovery exactly because central indexes don't scale for sparse
lookups.

---

## Deployment shape

Typical setup (LAN-scale edge, the design's target):

```
┌─────────────────────┐     LAN (≥ 1 Gbps)      ┌────────────────────────┐
│  edge device        │ ◄───────────────────────┤  context server        │
│  (Mac, prosumer PC) │                         │  (home rack / office)  │
│                     │                         │                        │
│  • decode           │  header broadcast       │  • prefill             │
│  • user prompt in   │  (Pulsar pub/sub)       │  • KV L1 (GPU)         │
│  • generated tokens │ ◄───────────────────────┤  • KV L2 (CPU RAM)     │
│    out (stay local) │                         │  • KV L3 (NVMe)        │
│                     │                         │                        │
│  • local radix      │  KV pull on hit         │  • HTTP server         │
│    prefix cache     │ ◄───────────────────────┤    (mmap safetensors)  │
└─────────────────────┘                         └────────────────────────┘
                                                             ▲
                                                             │ context push
                                                             │ (files, docs,
                                                             │  conversation
                                                             │  history)
                                                             │
                                                   ┌──────────────────────┐
                                                   │  user's source of    │
                                                   │  truth — repo, docs, │
                                                   │  agent memory        │
                                                   └──────────────────────┘
```

- **Network assumption**: sub-ms LAN between edge and context server.
  Not WAN. Home/office/campus network, not cloud-to-phone.
- **Model coupling**: edge and context server run the same model
  checkpoint. Heterogeneous edge fleets fragment the cache; key
  entities by `(entity, model_id, model_version)` to make this
  explicit.
- **Privacy model**: the context itself (user's code, docs) is
  uploaded once to the nearby context server — the same trust level
  as using any cloud LLM today, but over LAN. The *live interactive
  loop* (prompt, generated tokens) never leaves the edge. KV transfer
  from context server to edge is lossy-ish and harder to invert than
  raw text.

---

## Economic case — when this pays off

The question is always: **is fetching KV faster than recomputing it
locally?**

Let `C(doc, model, edge_hw)` be local prefill wall-time for a context
on the edge device, and `T(KV_size, bandwidth)` be transfer time. We
win when `T < C`.

Rough numbers for `Qwen2.5-1.5B` / 50 k-token doc:

- Local prefill on M-series Mac: ~10-30 s (CPU + MPS)
- Local prefill on 3080 Ti: ~3-8 s
- KV size: ~1 GB (28 layers × GQA × bf16)
- LAN transfer (1 Gbps): ~8-10 s
- WAN transfer (100 Mbps home internet): ~80-100 s

Takeaway: **LAN is the regime where the architecture pays**. WAN is
break-even-to-worse for small-medium models. This is the same reason
CDN edge PoPs are co-located with ISPs rather than run from a single
origin — propagation + bandwidth cost is load-bearing.

The headline figure for the paper should be this crossover curve,
varied over `(model_size, context_length, link_bandwidth)`.

---

## Content addressing: entity tags

Every cache block is keyed by a **semantic entity tag**, not by raw
token hash. Examples:

- `codebase:myrepo/file_diff_v2.py@sha=abc123` — a code file at a
  specific revision
- `doc:acme-manual@v7` — a versioned document
- `session:user-42/thread-9` — a conversation prefix
- `dataset:legal-contracts/record-7731@2025-Q1` — a fact block

Why entities, not prefix hashes: the requester knows *what it needs*
(a semantic identifier) but not *the exact tokenization context in
which it was previously cached*. An agent querying `"Analyze
file_diff_v2.py"` shouldn't have to reconstruct the tokenized prefix
of the previous user who cached it.

Structure of the tag (for hashing into the bloom):
- `entity` — application-defined string, unique within the fleet
- `model_id` — e.g. `Qwen/Qwen2.5-1.5B`
- `model_version` — checkpoint hash or version string
- `content_sha` — hash of the underlying content (for invalidation
  when `file_diff_v2.py` changes)

Publishers hash all four into one or more bloom entities. Consumers
construct the same tuple and query.

Prefix-hash entities (what we have today) remain a fallback — if a
consumer doesn't know the entity but does present the same tokenized
prefix, the prefix-hash path still hits.

---

## Tiered storage on the context server

A single context server should hold more KV than fits on its GPU. Use
a three-tier LRU:

- **L1 — GPU memory.** Hottest entries, served directly from paged
  buffer. Size: a fraction of VRAM (e.g. 4-8 GB).
- **L2 — pinned CPU RAM.** Recently evicted from L1. mmap-backed so
  HTTP serving is zero-copy. Size: tens of GB.
- **L3 — NVMe.** Cold. Still mmap-able, just slower. Size: TB-class.

Eviction policy: LRU within each tier, promote on hit (L3→L2→L1),
evict downward on pressure. Tombstones broadcast to catalog on
eviction-below-L3 so consumers don't chase dead entries.

Transport between tiers on the context server is internal to that
node; the protocol between nodes (edge↔context) is unchanged — HTTP
GET by UUID, mmap-served from whichever tier currently holds the file.

---

## Context push (write-through / origin-pull inverse)

The edge is the source of truth for the user's working set. When the
user edits `file_diff_v2.py`, the edge pushes the new file to the
context server and schedules async prefill:

1. Edge-side watcher detects file change (fs events for code,
   OS-level doc change hooks, app-level session logs).
2. Edge `POST`s the new file to the context server's ingest endpoint,
   along with the entity tag it should be registered as.
3. Context server prefills the new content (async, as a regular vLLM
   generate with the connector's publish path).
4. Header lands in catalog; stale entry (old sha) tombstoned.
5. On next decode request from edge referencing the entity, the new
   sha's KV is discovered and pulled.

For interactive tasks the push happens once per session or once per
edit, not per token; this doesn't saturate the upstream link.

---

## What's built (as of 2026-04-23)

| component | file | status |
|-----------|------|--------|
| Bloom-filter catalog over Pulsar | `edgeserve/semantic_cache/` | ✅ |
| HTTP retrieval + same-host mmap fast path | `CacheHttpServer`, `kv_io.py` | ✅ |
| vLLM `KVConnectorBase_V1` (paged-buffer gather/scatter) | `vllm_kv_connector.py` | ✅ |
| Prefix-hash bloom entries + longest-prefix scheduler lookup | same | ✅ |
| User-declared entity tags (semantic lookup) | `set_request_entities`, `_Scheduler` | ✅ |
| Entity versioning + tombstones | `CacheHeader.deleted`, `TieredStore.on_tombstone` | ✅ |
| Tiered storage L2 (pinned RAM) + L3 (NVMe) | `tiered_store.py` | ✅ |
| Context-push daemon (ingest endpoint + edge watcher) | `context_server.py`, `watcher.py` | ✅ |
| Correctness tests (bit-exact warm-path token match) | `tests/test_vllm_kv_connector.py` | ✅ |
| Cross-host LAN measurement (Mac Mini ↔ 3080 Ti) | `demo_kvconnector_lan.py` | ✅ |
| Bandwidth-vs-recompute crossover benchmark | `bench_bandwidth_crossover.py`, `bench_bandwidth_throttle.py` | ✅ |

What remains (ordered gap list for the paper):

1. **End-to-end edge inference demo** — a user on the Mac types a query; Mac
   fetches KV from GPU box; Mac generates answer locally; prompt and tokens
   never leave the Mac. Currently every demo runs inference on the GPU box.
   This is the single demo that proves the thesis. → Phase 6.

2. **LMCache direct comparison** — same workload, both systems, side-by-side
   TTFT and cold-node discovery latency. Required by any systems reviewer.
   → Phase 7.2.

3. **NIXL / NixlConnector comparison** — overhead vs same-host warm hit;
   cross-host capability difference. → Phase 7.3.

4. **B1 honest framing** — measure single-vLLM-with-APC as the hard same-host
   ceiling and explicitly state EdgeServe's niche (cross-host / Mac edge).
   → Phase 7.1.

5. **7B model + 32k-token evaluation** — the regime the thesis is strongest in.
   Requires A100 or multi-GPU. → Phase 8.

6. **Zero-copy transports** — CUDA IPC (same-host, −6–55 ms overhead) and RDMA
   (cross-host). Deferred. → Phase 5.

---

## Relationship to prior work

- **EdgeServe** (Shaowang & Krishnan, 2023): parent. Decentralized
  streaming model serving over Pulsar. This design reuses the
  catalog+pubsub substrate and extends the payload type to KV cache.

- **vLLM prefix caching / RadixAttention** (Zheng et al., 2023): the
  in-process ceiling we ride on top of. Our work is strictly a *layer
  above* vLLM's own APC; when vLLM's cache hits, our layer is bypassed.
  Our connector adds +6–55 ms overhead vs. the internal APC — this is
  the cost of cross-process capability.

- **LMCache** (MLSys'25): stores KV blocks on CPU/disk per vLLM process,
  retrieves on prefix hit. Core overlap with this work. **Key difference:**
  LMCache requires explicit configuration of which node holds what KV;
  EdgeServe uses bloom-filter broadcast for zero-configuration discovery —
  a new consumer on a cold node subscribes to the Pulsar topic and finds
  cached KV without any manual registry. **Measurement needed:** Phase 7.2.

- **NIXL / NixlConnector** (vLLM 0.19+): UCX-based KV transfer between
  vLLM processes, shipped in-tree. Registered alongside `EdgeServeKVConnector`
  in vLLM's connector factory. NIXL likely has lower latency on the same host
  (UCX shared memory vs. mmap safetensors) but requires RDMA fabric for
  cross-host; EdgeServe falls back to HTTP for arbitrary LAN topologies and
  adds the semantic entity discovery layer. **Measurement needed:** Phase 7.3.

- **Mooncake** (ATC'25): disaggregated prefill-decode over RDMA in a GPU
  datacenter. Same primitive (move KV between servers) but targets a very
  different operating point: high-bandwidth GPU cluster vs. heterogeneous
  LAN edge with CPU/MPS consumer nodes. Not a direct competitor; useful as
  an upper-bound reference for what zero-copy transport can achieve.

- **Coral** (NSDI '04) and **hierarchical web caches** (Squid, Varnish):
  bloom-filter-over-distributed-cache precedent. Coral filtered DHT lookups
  with per-node blooms to avoid network RTTs for cold misses. We apply the
  same pattern to KV-cache discovery. The difference: our entities are
  semantic (model + content SHA) rather than URL hashes.

- **PromptCache** (2023): caches KV for *schema-defined* prompt segments,
  reuses across requests whose prompts share those segments. Complements
  RadixAttention. EdgeServe is strictly at a higher layer — we move KV
  across process and host boundaries, regardless of how the KV was
  generated. PromptCache could feed our catalog as a publisher.

### How to differentiate (for the paper)

The **central novelty claim** is the combination of:
1. Semantic entity tagging (not raw URL or prefix hash) as the cache key,
   enabling cache discovery even when the exact tokenized context is unknown.
2. Bloom-filter broadcast over Pulsar for zero-configuration cross-node
   discovery — no central registry, no per-node config.
3. Tiered storage (L2 RAM + L3 NVMe) on the context server for durability
   across GPU eviction and process restarts.
4. HTTP fallback transport that works over any LAN without RDMA fabric.

LMCache has (3) partially; NIXL has better (4) transport but not (1)/(2).
No existing system combines all four for the edge LAN deployment context.

---

## Evaluation roadmap

Each paper claim maps to a specific experiment. Use this table to track coverage.

| claim | experiment | status | section |
|-------|-----------|--------|---------|
| Cross-process KV sharing works and is correct | Phase 2.3 fan-out, bit-exact token match | ✅ RESULTS §2.3 | §eval.correctness |
| Crossover: fetch beats prefill above ~1.5 Gbps (GPU edge) | Phase 2.2 bandwidth sweep | ✅ RESULTS §2.2 | §eval.crossover |
| Crossover: fetch beats prefill above ~125 Mbps (Mac edge) | Phase 2.2c Mac M4 baseline | ✅ RESULTS §2.2c | §eval.crossover |
| Tiered storage delivers LRU hit rates under Zipfian load | Phase 3.5 hit-rate sweep | ✅ RESULTS §3.5 | §eval.tiers |
| NVMe persistence survives GPU eviction, speedup on restore | Phase 3.6 tool eviction | ✅ RESULTS §3.6 | §eval.eviction |
| Context-push pipeline (edit → ingest → restore) works | Phase 4.4 end-to-end demo | ✅ RESULTS §4 | §eval.contextpush |
| **Decode stays at the edge; prompt never leaves** | Phase 6.1 Mac edge inference | 🔲 TODO §6 | §eval.privacy |
| **EdgeServe's niche: cross-host, not same-host vs APC** | Phase 7.1 B1 framing | 🔲 TODO §7.1 | §eval.baselines |
| **Differentiator over LMCache: zero-config discovery** | Phase 7.2 LMCache comparison | 🔲 TODO §7.2 | §eval.related |
| **Differentiator over NIXL: cross-host + no RDMA** | Phase 7.3 NIXL comparison | 🔲 TODO §7.3 | §eval.related |
| CDN economics improve at 7B / 32k tokens | Phase 8.1–8.2 scale evaluation | 🔲 deferred | §eval.scale |

**Priority order for next work sessions:**

1. Phase 6 (end-to-end Mac demo) — closes the thesis-vs-demo gap; builds
   the figure the paper's introduction should show.
2. Phase 7.1 (B1 framing) — one extra row in the existing results table;
   directly addresses the "why not just use vLLM APC" reviewer question.
3. Phase 7.2 (LMCache) — required for any systems venue submission.
4. Phase 7.3 (NIXL) — secondary; useful if we target a vLLM-aware audience.
5. Phase 8 (scale) — deferred until better GPU hardware is available.

---

## Non-goals

- **Faster than single-process vLLM prefix cache.** We're not. When
  vLLM's in-process cache applies, it's the best option; our layer
  adds a 6–55 ms per-hit overhead. Our value is cross-process
  discovery, not intra-process speed.
- **Cross-model KV transfer.** Keyed by `model_id + model_version`;
  moving KV across model versions is out of scope.
- **WAN-scale (cloud-to-home).** Economics flip at ~100 Mbps home
  internet for small-medium models. Target is LAN.
- **Replacing vLLM's scheduler.** We plug into vLLM's existing
  KVConnector interface; we don't rewrite paging.
- **"Permuted persona" KV sharing is fundamentally infeasible — not a
  non-goal but a non-possibility.** Under causal attention with RoPE
  (standard in all production transformers), the KV at position `j`
  depends on every preceding token's content AND on position `j`
  itself. Two prompts `persona_A + doc` and `persona_B + doc` — even
  when the doc segment is byte-identical — produce KV values at each
  doc-position that differ, because the doc tokens attend to a
  different persona *and* occupy different positions if the personas
  are different lengths. Our entity-tag matching therefore can ONLY
  share KV when the content preceding the shared segment is
  identical; in practice this is "doc as prefix, question as
  suffix." The paper chapter's pitch of arbitrary persona
  permutation is hand-wavy in the transformer sense; achieving it
  would require architecture changes (position-agnostic KV like
  PromptCache, attention-sinks, or KV-adjustment post-hoc) that are
  out of scope for this work. This is why our demos structure prompts
  as `doc + suffix_X`, not `persona_X + doc`.

---

## Open design questions

- **Catalog scale.** Pulsar topic-per-fleet; bloom TTL vs. replay
  cost; leader election for a consolidated view vs. all-to-all
  broadcast. At what fleet size does the gossip cost start to bite?
- **Privacy boundary.** Is entity-name-visibility a concern?
  (Broadcasting `codebase:my-private-repo/secret.py` to all nodes
  even if the KV itself stays on one node.) Hash entity names before
  broadcast?
- **Versioning and eventual consistency.** Two edge devices push
  conflicting versions of the same file — which content server is
  authoritative? CRDT-ish last-write-wins vs. explicit version
  requester.
- **Multi-tenant.** Per-tenant Pulsar topic? Entity namespace?
  ACL-on-retrieval?
