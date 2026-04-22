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

## What's built today vs. what's needed

Built:
- Catalog over Pulsar with bloom-filter header broadcast
  (`edgeserve.semantic_cache.*`)
- HTTP retrieval + same-host mmap fast path
  (`CacheHttpServer`, `kv_io.load_past_key_values_from_path`)
- vLLM `KVConnectorBase_V1` implementation with paged-buffer
  gather/scatter (`edgeserve.inference.vllm_kv_connector`)
- **Prefix-hash** bloom entries and longest-prefix scheduler lookup
- Correctness tests (bit-exact warm-path token match)
- Same-host multi-process benchmarks: 2.4-3.3× speedup (see
  RESULTS.md, caveats in "Benchmark honesty audit")

Not built (the ordered gap list):

1. **Entity-tag publish and lookup** — the single biggest gap. Today
   the bloom contains token-prefix hashes; the design needs
   user-declared entity tags. Code lives in
   `vllm_kv_connector._Worker.wait_for_save`,
   `_Scheduler.get_num_new_matched_tokens`.
2. **Cross-host LAN measurement** — the headline figure. Mac Mini ↔
   3080 Ti over home LAN. Existing `CacheHeader.hostname` logic falls
   back to HTTP automatically when hostnames differ; need the actual
   benchmark.
3. **Bandwidth-vs-recompute crossover benchmark** — the paper's money
   graph. Sweep model size × context length × simulated link
   bandwidth; plot curves for "prefill local" vs "pull KV" wall time.
4. **Tiered storage** — L1/L2/L3 policy on the context server. Today
   entries live only in the publisher's `local_cache_path` and are
   never evicted.
5. **Context-push daemon** — edge-side file watcher + ingest
   protocol. Today the context server is purely reactive to vLLM
   `generate` calls; it has no "receive updated context" API.
6. **Entity versioning + tombstones** — content-sha in the entity
   tag; invalidation on change.
7. **Zero-copy transports** — CUDA IPC (same-host) and RDMA / NCCL
   (cross-host) to drop the per-hit overhead. Would move the
   crossover point in favor of cache reuse for smaller contexts.

---

## Relationship to prior work

- **EdgeServe** (Shaowang & Krishnan, 2023): parent. Decentralized
  streaming model serving over Pulsar. This design reuses the
  catalog+pubsub substrate and extends the payload type to KV cache.
- **vLLM prefix caching / RadixAttention** (Kwon et al.;
  Zheng et al.): the in-process ceiling we ride on top of. Our work
  is strictly a *layer above* vLLM's own cache; when vLLM's cache
  hits, our layer is bypassed.
- **LMCache / NIXL / Mooncake**: in-datacenter disaggregated KV
  transfer. Same primitive (move KV between servers), different
  operating point (GPU cluster with fast interconnect vs. LAN-scale
  edge with heterogeneous hardware).
- **Coral** (NSDI '04) and **hierarchical web caches** (Squid, Traffic
  Server): bloom-filter-over-distributed-cache precedent for the
  discovery layer.
- **Content-addressable storage** (IPFS, Git): entity-tag-as-key is a
  content-addressing pattern.

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
