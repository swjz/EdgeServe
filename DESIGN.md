# DESIGN — Semantic Inference Artifact CDN for Edge LLM/VLM/VLA Serving

*EdgeServe-v2.* A natural extension of EdgeServe (Shaowang & Krishnan,
[arxiv 2303.08028](https://arxiv.org/pdf/2303.08028.pdf)) from routing
streaming feature data and model outputs across edge nodes to a
fleet-scale discovery substrate for **validated inference artifacts**
at the edge.  KV-cache blocks are the flagship artifact and the current
implementation focus; the same discovery plane applies to RAG
embeddings, token/block manifests, multimodal encoder outputs, context
packs, deterministic tool-call results, engine compile caches, and
eventually static visual state in VLA control loops.

---

## Thesis

**Live decode and control want to live at the edge; expensive context
processing wants to live next to the data.  Validated inference artifacts
are the transfer assets that bridge them, discovered probabilistically
by semantic entity and reused only after exact compatibility checks.**

Restated as a primitive: **semantic entity tags + probabilistic fleet
discovery + exact-validation gate.**  KV cache is the load-bearing case
study; embeddings, tool-call results, engine compile caches, and
multimodal visual artifacts exercise the same primitive with different
payloads.  The bloom-filter catalog + exact-match header metadata +
engine-provenance gate are payload-agnostic; §"Case studies" enumerates
three non-KV applications where the same code paths apply with a
different entity schema.

Small-to-medium LLMs/VLMs running on local hardware (M-series Macs,
prosumer GPUs, eventually phones and robots) are viable for interactive
decode/control but are starved on large context processing. A 50 k-token
codebase, high-resolution document image, or static camera scene can be
expensive to tokenize, encode, prefill, or analyze repeatedly; it is much
cheaper to discover and reuse the validated artifact produced by a nearby
context node.

This inverts the usual cloud-API pattern. The natural architecture is:

- **Edge inference node** near the user: runs decode, owns the live
  interactive loop, never sends user prompts or generated tokens over
  the wire.
- **Context/prefill server** on the nearby network: holds the user's
  working set (codebase, documents, long conversation history), runs
  context processing when the working set changes, serves artifacts on
  request.
- **Discovery fabric**: a lightweight catalog so the edge node can ask
  "does any nearby server already hold an artifact for `file_diff_v2.py`
  at revision `abc123` under this model/tokenizer?" before paying to
  materialize the context itself.

Put together, it is a **content-distribution network for inference artifacts**:

| CDN concept | inference-artifact CDN analogue |
|---|---|
| Origin server | Context server (holds durable artifacts over tiered storage) |
| Edge PoP | Edge inference node (decode, near user) |
| Cacheable asset | KV block, token manifest, visual tokens, tool/context artifact |
| URL / content ID | Semantic entity tag `(entity, model, version, sha)` |
| Cache-control / TTL | Catalog header timestamp + TTL |
| DNS / edge selection | Bloom-filter catalog lookup |
| Origin-pull | Edge pulls artifact on demand from context server |
| Origin write-through | Edge pushes changed working-set files/assets to context server; context server recomputes artifacts in background |

Why bloom filters specifically: in a many-entity, many-node fabric,
most lookups are misses (the agent asks about some doc that hasn't
been cached anywhere). A compact bloom per node lets the requester
filter locally — most queries short-circuit in microseconds, without
any RPC. Precedent: Coral (NSDI '04) used bloom filters over a DHT for
peer discovery exactly because central indexes don't scale for sparse
lookups.

**Correctness gate (Phase 7.0).**  Bloom filters have a non-zero
false-positive rate by design, and blindly trusting a bloom-positive
result would silently load the wrong KV into the model.  Every
`CacheHeader` therefore carries explicit exact-match metadata
alongside its bloom: the full list of published prefix hashes, the
full list of user-declared entity tags, and engine provenance
(`model_id`, `model_version` with dtype disambiguator, tokenizer hash,
block size).  A bloom-positive candidate is admitted only if (a) the
engine provenance matches and (b) every queried entity appears in one
of the explicit lists.  The bloom stays as the scalable prefilter; the
explicit lists are the correctness gate.  See
`tests/test_exact_validation.py` for the false-positive-rejection
stress tests.

**Current correctness caveat.**  The catalog-level gate is implemented,
but the vLLM connector still has end-to-end hardening work before the
paper can claim the Bloom-positive risk is fully closed.  Entity-first
hits must verify that the candidate also covers the consumer's exact
block-aligned token-prefix hash before loading KV.  Prefix-hash hits
must preserve the scheduler-selected header UUID, or re-run lookup with
the same engine provenance on the worker, so a later unqualified
`resolve()` cannot fetch a wrong-model header with the same token hash.
Tokenizer/checkpoint provenance is also incomplete when `revision` or
`tokenizer_hash` is missing, and legacy bloom-only fallback remains an
unsafe compatibility path.  These items are tracked in `TODO.md` §7.0.

---

## Deployment shape

Typical setup (LAN-scale edge, the design's target):

```
┌─────────────────────┐     LAN (≥ 1 Gbps)      ┌────────────────────────┐
│  edge device        │ ◄───────────────────────┤  context server        │
│  (Mac, prosumer PC) │                         │  (home rack / office)  │
│                     │                         │                        │
│  • decode           │  header broadcast       │  • artifact compute    │
│  • user prompt in   │  (Pulsar pub/sub)       │  • L1 hot KV/visual    │
│  • generated tokens │ ◄───────────────────────┤  • L2 CPU RAM          │
│    out (stay local) │                         │  • L3 NVMe             │
│                     │                         │                        │
│  • local radix      │  artifact pull on hit   │  • HTTP server         │
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

## Artifact classes

The paper should present KV cache as the first implemented artifact,
not the only artifact. The unifying rule is: artifacts are discovered
semantically but reused only when exact artifact-specific metadata
matches.

| artifact | semantic key | exact validation before reuse | why it matters | status |
|---|---|---|---|---|
| Text KV-cache block | `codebase:linux@vX`, `doc:id@sha` | model/checkpoint, tokenizer, block size, content SHA, block-aligned token-prefix hash | avoids repeated long-context prefill | built + evaluated |
| Token/block manifest | same entity + tokenizer | content SHA, tokenizer version, token ids or boundary hashes | lets cold edge decide hit/miss without downloading raw context | planned |
| Context pack | repo snapshot, RAG bundle, prompt template version | file/content SHAs, retrieval policy, prompt serializer, tokenizer | avoids repeated repo/RAG assembly and tokenization | planned |
| Multimodal encoder output / visual tokens | `image:sha`, `pdf_page:sha`, `scene:id@ts` | encoder model, preprocessing pipeline, resolution/crop, asset SHA | avoids repeated image/video/document encoding across questions | planned |
| Multimodal KV/prefix state | visual asset + text prefix entity | VLM checkpoint, projector/encoder, tokenizer, visual token layout, text prefix hash | same asset, many questions; analogous to text KV | planned |
| RAG/document chunk KV | `rag_chunk:id@sha`, `repo_chunk:path@sha` | chunk SHA, chunk order/position policy, tokenizer, recompute/fusion policy | avoids repeated document-side work when chunks recur across queries | related-work baseline first |
| VLA static-scene state | `scene:id@time-window`, `robot:task:env` | VLA checkpoint, camera calibration, frame SHA/window, action head/version | avoids re-encoding static visual regions across control steps | future target |
| Deterministic tool artifact | `repo:index@sha`, `test:result@sha`, `ast:path@sha` | tool version, command/config, input content SHA | avoids repeated static analysis, indexing, test summarization | planned |
| Structured-decoding artifact | schema/tool name + schema SHA | tokenizer, decoding backend, schema grammar/mask hash | avoids recompiling JSON/schema grammars | lower priority |

This broader framing still preserves the correctness boundary: semantic
meaning gets the requester to a small candidate set; exact hashes decide
whether an artifact can be used.

---

## Economic case — when this pays off

For KV cache, the question is: **is fetching KV faster than recomputing
prefill locally?** For other artifacts, replace "prefill" with the
artifact's local materialization cost: tokenization, repo/RAG assembly,
vision encoding, schema compilation, or deterministic tool execution.

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

### Discovery vs. reuse correctness

Semantic entity tags are an **index**, not a license to reuse KV for
arbitrary semantically similar text. KV cache reuse is only correct when
the cached prefix is exactly compatible with the consumer's model state:
same model checkpoint, same tokenizer, same block size, same content
version, and the same block-aligned token prefix.

The intended lookup pipeline is:

1. **Metadata discovery.** The edge asks the catalog for
   `(entity, model_id, model_version, content_sha)` and receives candidate
   KV holders without downloading or tokenizing the full document first.
2. **Exact validation.** Before loading KV, the consumer verifies exact
   metadata: checkpoint/tokenizer identity, content hash, block-aligned
   token-prefix hash, and `num_tokens`.
3. **KV load.** Only after exact validation does the worker fetch the blob
   and inject it into the model's paged buffer or HF cache.

This is the key distinction from prefix-tree / radix-cache systems:
prefix trees are excellent once the requester already has the token
sequence, but they do not answer "who has KV for this repository
revision?" from a compact semantic handle alone. EdgeServe's advantage is
**metadata-first discovery over large remote contexts**, followed by
exact-token validation for safety.

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

## Case studies — one primitive, four payloads

The catalog + bloom + exact-validation gate is payload-agnostic.  Below
are four workloads that exercise the same code paths with different
entity-tag schemas and serializers.  KV cache is the focal study
(Phases 2–6, 7.0–7.3); the other three establish the primitive's
generality and land as separate paper figures.

### Case study A — KV cache sharing (primary, Linux dataset)

Already measured in Phases 2–6.  Future scale-up (Phase 8) targets
Linux v6.12 as a realistic long-context document: kernel maintainers
pin a version, repeatedly embed/prefill the same subtrees, and
overlap heavily across subsystems.

### Case study B — RAG embedding cache sharing  (Phase E1, planned)

**Workload:** a fleet of kernel-developer agents running RAG over
Linux v6.12 (~1.3 M LOC across ~80 k files).  Each agent has a
working set of a few subsystems and would otherwise re-embed them
locally every time.  With EdgeServe, the first agent embeds and
publishes; the fleet reuses.

**Entity schema:**
```
emb:file:linux@v6.12:<path>#<chunk_idx>
    model=BAAI/bge-small-en-v1.5
    version=sha256(model_weights)[:16]
    content=sha256(chunk_text)[:16]
```

Published one `CacheHeader` per file with a safetensors blob of that
file's chunk embeddings.  Bloom holds all chunk-level tags so an
agent can query `emb:file:linux@v6.12:drivers/net/e1000/e1000_main.c#7`
and hit.  Exact-validation rejects v6.12→v6.13 sha changes.

**Payload sizes:** 300 MB for the whole tree (20 k files × ~30 KB
of embeddings each); 1.5 KB per chunk (BGE-small is 384-dim bf16).

**Baselines:** (a) B0: agents re-embed locally every time; (b) B1:
centralized vector DB (Chroma / LanceDB) with explicit per-tenant
provisioning.

**Headline figure:** total embedding compute across the fleet as
fleet size grows from 1 → 8 agents with 60 % working-set overlap.
B0 grows linearly (`N × 7 min` on GPU or `N × 33 min` on Mac);
EdgeServe plateaus at `7 min + N × fetch_time`; B1 tracks EdgeServe
once provisioned but pays a one-time setup tax on cold-node join.

### Case study C — Tool-call result cache (Phase E2, planned)

**Workload:** N kernel-developer agents running deterministic
commands over Linux v6.12 in overlapping subsystems.  Concrete
commands with measured per-run cost:

| command | cost | fleet commonality |
|---|---|---|
| `git grep -n "struct sk_buff" @ v6.12` | 0.8 s | very high |
| `scripts/checkpatch.pl patches/fix.diff` | 2–10 s | per-patch |
| `make defconfig` | 25 s | every fresh worktree |
| `make -j$(nproc) drivers/net/ethernet/intel/e1000/` | 3 min cold / 8 s warm | subsystem agents |
| `scripts/get_maintainer.pl -f <path>` | 0.3 s | PR prep |
| `cppcheck --enable=all drivers/net/` | 2 min | static analysis |

**Entity schema:**
```
tool:<cmd>:<args_sha>:<tree_sha>@repo=linux@tag=v6.12
```
where `tree_sha` is the git hash of the file/directory the command
reads.  Aliases arise naturally: `file://…@sha=X`,
`git://torvalds/linux@v6.12:…`, `url://raw.githubusercontent.com/…`
all resolve to the same `content_sha` via catalog lookup, one cache
entry covers three access patterns.

**Correctness demo (sharper than KV-cache):** Agent A publishes the
build result of `drivers/net/ethernet/intel/e1000/`.  Agent B edits
`e1000_main.c` → `tree_sha` differs by one byte.  The exact-validation
gate rejects A's result; without it, B would silently use A's stale
build and wrongly conclude its change compiles.  Consequences of a
silent false positive are visible here in a way they aren't for KV.

**Baselines:** B0 each agent runs every command locally; B1
`ccache`/`sccache` (local, no cross-host sharing); EdgeServe
cross-host.

**Headline figure:** fleet of 8 agents working on 2 of 10 overlapping
subsystems for 10 minutes each.  Wall-clock per agent and
fleet-wide compute minutes saved.

### Case study D — vLLM compile-cache / CUDA-graph sharing (Phase E3, planned)

**Workload:** every vLLM cold start pays ~8.6 s on `torch.compile`
+ CUDA graph capture (measured repeatedly in our logs, e.g. `INFO
core.py:283 init engine took 8.59 seconds`).  A fleet of identical
RTX 3080 Ti workstations all pay this tax independently.  vLLM
already writes deterministic compile artifacts to
`~/.cache/vllm/torch_compile_cache/<hash>/`.

**Entity schema:**
```
vllm-compile:model=Qwen/Qwen2.5-1.5B
    dtype=bfloat16
    torch=2.10.0+cu128
    vllm=0.19.1
    gpu_arch=sm86
    block_size=16
    cache_config_sha=<hash of CompilationConfig>
```

**Payload:** tarball of the compile-cache directory.  ~10–50 MB.

**Baselines:** B0 always cold (8.6 s every start); B1 local disk
cache (8.6 s first start, ~2 s warm on same machine only);
EdgeServe (8.6 s on first fleet member, ~1 s on every subsequent
fleet member).

**Correctness demo:** heterogeneous fleet of 3 × RTX 3080 Ti (sm86)
plus 1 × RTX 4090 (sm89).  The 4090 must miss the sm86 compile
cache — exact-validation rejects on `gpu_arch`.

### Why these three

- Three different **payload sizes**: embeddings 30 KB/file,
  tool-results KB–MB, compile-cache 10–50 MB tarball.  Demonstrates
  transport is payload-agnostic.
- Three different **correctness gate mechanics**:
  content+model version (E1), content+tree+args (E2), engine ABI +
  arch (E3).  Each exercises `CacheHeader.matches_engine()` plus
  `covers_prefix_hash` / `covers_entities` with different
  dimensions populated.
- Three different **hit-rate regimes**: high (tool-grep ~90 %),
  medium (embeddings at realistic fleet overlap ~60 %), low
  (tool-build ~30 %) plus the always-miss-on-mismatch negative
  cases.  Together they stress the catalog + bloom FPR behaviour
  honestly.

These are case-studies for the paper, not re-engineering of the
connector.  Each one adds a small `scripts/bench_*.py` and a
payload-specific entity-schema helper, nothing more.

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

1. ~~**End-to-end edge inference demo** — a user on the Mac types a query; Mac
   fetches KV from GPU box; Mac generates answer locally; prompt and tokens
   never leave the Mac.~~ ✅ **Done 2026-04-24** — `scripts/demo_edge_inference.py`,
   3.67× speedup at 64 doc-repeats with bit-exact token match over wired
   gigabit LAN.  See RESULTS.md §Phase 6.

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

- **vLLM prefix caching / RadixAttention**
  ([design doc](https://docs.vllm.ai/en/latest/design/prefix_caching/)):
  the
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
  same pattern to inference-artifact discovery. The difference: our entities are
  semantic (model + content SHA) rather than URL hashes.

- **PromptCache** (2023): caches KV for *schema-defined* prompt segments,
  reuses across requests whose prompts share those segments. Complements
  RadixAttention. EdgeServe is strictly at a higher layer — we move KV
  across process and host boundaries, regardless of how the KV was
  generated. PromptCache could feed our catalog as a publisher.

- **CacheBlend / KVLink for RAG KV reuse**
  ([CacheBlend](https://arxiv.org/abs/2405.16444),
  [KVLink](https://arxiv.org/html/2502.16002v2)): precompute or fuse
  document-side KV when retrieved chunks recur across questions. They are
  important context-pack baselines, especially for RAG. EdgeServe should
  not claim to solve cache fusion quality; it should claim a discovery
  layer that can find the right document/chunk artifacts before a cold edge
  node downloads and tokenizes the full corpus.

- **LMCache multimodal support**
  ([blog](https://blog.lmcache.ai/en/2025/07/03/lmcache-extends-its-turbo-boost-to-multimodal-models-in-vllm-v1/)):
  extends KV reuse to multimodal vLLM models by hashing image-side
  multimodal tokens and caching their KV. This is the hard baseline for
  VLM experiments. EdgeServe's differentiator is metadata-first discovery
  across cold edge nodes and artifact classes, not merely "VLM KV reuse
  exists." **Measurement needed:** Phase 7.5.

- **NVIDIA NIM VLM KV reuse**
  ([docs](https://docs.nvidia.com/nim/vision-language-models/latest/kv-cache-reuse.html)):
  production evidence that VLM prefix/KV reuse matters when most of the
  initial multimodal prompt is identical across requests. Treat as a
  commercial exact-prefix reference point, not a distributed discovery
  baseline unless the experiment runs on NIM.

- **VL-Cache** (ICLR '25,
  [abstract](https://proceedings.iclr.cc/paper_files/paper/2025/hash/00db17c36b5435195760520efa96d99c-Abstract-Conference.html)):
  compresses VLM KV caches with modality-aware scoring and layer-adaptive
  budgets. Complementary: it reduces the artifact size; EdgeServe
  discovers and routes artifacts.

- **VLA-Cache** (2025,
  [arXiv](https://arxiv.org/html/2502.02175v2)): reuses static visual
  tokens/KV across frames for vision-language-action robotic manipulation.
  Same broad principle (static visual context should not be recomputed),
  but the workload is robotics control rather than edge knowledge work.
  Treat as related work unless we build a simulator/hardware VLA benchmark.

- **OpenVLA**
  ([project](https://openvla.github.io/),
  [paper](https://proceedings.mlr.press/v270/kim25c.html)): a plausible
  future VLA benchmark target because it is open, 7B-parameter, and trained
  on large robot demonstration mixtures. It is not the next experiment:
  VLA evaluation needs simulator/hardware traces and action success metrics,
  so VLM document/image QA is the fairer near-term scope.

### How to differentiate (for the paper)

The **central novelty claim** is the combination of:
1. Semantic entity tagging as the discovery key for reusable inference
   artifacts, enabling cache lookup even when the exact tokenized context
   or visual-token sequence is not locally materialized yet.
2. Bloom-filter broadcast over Pulsar for zero-configuration cross-node
   discovery — no central registry, no per-node config.
3. Exact artifact validation after discovery: model/checkpoint,
   tokenizer or encoder/preprocessor, content SHA, and token/visual prefix
   hashes must match before reuse.
4. Tiered storage (L2 RAM + L3 NVMe) on the context server for durability
   across GPU eviction and process restarts.
5. HTTP fallback transport that works over any LAN without RDMA fabric.

LMCache overlaps heavily on KV storage and now multimodal KV reuse; NIXL has
better same-host/datacenter transport; CacheBlend/KVLink optimize RAG
document-KV reuse; VL-Cache/VLA-Cache optimize VLM/VLA artifact size or
temporal reuse. EdgeServe's narrower claim is the discovery fabric: a cold
edge node can find validated artifacts from compact semantic metadata before
downloading or tokenizing huge text/visual contexts.

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
| **Decode stays at the edge; prompt never leaves** | Phase 6.1 Mac edge inference | ✅ RESULTS §6 (3.67× at 64 repeats, bit-exact token match) | §eval.privacy |
| **EdgeServe's niche: cross-host, not same-host vs APC** | Phase 7.1 B1 framing | ✅ RESULTS §2.3 B1 table (EdgeServe 7× slower than B1 same-host — this is expected and correct) | §eval.baselines |
| **Correctness: Bloom false positives cannot inject wrong KV** | Phase 7.0 exact validation (catalog post-filter done; connector hardening tracked in TODO §7.0) | ⚠️ partial | §eval.correctness |
| Metadata-first discovery avoids raw-context materialization | Phase 7.4 remote corpus / cold-node lookup | 🔲 TODO §7.4 | §eval.discovery |
| **Differentiator over LMCache: zero-config discovery** | Phase 7.2 LMCache comparison | 🔲 TODO §7.2 | §eval.related |
| **Differentiator over NIXL: cross-host + no RDMA** | Phase 7.3 NIXL comparison | 🔲 TODO §7.3 | §eval.related |
| **Primitive generality, case study: embeddings** | Phase E1 Linux v6.12 RAG embedding cache sharing | 🔲 TODO §E1 | §eval.generality |
| **Primitive generality, case study: tool-call results** | Phase E2 Linux v6.12 dev-tool result cache | 🔲 TODO §E2 | §eval.generality |
| **Primitive generality, case study: compile artifacts** | Phase E3 vLLM compile-cache / CUDA-graph sharing | ✅ RESULTS §E3 (1.50× cold-start, 8 s saved per fleet member; arch + model rejection passes) | §eval.generality |
| Semantic discovery extends to VLM visual artifacts | Phase 7.5 VLM asset-cache discovery (optional, broader framing) | 🔲 TODO §7.5 | §eval.multimodal |
| CDN economics improve at 7B / 32k tokens | Phase 8.1–8.2 scale evaluation | 🔲 deferred | §eval.scale |

**Priority order for next work sessions:**

1. ~~Phase 6 (end-to-end Mac demo)~~ ✅ **done 2026-04-24**.
2. ~~Phase 7.1 (B1 framing)~~ ✅ **done 2026-04-27**.
3. ~~Phase 7.0 (correctness gate)~~ ✅ **done 2026-04-27**.
4. **Phase 7.2 (LMCache)** — the KV case-study comparison; reviewer-expected.
5. **Phase E3 (vLLM compile-cache)** — smallest non-KV case study; proves portability quickly.
6. **Phase E2 (Linux tool-call cache)** — the sharper correctness story (stale-tree false positive); also the natural home for Linux-scale metadata-only discovery (subsumes old Phase 7.4).
7. **Phase E1 (Linux embedding cache)** — the fleet-scaling headline figure.
8. Phase 7.3 (NIXL) — secondary KV comparison.
9. Phase 7.5 (VLM visual artifact discovery) — optional broader framing; only if time permits.
10. Phase 8 (7B / 32k scale) — deferred until better GPU hardware is available.

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
- **Semantic aliasing.** Many names can refer to the same bytes
  (`file:path`, `url`, `repo@commit`, vector-db ID). The catalog should
  support alias-to-`content_sha` resolution, but KV reuse still requires
  exact token-prefix validation after alias resolution.
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
