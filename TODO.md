# Pending work — for the remote session to pick up

Task IDs (#24, #28, …) reference Claude Code's TaskList. On the remote
machine those IDs won't match — just read these entries and create
equivalents via the TaskCreate tool.

Prioritized:

## #29 — Quick-win: "prefix-bloom vs semantic-bloom" honesty note

~15 min, docs only.

RESULTS.md today claims "Semantic Cache Routing" but the bloom filter
actually encodes block-aligned prefix HASHES, not semantic entity
tags. Add a short paragraph under "Benchmark honesty audit"
distinguishing:

- **Paper describes:** bloom filter encoding semantic tags (doc IDs,
  function names) that lets permuted contexts hit.
- **We shipped:** bloom filter encoding block-aligned prefix hashes,
  efficient for catalog lookup across many prefix boundaries of the
  same token sequence.
- **Consequence:** our impl hits on shared-prefix prompts; does NOT
  hit on permuted-prefix prompts (persona-before-doc). That's the one
  remaining honesty gap after the audit.

No code change. Do this first even if #24–#27 get punted.

## #24 — A1: publisher accepts user-declared semantic entity tags

Extend the vLLM KVConnector publish path so each request can carry
extra semantic tags (doc IDs, function names, entity strings)
alongside the existing block-aligned prefix hashes. Concretely:

- Plumb a way for the caller to attach entities to a request: a kwarg
  on `vllm.LLM.generate()` won't work cleanly — vLLM doesn't forward
  arbitrary metadata. Options: (a) read from `request.metadata` if
  vLLM exposes one, (b) a request-id → entities side-channel
  populated out-of-band before `generate`, (c) extend
  `EdgeServeKVConnector`'s `extra_config` to accept a callback that
  extracts entities from `request.prompt_token_ids`.
- Recommend (c): callback signature
  `entities_fn(token_ids: list[int]) -> set[str]`, registered once at
  connector init. For agent systems the user already has the
  entities; this just lets them hand them to us.
- In `_Worker.wait_for_save`, include those user entities in the bloom
  filter on top of the prefix-hash entities already there.
- Update `KV_CONNECTOR.md` to document the API.
- Do NOT change the lookup path in this task; A1 only changes what
  gets published.

## #25 — A2: scheduler entity-intersection lookup

After #24. Add an entity-intersection path to
`_Scheduler.get_num_new_matched_tokens`. When the request has
declared entities (per #24), try `SemanticCacheClient.catalog.lookup(
{entities})` BEFORE the prefix-hash fallback. On hit, figure out how
many tokens the hit header's KV actually covers (store covered-length
in the `CacheHeader` so the consumer knows how many tokens to
scatter). On miss, fall back to the existing prefix-boundary longest-
match logic. Coexists with #24's publish path.

## #26 — A3: permuted-persona demo + correctness test (blocked on #24, #25)

Write a demo that exercises the paper's motivating scenario:

- Seeder runs `"Persona A: Reply as scientist. " + doc`
- Consumer runs `"Persona B: Reply as historian. " + doc` (DIFFERENT
  first tokens)

Before #24/#25 this misses — no shared prefix. After #24/#25, both
sides declare the same `doc_id` entity, and consumer hits via entity
match rather than prefix.

Deliverables:
- `scripts/demo_kvconnector_permuted.py`
- Verify: consumer's next-token id under warm cache equals the cold
  no-cache baseline of the SAME permuted prompt (correctness).
- Speedup number in RESULTS.md headline table.

## #27 — A4: update RESULTS.md framing (blocked on #26)

After #24–#26 land, rewrite the relevant RESULTS.md sections:

- Add permuted-persona numbers to the headline TL;DR table.
- Explain: prefix-hash path still works for same-prefix workloads;
  entity path handles permuted contexts that prefix-hash can't; both
  can be active simultaneously (multi-tier bloom lookup).
- Retire the "prefix-bloom vs semantic-bloom" note from #29 now that
  the gap is closed.

## #28 — B: Cross-host LAN validation

Independent of A. Run the two-stage demo across two physical
machines on the home LAN:

- Seeder: 3080 Ti (vLLM). Consumer: Mac Mini (CPU or MPS) OR another
  Linux box.
- Both point at the SAME Pulsar broker (one of the two hosts, or a
  third docker).
- Consumer's `SemanticCacheClient` must talk to the seeder's HTTP
  endpoint over LAN. Existing `CacheHeader` carries `node_uri` with
  hostname — as long as hostnames resolve, the HTTP fallback fires
  automatically since `_is_local_readable()` returns False across
  hosts.
- Measure: (a) HTTP fetch wall time on LAN for 50-200 MB KV blobs,
  (b) end-to-end consumer gen time, (c) vs same-host mmap fast path.
- Deliverables: `scripts/demo_kvconnector_lan.py`; numbers in
  RESULTS.md "What this run does NOT demonstrate" section (currently
  blank for cross-host).

Tests the HTTP transport under realistic conditions — today's 1.54×
number was measured by disabling the mmap fast path on one box, not
actual LAN.

## #18 — CUDA IPC / RDMA transport (already pending; long-term)

The overhead-reduction play. Drops the +6/+55 ms same-host overhead
(RESULTS.md "Ceiling comparison") to near zero via CUDA IPC
(torch.multiprocessing + shared CUDA contexts). For cross-host, RDMA
or NCCL. Big engineering lift, modest narrative payoff — only pursue
after A (#24–#27) and B (#28) are done.
