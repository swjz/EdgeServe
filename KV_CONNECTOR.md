# EdgeServeKVConnector — vLLM integration

`edgeserve/inference/vllm_kv_connector.py` is a `vllm.KVConnectorBase_V1`
that lets independent vLLM instances share KV cache through EdgeServe's
Semantic Cache Routing layer (Pulsar catalog + HTTP/same-host transport).

It mirrors the structure of vLLM's own `ExampleConnector` but replaces
per-host shared-disk storage with `SemanticCacheClient`.

## Quick start

Prerequisites:
- Pulsar broker reachable (`docker run -d apachepulsar/pulsar:3.1.0
  bin/pulsar standalone`).
- `pip install vllm torch safetensors` alongside this repo.

```python
from edgeserve.inference.vllm_kv_connector import register
register()  # adds EdgeServeKVConnector to vLLM's factory

from vllm import LLM, SamplingParams
from vllm.config import KVTransferConfig

llm = LLM(
    model='Qwen/Qwen2.5-1.5B',
    enable_prefix_caching=False,   # let our connector do the caching
    kv_transfer_config=KVTransferConfig(
        kv_connector='EdgeServeKVConnector',
        # Subprocess EngineCore needs this to import the connector module:
        kv_connector_module_path='edgeserve.inference.vllm_kv_connector',
        kv_role='kv_both',
        kv_connector_extra_config={
            'pulsar_url': 'pulsar://localhost:6650',
            'topic': 'kvcache-headers',       # share across vLLM instances
            'local_cache_path': '/tmp/edgeserve-kv-A',
            'node_id': 'worker-A',
        },
    ),
)
out = llm.generate(prompts=['Long shared document ...'],
                   sampling_params=SamplingParams(max_tokens=1))
```

First call on a fresh topic: the scheduler sees no matching header, the
request goes through as a cache MISS, the worker saves per-layer KV, and
`wait_for_save` publishes the combined safetensors blob via
`SemanticCacheClient.publish`.

Second call on a DIFFERENT vLLM instance pointed at the same topic +
same prompt: the scheduler sees the header, marks the request as a
cache HIT, allocates blocks, and `start_load_kv` pulls the blob and
scatters it into the paged buffer. The forward pass skips the prefill
work for those tokens.

## How it works

- **Key**: the request's aligned prompt token prefix, SHA-256'd
  (`_hash_token_ids`). Matches `ExampleConnector`'s convention.
- **Block-size alignment**: tokens past the last full block of
  `block_size` (default 16 in vLLM) are ignored for caching. Same
  convention as `ExampleConnector`.
- **One blob per request**: `_Worker` stashes every layer's KV during
  the forward pass and flushes all layers as one safetensors file in
  `wait_for_save`. Keeps the catalog's header count small and the
  fetch atomic.
- **Transport**: handled by `SemanticCacheClient`. Same-host workers go
  through the filesystem fast path (mmap via safetensors `safe_open`);
  cross-host workers go through HTTP.

## Observed speedup

On 3080 Ti / Qwen2.5-1.5B / bf16 / 1 new token:

| doc chars | seeder gen | consumer gen | speedup |
|----------:|-----------:|-------------:|--------:|
|    ~2.6k  |       99ms |         46ms |   2.15× |
|    ~5.1k  |      163ms |         66ms |   2.49× |
|    ~10k   |      310ms |        102ms |   3.04× |
|    ~20k   |      585ms |        177ms |   3.30× |

See `scripts/demo_kvconnector_two_stage.py` for the reproducer and
`RESULTS.md` for full methodology.

## Prefix-boundary matching

A consumer whose prompt *shares only a prefix* with a cached entry
still hits. `wait_for_save` emits entity tags at every block boundary
of the full prompt:

    hash(tokens[:16]), hash(tokens[:32]), ..., hash(tokens[:L])

all encoded in the same header's bloom filter. The scheduler's
`get_num_new_matched_tokens` tries hashes longest-first; the first hit
determines how many tokens the consumer can reuse. `start_load_kv`
fetches the full blob, slices each layer tensor to the matched length,
and scatters into the consumer's shorter slot_mapping.

This covers the paper's motivating scenario — multiple agents with
different personas/queries over the same document hit the shared
document prefix. See `scripts/demo_kvconnector_prefix_share.py` and
`scripts/demo_kvconnector_multi_agent.py`.

## Limitations / future work

- **No per-layer lazy publish.** We accumulate all layers on the
  worker side then publish one blob. For very long contexts on
  memory-tight hosts, per-layer streaming to the SemanticCacheClient
  would reduce peak CPU memory; not needed at current model sizes.
- **Single SemanticCacheClient per (pulsar_url, topic, node_id).** The
  connector caches backend handles process-wide so the scheduler and
  worker roles in one vLLM engine share one Pulsar subscription. If a
  single process runs multiple vLLM engines under the same node_id
  with different topics, ensure each has a unique node_id.
- **CPU round trip on transport.** Today the path is GPU → CPU
  safetensors bytes → HTTP/mmap → CPU → GPU. Overhead is ~55 ms per
  ~20 k-token blob on a 3080 Ti. CUDA IPC (same-host) or NCCL/RDMA
  (cross-host) would drop this to near zero. Tracked on the roadmap.

## Registration

`register()` is idempotent and adds the connector to vLLM's
`KVConnectorFactory._registry`. After registration (or after passing
`kv_connector_module_path=...` to `KVTransferConfig`, which makes vLLM's
subprocess engine core discover it too), the connector is indistinguishable
from vLLM's built-ins (`LMCacheConnectorV1`, `NixlConnector`, etc.)
from the factory's perspective.
