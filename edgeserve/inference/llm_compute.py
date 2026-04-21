"""LLM-specialized compute operator wired into EdgeServe's Pulsar graph.

Unlike the generic `edgeserve.compute.Compute` (which joins N upstream
streams), `LLMCompute` handles the typical single-prompt-in, tokens-out
shape. It consumes msgpack-encoded prompt records from an input topic,
optionally resolves a peer KV cache via `semantic_cache`, runs the
configured `InferenceEngine`, and emits generated text on an output topic.

Input message format (msgpack):
    {
        "prompt":          str,              # required
        "cache_tags":      [str, ...],       # optional; entities identifying this request's shared context
        "publish_cache":   bool,             # optional; if True, publish KV after prefill
        "max_new_tokens":  int,              # optional; default 64
    }

Output message (bytes): the generated text, utf-8 encoded. A GraphCodec with
the operator's `worker_id` as `op_from` wraps it for downstream consumers.
"""

import time
from typing import Optional

import msgpack
import pulsar
from _pulsar import ConsumerType, InitialPosition

from edgeserve.inference.engine import InferenceEngine
from edgeserve.message_format import GraphCodec


class LLMCompute:
    def __init__(
        self,
        engine: InferenceEngine,
        pulsar_node: str,
        worker_id: str,
        topic_in: str,
        topic_out: str,
        semantic_cache=None,
        default_max_new_tokens: int = 64,
    ):
        self.engine = engine
        self.worker_id = worker_id
        self.semantic_cache = semantic_cache
        self.default_max_new_tokens = default_max_new_tokens

        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(
            topic_out, schema=pulsar.schema.BytesSchema(),
        )
        self.consumer = self.client.subscribe(
            topic_in,
            subscription_name=f'llm-{worker_id}',
            consumer_type=ConsumerType.Shared,
            schema=pulsar.schema.BytesSchema(),
            initial_position=InitialPosition.Earliest,
        )
        self._codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

        # Simple per-call metrics surfaced for tests / telemetry.
        self.last_stats = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def _resolve_cache(self, cache_tags):
        """Try to reuse a peer's KV cache. Returns (handle | None, stats)."""
        stats = {'cache_hit': False, 'resolve_ms': 0.0, 'source_uri': None}
        if not self.semantic_cache or not cache_tags:
            return None, stats
        t0 = time.perf_counter()
        hit = self.semantic_cache.resolve(cache_tags)
        stats['resolve_ms'] = (time.perf_counter() - t0) * 1000
        if hit is None:
            return None, stats
        blob, header = hit
        cache = self.engine.deserialize_cache(blob)
        stats['cache_hit'] = True
        stats['source_uri'] = header.node_uri
        stats['bytes'] = len(blob)
        return cache, stats

    def _publish_cache(self, cache_tags, cache):
        if not self.semantic_cache or not cache_tags or cache is None:
            return
        blob = self.engine.serialize_cache(cache)
        self.semantic_cache.publish(cache_tags, blob)

    def __next__(self):
        msg_in = self.consumer.receive()
        try:
            msg_uuid, _op_from, _, payload = self._codec.decode(msg_in.value())
            record = msgpack.unpackb(payload, raw=False)
            prompt = record['prompt']
            cache_tags = set(record.get('cache_tags', []) or [])
            publish_cache = bool(record.get('publish_cache', False))
            max_new = int(record.get('max_new_tokens', self.default_max_new_tokens))

            reused_cache, stats = self._resolve_cache(cache_tags)

            t0 = time.perf_counter()
            prompt_tokens = self.engine.tokenize(prompt)
            new_tokens, final_cache = self.engine.generate(
                prompt_tokens, max_new_tokens=max_new, cache=reused_cache,
            )
            stats['generate_ms'] = (time.perf_counter() - t0) * 1000
            stats['prompt_tokens'] = len(prompt_tokens)
            stats['new_tokens'] = len(new_tokens)

            if publish_cache and not stats['cache_hit']:
                # Publish the cache we just built so peers can reuse it next time.
                self._publish_cache(cache_tags, final_cache)
                stats['published'] = True

            text = self.engine.detokenize(new_tokens)
            self.last_stats = stats

            out_msg = self._codec.encode(
                msg_uuid=msg_uuid, op_from=self.worker_id, payload=text.encode('utf-8'),
            )
            self.producer.send(out_msg)
            self.consumer.acknowledge(msg_in)
            return text
        except Exception:
            self.consumer.negative_acknowledge(msg_in)
            raise


def pack_prompt(prompt: str, cache_tags=None, publish_cache: bool = False,
                max_new_tokens: Optional[int] = None) -> bytes:
    """Helper to build an input message for `LLMCompute`.

    Producers upstream of the LLM operator use this to keep the wire format
    consistent.
    """
    rec = {'prompt': prompt}
    if cache_tags:
        rec['cache_tags'] = list(cache_tags)
    if publish_cache:
        rec['publish_cache'] = True
    if max_new_tokens is not None:
        rec['max_new_tokens'] = max_new_tokens
    return msgpack.packb(rec, use_bin_type=True)
