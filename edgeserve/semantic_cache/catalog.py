import threading
import time
import uuid
from typing import Callable, Dict, Iterable, List, Optional

import pulsar
from _pulsar import ConsumerType, InitialPosition

from edgeserve.semantic_cache.header import CacheHeader


def _entities_covered_exact(header: CacheHeader, entities: List[str]) -> bool:
    """Every requested entity is present in the header's explicit lists.

    We don't know at lookup time whether a requested tag is a prefix hash or a
    user-declared entity; accept a match from either explicit list.  This is
    the gate that turns a Bloom false-positive into a miss.
    """
    for e in entities:
        if e in header.prefix_hashes or e in header.entity_keys:
            continue
        return False
    return True


class HeaderCatalog:
    """In-memory index of recent cache headers broadcast on a Pulsar topic.

    A background thread subscribes to the headers topic and maintains a
    dict keyed by block_uuid. `lookup()` tests required entities against
    each header's bloom filter and returns ranked candidate holders.
    """

    def __init__(
        self,
        pulsar_node: str,
        node_id: str,
        topic: str = 'kvcache-headers',
        ttl_ms: float = 10 * 60 * 1000,
        rank_fn: Optional[Callable[[CacheHeader], float]] = None,
    ):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(
            topic,
            subscription_name=f'catalog-{node_id}',
            consumer_type=ConsumerType.Exclusive,
            schema=pulsar.schema.BytesSchema(),
            initial_position=InitialPosition.Earliest,
        )
        self.ttl_ms = ttl_ms
        self.rank_fn = rank_fn if rank_fn is not None else (lambda h: h.created_ms)
        self._headers: Dict[uuid.UUID, CacheHeader] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                msg = self.consumer.receive(timeout_millis=500)
            except Exception:
                self._evict_expired()
                continue
            try:
                header = CacheHeader.from_bytes(msg.value())
                with self._lock:
                    if header.deleted:
                        self._headers.pop(header.block_uuid, None)
                    else:
                        self._headers[header.block_uuid] = header
                self.consumer.acknowledge(msg)
            except Exception:
                self.consumer.negative_acknowledge(msg)
            self._evict_expired()

    def _evict_expired(self) -> None:
        cutoff = time.time() * 1000 - self.ttl_ms
        with self._lock:
            stale = [u for u, h in self._headers.items() if h.created_ms < cutoff]
            for u in stale:
                del self._headers[u]

    def lookup(
        self,
        entities: Iterable[str],
        *,
        exact_validate: bool = True,
        engine_model_id: Optional[str] = None,
        engine_model_version: Optional[str] = None,
        engine_tokenizer_hash: Optional[str] = None,
        engine_block_size: int = 0,
    ) -> List[CacheHeader]:
        """Return headers whose bloom filter is positive for every entity.

        Bloom filters have a nonzero false-positive rate.  When a header
        carries Phase-7.0 exact-match metadata (``prefix_hashes`` /
        ``entity_keys``), we treat the bloom as a prefilter and require that
        every requested entity be present in the exact list as well.

        Set ``exact_validate=False`` to get legacy bloom-only behavior (used
        by the false-positive stress tests and by same-node-publisher
        resolve-by-uuid paths that have already verified exactness).

        Engine provenance filters (``engine_model_id``, ``engine_block_size``,
        etc.) reject headers whose publisher differs on any non-empty field.
        Pass ``None`` / ``0`` to skip that dimension.

        Ranked by ``rank_fn`` descending (default: most-recent first).
        """
        ents = list(entities)
        with self._lock:
            bloom_positive = [
                h for h in self._headers.values()
                if all(e in h.bloom for e in ents)
            ]

        if exact_validate:
            validated = []
            for h in bloom_positive:
                if not h.matches_engine(
                    engine_model_id, engine_model_version,
                    engine_tokenizer_hash, engine_block_size,
                ):
                    continue
                # Exact-match gate: if the header advertises exact-match
                # metadata, every requested entity MUST be explicitly listed.
                # Legacy headers (pre-7.0) without exact metadata pass
                # through on bloom alone — with a warning issued by the
                # connector layer, not here.
                if h.has_exact_metadata():
                    if not _entities_covered_exact(h, ents):
                        continue
                validated.append(h)
            candidates = validated
        else:
            candidates = bloom_positive

        candidates.sort(key=self.rank_fn, reverse=True)
        return candidates

    def insert(self, header: CacheHeader) -> None:
        """Directly inject a header (useful for tests and same-node publishers)."""
        with self._lock:
            self._headers[header.block_uuid] = header

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        try:
            # Delete durable subscription so the next connection with this
            # name starts from InitialPosition.Earliest (not the acked cursor).
            self.consumer.unsubscribe()
        except Exception:
            pass
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
