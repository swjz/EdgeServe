"""In-process mock of the `pulsar` and `_pulsar` modules.

Enough fidelity to back `HeaderPublisher` and `HeaderCatalog`:
- `pulsar.Client(url)` -> mock client bound to a shared in-memory broker.
- `client.create_producer(topic, schema=...).send(bytes)` -> fanout to every
  subscription on that topic.
- `client.subscribe(topic, subscription_name=..., ...)` -> consumer whose
  `receive(timeout_millis=...)` pops from that subscription's queue (or raises
  a timeout-shaped exception, matching real Pulsar behavior).

Install by calling `install()` BEFORE any `import pulsar` / `import _pulsar`
happens in your process.
"""

import queue
import sys
import threading
import time
import types
from collections import defaultdict


class _Broker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subs: dict = defaultdict(dict)  # topic -> {sub_name: Queue}
        self._log: dict = defaultdict(list)   # topic -> [payload, ...]

    def subscribe(self, topic: str, sub_name: str, replay: bool = True) -> queue.Queue:
        with self._lock:
            q = self._subs[topic].get(sub_name)
            if q is None:
                q = queue.Queue()
                self._subs[topic][sub_name] = q
                if replay:
                    # Mimic Pulsar InitialPosition.Earliest: replay the topic
                    # backlog to newly-joining subscriptions.
                    for payload in self._log[topic]:
                        q.put(payload)
            return q

    def publish(self, topic: str, payload: bytes) -> None:
        with self._lock:
            self._log[topic].append(payload)
            subs = list(self._subs[topic].values())
        for q in subs:
            q.put(payload)


_BROKER = _Broker()


class _Message:
    def __init__(self, payload: bytes, publish_ms: float) -> None:
        self._payload = payload
        self._publish_ms = publish_ms

    def value(self) -> bytes:
        return self._payload

    def publish_timestamp(self) -> float:
        return self._publish_ms


class _Producer:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def send(self, payload: bytes) -> None:
        _BROKER.publish(self._topic, payload)

    def close(self) -> None:
        pass


class _Consumer:
    def __init__(self, topic: str, sub_name: str) -> None:
        self._queue = _BROKER.subscribe(topic, sub_name)

    def receive(self, timeout_millis: int = 0):
        try:
            payload = self._queue.get(
                timeout=(timeout_millis / 1000.0) if timeout_millis else None
            )
        except queue.Empty:
            raise _PulsarTimeout('receive timed out')
        return _Message(payload, publish_ms=time.time() * 1000)

    def acknowledge(self, _msg) -> None:
        pass

    def negative_acknowledge(self, _msg) -> None:
        pass

    def close(self) -> None:
        pass


class _Client:
    def __init__(self, _url: str) -> None:
        self._producers = []
        self._consumers = []

    def create_producer(self, topic: str, schema=None):
        p = _Producer(topic)
        self._producers.append(p)
        return p

    def subscribe(self, topic: str, subscription_name: str,
                  consumer_type=None, schema=None, initial_position=None):
        c = _Consumer(topic, subscription_name)
        self._consumers.append(c)
        return c

    def close(self) -> None:
        for p in self._producers:
            p.close()
        for c in self._consumers:
            c.close()


class _PulsarTimeout(Exception):
    """Raised by consumer.receive() on timeout, mirroring real pulsar behavior."""


class _BytesSchema:
    pass


class _Record:
    pass


def _String(*_a, **_kw):
    return None


def _Bytes(*_a, **_kw):
    return None


def _make_pulsar_module() -> types.ModuleType:
    mod = types.ModuleType('pulsar')
    mod.Client = _Client

    schema_mod = types.ModuleType('pulsar.schema')
    schema_mod.BytesSchema = _BytesSchema
    schema_mod.Record = _Record
    schema_mod.String = _String
    schema_mod.Bytes = _Bytes
    mod.schema = schema_mod
    return mod


def _make_lowlevel_module() -> types.ModuleType:
    mod = types.ModuleType('_pulsar')

    class ConsumerType:
        Exclusive = 0
        Shared = 1
        Failover = 2

    class InitialPosition:
        Earliest = 0
        Latest = 1

    mod.ConsumerType = ConsumerType
    mod.InitialPosition = InitialPosition
    return mod


def install() -> None:
    """Install the mock into sys.modules.

    Must be called before any `import pulsar` happens in the process (so before
    importing `edgeserve.semantic_cache.catalog` / `.publisher`).
    """
    sys.modules['pulsar'] = _make_pulsar_module()
    sys.modules['pulsar.schema'] = sys.modules['pulsar'].schema
    sys.modules['_pulsar'] = _make_lowlevel_module()


def reset_broker() -> None:
    """Drop all topics and subscriptions. Handy between tests."""
    global _BROKER
    _BROKER = _Broker()
