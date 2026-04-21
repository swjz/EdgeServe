import pulsar

from edgeserve.semantic_cache.header import CacheHeader


class HeaderPublisher:
    """Publishes CacheHeader messages to a dedicated Pulsar topic.

    The tensor payload is never sent through Pulsar; only the lightweight
    header (uuid, node URI, prefix hash, semantic bloom filter).
    """

    def __init__(self, pulsar_node: str, topic: str = 'kvcache-headers'):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic, schema=pulsar.schema.BytesSchema())

    def publish(self, header: CacheHeader) -> None:
        self.producer.send(header.to_bytes())

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
