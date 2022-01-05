import pulsar


class Materialize:
    def __init__(self, materialize, pulsar_node, gate=None, topic='dst-topic'):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(topic, subscription_name='my-sub')
        self.materialize = materialize
        self.gate = lambda x: x if gate is None else gate

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        if self.gate is not None:
            data = self.gate(msg.data())
        self.consumer.acknowledge(msg)

        self.materialize(data)


