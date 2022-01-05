import pulsar


class Compute:
    def __init__(self, task, pulsar_node, gate=None, topic_in='src-topic', topic_out='dst-topic'):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out)
        self.consumer = self.client.subscribe(topic_in, subscription_name='my-sub')
        self.task = task
        self.gate = lambda x: x if gate is None else gate

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        data = self.gate(msg.data())

        output = self.task(data)
        self.producer.send(output)

        self.consumer.acknowledge(msg)
        return output
