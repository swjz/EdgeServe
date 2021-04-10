from kafka import KafkaProducer, KafkaConsumer

class Device:

    def __init__(self, group_id, data):
        """
        group_no: kafka group id, different so that different devices/consumers
        can get the same message from the publisher

        data: {topic: number}, e.g., data1: '331'
        # TODO: number is just a stub, use complex data object like video
        """
        self.group_id = group_id
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: x.encode('utf-8'),
            )
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            # group_id=group_id,
            group_id=None,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: x.decode('utf-8'),
            )
        self.data = data

    def publish(self, topic):
        self.producer.send(topic, self.data[topic])
        print(self.group_id, 'sent', self.data[topic], 'to', topic)

    def subscribe(self, topic):
        self.consumer.subscribe(topic)
        print(self.group_id, 'has subscribed to', topic)

    def read_messages(self):
        # call this only after subscribing to something
        # TODO: refactor
        print(self.group_id, 'is reading stuff')
        for message in self.consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

    def unsubscribe(self):
        self.consumer.unsubscribe()
