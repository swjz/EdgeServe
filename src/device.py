import requests
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
            # without the two following lines, the consumer hangs when consuming messages
            group_id=None,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: x.decode('utf-8'),
            request_timeout_ms=1000,
            consumer_timeout_ms=1000,
            )
        self.data = data

    def publish(self, topic):
        self.producer.send(topic, self.data[topic])
        print(self.group_id, 'sent', self.data[topic], 'to', topic)

    def subscribe(self, topic):
        self.consumer.subscribe(topic)
        print(self.group_id, 'has subscribed to', topic)

    def handle_messages(self):
        # call this only after subscribing to something
        print(self.group_id, 'is reading stuff')
        for message in self.consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
            # TODO: connect to model serving REST API
            # url = 'http://127.0.0.1:8080/predictions/densenet161'
            # files = {'file': open('../../kitten_small.jpg', 'rb')}
            # r = requests.post(url, files=files)
            r = requests.post('http://localhost:8080/ping')
            print(r.text)
        self.consumer.close()

    def unsubscribe(self):
        self.consumer.unsubscribe()
