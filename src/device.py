import json
import requests
from kafka import KafkaProducer, KafkaConsumer

from constants import *

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
            bootstrap_servers=['ted-driver:9092'],
            )
        self.consumer = KafkaConsumer(
            TOPIC_STATUS,
            bootstrap_servers=['ted-driver:9092'],
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            # consumer_timeout_ms=1000,
            )
        self.data = data
        self.subscribed_topics = set([TOPIC_STATUS])

    # TODO: let device publish to TOPIC_STATUS

    def publish(self, topic):
        self.producer.send(topic, self.data[topic])
        # print(self.group_id, 'sent', self.data[topic], 'to', topic)

    def subscribe(self, topic):
        self.consumer.unsubscribe()
        self.subscribed_topics.add(topic)
        self.consumer.subscribe(self.subscribed_topics)
        print(self.group_id, 'has subscribed to', self.subscribed_topics)

    def handle_messages(self):
        # call this only after subscribing to something
        print(self.group_id, 'is reading stuff')
        for message in self.consumer:
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
            #                              message.offset, message.key,
            #                              message.value))
            # TODO: connect to model serving REST API

            # check if message is from STATUS or is a data message
            # ignore it a message from STATUS is not directed to my group_id
            if message.topic == TOPIC_STATUS and message.key.decode('utf-8') == self.group_id:
                print('Topic', message.topic, type(message.topic),
                      'key', message.key.decode('utf-8'), type(message.key.decode('utf-8')))
                self.handle_status_topic(message)
            else:
                # make predictions
                # TODO: refactor into a method
                url = 'http://127.0.0.1:8080/predictions/densenet161'
                response = requests.post(url, data=message.value)
                print('Got prediction', json.dumps(response.text))
        # don't close the consumer since we still need status updates
        # self.consumer.close()

    def handle_status_topic(self, message):
        print('Inside handle_status_topic', message.value)
        action, topic = message.value.decode('utf-8').split('_')
        if action == KEY_SUBSCRIBE:
            self.subscribe(topic)
        elif action == KEY_PUBLISH:
            self.publish(topic)

    def unsubscribe(self, topic):
        if topic not in self.subscribed_topics:
            print("ERROR: Unable to unsubscribe -- topic '%s' does not exist." % topic)
            return
        self.subscribed_topics.remove(topic)
        self.consumer.subscribe(self.subscribed_topics) # subscribe the remaining topics

    def unsubscribe_all(self):
        self.consumer.unsubscribe()
        self.subscribed_topics = set([TOPIC_STATUS])
