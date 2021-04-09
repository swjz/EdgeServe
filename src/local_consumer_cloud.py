from time import sleep, time
from kafka import KafkaConsumer

if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'ResultOut'
    # parsed_topic_name = 'TutorialTopic'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    while True:
        msg = consumer.poll()
        if len(msg) > 0:
            print(msg, time())
