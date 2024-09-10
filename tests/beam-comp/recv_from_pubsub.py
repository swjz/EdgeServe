from google.cloud import pubsub_v1
import pickle

subscriber = pubsub_v1.SubscriberClient()

project_id = "apache-beam-baseline"
small_subscription_id = "text-small-chunks-sub"
small_subscription_path = subscriber.subscription_path(project_id, small_subscription_id)


def callback(message):
    msg_raw = message.data
    msg_tuple = pickle.loads(msg_raw)
    print(f"Received message: {msg_tuple}")
    message.ack()


streaming_pull_future = subscriber.subscribe(small_subscription_path, callback=callback)
print(f"Listening for messages on {small_subscription_path}...")

try:
    # Keep the main thread running to allow the subscriber to receive messages
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped receiving messages.")
