from google.cloud import pubsub_v1
import time

project_id = "apache-beam-baseline"
small_topic_id = "audio-small-chunks"
large_topic_id = "audio-large-chunks"

publisher = pubsub_v1.PublisherClient()
small_topic_path = publisher.topic_path(project_id, small_topic_id)
large_topic_path = publisher.topic_path(project_id, large_topic_id)


def audio_source(audio_path, chunk_size_small=1, chunk_size_large=5):
    from edgeserve.util import load_audio_chunk
    last_small_chunk_time = 0
    last_large_chunk_time = 0
    last_small_sent_wall_time = time.time()
    last_large_sent_wall_time = time.time()
    while True:
        # receive new audio chunk (and e.g. wait for min_chunk_size seconds first, ...)
        audio_chunk = load_audio_chunk(audio_path, last_small_chunk_time,
                                       last_small_chunk_time + chunk_size_small)
        if len(audio_chunk) == 0:
            break
        audio_chunk = audio_chunk.tobytes()
        last_small_chunk_time += chunk_size_small

        # Send both small and large chunks when the time comes
        if last_large_chunk_time + chunk_size_large <= last_small_chunk_time:
            audio_chunk_large = load_audio_chunk(audio_path, last_large_chunk_time,
                                                 last_small_chunk_time)
            last_large_chunk_time += chunk_size_large
            if len(audio_chunk_large) > 0:
                audio_chunk_large = audio_chunk_large.tobytes()
                # Simulate the actual wall time speed of audio playing
                while time.time() - last_large_sent_wall_time < chunk_size_large:
                    time.sleep(0.001)
                last_large_sent_wall_time = time.time()
                yield audio_chunk, audio_chunk_large

        # Simulate the actual wall time speed of audio playing
        while time.time() - last_small_sent_wall_time < chunk_size_small:
            time.sleep(0.001)
        last_small_sent_wall_time = time.time()
        # Always send the small chunk no matter if the large chunk is ready or not
        yield audio_chunk, None


def stream():
    import urllib.request
    audio_path = 'gettysburg.wav'
    urllib.request.urlretrieve("https://www2.cs.uic.edu/~i101/SoundFiles/gettysburg.wav", audio_path)

    futures_large, futures_small = [], []

    for audio_chunk_small, audio_chunk_large in audio_source(audio_path):
        if audio_chunk_large is not None:
            futures_large.append(publisher.publish(large_topic_path, data=audio_chunk_large))
        futures_small.append(publisher.publish(small_topic_path, data=audio_chunk_small))

    try:
        time.sleep(1)  # Wait for the result of the publish operation
        for future in futures_large:
            message_id = future.result()
            print(f"(large) Message published successfully with message ID: {message_id}")
        for future in futures_small:
            message_id = future.result()
            print(f"(small) Message published successfully with message ID: {message_id}")
    except Exception as e:
        # Handle errors that occur during publishing
        print(f"Failed to publish message: {e}")


if __name__ == "__main__":
    stream()
