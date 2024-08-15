from edgeserve.data_source import AudioSource
import pytest


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650',
            'audio_path': 'gettysburg.wav'}


def test_pipeline(raw_data):
    import urllib.request
    urllib.request.urlretrieve("https://www2.cs.uic.edu/~i101/SoundFiles/gettysburg.wav", "gettysburg.wav")
    with AudioSource(raw_data['audio_path'], raw_data['node'], source_id='data', topic='audio-src-small',
                     topic_extra='audio-src-large', chunk_size_small=1, chunk_size_large=5) as audio_source:
        while True:
            try:
                next(audio_source)
            except StopIteration:
                break
