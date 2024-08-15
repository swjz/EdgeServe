from edgeserve.compute import Compute
from edgeserve.data_source import AudioSource
from whisper_online import FasterWhisperASR, OnlineASRProcessor

import numpy as np
import pickle
import pytest


@pytest.fixture()
def raw_data():
    online_small = OnlineASRProcessor(FasterWhisperASR('en', 'tiny'))

    def small(data):
        online_small.insert_audio_chunk(data)
        cur_output = online_small.process_iter()
        print(cur_output)
        return cur_output

    def finish_small():
        print(online_small.finish())

    return {'node': 'pulsar://localhost:6650',
            'gate-in': lambda x: np.frombuffer(x, dtype=np.float32),
            'gate-out': lambda x: pickle.dumps(x),
            'task-small': small,
            'task-finish-small': finish_small}


def test_pipeline(raw_data):
    with Compute(raw_data['task-small'], raw_data['node'], topic_in='audio-src-small', gate_in=raw_data['gate-in'],
                 gate_out=raw_data['gate-out'], worker_id='small', topic_out='audio-aggr',
                 log_path='/tmp/edgeserve/logs', log_filename='small', is_log_verbose=True) as small_model:
        while True:
            try:
                next(small_model)
            except StopIteration:
                break
        raw_data['task-finish-small']()
