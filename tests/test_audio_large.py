from edgeserve.compute import Compute
from edgeserve.data_source import AudioSource
from whisper_online import FasterWhisperASR, OnlineASRProcessor

import numpy as np
import pickle
import pytest


@pytest.fixture()
def raw_data():
    online_large = OnlineASRProcessor(FasterWhisperASR('en', 'small'))

    def large(data):
        online_large.insert_audio_chunk(data)
        cur_output = online_large.process_iter()
        print(cur_output)
        return cur_output

    def finish_large():
        print(online_large.finish())

    return {'node': 'pulsar://localhost:6650',
            'gate-in': lambda x: np.frombuffer(x, dtype=np.float32),
            'gate-out': lambda x: pickle.dumps(x),
            'task-large': large,
            'task-finish-large': finish_large}


def test_pipeline(raw_data):
    with Compute(raw_data['task-large'], raw_data['node'], topic_in='audio-src-large', gate_in=raw_data['gate-in'],
                 gate_out=raw_data['gate-out'], worker_id='large', topic_out='audio-aggr',
                 log_path='/tmp/edgeserve/logs', log_filename='large', is_log_verbose=True) as large_model:
        while True:
            try:
                next(large_model)
            except StopIteration:
                break
        raw_data['task-finish-large']()
