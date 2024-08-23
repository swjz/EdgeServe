from edgeserve.compute import Compute

import pytest


@pytest.fixture()
def raw_data():
    def aggregate(text, image):
        aggregated_text = text + image
        return aggregated_text

    return {'node': 'pulsar://localhost:6650',
            'gate-aggr-in': lambda x: x.decode('utf-8'),
            'task-aggr': aggregate}


def test_pipeline(raw_data):
    with Compute(raw_data['task-aggr'], raw_data['node'], topic_in='multimodal-input', gate_in=raw_data['gate-aggr-in'],
                 gate_out=lambda x: x.encode('utf-8'), worker_id='multimodal-aggr', log_path='/tmp/edgeserve/logs',
                 log_filename='multimodal-aggr', is_log_verbose=True, max_time_diff_ms=10 ** 10) as aggregator:
        while True:
            try:
                print('Aggregator Output:', next(aggregator))
            except StopIteration:
                break
