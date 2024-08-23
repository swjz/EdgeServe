from edgeserve.data_source import DataSource
import pytest


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650',
            'text': ['The University of Chicago']*10}


def test_pipeline(raw_data):
    with DataSource(raw_data['text'], raw_data['node'], source_id='text', topic='multimodal-input',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='multimodal-text', is_payload_logged=False) as data_source:
        while True:
            try:
                next(data_source)
            except StopIteration:
                break
