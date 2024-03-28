from edgeserve.compute import Compute
from edgeserve.data_source import DataSource
from edgeserve.materialize import Materialize
import pytest


@pytest.fixture()
def raw_data():
    def task(stream1, stream2):
        return stream1 + ' ' + stream2

    return {'node': 'pulsar://localhost:6650',
            'stream1': ['hello', 'HELLO'],
            'stream2': ['world', 'WORLD'],
            'task': task}

@pytest.fixture()
def ftp_data():
    def ftp_task_file(file_path):
        with open(file_path, 'r') as f:
            content = f.read()
            assert content == 'Hello World!'
            return content

    def ftp_task_memory(data):
        return data.decode('utf-8') * 2

    return {'node': 'pulsar://localhost:6650',
            'stream': ['ftp://localhost/files/1.txt', 'ftp://localhost/files/2.txt', 'ftp://localhost/files/3.txt'],
            'local': ['/srv/ftp/files/1.txt', '/srv/ftp/files/2.txt', '/srv/ftp/files/3.txt'],
            'task': {'file': ftp_task_file, 'memory': ftp_task_memory}}

#
# def test_simple(raw_data):
#     # Data source 1 ('hello') \
#     #                          --> Compute --> Materialize
#     # Data source 2 ('world') /
#     with DataSource(raw_data['stream1'], raw_data['node'], source_id='stream1', topic='data',
#                     gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs') as data_source_1, \
#             DataSource(raw_data['stream2'], raw_data['node'], source_id='stream2', topic='data',
#                        gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs') as data_source_2, \
#             Compute(raw_data['task'], raw_data['node'], topic_in='data', gate_in=lambda x: x.decode('utf-8'),
#                     gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
#                     log_verbose=True) as compute, \
#             Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
#                         log_path='/tmp/edgeserve/logs') as materialize:
#         assert next(data_source_1) == raw_data['stream1'][0].encode('utf-8')
#         assert next(data_source_2) == raw_data['stream2'][0].encode('utf-8')
#         assert next(compute) is None
#         assert next(compute) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0]).encode('utf-8')
#         assert next(materialize) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0])


def test_pipeline(raw_data):
    with DataSource(raw_data['stream1'], raw_data['node'], source_id='stream1', topic='data',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='stream1') as data_source_1, \
            DataSource(raw_data['stream2'], raw_data['node'], source_id='stream2', topic='data',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream2') as data_source_2, \
            Compute(raw_data['task'], raw_data['node'], topic_in='data', gate_in=lambda x: x.decode('utf-8'),
                    gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model1', log_verbose=True) as compute, \
            Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
                        log_path='/tmp/edgeserve/logs', log_filename='dest') as materialize:
        assert next(data_source_1) == raw_data['stream1'][0].encode('utf-8')
        assert next(data_source_2) == raw_data['stream2'][0].encode('utf-8')
        assert next(compute) is None
        assert next(compute) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0]).encode('utf-8')
        assert next(materialize) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0])
        assert next(data_source_2) == raw_data['stream2'][1].encode('utf-8')
        assert next(compute) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][1]).encode('utf-8')
        assert next(materialize) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][1])
        assert next(data_source_1) == raw_data['stream1'][1].encode('utf-8')
        assert next(compute) == raw_data['task'](raw_data['stream1'][1], raw_data['stream2'][1]).encode('utf-8')
        assert next(materialize) == raw_data['task'](raw_data['stream1'][1], raw_data['stream2'][1])
