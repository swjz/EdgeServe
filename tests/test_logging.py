import os
import time

from edgeserve.compute import Compute
from edgeserve.data_source import DataSource
from edgeserve.materialize import Materialize
import pytest


@pytest.fixture()
def raw_data():
    def task(stream1, stream2):
        return stream1 + ' ' + stream2

    def task_model2(model1, stream3):
        return model1 + ' ' + stream3

    return {'node': 'pulsar://localhost:6650',
            'stream1': ['hello', 'HELLO'],
            'stream2': ['world', 'WORLD'],
            'stream3': ['world', '!!'],
            'task': task,
            'task_model2': task_model2}


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


@pytest.fixture()
def ftp_data():
    def ftp_task_file(size_in_bytes):
        with open('/srv/ftp/files/custom_size.bin', 'wb') as f:
            content = os.urandom(size_in_bytes)
            f.write(content)
            return content

    def ftp_task_memory(data):
        return data.decode('utf-8') * 2

    return {'node': 'pulsar://localhost:6650',
            'stream': ['ftp://localhost/files/1.txt', 'ftp://localhost/files/2.txt', 'ftp://localhost/files/3.txt'],
            'local': ['/srv/ftp/files/1.txt', '/srv/ftp/files/2.txt', '/srv/ftp/files/3.txt'],
            'task': {'file': ftp_task_file, 'memory': ftp_task_memory}}


"""
Data source 1 ('hello') \
                         --> Compute --> Materialize
Data source 2 ('world') /
"""


def test_pipeline(raw_data):
    with DataSource(raw_data['stream1'], raw_data['node'], source_id='stream1', topic='data',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='stream1', is_overhead_logged=True) as data_source_1, \
            DataSource(raw_data['stream2'], raw_data['node'], source_id='stream2', topic='data',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream2', is_overhead_logged=True) as data_source_2, \
            Compute(raw_data['task'], raw_data['node'], topic_in='data', gate_in=lambda x: x.decode('utf-8'),
                    gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model1', is_log_verbose=True, is_overhead_logged=True) as compute, \
            Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
                        log_path='/tmp/edgeserve/logs', log_filename='dest', is_overhead_logged=True) as materialize:
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


def test_2models(raw_data):
    with DataSource(raw_data['stream1'], raw_data['node'], source_id='stream1', topic='m1-in',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='stream1', is_overhead_logged=True) as data_source_1, \
            DataSource(raw_data['stream2'], raw_data['node'], source_id='stream2', topic='m1-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream2', is_overhead_logged=True) as data_source_2, \
            DataSource(raw_data['stream3'], raw_data['node'], source_id='stream3', topic='m2-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream3', is_overhead_logged=True) as data_source_3, \
            Compute(raw_data['task'], raw_data['node'], topic_in='m1-in', gate_in=lambda x: x.decode('utf-8'),
                    topic_out='m2-in', gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model1', is_log_verbose=True, is_overhead_logged=True, worker_id='model1') as model1, \
            Compute(raw_data['task_model2'], raw_data['node'], topic_in='m2-in', gate_in=lambda x: x.decode('utf-8'),
                    gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model2', is_log_verbose=True, is_overhead_logged=True) as model2, \
            Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
                        log_path='/tmp/edgeserve/logs', log_filename='dest', is_overhead_logged=True) as materialize:
        assert next(data_source_1) == raw_data['stream1'][0].encode('utf-8')
        assert next(data_source_2) == raw_data['stream2'][0].encode('utf-8')
        assert next(data_source_3) == raw_data['stream3'][0].encode('utf-8')
        assert next(model1) is None
        assert next(model1) == raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0]).encode('utf-8')
        assert next(model2) is None
        assert next(model2) == raw_data['task'](raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0]),
                                                raw_data['stream3'][0]).encode('utf-8')
        assert next(materialize) == raw_data['task'](raw_data['task'](raw_data['stream1'][0], raw_data['stream2'][0]),
                                                     raw_data['stream3'][0])


# TODO: fix faulty test
def test_time_interval(raw_data):
    with DataSource(raw_data['stream1'] * 5, raw_data['node'], source_id='stream1', topic='m1-in',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='stream1', is_overhead_logged=True) as data_source_1, \
            DataSource(raw_data['stream2'] * 5, raw_data['node'], source_id='stream2', topic='m1-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream2', is_overhead_logged=True) as data_source_2, \
            DataSource(raw_data['stream3'] * 5, raw_data['node'], source_id='stream3', topic='m2-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream3', is_overhead_logged=True) as data_source_3, \
            Compute(raw_data['task'], raw_data['node'], topic_in='m1-in', gate_in=lambda x: x.decode('utf-8'),
                    topic_out='m2-in', gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model1', is_log_verbose=True, is_overhead_logged=True, worker_id='model1') as model1, \
            Compute(raw_data['task_model2'], raw_data['node'], topic_in='m2-in', gate_in=lambda x: x.decode('utf-8'),
                    gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model2', is_log_verbose=True, is_overhead_logged=True, min_interval_ms=100) as model2, \
            Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
                        log_path='/tmp/edgeserve/logs', log_filename='dest', is_overhead_logged=True) as materialize:
        for _ in range(5):
            assert next(data_source_1) == raw_data['stream1'][0].encode('utf-8')
            assert next(data_source_1) == raw_data['stream1'][1].encode('utf-8')
            assert next(data_source_2) == raw_data['stream2'][0].encode('utf-8')
            assert next(data_source_2) == raw_data['stream2'][1].encode('utf-8')
            assert next(data_source_3) == raw_data['stream3'][0].encode('utf-8')
            assert next(data_source_3) == raw_data['stream3'][1].encode('utf-8')

        for i in range(5):
            print(f'Model 1, Run {i}-1', next(model1))
            print(f'Model 1, Run {i}-2', next(model1))
            print(f'Model 2, Run {i}-1', next(model2))
            print(f'Model 2, Run {i}-2', next(model2))
            time.sleep(0.1)


def test_time_interval_prune(raw_data):
    with DataSource(raw_data['stream1'] * 5, raw_data['node'], source_id='stream1', topic='m1-in',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='stream1', is_overhead_logged=True) as data_source_1, \
            DataSource(raw_data['stream2'] * 5, raw_data['node'], source_id='stream2', topic='m1-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream2', is_overhead_logged=True) as data_source_2, \
            DataSource(raw_data['stream3'] * 5, raw_data['node'], source_id='stream3', topic='m2-in',
                       gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                       log_filename='stream3', is_overhead_logged=True) as data_source_3, \
            Compute(raw_data['task'], raw_data['node'], topic_in='m1-in', gate_in=lambda x: x.decode('utf-8'),
                    topic_out='m2-in', gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model1', is_log_verbose=True, is_overhead_logged=True, worker_id='model1',
                    enable_prune=True) as model1, \
            Compute(raw_data['task_model2'], raw_data['node'], topic_in='m2-in', gate_in=lambda x: x.decode('utf-8'),
                    gate_out=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='model2', is_log_verbose=True, is_overhead_logged=True, min_interval_ms=100,
                    enable_prune=True) as model2, \
            Materialize(lambda x: x, raw_data['node'], gate=lambda x: x.decode('utf-8'),
                        log_path='/tmp/edgeserve/logs', log_filename='dest', is_overhead_logged=True) as materialize:
        for _ in range(5):
            assert next(data_source_1) == raw_data['stream1'][0].encode('utf-8')
            assert next(data_source_1) == raw_data['stream1'][1].encode('utf-8')
            assert next(data_source_2) == raw_data['stream2'][0].encode('utf-8')
            assert next(data_source_2) == raw_data['stream2'][1].encode('utf-8')
            assert next(data_source_3) == raw_data['stream3'][0].encode('utf-8')
            assert next(data_source_3) == raw_data['stream3'][1].encode('utf-8')

        for i in range(5):
            print(f'Model 1, Run {i}-1', next(model1))
            print(f'Model 1, Run {i}-2', next(model1))
            print(f'Model 2, Run {i}-1', next(model2))
            print(f'Model 2, Run {i}-2', next(model2))
            time.sleep(0.1)
