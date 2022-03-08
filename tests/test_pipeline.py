from edgeserve.compute import Compute
from edgeserve.data_source import DataSource
from edgeserve.materialize import Materialize
from urllib.request import urlopen
import pytest


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650',
            'stream': 'hello',
            'task': lambda data: data*2}


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


def test_pipeline(raw_data):
    with DataSource(raw_data['stream'], raw_data['node'], source_id='data', topic='data') as data_source, \
            Compute(raw_data['task'], raw_data['node'], topic_in='data') as compute, \
            Materialize(lambda x: x, raw_data['node']) as materialize:
        for letter in raw_data['stream']:
            assert next(data_source) == letter.encode('utf-8')
            assert next(compute) == raw_data['task'](letter).encode('utf-8')
            assert next(materialize) == raw_data['task'](letter)


def test_data_source(raw_data):
    with DataSource(raw_data['stream'], raw_data['node'], source_id='data', topic='data') as data_source:
        for letter in raw_data['stream']:
            assert next(data_source) == letter.encode('utf-8')


def test_compute(raw_data):
    with Compute(raw_data['task'], raw_data['node'], topic_in='data') as compute:
        for letter in raw_data['stream']:
            assert next(compute) == raw_data['task'](letter).encode('utf-8')


def test_materialize(raw_data):
    with Materialize(lambda x: x, raw_data['node']) as materialize:
        for letter in raw_data['stream']:
            assert next(materialize) == raw_data['task'](letter)


def test_ftp_file(ftp_data):
    for file in ftp_data['local']:
        with open(file, 'w') as f:
            f.write('Hello World!')

    with DataSource(ftp_data['stream'], ftp_data['node'], source_id='file_path', topic='file_path') as data_source, \
            Compute(ftp_data['task']['file'], ftp_data['node'], ftp=True, local_ftp_path='/srv/ftp/',
                    ftp_memory=False, ftp_delete=True, topic_in='file_path') as compute, \
            Materialize(lambda x: x, ftp_data['node'], ftp=True) as materialize:
        for path in ftp_data['stream']:
            assert next(data_source) == path.encode('utf-8')
            assert urlopen(next(compute).decode('utf-8')).read() == b'Hello World!'
            assert urlopen(next(materialize)).read() == b'Hello World!'


def test_ftp_memory(ftp_data):
    for file in ftp_data['local']:
        with open(file, 'w') as f:
            f.write('Hello World!')

    with DataSource(ftp_data['stream'], ftp_data['node'], source_id='data', topic='data') as data_source, \
            Compute(ftp_data['task']['memory'], ftp_data['node'], ftp=True, local_ftp_path='/srv/ftp/',
                    ftp_memory=True, ftp_delete=True, topic_in='data') as compute:
        for path in ftp_data['stream']:
            assert next(data_source) == path.encode('utf-8')
            assert next(compute) == b'Hello World!' * 2
