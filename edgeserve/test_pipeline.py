from compute import Compute
from data_source import DataSource
from materialize import Materialize
from urllib.request import urlopen
import pytest


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650',
            'stream': 'hello',
            'task': lambda x: x*2}


@pytest.fixture()
def ftp_data():
    def ftp_task(local_file_path):
        with open(local_file_path) as f:
            content = f.read()
            assert content == 'Hello World!'
    return {'node': 'pulsar://localhost:6650',
            'stream': ['ftp://localhost/files/1.txt', 'ftp://localhost/files/2.txt', 'ftp://localhost/files/3.txt'],
            'local': ['/srv/ftp/files/1.txt', '/srv/ftp/files/2.txt', '/srv/ftp/files/3.txt'],
            'task': ftp_task}


def test_pipeline(raw_data):
    with DataSource(raw_data['stream'], raw_data['node']) as data_source,\
            Compute(raw_data['task'], raw_data['node']) as compute,\
            Materialize(lambda x: x, raw_data['node']) as materialize:
        for letter in raw_data['stream']:
            assert next(data_source) == letter.encode('utf-8')
            assert next(compute) == raw_data['task'](letter).encode('utf-8')
            assert next(materialize) == raw_data['task'](letter)


def test_data_source(raw_data):
    with DataSource(raw_data['stream'], raw_data['node']) as data_source:
        for letter in raw_data['stream']:
            assert next(data_source) == letter.encode('utf-8')


def test_compute(raw_data):
    with Compute(raw_data['task'], raw_data['node']) as compute:
        for letter in raw_data['stream']:
            assert next(compute) == raw_data['task'](letter).encode('utf-8')


def test_materialize(raw_data):
    with Materialize(lambda x: x, raw_data['node']) as materialize:
        for letter in raw_data['stream']:
            assert next(materialize) == raw_data['task'](letter)


def test_ftp(ftp_data):
    for file in ftp_data['local']:
        with open(file, 'w') as f:
            f.write('Hello World!')

    with DataSource(ftp_data['stream'], ftp_data['node']) as data_source, \
            Compute(ftp_data['task'], ftp_data['node'], ftp=True, local_ftp_path='/srv/ftp/') as compute, \
            Materialize(lambda x: x, ftp_data['node'], ftp=True) as materialize:
        for path in ftp_data['stream']:
            assert next(data_source) == path.encode('utf-8')
            assert urlopen(next(compute).decode('utf-8')).read() == b'Hello World!'
            assert urlopen(next(materialize)).read() == b'Hello World!'
