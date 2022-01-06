from compute import Compute
from data_source import DataSource
from materialize import Materialize
import pytest


@pytest.fixture()
def data():
    return {'node': 'pulsar://localhost:6650',
            'stream': 'hello',
            'task': lambda x: x*2}

def test_pipeline(data):
    with DataSource(data['stream'], data['node']) as data_source,\
            Compute(data['task'], data['node']) as compute,\
            Materialize(lambda x: x, data['node']) as materialize:
        for letter in data['stream']:
            assert next(data_source) == letter.encode('utf-8')
            assert next(compute) == data['task'](letter).encode('utf-8')
            assert next(materialize) == data['task'](letter)

def test_data_source(data):
    with DataSource(data['stream'], data['node']) as data_source:
        for letter in data['stream']:
            assert next(data_source) == letter.encode('utf-8')

def test_compute(data):
    with Compute(data['task'], data['node']) as compute:
        for letter in data['stream']:
            assert next(compute) == data['task'](letter).encode('utf-8')

def test_materialize(data):
    with Materialize(lambda x: x, data['node']) as materialize:
        for letter in data['stream']:
            assert next(materialize) == data['task'](letter)
