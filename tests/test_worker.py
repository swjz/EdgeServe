from edgeserve.data_source import DataSource
import pytest


data_source_code = """
from edgeserve.data_source import DataSource
node = 'pulsar://localhost:6650'
stream = ['Hello World!']
with DataSource(stream, node) as data_source:
    next(data_source)
print('Done!', flush=True)
"""

compute_code = """
from edgeserve.compute import Compute
node = 'pulsar://localhost:6650'
task = lambda x: x
with Compute(task, node) as compute:
    assert next(compute) == b'Hello World!'
print('Done!', flush=True)
"""


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650'}


def test_worker(raw_data):
    with DataSource([data_source_code, compute_code], raw_data['node'], topic='code-raw') as data_source:
        next(data_source)
        next(data_source)

    with open('/tmp/pipe-raw', 'r') as pipe:
        count = 0
        while True:
            line = pipe.readline().rstrip()
            if line == 'Done!':
                count += 1
            if count >= 2:
                return


@pytest.fixture()
def ftp_data():
    return {'node': 'pulsar://localhost:6650',
            'data_source_local': '/srv/ftp/files/data_source_code.py',
            'compute_local': '/srv/ftp/files/compute_code.py',
            'data_source_global': 'ftp://localhost/files/data_source_code.py',
            'compute_global': 'ftp://localhost/files/compute_code.py'}


def test_worker_ftp(ftp_data):
    with open(ftp_data['data_source_local'], 'w') as f:
        f.write(data_source_code)
    with open(ftp_data['compute_local'], 'w') as f:
        f.write(compute_code)

    with DataSource([ftp_data['data_source_global'], ftp_data['compute_global']], ftp_data['node'],
                    topic='code-ftp') as data_source:
        next(data_source)
        next(data_source)

    with open('/tmp/pipe-ftp', 'r') as pipe:
        count = 0
        while True:
            line = pipe.readline().rstrip()
            if line == 'Done!':
                count += 1
            if count >= 2:
                return


def test_worker_ftp_memory(ftp_data):
    with open(ftp_data['data_source_local'], 'w') as f:
        f.write(data_source_code)
    with open(ftp_data['compute_local'], 'w') as f:
        f.write(compute_code)

    with DataSource([ftp_data['data_source_global'], ftp_data['compute_global']], ftp_data['node'],
                    topic='code-ftp-memory') as data_source:
        next(data_source)
        next(data_source)

    with open('/tmp/pipe-ftp-memory', 'r') as pipe:
        count = 0
        while True:
            line = pipe.readline().rstrip()
            if line == 'Done!':
                count += 1
            if count >= 2:
                return
