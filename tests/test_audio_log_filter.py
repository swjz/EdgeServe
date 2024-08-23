from edgeserve.log_filter import OrlFilter

import pickle
import pytest


@pytest.fixture()
def raw_data():
    def filter_method(line):
        msg_in_uuid = line.split(',')[0]
        path = '/srv/ftp/ftp_output/' + msg_in_uuid + '.ftp'
        with open(path, 'rb') as f:
            msg_in = pickle.load(f)
        if len(msg_in[2]) == 0:
            return False, True
        return False, False

    return {'node': 'pulsar://localhost:6650',
            'filter': filter_method}


def test_log_filter(raw_data):
    filter = OrlFilter(raw_data['node'], '/tmp/edgeserve/logs/aggregator.orl', raw_data['filter'], 'aggregator',
                   ['large', 'small'])
    filter.scan()
