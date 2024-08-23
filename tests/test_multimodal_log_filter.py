from edgeserve.log_filter import WalFilter
from PIL import Image

import pickle
import pytest


@pytest.fixture()
def raw_data():
    def filter_method(line):
        msg_uuid = line.split(',')[0]
        path = '/srv/ftp/ftp_output/' + msg_uuid + '.ftp'
        with open(path, 'rb') as f:
            msg_in = pickle.load(f)
        image_path = msg_in.decode('utf-8')
        # Read the image from the file. If the resolution is lower than 1000x1000, return True, False
        img = Image.open(image_path)
        if img.width < 1000 or img.height < 1000:
            return True, False
        return False, False

    return {'node': 'pulsar://localhost:6650',
            'filter': filter_method}


def test_log_filter(raw_data):
    filter = WalFilter(raw_data['node'], '/tmp/edgeserve/logs/multimodal-image-multimodal-input.wal', raw_data['filter'],
                       'multimodal-image', ['multimodal-aggr'])
    filter.scan()
