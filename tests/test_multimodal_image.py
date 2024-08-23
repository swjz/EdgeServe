from edgeserve.data_source import DataSource
import pytest


@pytest.fixture()
def raw_data():
    return {'node': 'pulsar://localhost:6650',
            'images': ['lowres.png', 'highres.png']*5}


def test_pipeline(raw_data):
    import urllib.request
    urllib.request.urlretrieve("https://upload.wikimedia.org/wikipedia/en/thumb/7/79/University_of_Chicago_shield.svg"
                               "/189px-University_of_Chicago_shield.svg.png", "lowres.png")
    urllib.request.urlretrieve("https://upload.wikimedia.org/wikipedia/en/thumb/7/79/University_of_Chicago_shield.svg"
                               "/1614px-University_of_Chicago_shield.svg.png", "highres.png")
    with DataSource(raw_data['images'], raw_data['node'], source_id='image', topic='multimodal-input',
                    gate=lambda x: x.encode('utf-8'), log_path='/tmp/edgeserve/logs',
                    log_filename='multimodal-image', is_payload_logged=False) as data_source:
        while True:
            try:
                next(data_source)
            except StopIteration:
                break
