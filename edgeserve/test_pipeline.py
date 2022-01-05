from compute import Compute
from data_source import DataSource
from materialize import Materialize


class TestPipeline():
    def test_pipeline(self):
        node = 'pulsar://swjz-nuc2:6650'
        s = 'hello'
        with DataSource(stream=s, pulsar_node=node) as data_source, Compute(task=lambda x: x*2, pulsar_node=node) as compute, Materialize(print, pulsar_node=node) as materialize:
            for _ in s:
                next(data_source)
                next(compute)
                next(materialize)
