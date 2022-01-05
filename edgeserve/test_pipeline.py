from compute import Compute
from data_source import DataSource
from materialize import Materialize


class TestPipeline():
    def test_pipeline(self):
        node = 'pulsar://swjz-nuc2:6650'
        s = 'hello'
        task = lambda x: x*2
        with DataSource(stream=s, pulsar_node=node) as data_source,\
                Compute(task=task, pulsar_node=node) as compute,\
                Materialize(lambda x: x, pulsar_node=node) as materialize:
            for l in s:
                assert next(data_source) == l.encode('utf-8')
                assert next(compute) == task(l).encode('utf-8')
                assert next(materialize) == task(l)
