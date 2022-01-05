from compute import Compute
from data_source import DataSource
from materialize import Materialize


class TestPipeline():
    def test_pipeline(self):
        node = 'pulsar://localhost:6650'
        stream = 'hello'
        task = lambda x: x*2
        with DataSource(stream, node) as data_source,\
                Compute(task, node) as compute,\
                Materialize(lambda x: x, node) as materialize:
            for l in stream:
                assert next(data_source) == l.encode('utf-8')
                assert next(compute) == task(l).encode('utf-8')
                assert next(materialize) == task(l)


    def test_pipeline_separate(self):
        node = 'pulsar://localhost:6650'
        stream = 'hello'
        task = lambda x: x*2
        with DataSource(stream, node) as data_source:
            for l in stream:
                assert next(data_source) == l.encode('utf-8')

        with Compute(task, node) as compute:
            for l in stream:
                assert next(compute) == task(l).encode('utf-8')

        with Materialize(lambda x: x, node) as materialize:
            for l in stream:
                assert next(materialize) == task(l)
