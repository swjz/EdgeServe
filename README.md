# EdgeServe

EdgeServe is a decentralized model serving system that optimizes data movements and model placements within an edge cluster.

## Installation
```bash
git clone https://github.com/swjz/EdgeServe.git
cd EdgeServe
pip3 -r requirements.txt
pip3 install -e .
```

For how to set up an Apache Pulsar server in docker, please refer to [Apache Pulsar Doc](https://pulsar.apache.org/docs/2.11.x/getting-started-docker/).

## Usage
### Working as a data source node
```python
from edgeserve.data_source import DataSource

node = 'pulsar://server-address:6650'

def stream():
    while True:
        yield sensor.read()

with DataSource(stream(), node, source_id='data1', topic='topic_in') as ds:
    # every next() call sends an example to the message queue
    next(ds)
```

### Working as a prediction node
```python
from edgeserve.compute import Compute

node = 'pulsar://server-address:6650'

def task(data1, data2, data3, data4):
    # actual prediction work here
    return aggregate_and_predict(data1, data2, data3, data4)

with Compute(task, node, worker_id='worker1', topic_in='topic_in', topic_out='topic_out', min_interval_ms=30) as compute:
    # every next() call consumes a message from the queue and runs task() on it
    # the result is sent to another message queue
    next(compute)
```

### Working as a destination node
```python
from edgeserve.materialize import Materialize

node = 'pulsar://server-address:6650'

def final_process(y_pred):
    # reformat the prediction result, materialize and/or use it for decision making
    make_decisions(y_pred)

with Materialize(final_process, node, topic='topic_out') as materialize:
    # every next() call consumes a message from the prediction output queue and runs final_process() on it
    next(materialize)
```
