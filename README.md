# EdgeServe
[[Docs]](https://docs.edgeserve.org/)

EdgeServe is a distributed streaming system that can serve predictions from machine learning models in real time.

## Motivation and Design Principles
Machine learning deployments are getting more complex where models may source feature data from a variety of disparate services hosted on different devices. Examples include:
- **Network Security.** Packet capture data collected from different nodes in a network may have to be combined to make global decisions about whether an attack is taking place.
- **Systematic Stock Trading.** Data from a variety of data streams, often sourced from different vendors, are combined to predict the movement of stock prices. 
- **Multimodal Augmented Reality.** Data from different sensors placed around a room, e.g., multiple video cameras, can be integrated into a single global model to make inferences about activities taking place in the room.

EdgeServe recognizes a need for a model serving system that can coordinate data from multiple data services and/or connect the results from multiple models. We have currently developed a research prototype with a Python programming interface and we encourage external usage and collaboration for feedback. 

## Installation
```bash
git clone https://github.com/swjz/EdgeServe.git
cd EdgeServe
pip3 -r requirements.txt
pip3 install -e .
```

EdgeServe depends on Apache Pulsar as its default message broker service. Here is an easy way to set up a standalone Apache Pulsar server within a Docker container:
```bash
docker run -it --name pulsar -p 6650:6650 -p 8080:8080 \
   --mount source=pulsardata,target=/pulsar/data \
   --mount source=pulsarconf,target=/pulsar/conf \
    apachepulsar/pulsar:3.1.0 bin/pulsar standalone
```
For more details, please refer to [Apache Pulsar Doc](https://pulsar.apache.org/docs/2.11.x/getting-started-docker/).

## Usage
EdgeServe has an iterator-based Python interface designed for streaming data.
Each node can run multiple tasks at the same time, and each task can be replicated on multiple nodes as they consume the shared message queue in parallel.
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

with Compute(task, node, worker_id='worker1', topic_in='topic_in', topic_out='topic_out',\
             min_interval_ms=30, drop_if_older_than_ms=500) as compute:
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

## References
EdgeServe is a product of a multi-year research effort in edge computing at University of Chicago (ChiData)

1. Ted Shaowang and Sanjay Krishnan. EdgeServe: A Streaming System for Decentralized Model Serving. [https://arxiv.org/pdf/2303.08028.pdf](https://arxiv.org/pdf/2303.08028.pdf)
2. Ted Shaowang, Xi Liang and Sanjay Krishnan. Sensor Fusion on the Edge: Initial Experiments in the EdgeServe System. [https://tedshaowang.com/assets/pdf/bidede22-shaowang.pdf](https://tedshaowang.com/assets/pdf/bidede22-shaowang.pdf)
3. Ted Shaowang, Nilesh Jain, Dennis Mathews, Sanjay Krishnan. Declarative Data Serving: The Future of Machine Learning Inference on the Edge. [http://www.vldb.org/pvldb/vol14/p2555-shaowang.pdf](http://www.vldb.org/pvldb/vol14/p2555-shaowang.pdf)
