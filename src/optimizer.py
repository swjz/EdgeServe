import numpy as np
import cvxpy as cp
from kafka import KafkaProducer, KafkaConsumer

from constants import *

class Optimizer:
    """
    simultaneously a Kafka publisher and subscriber to the STATUS channel
    """
    def __init__(self):
        self.producer = KafkaProducer(
            TOPIC_STATUS,
            bootstrap_servers=['ted-driver:9092', 'ted-worker1:9092', 'ted-worker2:9092'],
            )
        self.consumer = KafkaConsumer(
            TOPIC_STATUS,
            bootstrap_servers=['ted-driver:9092', 'ted-worker1:9092', 'ted-worker2:9092']
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            # consumer_timeout_ms=1000,
            )

    # TODO: can refactor the functions below

    def notify_publish(self, group_id, topic):
        # key is device name, value is what to do, ex. publish data0
        print('Optimizer tells', group_id, 'to publish to', topic)
        self.producer.send(
            TOPIC_STATUS,
            key=group_id.encode('utf-8'),
            value='{}_{}'.format(KEY_PUBLISH, topic).encode('utf-8')
            )

    def notify_subscribe(self, group_id, topic):
        print('Optimizer tells', group_id, 'to subscribe to', topic)
        self.producer.send(
            TOPIC_STATUS,
            key=group_id.encode('utf-8'),
            value='{}_{}'.format(KEY_SUBSCRIBE, topic).encode('utf-8')
            )

    # not called anywehre, is this necessary given the device should always be looping?
    def notify_read(self, group_id):
        print('Optimizer tells', group_id, 'to read data')
        self.producer.send(
            TOPIC_STATUS,
            key=group_id.encode('utf-8'),
            value=KEY_READ_DATA.encode('utf-8')
            )

    # TODO: Functions to listen to device status update
    # self.consumer

    def solve(self, source_matrix, size_vec, replication_vec, storage_vec, colocation_list):
        """
        returns (cost, transmit_matrix)
        """
        transmit_matrix = cp.Variable(source_matrix.shape, boolean=True)
        objective = cp.Minimize(cp.sum((transmit_matrix - source_matrix) @ size_vec))
        constraints = [
            transmit_matrix >= source_matrix,
            cp.sum(transmit_matrix, axis=0) >= replication_vec,
            cp.matmul(transmit_matrix, size_vec) <= storage_vec
            ]
        for colocate in colocation_list:
            for i in range(len(colocate) - 1):
                col1 = colocate[i]
                col2 = colocate[i + 1]
                constraints.append(transmit_matrix[:, col1] == transmit_matrix[:, col2])
        prob = cp.Problem(objective, constraints)
        cost = prob.solve()
        return cost, transmit_matrix.value
