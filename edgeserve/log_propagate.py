from collections import defaultdict

import pulsar
import os
import pickle
import uuid
import argparse

from edgeserve.message_format import GraphCodec


class PropagateKeepLog:
    """
    PropagateKeepLog class is used to propagate the keep files. Logs are kept if they pass a local or remote filter.
    When an n-tuple message is marked as upstream-kept by a downstream model, it would send a message back to the
    upstream model as an indication that the n-tuple message is needed there.
    When an n-tuple message is marked as downstream-kept by an upstream model, it would send a message to the downstream
    model as an indication that the n-tuple message is needed there.
    """
    def __init__(self, pulsar_node, topic_keep_prefix, log_path, log_prefix, propagator_id, outgoing_op):
        self.client = pulsar.Client(pulsar_node)
        self.topic_keep_prefix = topic_keep_prefix
        self.log_name = os.path.join(log_path, log_prefix)
        self.producers = dict()
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)
        self.propagator_id = propagator_id
        topic = f'{self.topic_keep_prefix}-{propagator_id}'
        self.consumer = self.client.subscribe(topic, subscription_name=f'propagator-{self.propagator_id}',
                                              schema=pulsar.schema.BytesSchema())
        self.outgoing_op = outgoing_op  # FIXME: hardcoded for now. This should be read from the graph.
        # self.incoming_ops = []
        # self.load_graph()

    def load_graph(self):
        """
        This method is responsible for loading the local graph.
        The local graph only has to contain input and output op names of the model at this node.
        """
        with open(self.log_name + '.graph', 'rb') as f:
            graph = pickle.load(f)
            self.incoming_ops = graph['incoming_ops']
            # self.outgoing_ops = graph['outgoing_ops']

    def write_to_keep(self, keep_file, line):
        with open(keep_file, 'a') as keep:
            keep.write(line)

    def find_outgoing_msg(self, incoming_msg_uuid):
        # find the outgoing messages that corresponds to the incoming message
        # FIXME: We should generalize the number of outgoing ops in ORL too.
        outgoing_msg_uuids = []
        with open(self.log_name + '.orl', 'r') as f:
            logs = f.readlines()
            for line in logs:
                if incoming_msg_uuid in line:
                    outgoing_msg_uuids.append(line.split(',')[-1])
                    self.write_to_keep(self.log_name + '.orl.keep', line)
        return outgoing_msg_uuids

    def find_incoming_msg(self, outgoing_msg_uuid):
        # find the incoming messages that corresponds to the outgoing message
        incoming_msg_uuids_dict = defaultdict(list)
        with open(self.log_name + '.wal', 'r') as f:
            header = f.readline()
            logs = f.readlines()
            for line in logs:
                if outgoing_msg_uuid in line:
                    num_input_ops = len(line.split(',')) - 5
                    is_kept = False
                    for i in range(num_input_ops):
                        if line.split(',')[i] != 'None':
                            incoming_msg_uuids_dict[header.split(',')[i]].append(line.split(',')[i])
                            is_kept = True
                    if is_kept:
                        self.write_to_keep(self.log_name + '.wal.keep', line)
        return incoming_msg_uuids_dict

    def receive_keep_message(self):
        """
        This method is responsible for handling the keep messages from other models.
        """
        while True:
            msg = self.consumer.receive()
            msg_uuid, op_from, _, payload = self.graph_codec.decode(msg.value())
            print(op_from, str(msg_uuid), payload.decode('utf-8'))
            if payload == b'backward':
                # The msg_id is an outgoing message of this op. We find the incoming message and backward propagate.
                incoming_msg_uuids_dict = self.find_incoming_msg(str(msg_uuid))
                for op_forward, incoming_msg_uuids in incoming_msg_uuids_dict.items():
                    self.send_backward(op_forward, incoming_msg_uuids)
            elif payload == b'forward':
                # The msg_id is an incoming message of this op. We find the outgoing message and forward propagate.
                outgoing_msg_uuids = self.find_outgoing_msg(str(msg_uuid))
                self.send_forward(outgoing_msg_uuids)

            self.consumer.acknowledge(msg)

    def send_backward(self, op_backward, incoming_msg_uuids):
        """
        This method is responsible for propagating the keep messages to upstream models.
        """
        for incoming_msg_uuid_str in incoming_msg_uuids:
            if op_backward not in self.producers:
                self.producers[op_backward] = self.client.create_producer(f'{self.topic_keep_prefix}-{op_backward}',
                                                                         schema=pulsar.schema.BytesSchema())
            msg_keep = self.graph_codec.encode(msg_uuid=uuid.UUID(incoming_msg_uuid_str), op_from=self.propagator_id,
                                               payload=b'backward')
            self.producers[op_backward].send(msg_keep)

    def send_forward(self, outgoing_msg_uuids):
        """
        This method is responsible for propagating the keep messages to downstream models.
        TODO: Currently, it is assumed that we only have one output op from the local node.
        TODO: Later it should be deprecated and combined with the send_backward method.
        """
        for outgoing_msg_uuid_str in outgoing_msg_uuids:
            if self.outgoing_op not in self.producers:
                self.producers[self.outgoing_op] = self.client.create_producer(
                    f'{self.topic_keep_prefix}-{self.outgoing_op}', schema=pulsar.schema.BytesSchema())
            msg_keep = self.graph_codec.encode(msg_uuid=uuid.UUID(outgoing_msg_uuid_str), op_from=self.propagator_id,
                                               payload=b'forward')
            self.producers[self.outgoing_op].send(msg_keep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Propagate Keep Log')
    parser.add_argument('--pulsar-node', type=str, default='pulsar://localhost:6650',
                        help='Pulsar node address')
    parser.add_argument('--topic-keep-prefix', type=str, default='keep', help='Keep topic prefix')
    parser.add_argument('--log-path', type=str, default='./', help='Log path')
    parser.add_argument('--log-prefix', type=str, default='model1', help='Log prefix')
    parser.add_argument('--propagator-id', type=str, default='model1', help='Propagator ID')
    parser.add_argument('--outgoing-op', type=str, help='Outgoing op name from this node')  # FIXME: temporary hack
    args = parser.parse_args()

    propagate_keep_log = PropagateKeepLog(args.pulsar_node, args.topic_keep_prefix, args.log_path, args.log_prefix,
                                          args.propagator_id, args.outgoing_op)
    propagate_keep_log.receive_keep_message()
