from collections import defaultdict

import pulsar
import time
import os
import pickle
import uuid
import pathlib
import argparse
import pandas as pd
from io import StringIO

from edgeserve.message_format import GraphCodec


class PruneLog:
    """
    PruneLog class is used to prune the log files. Local logs are pruned if they are not used by downstream models.
    When a n-tuple message is skipped by a downstream model, it would send a message back to the upstream model as an
    indication that the n-tuple message is not needed. If the upstream model receives such after-skip messages from all
    of its downstream models, it would prune the log files.
    """
    def __init__(self, pulsar_node, topic_prune_prefix, log_path, log_prefix, pruner_id):
        self.client = pulsar.Client(pulsar_node)
        self.topic_prune_prefix = topic_prune_prefix
        self.log_name = os.path.join(log_path, log_prefix)
        self.inputs = []
        self.outputs = []
        self.outgoing_to_prune = defaultdict(list)  # op_from: [uuids]
        self.incoming_to_prune = []  # uuids
        self.prune_producers = dict()
        # self.load_graph()
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)
        self.pruner_id = pruner_id
        ops_from = ['model1', 'model2']  # TODO: hardcoded for now. This should be read from the graph.
        topics = [f'{self.topic_prune_prefix}-{op_from}' for op_from in ops_from]
        self.consumer = self.client.subscribe(topics, subscription_name=f'pruner-{self.pruner_id}',
                                             schema=pulsar.schema.BytesSchema())
        self.wal_prune_lines = set()
        self.orl_prune_lines = set()

    def load_graph(self):
        """
        This method is responsible for loading the graph.
        """
        with open(self.log_name + '.graph', 'rb') as f:
            graph = pickle.load(f)
            self.inputs = graph['inputs']
            self.outputs = graph['outputs']

    def scan_and_prune_wal(self):
        # scan self.outgoing_to_prune
        # if all downstream models have pruned the message, prune the message from WAL
        with open(self.log_name + '.wal.prune', 'a') as f_prune:
            # with open(self.log_name + '.wal', 'r') as f:
            #     logs = f.readlines()
            #     for line in logs:
            #         # if all downstream models have pruned the message, prune the message from WAL
            #         to_prune = True
            #         for uuid in line['output_uuids']:
            #             if uuid not in self.outgoing_to_prune:
            #                 to_prune = False
            #                 break
            #         if to_prune:
            #             # prune the message from WAL
            #             f_prune.write(line)
            #             # if (d,e,f) is pruned in (a,b,c) -> (d,e,f), also put (a,b,c) in incoming_to_prune cache
            #             self.scan_and_prune_orl(line['inputs'])
            df = pd.read_csv(self.log_name + '.wal', dtype=str)  # TODO: scan the CSV incrementally
            incoming_columns = df.columns[:df.columns.get_loc('msg_out_uuid')]  # TODO: generalize to more than one outgoing ops
            for index, row in df.iterrows():
                # if all downstream models have pruned the message, prune the message from WAL
                to_prune = True
                # for _, uuid in row['msg_out_uuid'].items():  # TODO: generalize to more than one outgoing ops
                if not any(row['msg_out_uuid'] in sublist for sublist in self.outgoing_to_prune.values()):
                    to_prune = False
                    # break  # TODO: generalize to more than one outgoing ops
                if to_prune:
                    # prune the message from WAL
                    output = StringIO()
                    row.to_csv(output, index=False, header=False)
                    csv_line = output.getvalue().strip().replace('\n', ',')
                    if csv_line not in self.wal_prune_lines:
                        f_prune.write(csv_line + '\n')
                        self.wal_prune_lines.add(csv_line)

                        # if (d,e,f) is pruned in (a,b,c) -> (d,e,f), also put (a,b,c) in incoming_to_prune cache
                        row_incoming = row[incoming_columns]
                        self.scan_and_prune_orl(row_incoming)

    def scan_and_prune_orl(self, row_incoming):
        # do not scan, simply prune.
        output_orl = StringIO()
        row_incoming.to_frame().T.to_csv(output_orl, index=False, header=False)
        # treat multiple data sources as multiple lines
        csv_line_orl = output_orl.getvalue().strip().replace(',', '\n')
        if csv_line_orl not in self.orl_prune_lines:
            with open(self.log_name + '.orl.prune', 'a') as f_prune:
                f_prune.write(str(csv_line_orl) + '\n')
            self.orl_prune_lines.add(csv_line_orl)

        # send (op_from, msg_uuid) messages to the upstream op.
        row_tuples = list(row_incoming.items())
        self.send_upstream(row_tuples)

    def receive_skip_message(self):
        """
        This method is responsible for handling the skip messages from downstream models.
        """
        while True:
            msg = self.consumer.receive()
            msg_uuid, op_from, _, _ = self.graph_codec.decode(msg.value())

            # Mark that this message is not needed in this particular downstream model.
            # Note that self.outgoing_to_prune is not persisted to disk.
            # If it is lost, there might be fewer lines in prune log than optimal.
            self.outgoing_to_prune[op_from].append(str(msg_uuid))
            print(op_from, str(msg_uuid))

            # If this message is skipped by all downstream models, prune it.
            self.scan_and_prune_wal()

            self.consumer.acknowledge(msg)

    def send_upstream(self, msgs):
        """
        This method is responsible for sending the skip messages to upstream models.
        """

        # If the message is deleted in ORL, also prune it.
        for msg in msgs:
            op_from, msg_uuid_str = msg

            if op_from not in self.prune_producers:
                self.prune_producers[op_from] = self.client.create_producer(f'{self.topic_prune_prefix}-{op_from}',
                                                                            schema=pulsar.schema.BytesSchema())
            msg_prune = self.graph_codec.encode(msg_uuid=uuid.UUID(msg_uuid_str), op_from=self.pruner_id, payload=b'')
            self.prune_producers[op_from].send(msg_prune)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prune Log')
    parser.add_argument('--pulsar_node', type=str, default='pulsar://localhost:6650',
                        help='Pulsar node address')
    parser.add_argument('--topic_prune_prefix', type=str, default='prune', help='Prune topic prefix')
    parser.add_argument('--log_path', type=str, default='./', help='Log path')
    parser.add_argument('--log_prefix', type=str, default='log', help='Log prefix')
    parser.add_argument('--pruner_id', type=str, default='pruner', help='Pruner ID')
    args = parser.parse_args()

    prune_log = PruneLog(args.pulsar_node, args.topic_prune_prefix, args.log_path, args.log_prefix, args.pruner_id)
    prune_log.receive_skip_message()
