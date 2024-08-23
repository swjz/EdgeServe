import uuid
import pulsar
from edgeserve.util import tail_generator
from edgeserve.message_format import GraphCodec


class LogFilter:
    """
    LogFilter class is used to filter logs. Logs are kept if they pass the given filter_method.
    Keep-log messages can be propagated to upstream and downstream operators based on return values of filter_method.
    """
    def __init__(self, pulsar_node, log_file, filter_method, worker_id, topics_in=None, topics_out=None,
                 topic_keep_prefix='keep'):
        self.client = pulsar.Client(pulsar_node)
        self.log_file = log_file
        self.keep_file = log_file + '.keep'
        self.filter_method = filter_method
        self.worker_id = worker_id
        self.topics_in = topics_in
        self.topics_out = topics_out
        self.producers = dict()
        self.topic_keep_prefix = topic_keep_prefix
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

    def get_msg_id_from_line(self, line):
        raise NotImplementedError

    def scan(self):
        with open(self.keep_file, 'a') as keep:
            for line in tail_generator(self.log_file):
                keep_forward, keep_backward = self.filter_method(line)
                if keep_forward or keep_backward:
                    keep.write(line + '\n')
                    if keep_forward:
                        self.forward_propagate(keep, line)
                    if keep_backward:
                        self.backward_propagate(keep, line)

    def backward_propagate(self, keep, line):
        in_msg_ids, out_msg_ids, ops_from = self.get_msg_id_from_line(line)
        # Send each msg_id to respective upstream (incoming) ops
        for i in range(len(in_msg_ids)):
            in_msg_id, op_from = in_msg_ids[i], ops_from[i]
            if op_from not in self.producers:
                self.producers[op_from] = self.client.create_producer(
                    f'{self.topic_keep_prefix}-{op_from}',
                    schema=pulsar.schema.BytesSchema())

            # It is okay if the same msg_id is sent multiple times to the upstream op (idempotent)
            msg_keep = self.graph_codec.encode(msg_uuid=uuid.UUID(in_msg_id),
                                               op_from=self.worker_id,
                                               payload=b'backward')
            self.producers[op_from].send(msg_keep)

    def forward_propagate(self, keep, line):
        in_msg_ids, out_msg_ids, ops_to = self.get_msg_id_from_line(line)
        # NOTE: For now, we assume there is only one outgoing/downstream op.
        # Send msg_id to downstream (outgoing) ops
        if not ops_to or len(ops_to) == 0:
            return
        op_to = ops_to[0]
        if op_to not in self.producers:
            self.producers[op_to] = self.client.create_producer(
                f'{self.topic_keep_prefix}-{op_to}',
                schema=pulsar.schema.BytesSchema())
        # It is okay if the same msg_id is sent multiple times to the downstream op (idempotent)
        msg_keep = self.graph_codec.encode(msg_uuid=uuid.UUID(out_msg_ids[0]),
                                           op_from=self.worker_id,
                                           payload=b'forward')
        self.producers[op_to].send(msg_keep)


class WalFilter(LogFilter):
    def __init__(self, pulsar_node, log_file, filter_method, worker_id, topics_out):
        super().__init__(pulsar_node, log_file, filter_method, worker_id, topics_out=topics_out)

    def get_msg_id_from_line(self, line):
        num_columns = len(line.split(','))
        num_inputs = num_columns - 5
        in_msg_ids = []
        for i in range(num_inputs):
            in_msg_ids.append(line.split(',')[i])
        out_msg_id = line.split(',')[num_inputs]
        out_msg_payload = line.split(',')[num_inputs+1]
        return in_msg_ids, [out_msg_id], self.topics_out

    def scan(self):
        # Watch the WAL file for updates. If a new line is added, apply the filter method to the line.
        # If the filter method returns True, write the line to the keep file and propagate the message UUID.
        assert 'wal' in self.log_file
        super().scan()


class OrlFilter(LogFilter):
    def __init__(self, pulsar_node, log_file, filter_method, worker_id, topics_in):
        super().__init__(pulsar_node, log_file, filter_method, worker_id, topics_in=topics_in)

    def get_msg_id_from_line(self, line):
        in_msg_id = line.split(',')[0]
        op_from = line.split(',')[1]
        out_msg_id = line.split(',')[3]
        return [in_msg_id], [out_msg_id], [op_from]

    def get_path_from_msg_id(self, op_from, msg_id):
        nodes = {'stream1': 'swjz-nuc2', 'stream2': 'swjz-nuc2'}
        return 'ftp://' + nodes[op_from] + '/ftp_output/' + msg_id + '.ftp'

    def scan(self):
        # Watch the ORL file for updates. If a new line is added, apply the filter method to the line.
        # If the filter method returns True, write the line to the keep file and propagate the message UUID.
        assert 'orl' in self.log_file
        super().scan()
