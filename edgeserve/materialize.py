import os
import pickle
import time
import pulsar
from _pulsar import InitialPosition

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import GraphCodec


class Materialize:
    def __init__(self, materialize, pulsar_node, gate=None, ftp=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic='dst', log_path=None, log_filename=None):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(topic, subscription_name='my-sub',
                                              schema=pulsar.schema.BytesSchema(),
                                              initial_position=InitialPosition.Earliest)
        self.materialize = materialize
        self.gate = (lambda x: x) if gate is None else gate
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_delete = ftp_delete
        self.log_path = log_path
        self.log_filename = str(time.time() * 1000) if log_filename is None else log_filename
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        msg_uuid, op_from, _, payload = self.graph_codec.decode(msg.value())
        data = self.gate(payload)

        if self.ftp:
            # download the file from FTP server and then delete the file from server
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=self.ftp_delete)
            self.materialize(local_file_path)
            global_file_path = local_to_global_path(local_file_path, self.local_ftp_path)
            self.consumer.acknowledge(msg)
            return global_file_path

        data_collection_time_ms = time.time() * 1000
        output = self.materialize(data)
        task_finish_time_ms = time.time() * 1000

        # If log_path is not None, we write timestamps to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            replay_log = {'msg_uuid': msg_uuid,
                          'op_from': op_from,
                          'payload': payload,
                          'data_collection_time_ms': data_collection_time_ms,
                          'task_finish_time_ms': task_finish_time_ms}
            with open(os.path.join(self.log_path, self.log_filename + '.destination'), 'ab') as f:
                pickle.dump(replay_log, f)

        self.consumer.acknowledge(msg)
        return output
