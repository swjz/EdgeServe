import os
import pickle
import time
import pulsar
from _pulsar import InitialPosition

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import GraphCodec


class Materialize:
    def __init__(self, materialize, pulsar_node, gate=None, ftp_in=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic='dst', log_path=None, log_filename=None):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(topic, subscription_name='my-sub',
                                              schema=pulsar.schema.BytesSchema(),
                                              initial_position=InitialPosition.Earliest)
        self.materialize = materialize
        self.gate = (lambda x: x) if gate is None else gate
        self.ftp_in = ftp_in
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
        output = data

        if self.ftp_in:
            # download the file from FTP server and then delete the file from server (if set)
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=self.ftp_delete)
            self.materialize(local_file_path)
            global_file_path = local_to_global_path(local_file_path, self.local_ftp_path)
            output = global_file_path

        task_finish_time_ms = time.time() * 1000

        # If log_path is not None, we write timestamps to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            log_file = os.path.join(self.log_path, self.log_filename + '.destination')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    f.write('msg_uuid,op_from,payload,task_finish_time_ms\n')
            with open(log_file, 'a') as f:
                f.write(str(msg_uuid) + ',' + op_from + ',' + str(output) + ',' + str(task_finish_time_ms) + '\n')

        self.consumer.acknowledge(msg)
        return output
