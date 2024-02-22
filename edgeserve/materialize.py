import pulsar
from _pulsar import InitialPosition

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import GraphCodec


class Materialize:
    def __init__(self, materialize, pulsar_node, gate=None, ftp=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic='dst'):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(topic, subscription_name='my-sub',
                                              schema=pulsar.schema.BytesSchema(),
                                              initial_position=InitialPosition.Earliest)
        self.materialize = materialize
        self.gate = (lambda x: x) if gate is None else gate
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_delete = ftp_delete
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

        output = self.materialize(data)
        self.consumer.acknowledge(msg)
        return output
