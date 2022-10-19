import os
import pulsar
from _pulsar import InitialPosition
from pulsar.schema import AvroSchema
from inspect import signature

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import MessageFormat


class Compute:
    def __init__(self, task, pulsar_node, gate_in=None, gate_out=None, ftp=False, ftp_memory=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic_in='src', topic_out='dst', max_time_diff_ms=10 * 1000, no_overlap=False):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out, schema=AvroSchema(MessageFormat))
        self.consumer = self.client.subscribe(topic_in, subscription_name='compute-sub',
                                              schema=AvroSchema(MessageFormat),
                                              initial_position=InitialPosition.Earliest)
        self.task = task
        self.gate_in = (lambda x: x.decode('utf-8')) if gate_in is None else gate_in
        self.gate_out = (lambda x: x.encode('utf-8')) if gate_out is None else gate_out
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory
        self.ftp_delete = ftp_delete
        self.latest_msg = dict()
        self.latest_msg_time_ms = dict()
        self.max_time_diff_ms = max_time_diff_ms
        self.no_overlap = no_overlap

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self):
        if len(self.latest_msg) < len(signature(self.task).parameters):
            return False, None

        earliest = None
        latest = None
        for source_id in self.latest_msg.keys():
            if earliest is None or self.latest_msg_time_ms[source_id] < earliest:
                earliest = self.latest_msg_time_ms[source_id]
            if latest is None or self.latest_msg_time_ms[source_id] > latest:
                latest = self.latest_msg_time_ms[source_id]
            if latest - earliest > self.max_time_diff_ms:
                return False, None
        output = self.task(**self.latest_msg)

        # If no_overlap, reset latest_msg and latest_msg_time_ms so a message won't be processed twice.
        if self.no_overlap:
            self.latest_msg = dict()
            self.latest_msg_time_ms = dict()

        return True, output

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        value = msg.value()
        source_id = value.source_id
        data = self.gate_in(value.payload)  # path to file if ftp, raw data in bytes otherwise
        if data is not None:
            self.latest_msg_time_ms[source_id] = msg.publish_timestamp()
            self.latest_msg[source_id] = data

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            if not data.startswith('ftp://'):
                return None
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=self.ftp_delete)
            self.latest_msg[source_id] = local_file_path
            ret, output = self._try_task()
            if ret:
                with open(local_file_path + '.output', 'w') as f:
                    f.write(output)
                global_file_path = local_to_global_path(local_file_path + '.output', self.local_ftp_path)
                output = self.gate_out(global_file_path)
            else:
                return None
        else:
            if self.ftp:  # FTP memory mode
                self.latest_msg[source_id] = ftp_fetch(data, self.local_ftp_path,
                                                        memory=True, delete=self.ftp_delete)
            # memory mode
            ret, output = self._try_task()
            if ret:
                output = self.gate_out(output)
            else:
                return None

        output = MessageFormat(source_id=source_id, payload=output)
        self.producer.send(output)
        self.consumer.acknowledge(msg)
        return output.payload
