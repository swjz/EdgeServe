import os
import pulsar
from _pulsar import InitialPosition
from edgeserve.util import ftp_fetch, local_to_global_path


class Compute:
    def __init__(self, task, pulsar_node, gate_in=None, gate_out=None, ftp=False, ftp_memory=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic_in='src', topic_out='dst', max_time_diff_ms=10 * 1000):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out)
        self.consumer = self.client.subscribe(topic_in, subscription_name='compute-sub',
                                              initial_position=InitialPosition.Earliest)
        self.task = task
        self.gate_in = (lambda x: x.decode('utf-8')) if gate_in is None else gate_in
        self.gate_out = (lambda x: x.encode('utf-8')) if gate_out is None else gate_out
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory
        self.ftp_delete = ftp_delete
        self.topic_in = topic_in if isinstance(topic_in, list) else [topic_in]
        self.latest_msg = {topic: None for topic in self.topic_in}
        self.latest_msg_time_ms = {topic: -1 for topic in self.topic_in}
        self.max_time_diff_ms = max_time_diff_ms

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self):
        earliest = None
        latest = None
        for topic in self.latest_msg.keys():
            if self.latest_msg[topic] is None:
                return False, None
            if earliest is None or self.latest_msg_time_ms[topic] < earliest:
                earliest = self.latest_msg_time_ms[topic]
            if latest is None or self.latest_msg_time_ms[topic] > latest:
                latest = self.latest_msg_time_ms[topic]
            if latest - earliest > self.max_time_diff_ms:
                return False, None
        output = self.task(**self.latest_msg)
        return True, output

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        data = self.gate_in(msg.data())  # path to file if ftp, raw data in bytes otherwise
        this_topic = os.path.basename(msg.topic_name())
        self.latest_msg_time_ms[this_topic] = msg.publish_timestamp()
        self.latest_msg[this_topic] = data

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=self.ftp_delete)
            self.latest_msg[this_topic] = local_file_path
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
                self.latest_msg[this_topic] = ftp_fetch(data, self.local_ftp_path,
                                                        memory=True, delete=self.ftp_delete)
            # memory mode
            ret, output = self._try_task()
            if ret:
                output = self.gate_out(output)
            else:
                return None

        self.producer.send(output)
        self.consumer.acknowledge_cumulative(msg)
        return output
