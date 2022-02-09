import pulsar
from edgeserve.util import ftp_fetch, local_to_global_path


class Compute:
    def __init__(self, task, pulsar_node, gate_in=None, gate_out=None, ftp=False, ftp_memory=False, ftp_delete=False,
                 local_ftp_path='/srv/ftp/', topic_in='src-topic', topic_out='dst-topic'):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out)
        self.consumer = self.client.subscribe(topic_in, subscription_name='my-sub')
        self.task = task
        self.gate_in = (lambda x: x.decode('utf-8')) if gate_in is None else gate_in
        self.gate_out = (lambda x: x.encode('utf-8')) if gate_out is None else gate_out
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory
        self.ftp_delete = ftp_delete

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        data = self.gate_in(msg.data())  # path to file if ftp, raw data in bytes otherwise

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=self.ftp_delete)
            self.task(local_file_path)  # TODO(swjz): no return value in file mode, might change later
            global_file_path = local_to_global_path(local_file_path, self.local_ftp_path)
            output = self.gate_out(global_file_path)
        else:
            if self.ftp:  # FTP memory mode
                data = ftp_fetch(data, self.local_ftp_path, memory=True, delete=self.ftp_delete)
            # memory mode
            output = self.gate_out(self.task(data))

        self.producer.send(output)
        self.consumer.acknowledge(msg)
        return output
