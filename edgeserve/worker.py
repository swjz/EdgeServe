import sys
import pulsar
from edgeserve.util import ftp_fetch
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr


class Worker:
    def __init__(self, pulsar_node, topic='code-topic', ftp=False, ftp_memory=True, local_ftp_path='/srv/ftp/'):
        self.client = pulsar.Client(pulsar_node)
        self.consumer = self.client.subscribe(topic, subscription_name='my-sub')
        self.gate = lambda x: x.decode('utf-8')
        self.ftp = ftp
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        data = self.gate(msg.data())

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            local_file_path = ftp_fetch(data, self.local_ftp_path, memory=False, delete=False)
            data = open(local_file_path).read()
        else:
            if self.ftp:  # FTP memory mode
                data = ftp_fetch(data, self.local_ftp_path, memory=True, delete=False)

        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exec(data)

        print(stdout.getvalue(), flush=True)
        print(stderr.getvalue(), file=sys.stderr, flush=True)

        self.consumer.acknowledge(msg)


if __name__ == "__main__":
    # Example: python worker.py pulsar://localhost:6650 code-ftp true false /srv/ftp/
    node = sys.argv[1]
    topic = sys.argv[2] if len(sys.argv) >= 3 else 'code-topic'
    ftp = sys.argv[3] in ['true', 'True'] if len(sys.argv) >= 4 else False
    ftp_memory = sys.argv[4] in ['true', 'True'] if len(sys.argv) >= 5 else True
    local_ftp_path = sys.argv[5] if len(sys.argv) >= 6 else '/srv/ftp/'
    with Worker(node, topic, ftp, ftp_memory, local_ftp_path) as worker:
        while True:
            next(worker)
