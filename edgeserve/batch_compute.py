import os
import time
import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType
from inspect import signature

from edgeserve.util import ftp_fetch, local_to_global_path


# This version intentionally drops schema support. Data sources cannot be combined adaptively.
class BatchCompute:
    def __init__(self, task, pulsar_node, worker_id='worker1', gate_in=None, gate_out=None, ftp=False, ftp_memory=False,
                 ftp_delete=False, local_ftp_path='/srv/ftp/', topic_in='src', topic_out='dst',
                 max_time_diff_ms=10 * 1000, no_overlap=False, min_interval_ms=0, log_path=None, log_filename=None,
                 batch_max_num_msg=100, batch_max_bytes=10 * 1024 * 1024, batch_timeout_ms=10, acknowledge=False):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out, schema=pulsar.schema.BytesSchema())
        self.consumer = self.client.subscribe(topic_in, subscription_name='compute-sub',
                                              consumer_type=ConsumerType.Shared,
                                              schema=pulsar.schema.BytesSchema(),
                                              initial_position=InitialPosition.Earliest,
                                              batch_receive_policy=pulsar.ConsumerBatchReceivePolicy(batch_max_num_msg,
                                                                                                     batch_max_bytes,
                                                                                                     batch_timeout_ms))
        self.task = task
        self.worker_id = worker_id
        self.gate_in = (lambda x: x) if gate_in is None else gate_in
        self.gate_out = (lambda x: x) if gate_out is None else gate_out
        self.ftp = ftp  # consider changing this name to ftp_in
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory  # consider changing this name to (negate) ftp_out
        self.ftp_delete = ftp_delete
        self.max_time_diff_ms = max_time_diff_ms
        self.no_overlap = no_overlap
        self.min_interval_ms = min_interval_ms  # prediction frequency
        self.last_run_start_ms = 0
        self.log_path = log_path
        self.log_filename = str(time.time() * 1000) if log_filename is None else log_filename
        self.last_log_duration_ms = -1
        self.acknowledge = acknowledge

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self, msgs):
        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None

        self.last_run_start_ms = time.time() * 1000

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp:
            for i, msg in enumerate(msgs):
                local_file_path = ftp_fetch(msg, self.local_ftp_path, memory=self.ftp_memory, delete=self.ftp_delete)
                msgs[i] = local_file_path

        output = self.task(msgs)
        last_run_finish_ms = time.time() * 1000

        # Filename: for now, use the completion timestamp as the filename of output FTP file
        # Batching: for now, we batch the output of messages received in batch in case of lazy data routing.
        if self.ftp and not self.ftp_memory:
            ftp_output_dir = os.path.join(os.path.dirname(local_file_path), 'ftp_output')
            pathlib.Path(ftp_output_dir).mkdir(exist_ok=True)
            with open(os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp', 'w') as f:
                f.write(output)
            output = os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp'

        return True, output

    def __iter__(self):
        return self

    def __next__(self):
        messages = self.consumer.batch_receive()
        msgs = [self.gate_in(msg.value()) for msg in messages]

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            for msg in msgs:
                if not msg.startswith('ftp://'):
                    return None
            ret, output = self._try_task(msgs)
            if ret:
                global_file_path = local_to_global_path(output, self.local_ftp_path)
                output = self.gate_out(global_file_path)
        else:
            # memory mode
            ret, output = self._try_task(msgs)
            if ret:
                output = self.gate_out(output)

        if output:
            if self.log_path and os.path.isdir(self.log_path):
                with open(os.path.join(self.log_path, self.log_filename + '.output'), 'ab') as f:
                    pickle.dump(output, f)
            self.producer.send(output)

        if self.acknowledge:
            for message in messages:
                self.consumer.acknowledge(message)

        return output if output else None
