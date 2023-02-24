import os
import time
import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType
from pulsar.schema import AvroSchema
from inspect import signature

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import MessageFormat


class Compute:
    def __init__(self, task, pulsar_node, worker_id='worker1', gate_in=None, gate_out=None, ftp=False, ftp_memory=False,
                 ftp_delete=False, local_ftp_path='/srv/ftp/', topic_in='src', topic_out='dst',
                 max_time_diff_ms=10 * 1000, no_overlap=False, min_interval_ms=0, log_path=None, log_filename=None):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out, schema=AvroSchema(MessageFormat))
        self.consumer = self.client.subscribe(topic_in, subscription_name='compute-sub',
                                              consumer_type=ConsumerType.Shared,
                                              schema=AvroSchema(MessageFormat),
                                              initial_position=InitialPosition.Earliest)
        self.task = task
        self.worker_id = worker_id
        self.gate_in = (lambda x: x.decode('utf-8')) if gate_in is None else gate_in
        self.gate_out = (lambda x: x.encode('utf-8')) if gate_out is None else gate_out
        self.ftp = ftp  # consider changing this name to ftp_in
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory  # consider changing this name to (negate) ftp_out
        self.ftp_delete = ftp_delete
        self.latest_msg = dict()
        self.latest_msg_id = dict()
        self.latest_msg_publish_time_ms = dict()
        self.latest_msg_consumed_time_ms = dict()
        self.max_time_diff_ms = max_time_diff_ms
        self.no_overlap = no_overlap
        self.min_interval_ms = min_interval_ms  # prediction frequency
        self.last_run_start_ms = 0
        self.log_path = log_path
        self.log_filename = str(time.time() * 1000) if log_filename is None else log_filename
        self.last_log_duration_ms = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self):
        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None

        if len(self.latest_msg) < len(signature(self.task).parameters):
            return False, None

        earliest = None
        latest = None
        for source_id in self.latest_msg.keys():
            if earliest is None or self.latest_msg_publish_time_ms[source_id] < earliest:
                earliest = self.latest_msg_publish_time_ms[source_id]
            if latest is None or self.latest_msg_publish_time_ms[source_id] > latest:
                latest = self.latest_msg_publish_time_ms[source_id]
            if latest - earliest > self.max_time_diff_ms:
                return False, None
        self.last_run_start_ms = time.time() * 1000

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp:
            for source_id in self.latest_msg.keys():
                local_file_path = ftp_fetch(self.latest_msg[source_id], self.local_ftp_path, memory=self.ftp_memory, delete=self.ftp_delete)
                self.latest_msg[source_id] = local_file_path

        output = self.task(**self.latest_msg)
        last_run_finish_ms = time.time() * 1000

        # For now, use the completion timestamp as the filename of output FTP file
        if self.ftp and not self.ftp_memory:
            ftp_output_dir = os.path.join(os.path.dirname(local_file_path), 'ftp_output')
            pathlib.Path(ftp_output_dir).mkdir(exist_ok=True)
            with open(os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp', 'w') as f:
                f.write(output)
            output = os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp'

        # If log_path is not None, we write aggregation decisions to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            replay_log = {'msg_id': self.latest_msg_id,
                          'msg_publish_time_ms': self.latest_msg_publish_time_ms,
                          'msg_consumed_time_ms': self.latest_msg_consumed_time_ms,
                          'start_compute_time_ms': self.last_run_start_ms,
                          'finish_compute_time_ms': last_run_finish_ms,
                          'last_log_duration_ms': self.last_log_duration_ms,
                          'msg_payload': self.latest_msg}
            with open(os.path.join(self.log_path, self.log_filename + '.log'), 'ab') as f:
                pickle.dump(replay_log, f)
            self.last_log_duration_ms = time.time() * 1000 - last_run_finish_ms

        # If no_overlap, reset latest_msg and latest_msg_time_ms so a message won't be processed twice.
        if self.no_overlap:
            self.latest_msg = dict()
            self.latest_msg_id = dict()
            self.latest_msg_publish_time_ms = dict()
            self.latest_msg_consumed_time_ms = dict()

        return True, output

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        msg_id = msg.message_id()
        value = msg.value()
        source_id = value.source_id
        data = self.gate_in(value.payload)  # path to file if ftp, raw data in bytes otherwise
        if data is not None:
            self.latest_msg_publish_time_ms[source_id] = msg.publish_timestamp()
            self.latest_msg_consumed_time_ms[source_id] = time.time() * 1000
            self.latest_msg[source_id] = data
            self.latest_msg_id[source_id] = msg_id.serialize()

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            if not data.startswith('ftp://'):
                return None
            self.latest_msg[source_id] = data
            ret, output = self._try_task()
            if ret:
                global_file_path = local_to_global_path(output, self.local_ftp_path)
                output = self.gate_out(global_file_path)
        else:
            if self.ftp:  # FTP memory mode
                self.latest_msg[source_id] = data
            # memory mode
            ret, output = self._try_task()
            if ret:
                output = self.gate_out(output)

        if output:
            if self.log_path and os.path.isdir(self.log_path):
                with open(os.path.join(self.log_path, self.log_filename + '.output'), 'ab') as f:
                    pickle.dump(output, f)
            output = MessageFormat(source_id=self.worker_id, payload=output)
            self.producer.send(output)
            self.consumer.acknowledge(msg)
            return output.payload
        else:
            # No output is given, no need to materialize
            self.consumer.acknowledge(msg)
            return None
