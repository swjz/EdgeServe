import os
import time
import uuid

import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType
from inspect import signature

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import GraphCodec


class Compute:
    def __init__(self, task, pulsar_node, worker_id='worker1', gate_in=None, gate_out=None, ftp_in=False, ftp_out=False,
                 ftp_delete=False, local_ftp_path='/srv/ftp/', topic_in='src', topic_out='dst',
                 max_time_diff_ms=10 * 1000, no_overlap=False, min_interval_ms=0, log_path=None, log_filename=None,
                 drop_if_older_than_ms=None, log_verbose=False):
        """Initializes a compute operator.

        Args:
            task: The actual task to be performed.
            pulsar_node: Address of Apache Pulsar server.
            worker_id: The unique identifier of this operator.
            gate_in: The gating function applied to input stream.
            gate_out: The gating function applied to output stream.
            ftp_in: When set to `True`, lazy routing mode is enabled for the input stream.
            ftp_out: When set to `True`, lazy routing mode is enabled for the output stream.
            ftp_delete: When set to `True`, delete remote data after fetching is complete. Only effective when `ftp=True`.
            local_ftp_path: The local FTP path served by an active FTP server. Other nodes fetch data from this path.
            topic_in: Pulsar topic of the input data stream.
            topic_out: Pulsar topic of the output data stream.
            max_time_diff_ms: The maximum timestamp difference we tolerate between data sources to aggregate together.
            no_overlap: When set to `True`, we ensure that every message is at most processed once.
            min_interval_ms: The minimum time interval between two consecutive runs.
            log_path: Path to store the replay log. When set to `None`, log is disabled.
            log_filename: File name of replay log. When set to `None`, the current timestamp is used as file name.
            drop_if_older_than_ms: When set to a value, messages older than this value relative to current time are dropped.
            log_verbose: When set to `False`, we only log in the replay log when a join operation is performed.
        """
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic_out, schema=pulsar.schema.BytesSchema())
        self.consumer = self.client.subscribe(topic_in, subscription_name='compute-sub',
                                              consumer_type=ConsumerType.Shared,
                                              schema=pulsar.schema.BytesSchema(),
                                              initial_position=InitialPosition.Earliest)
        self.task = task
        self.worker_id = worker_id
        self.gate_in = (lambda x: x) if gate_in is None else gate_in
        self.gate_out = (lambda x: x) if gate_out is None else gate_out
        self.ftp_in = ftp_in  # consider changing this name to ftp_in
        self.ftp_out = ftp_out
        self.ftp_delete = ftp_delete
        self.local_ftp_path = local_ftp_path
        self.latest_msg = dict()
        self.latest_msg_in_uuid = dict()
        self.latest_msg_publish_time_ms = dict()
        self.latest_msg_consumed_time_ms = dict()
        self.max_time_diff_ms = max_time_diff_ms
        self.no_overlap = no_overlap
        self.min_interval_ms = min_interval_ms  # prediction frequency
        self.last_run_start_ms = 0
        self.last_run_finish_ms = 0
        self.log_path = log_path
        self.log_filename = worker_id if log_filename is None else log_filename
        self.drop_if_older_than_ms = drop_if_older_than_ms
        self.log_verbose = log_verbose
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self):
        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None, None

        if len(self.latest_msg) < len(signature(self.task).parameters):
            return False, None, None

        earliest = None
        latest = None
        for source_id in self.latest_msg.keys():
            if earliest is None or self.latest_msg_publish_time_ms[source_id] < earliest:
                earliest = self.latest_msg_publish_time_ms[source_id]
            if latest is None or self.latest_msg_publish_time_ms[source_id] > latest:
                latest = self.latest_msg_publish_time_ms[source_id]
            if latest - earliest > self.max_time_diff_ms:
                return False, None, None
        self.last_run_start_ms = time.time() * 1000

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp_in:
            for source_id in self.latest_msg.keys():
                if 'ftp://' in self.latest_msg[source_id]:
                    local_file_path = ftp_fetch(self.latest_msg[source_id], self.local_ftp_path, memory=not self.ftp_out, delete=self.ftp_delete)
                    with open(local_file_path, 'rb') as f:
                        self.latest_msg[source_id] = pickle.load(f)

        output = self.task(**self.latest_msg)
        self.last_run_finish_ms = time.time() * 1000
        msg_out_uuid = uuid.uuid4()

        # Write output to disk for lazy data routing and logging purposes.
        if output and (self.ftp_out or self.log_path):
            ftp_output_dir = os.path.join(self.local_ftp_path, 'ftp_output')
            pathlib.Path(ftp_output_dir).mkdir(exist_ok=True)
            with open(os.path.join(ftp_output_dir, str(msg_out_uuid) + '.ftp'), 'wb') as f:
                pickle.dump(output, f)
            self.output_path = os.path.join(ftp_output_dir, str(msg_out_uuid) + '.ftp')
            output = self.output_path if self.ftp_out else output

        # If no_overlap, reset latest_msg and latest_msg_time_ms so a message won't be processed twice.
        if self.no_overlap:
            self.latest_msg = dict()
            self.latest_msg_in_uuid = dict()
            self.latest_msg_publish_time_ms = dict()
            self.latest_msg_consumed_time_ms = dict()

        return True, msg_out_uuid, output

    def __iter__(self):
        return self

    def __next__(self):
        msg_in = self.consumer.receive()

        if self.drop_if_older_than_ms is not None:
            assert isinstance(self.drop_if_older_than_ms, int)
            if msg_in.publish_timestamp() + self.drop_if_older_than_ms < time.time() * 1000:
                # incoming message is too old, skip it.
                self.consumer.acknowledge(msg_in)
                return None

        msg_in_uuid, op_from, _, payload = self.graph_codec.decode(msg_in.value())

        data = self.gate_in(payload)  # path to file if ftp, raw data in bytes otherwise
        if data is not None:
            self.latest_msg_publish_time_ms[op_from] = msg_in.publish_timestamp()
            self.latest_msg_consumed_time_ms[op_from] = time.time() * 1000
            self.latest_msg[op_from] = data
            self.latest_msg_in_uuid[op_from] = msg_in_uuid

        if self.ftp_in:
            # download the file from FTP server and then delete the file from server
            if not data.startswith('ftp://'):
                return None
        ret, msg_out_uuid, output = self._try_task()

        if ret and output:
            if self.ftp_out:
                output = local_to_global_path(output, self.local_ftp_path)
            output = self.gate_out(output)

        if output:
            if type(output) == str:
                output = output.encode('utf-8')
            msg_out = self.graph_codec.encode(msg_uuid=msg_out_uuid, op_from=self.worker_id, payload=output)
            self.producer.send(msg_out)

        if self.log_path and (ret or self.log_verbose):
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_file = os.path.join(self.log_path, self.log_filename + '.compute')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    for k in sorted(signature(self.task).parameters.keys()):
                        f.write(k + ',')
                    f.write('msg_out_uuid,msg_out_payload,start_compute_time_ms,finish_compute_time_ms,worker_id,'
                            'is_join_performed\n')

            with open(log_file, 'a') as f:
                for k in sorted(signature(self.task).parameters.keys()):
                    if k in self.latest_msg_in_uuid:
                        f.write(str(self.latest_msg_in_uuid[k]) + ',')
                    else:
                        f.write('None,')
                if output:
                    f.write(str(msg_out_uuid) + ',' + str(self.output_path) + ',' + str(self.last_run_start_ms) + ',' +
                            str(self.last_run_finish_ms) + ',' + str(self.worker_id) + ',' + str(ret) + '\n')
                else:
                    f.write(str(msg_out_uuid) + ',None,' + str(self.last_run_start_ms) + ',' +
                            str(self.last_run_finish_ms) + ',' + str(self.worker_id) + ',' + str(ret) + '\n')

        self.consumer.acknowledge(msg_in)
        return output if output else None
