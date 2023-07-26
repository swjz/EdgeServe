import os
import time
from typing import Callable, Optional, Tuple

import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType
from pulsar.schema import AvroSchema
from inspect import signature

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import MessageFormat


# This version intentionally drops schema support. Data sources cannot be combined adaptively.
class Model:
    """A model that takes in two topics and outputs to two topics.
    For input topics, one is the original data stream and the other is an intermittent signal stream.
    The original data stream is cached locally, waiting to be served.
    Actual model serving is only performed when a request is received from the intermittent signal stream.
    The model may choose to send its prediction to the destination topic, if it satisfies a user-defined SLO;
    otherwise, it sends the prediction to the intermittent signal stream of a more expensive model.
    """

    def __init__(self,
                 task: Callable,
                 pulsar_node: str,
                 topic_in_data: str,
                 topic_out_destination: str,
                 topic_in_signal: Optional[str] = None,
                 topic_out_signal: Optional[str] = None,
                 gate_in: Optional[Callable] = None,
                 gate_out: Optional[Callable] = None,
                 ftp: Optional[bool] = False,
                 ftp_memory: Optional[bool] = False,
                 ftp_delete: Optional[bool] = False,
                 local_ftp_path: Optional[str] = '/srv/ftp/',
                 max_time_diff_ms: Optional[int] = 10 * 1000,
                 no_overlap: Optional[bool] = False,
                 min_interval_ms: Optional[int] = 0,
                 log_path: Optional[str] = None,
                 log_filename: Optional[str] = None) -> None:
        """Initializes a model.

        Args:
            task: The actual task to be performed.
            pulsar_node: Address of Apache Pulsar server.
            topic_in_data: Pulsar topic of the input data stream.
            topic_in_signal: Pulsar topic of the input signal stream. When set to `None`, no signal stream is present.
            topic_out_destination: Pulsar topic of the output destination stream.
            topic_out_signal: Pulsar topic of the output signal stream. When set to `None`, no signal stream is present.
            gate_in: The gating function applied to input data stream (but not the signal stream).
            gate_out: The gating function applied to output prediction stream (to the destination topic).
            ftp: When set to `True`, lazy routing mode is enabled.
            ftp_memory: When set to `True`, in-memory lazy routing mode is enabled. Only effective when `ftp=True`.
            ftp_delete: When set to `True`, delete remote data after fetching is complete. Only effective when `ftp=True`.
            local_ftp_path: The local FTP path served by an active FTP server. Other nodes fetch data from this path.
            max_time_diff_ms: The maximum timestamp difference we tolerate between data sources to aggregate together.
            no_overlap: When set to `True`, we ensure that every message is at most processed once.
            min_interval_ms: The minimum time interval between two consecutive runs.
            log_path: Path to store the replay log. When set to `None`, log is disabled.
            log_filename: File name of replay log. When set to `None`, the current timestamp is used as file name.
        """
        self.client = pulsar.Client(pulsar_node)
        self.producer_destination = self.client.create_producer(topic_out_destination,
                                                                schema=pulsar.schema.AvroSchema(MessageFormat))
        self.topic_out_signal = topic_out_signal
        if topic_out_signal:
            self.producer_signal = self.client.create_producer(topic_out_signal,
                                                               schema=pulsar.schema.AvroSchema(MessageFormat))
        self.consumer = self.client.subscribe([topic_in_data, topic_in_signal],
                                              subscription_name='compute-sub',
                                              consumer_type=ConsumerType.Shared,
                                              schema=pulsar.schema.AvroSchema(MessageFormat),
                                              initial_position=InitialPosition.Earliest) if topic_in_signal else \
            self.client.subscribe(
                topic_in_data, subscription_name='compute-sub', consumer_type=ConsumerType.Shared,
                schema=pulsar.schema.AvroSchema(MessageFormat), initial_position=InitialPosition.Earliest)

        self.task = task
        self.topic_in_data = topic_in_data
        self.topic_in_signal = topic_in_signal
        self.gate_in = (lambda x: x) if gate_in is None else gate_in
        self.gate_out = (lambda x: x) if gate_out is None else gate_out
        self.ftp = ftp  # consider changing this name to ftp_in
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory  # consider changing this name to (negate) ftp_out
        self.ftp_delete = ftp_delete
        self.cached = dict()
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

        self.cached_data = dict()
        self.cached_signals = dict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self, flow_id) -> Tuple[bool, Optional[bool], Optional[bytes]]:
        """Performs actual computation.

        Args:
            flow_id: ID of the data flow to be processed.

        Returns:
            is_performed: Whether the job is actually performed.
            is_satisfied: Whether the prediction is considered satisfied. If set to `True`, the result is sent to the
                          destination topic; if set to `False`, the result is sent to a more expensive model.
            output: The prediction result in bytes.
        """
        # When `topic_in_signal` is not `None`, we only do actual computation when both data stream and signal stream
        # from the same `flow_id` are present.
        if self.topic_in_signal and (flow_id not in self.cached_data or flow_id not in self.cached_signals):
            return False, None, None

        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None, None

        self.last_run_start_ms = time.time() * 1000

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp:
            local_file_path = ftp_fetch(self.cached_data[flow_id], self.local_ftp_path, memory=self.ftp_memory,
                                        delete=self.ftp_delete)
            self.cached_data[flow_id] = local_file_path

        if self.topic_in_signal:
            is_satisfied, output = self.task(self.cached_data[flow_id], self.cached_signals[flow_id])
        else:
            is_satisfied, output = self.task(self.cached_data[flow_id])
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
                          'data_payload': self.cached_data[flow_id],
                          'signal_payload': self.cached_signals[flow_id] if self.topic_in_signal else None}
            with open(os.path.join(self.log_path, self.log_filename + '.log'), 'ab') as f:
                pickle.dump(replay_log, f)
            self.last_log_duration_ms = time.time() * 1000 - last_run_finish_ms

        # If no_overlap, reset latest_msg and latest_msg_time_ms so a message won't be processed twice.
        if self.no_overlap:
            self.latest_msg_id = dict()
            self.latest_msg_publish_time_ms = dict()
            self.latest_msg_consumed_time_ms = dict()
            del self.cached_data[flow_id]
            if self.topic_in_signal:
                del self.cached_signals[flow_id]

        return True, is_satisfied, output

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.consumer.receive()
        msg_id = msg.message_id()
        value = msg.value()
        flow_id = value.source_id
        actual_topic_in = msg.topic_name().split('/')[-1]

        if actual_topic_in == self.topic_in_signal:
            # We locally cache the prediction result ("signal") from another model.
            self.cached_signals[flow_id] = value.payload
        elif actual_topic_in == self.topic_in_data:
            self.cached_data[flow_id] = self.gate_in(value.payload)
        else:
            raise ValueError(
                "The consumer's topic name does not match that of incoming message. The topic of incoming message is",
                actual_topic_in)

        if flow_id not in self.latest_msg_publish_time_ms:
            self.latest_msg_publish_time_ms[flow_id] = [msg.publish_timestamp()]
            self.latest_msg_id[flow_id] = [msg_id.serialize()]
        else:
            self.latest_msg_publish_time_ms[flow_id].append(msg.publish_timestamp())
            self.latest_msg_id[flow_id].append(msg_id.serialize())

        self.latest_msg_consumed_time_ms[flow_id] = time.time() * 1000

        if self.ftp and not self.ftp_memory:  # FTP file mode
            # download the file from FTP server and then delete the file from server
            if not self.cached_data[flow_id].startswith('ftp://'):
                raise ValueError("FTP mode is enabled but incoming message does not have FTP format")
            is_performed, is_satisfied, output = self._try_task(flow_id)
            if is_performed:
                global_file_path = local_to_global_path(output, self.local_ftp_path)
                output = self.gate_out(global_file_path)
        else:
            # memory mode
            is_performed, is_satisfied, output = self._try_task(flow_id)
            if is_performed:
                output = self.gate_out(output)

        if output:
            if self.log_path and os.path.isdir(self.log_path):
                with open(os.path.join(self.log_path, self.log_filename + '.output'), 'ab') as f:
                    pickle.dump(output, f)
            if is_satisfied:
                self.producer_destination.send(MessageFormat(source_id=flow_id, payload=output))
            elif self.topic_out_signal:
                self.producer_signal.send(MessageFormat(source_id=flow_id, payload=output))
            else:
                raise ValueError("The result is not satisfactory but output signal topic is not present.")
            self.consumer.acknowledge(msg)
            return output
        else:
            # No output is given, no need to materialize
            self.consumer.acknowledge(msg)
            return None
