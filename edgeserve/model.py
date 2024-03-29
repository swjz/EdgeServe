import os
import time
import uuid
from typing import Callable, Optional, Tuple

import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import GraphCodec


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
                 gate_in_data: Optional[Callable] = None,
                 gate_out_destination: Optional[Callable] = None,
                 gate_in_signal: Optional[Callable] = None,
                 gate_out_signal: Optional[Callable] = None,
                 model_id: Optional[str] = 'model1',
                 header_size: Optional[int] = None,
                 ftp: Optional[bool] = False,
                 ftp_memory: Optional[bool] = False,
                 ftp_delete: Optional[bool] = False,
                 local_ftp_path: Optional[str] = '/srv/ftp/',
                 max_time_diff_ms: Optional[int] = 10 * 1000,
                 no_overlap: Optional[bool] = False,
                 min_interval_ms: Optional[int] = 0,
                 log_path: Optional[str] = None,
                 log_filename: Optional[str] = None,
                 acknowledge: Optional[bool] = False,
                 receiver_queue_size: Optional[int] = 10000) -> None:
        """Initializes a model.

        Args:
            task: The actual task to be performed.
            pulsar_node: Address of Apache Pulsar server.
            topic_in_data: Pulsar topic of the input data stream.
            topic_in_signal: Pulsar topic of the input signal stream. When set to `None`, no signal stream is present.
            topic_out_destination: Pulsar topic of the output destination stream.
            topic_out_signal: Pulsar topic of the output signal stream. When set to `None`, no signal stream is present.
            gate_in_data: The gating function applied to input data stream (but not the signal stream).
            gate_out_destination: The gating function applied to output prediction stream (to the destination topic).
            gate_in_signal: The gating function applied to input signal stream.
            gate_out_signal: The gating function applied to output signal stream.
            model_id: The unique identifier of the model.
            ftp: When set to `True`, lazy routing mode is enabled.
            ftp_memory: When set to `True`, in-memory lazy routing mode is enabled. Only effective when `ftp=True`.
            ftp_delete: When set to `True`, delete remote data after fetching is complete. Only effective when `ftp=True`.
            local_ftp_path: The local FTP path served by an active FTP server. Other nodes fetch data from this path.
            max_time_diff_ms: The maximum timestamp difference we tolerate between data sources to aggregate together.
            no_overlap: When set to `True`, we ensure that every message is at most processed once.
            min_interval_ms: The minimum time interval between two consecutive runs.
            log_path: Path to store the replay log. When set to `None`, log is disabled.
            log_filename: File name of replay log. When set to `None`, the current timestamp is used as file name.
            acknowledge: When set to `True`, the model acknowledges every message it receives.
            receiver_queue_size: The size of the receiver queue for Pulsar.
        """
        self.client = pulsar.Client(pulsar_node)
        self.producer_destination = self.client.create_producer(topic_out_destination,
                                                                schema=pulsar.schema.BytesSchema())
        self.topic_out_signal = topic_out_signal
        if topic_out_signal:
            self.producer_signal = self.client.create_producer(topic_out_signal,
                                                               schema=pulsar.schema.BytesSchema())
        self.task = task
        self.topic_in_data = topic_in_data
        self.topic_in_signal = topic_in_signal
        self.consumer = self._subscribe()
        self.gate_in_data = (lambda x: x) if gate_in_data is None else gate_in_data
        self.gate_out_destination = (lambda x: x) if gate_out_destination is None else gate_out_destination
        self.gate_in_signal = (lambda x: x) if gate_in_signal is None else gate_in_signal
        self.gate_out_signal = (lambda x: x) if gate_out_signal is None else gate_out_signal
        self.model_id = model_id
        self.ftp = ftp  # consider changing this name to ftp_in
        self.local_ftp_path = local_ftp_path
        self.ftp_memory = ftp_memory  # consider changing this name to (negate) ftp_out
        self.ftp_delete = ftp_delete
        self.cached = dict()
        self.latest_msg_uuid = dict()
        self.latest_msg_publish_time_ms = dict()
        self.latest_msg_consumed_time_ms = dict()
        self.max_time_diff_ms = max_time_diff_ms
        self.no_overlap = no_overlap
        self.min_interval_ms = min_interval_ms  # prediction frequency
        self.last_run_start_ms = 0
        self.log_path = log_path
        self.log_filename = str(time.time() * 1000) if log_filename is None else log_filename
        self.last_log_duration_ms = -1
        self.acknowledge = acknowledge
        self.receiver_queue_size = receiver_queue_size

        self.header_size = header_size if header_size else 32
        self.graph_codec = GraphCodec(header_size=self.header_size)
        self.cached_data = dict()
        self.cached_signals = dict()

    def _subscribe(self):
        if self.topic_in_signal:
            return self.client.subscribe([self.topic_in_data, self.topic_in_signal],
                                         subscription_name='compute-sub',
                                         consumer_type=ConsumerType.Shared,
                                         schema=pulsar.schema.BytesSchema(),
                                         initial_position=InitialPosition.Earliest,
                                         receiver_queue_size=self.receiver_queue_size)

        return self.client.subscribe(self.topic_in_data, subscription_name='compute-sub',
                                     consumer_type=ConsumerType.Shared, schema=pulsar.schema.BytesSchema(),
                                     initial_position=InitialPosition.Earliest,
                                     receiver_queue_size=self.receiver_queue_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_task(self, flow_id) -> Tuple[bool, Optional[bool], Optional[uuid.UUID], Optional[bytes]]:
        """Performs actual computation.

        Args:
            flow_id: ID of the data flow to be processed.

        Returns:
            is_performed: Whether the job is actually performed.
            is_satisfied: Whether the prediction is considered satisfied. If set to `True`, the result is sent to the
                          destination topic; if set to `False`, the result is sent to a more expensive model.
            msg_out_uuid: The UUID of the output message. If set to `None`, no message is sent out.
            output: The prediction result in bytes.
        """
        # When `topic_in_signal` is not `None`, we only do actual computation when both data stream and signal stream
        # from the same `flow_id` are present.
        if self.topic_in_signal and (flow_id not in self.cached_data or flow_id not in self.cached_signals):
            return False, None, None, None

        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None, None, None

        self.last_run_start_ms = time.time() * 1000

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp:
            if not self.cached_data[flow_id].startswith('ftp://'):
                raise ValueError("FTP mode is enabled but incoming message does not have FTP format")
            local_file_path = ftp_fetch(self.cached_data[flow_id], self.local_ftp_path, memory=self.ftp_memory,
                                        delete=self.ftp_delete)
            self.cached_data[flow_id] = local_file_path

        if self.topic_in_signal:
            is_satisfied, output = self.task(self.cached_data[flow_id], self.cached_signals[flow_id])
        else:
            is_satisfied, output = self.task(self.cached_data[flow_id])
        last_run_finish_ms = time.time() * 1000

        if self.ftp and not self.ftp_memory:
            output = self._write_ftp(output, local_file_path, last_run_finish_ms)

        msg_out_uuid = uuid.uuid4()
        # If log_path is not None, we write aggregation decisions to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            self._log_aggregation_decisions(flow_id, last_run_finish_ms, is_satisfied, msg_out_uuid, output)

        self._clear_cache(flow_id)
        return True, is_satisfied, msg_out_uuid, output

    def _write_ftp(self, output, local_file_path, last_run_finish_ms):
        # For now, use the completion timestamp as the filename of output FTP file
        ftp_output_dir = os.path.join(os.path.dirname(local_file_path), 'ftp_output')
        pathlib.Path(ftp_output_dir).mkdir(exist_ok=True)
        with open(os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp', 'w') as f:
            f.write(output)
        output = os.path.join(ftp_output_dir, str(last_run_finish_ms)) + '.ftp'
        return local_to_global_path(output, self.local_ftp_path)

    def _clear_cache(self, flow_id):
        del self.cached_data[flow_id]
        if self.topic_in_signal:
            del self.cached_signals[flow_id]

    def _log_incoming_msg(self, msg_in_uuid, flow_id, msg_in):
        self.latest_msg_consumed_time_ms[flow_id] = time.time() * 1000
        if flow_id not in self.latest_msg_publish_time_ms:
            self.latest_msg_publish_time_ms[flow_id] = [msg_in.publish_timestamp()]
            self.latest_msg_uuid[flow_id] = [str(msg_in_uuid)]
        else:
            self.latest_msg_publish_time_ms[flow_id].append(msg_in.publish_timestamp())
            self.latest_msg_uuid[flow_id].append(str(msg_in_uuid))

    def _log_aggregation_decisions(self, flow_id, last_run_finish_ms, is_satisfied, msg_out_uuid, msg_out_payload):
        replay_log = {'msg_in_uuid': self.latest_msg_uuid,
                      'msg_in_publish_time_ms': self.latest_msg_publish_time_ms,
                      'msg_in_consumed_time_ms': self.latest_msg_consumed_time_ms,
                      'start_compute_time_ms': self.last_run_start_ms,
                      'finish_compute_time_ms': last_run_finish_ms,
                      'last_log_duration_ms': self.last_log_duration_ms,
                      'data_in_payload': self.cached_data[flow_id],
                      'signal_in_payload': self.cached_signals[flow_id] if self.topic_in_signal else None,
                      'msg_out_is_satisfied': is_satisfied,
                      'msg_out_uuid': msg_out_uuid,
                      'msg_out_payload': msg_out_payload}
        with open(os.path.join(self.log_path, self.log_filename + '.log'), 'ab') as f:
            pickle.dump(replay_log, f)
        self.last_log_duration_ms = time.time() * 1000 - last_run_finish_ms

        # If no_overlap, reset latest_msg and latest_msg_time_ms so a message won't be processed twice.
        if self.no_overlap:
            self.latest_msg_uuid = dict()
            self.latest_msg_publish_time_ms = dict()
            self.latest_msg_consumed_time_ms = dict()

    def __iter__(self):
        return self

    def __next__(self):
        msg_in = self.consumer.receive()
        msg_in_uuid, op_from, flow_id, payload = self.graph_codec.decode(msg_in.value())
        actual_topic_in = msg_in.topic_name().split('/')[-1]

        if actual_topic_in == self.topic_in_signal or msg_in.topic_name() == self.topic_in_signal:
            # We locally cache the prediction result ("signal") from another model.
            self.cached_signals[flow_id] = self.gate_in_signal(payload)
        elif actual_topic_in == self.topic_in_data or msg_in.topic_name() == self.topic_in_data:
            self.cached_data[flow_id] = self.gate_in_data(payload)
        else:
            raise ValueError(
                "The consumer's topic name does not match that of incoming message. The topic of incoming message is",
                msg_in.topic_name())

        if self.log_path and os.path.isdir(self.log_path):
            self._log_incoming_msg(msg_in_uuid, flow_id, msg_in)

        is_performed, is_satisfied, msg_out_uuid, output = self._try_task(flow_id)
        if is_performed:
            output = self.gate_out_destination(output)
        else:
            output = self.gate_out_signal(output)

        if output:
            if self.log_path and os.path.isdir(self.log_path):
                with open(os.path.join(self.log_path, self.log_filename + '.output'), 'ab') as f:
                    pickle.dump(output, f)
            if is_satisfied:
                self.producer_destination.send(self.graph_codec.encode(msg_out_uuid, self.model_id, output, flow_id))
            elif self.topic_out_signal:
                self.producer_signal.send(self.graph_codec.encode(msg_out_uuid, self.model_id, output, flow_id))
            else:
                raise ValueError("The result is not satisfactory but output signal topic is not present.")

        if self.acknowledge:
            self.consumer.acknowledge(msg_in)

        return output if output else None
