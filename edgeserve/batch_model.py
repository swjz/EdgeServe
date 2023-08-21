import os
import time
from typing import Callable, Optional, Tuple, Set

import pulsar
import pickle
import pathlib
from _pulsar import InitialPosition, ConsumerType

from edgeserve.util import ftp_fetch, local_to_global_path
from edgeserve.message_format import NetworkCodec
from edgeserve.model import Model


# This version intentionally drops schema support. Data sources cannot be combined adaptively.
class BatchModel(Model):
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
                 batch_max_num_msg: Optional[int] = 100,
                 batch_max_bytes: Optional[int] = 10 * 1024 * 1024,
                 batch_timeout_ms: Optional[int] = 10) -> None:
        """Initializes a model.

        Args:
            task: The actual task to be performed, in the format of a callable method.
                Arg:
                    batch_input: a dict of (flow_id: int -> (data, signal)), or (flow_id -> data) if no signal present.
                Returns:
                    is_satisfied: a dict of (flow_id: int -> bool) indicating whether each flow_id is satisfied.
                    output: a dict of (flow_id: int -> bytes) indicating model output for each flow_id.
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
            batch_max_num_msg: The maximum number of messages to include in a single batch.
            batch_max_bytes: The maximum size of messages to include in a single batch.
            batch_timeout_ms: The maximum timeout to wait (in ms) for enough messages for this batch.
        """
        super().__init__(task, pulsar_node, topic_in_data, topic_out_destination, topic_in_signal, topic_out_signal,
                         gate_in, gate_out, header_size, ftp, ftp_memory, ftp_delete, local_ftp_path, max_time_diff_ms,
                         no_overlap, min_interval_ms, log_path, log_filename, acknowledge)
        self.batch_max_num_msg = batch_max_num_msg
        self.batch_max_bytes = batch_max_bytes
        self.batch_timeout_ms = batch_timeout_ms

    def _subscribe(self):
        if self.topic_in_signal:
            return self.client.subscribe([self.topic_in_data, self.topic_in_signal],
                                         subscription_name='compute-sub',
                                         consumer_type=ConsumerType.Shared,
                                         schema=pulsar.schema.BytesSchema(),
                                         initial_position=InitialPosition.Earliest,
                                         batch_receive_policy=pulsar.ConsumerBatchReceivePolicy(self.batch_max_num_msg,
                                                                                                self.batch_max_bytes,
                                                                                                self.batch_timeout_ms))

        return self.client.subscribe(self.topic_in_data, subscription_name='compute-sub',
                                     consumer_type=ConsumerType.Shared, schema=pulsar.schema.BytesSchema(),
                                     initial_position=InitialPosition.Earliest,
                                     batch_receive_policy=pulsar.ConsumerBatchReceivePolicy(self.batch_max_num_msg,
                                                                                            self.batch_max_bytes,
                                                                                            self.batch_timeout_ms))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _try_tasks(self, flow_ids: Set[int]) -> Tuple[bool, Optional[dict], Optional[dict]]:
        """Performs actual computation.

        Args:
            flow_ids: Set of IDs of the data flows to be processed.

        Returns:
            is_performed: Whether the job is actually performed.
            is_satisfied: Whether the prediction is considered satisfied for each flow_id in a (flow_id -> bool) dict.
                          If set to `True`, the result is sent to the destination topic;
                          if set to `False`, the result is sent to a more expensive model.
            outputs: The prediction results in the format of dict of (flow_id -> bytes).
        """
        # Avoid running too frequently for expensive tasks
        if time.time() * 1000 < self.last_run_start_ms + self.min_interval_ms:
            return False, None, None

        self.last_run_start_ms = time.time() * 1000

        # When `topic_in_signal` is present, we only do actual computation when both data stream and signal stream
        # from the same `flow_id` are present.
        if self.topic_in_signal:
            flow_ids = flow_ids & self.cached_data.keys() & self.cached_signals.keys()

        # Lazy data routing: only fetch data from FTP counterpart when we actually need it.
        if self.ftp:
            for flow_id in flow_ids:
                if not self.cached_data[flow_id].startswith('ftp://'):
                    raise ValueError("FTP mode is enabled but incoming message does not have FTP format")
                local_file_path = ftp_fetch(self.cached_data[flow_id], self.local_ftp_path, memory=self.ftp_memory,
                                            delete=self.ftp_delete)
                self.cached_data[flow_id] = local_file_path

        cached_data_filtered = {flow_id: self.cached_data[flow_id] for flow_id in flow_ids}
        if self.topic_in_signal:
            cached_tuple_filtered = {flow_id: (self.cached_data[flow_id],
                                               self.cached_signals[flow_id]) for flow_id in flow_ids}
            is_satisfied, output = self.task(cached_tuple_filtered)
        else:
            is_satisfied, output = self.task(cached_data_filtered)
        last_run_finish_ms = time.time() * 1000

        if self.ftp and not self.ftp_memory:
            output = self._write_ftp(output, local_file_path, last_run_finish_ms)

        self._clear_cache(flow_ids)
        return True, is_satisfied, output

    def _clear_cache(self, flow_ids):
        for flow_id in flow_ids:
            del self.cached_data[flow_id]
            if self.topic_in_signal:
                del self.cached_signals[flow_id]

    def _send_async_callback(res, msg_id):
        print('Message published res=%s msg_id=%s', res, msg_id)

    def __iter__(self):
        return self

    def __next__(self):
        messages = self.consumer.batch_receive()
        while len(messages) == 0:
            messages = self.consumer.batch_receive()

        flow_ids = set()
        for msg in messages:

            flow_id, payload = self.network_codec.decode(msg.data())
            actual_topic_in = msg.topic_name().split('/')[-1]

            if actual_topic_in == self.topic_in_signal:
                # We locally cache the prediction result ("signal") from another model.
                self.cached_signals[flow_id] = payload
            elif actual_topic_in == self.topic_in_data:
                self.cached_data[flow_id] = self.gate_in(payload)
            else:
                raise ValueError(
                    "The consumer's topic name does not match that of incoming message. "
                    "The topic of incoming message is", actual_topic_in)

            flow_ids.add(flow_id)

            if self.log_path and os.path.isdir(self.log_path):
                self._log_incoming_msg(flow_id, msg)

            self.latest_msg_consumed_time_ms[flow_id] = time.time() * 1000

        is_performed, is_satisfied, outputs = self._try_tasks(flow_ids)
        if not is_performed:
            return None

        do_return = False
        for flow_id in outputs.keys():
            if not outputs[flow_id]:
                continue
            do_return = True
            output = self.gate_out(outputs[flow_id])
            if self.log_path and os.path.isdir(self.log_path):
                with open(os.path.join(self.log_path, self.log_filename + '.output'), 'ab') as f:
                    pickle.dump(output, f)
            if is_satisfied[flow_id]:
                self.producer_destination.send_async(self.network_codec.encode([flow_id, output]),
                                                     self._send_async_callback)
            elif self.topic_out_signal:
                self.producer_signal.send_async(self.network_codec.encode([flow_id, output]), self._send_async_callback)
            else:
                raise ValueError("The result is not satisfactory but output signal topic is not present.")

        if self.acknowledge:
            for message in messages:
                self.consumer.acknowledge(message)

        return outputs if do_return else None
