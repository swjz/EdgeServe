import pulsar
import time
import os
import pickle
import uuid
import pathlib

from edgeserve.message_format import GraphCodec
from edgeserve.util import local_to_global_path
from edgeserve.loggable import Loggable


class DataSource(Loggable):
    def __init__(self, stream, pulsar_node, source_id, gate=None, topic='src',
                 topic_extra=None, ftp_out=False, local_ftp_path='/srv/ftp/',
                 log_path=None, log_filename=None, is_payload_logged=True, is_overhead_logged=False):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic, schema=pulsar.schema.BytesSchema())
        if topic_extra:
            self.producer_extra = self.client.create_producer(topic_extra, schema=pulsar.schema.BytesSchema())
        self.topic = topic
        self.topic_extra = topic_extra
        self.stream = iter(stream)
        self.gate = (lambda x: x) if gate is None else gate
        assert len(source_id) <= 16, 'source_id must be at most 16 bytes long'
        self.source_id = source_id
        self.ftp_out = ftp_out
        self.local_ftp_path = local_ftp_path
        self.log_path = log_path
        self.log_filename = source_id if log_filename is None else log_filename
        self.is_payload_logged = is_payload_logged
        self.is_overhead_logged = is_overhead_logged
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def write_ahead_log_to_file(self, topic, msg_uuid, logged_data, data_collection_time_ms):
        # TODO: support logging more than one outputs (extra!)
        if self.log_path:
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_start_time_ms = time.time() * 1000
            log_file = os.path.join(self.log_path, f'{self.log_filename}-{topic}.wal')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    f.write('msg_uuid,payload,data_collection_time_ms\n')
            with open(log_file, 'a') as f:
                f.write(f'{msg_uuid},{logged_data},{data_collection_time_ms}\n')
            if self.is_overhead_logged:
                self.overhead_log(msg_uuid, log_file, log_start_time_ms)

    def write_ahead_log_to_rocksdb(self, msg_uuid, logged_data, data_collection_time_ms):
        import rocksdb
        if self.log_path:
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_start_time_ms = time.time() * 1000
            log_file = os.path.join(self.log_path, self.log_filename + '.wal')
            log_db = rocksdb.DB(log_file + '.db', rocksdb.Options(create_if_missing=True))
            log_db.put(msg_uuid.bytes, f'{logged_data},{data_collection_time_ms}'.encode())

            if self.is_overhead_logged:
                self.overhead_log(msg_uuid, log_file, log_start_time_ms)

    def __iter__(self):
        return self

    def __next__(self):
        has_extra = False
        incoming = next(self.stream)
        if incoming is None:
            return None
        if isinstance(incoming, tuple):
            incoming, extra = incoming
            if extra is not None:
                has_extra = True
                extra = self.gate(extra)
        data = self.gate(incoming)
        if data is None:
            return None
        data_collection_time_ms = time.time() * 1000

        msg_uuid = uuid.uuid4()
        # For now, assume that lazy data routing only applies to the more frequent payload.
        # Extra data is always sent to the extra topic in eager mode.
        if self.ftp_out or (self.log_path and not self.is_payload_logged):
            local_file_path = os.path.join(self.local_ftp_path, str(msg_uuid) + '.ftp')
            with open(local_file_path, 'wb') as f:
                pickle.dump(data, f)
            global_file_path = local_to_global_path(local_file_path, self.local_ftp_path)
            # Do not modify the data variable when lazy data routing is disabled.
            if self.ftp_out:
                data = global_file_path

        if has_extra:
            msg_uuid_extra = uuid.uuid4()
            self.write_ahead_log_to_file(self.topic_extra, msg_uuid_extra, extra, data_collection_time_ms)
            message_extra = self.graph_codec.encode(msg_uuid=msg_uuid_extra, op_from=self.source_id, payload=extra)
            self.producer_extra.send(message_extra)

        # Write ahead log.
        if self.is_payload_logged:
            self.write_ahead_log_to_file(self.topic, msg_uuid, data, data_collection_time_ms)
        else:
            self.write_ahead_log_to_file(self.topic, msg_uuid, global_file_path, data_collection_time_ms)

        message = self.graph_codec.encode(msg_uuid=msg_uuid, op_from=self.source_id, payload=data)
        self.producer.send(message)
        return data


class CodeSource(DataSource):
    def __init__(self, stream, pulsar_node, gate=lambda x: x.encode('utf-8'), topic='src'):
        super().__init__(stream, pulsar_node, 'code-source', gate, topic)


class CameraSource(DataSource):
    # local_ftp_path: str, e.g. '/srv/ftp/files/'
    # remote_ftp_path: str, e.g. 'ftp://192.168.1.101/files/'
    def __init__(self, pulsar_node, local_ftp_path, global_ftp_path, width, height, source_id,
                 gate=None, topic='src', cam_id=0):
        super().__init__(self.stream(), pulsar_node, source_id, gate, topic)
        self.cam_id = cam_id
        self.local_ftp_path = local_ftp_path
        self.global_ftp_path = global_ftp_path
        self.width = width
        self.height = height

    def stream(self):
        import cv2
        from time import time
        cap = cv2.VideoCapture(self.cam_id)
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        while cap.isOpened():
            ret, frame = cap.read()
            if ret is True:
                cur_time = str(time())
                cv2.imwrite(self.local_ftp_path + '/' + cur_time + '.jpg', frame)
                yield self.global_ftp_path + '/' + cur_time + '.jpg'
            else:
                break
        cap.release()
        cv2.destroyAllWindows()


# read in a csv file with first column as timestamp in seconds.
class SimulateTimeSeries(DataSource):
    def __init__(self, filename, pulsar_node, source_id, gate=None, topic='src'):
        super().__init__(self.stream(), pulsar_node, source_id, gate, topic)
        self.timestamps = []
        self.values = []
        self._csv_to_list(filename)

    def _csv_to_list(self, filename):
        import csv
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                self.timestamps.append(row[0])
                self.values.append(row[1:])

    def stream(self):
        if len(self.timestamps) == 0:
            raise StopIteration

        import time
        time_diff = time.time() - float(self.timestamps[0])
        for i in range(len(self.timestamps)):
            this_time_diff = time.time() - float(self.timestamps[i])
            if this_time_diff < time_diff:
                time.sleep(time_diff - this_time_diff)
            time_diff = this_time_diff
            yield self.values[i]


class SimulateVideoWithTimestamps(CameraSource):
    def __init__(self, timestamps, pulsar_node, local_ftp_path, global_ftp_path, width, height,
                 source_id, gate=None, topic='src', cam_id=0):
        super().__init__(pulsar_node, local_ftp_path, global_ftp_path, width, height, source_id, gate, topic, cam_id)
        self.timestamps = timestamps

    def stream(self):
        if len(self.timestamps) == 0:
            raise StopIteration

        import cv2
        from time import time
        cap = cv2.VideoCapture(self.cam_id)
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        fps = cap.get(cv2.CAP_PROP_FPS)
        i = 0

        while cap.isOpened():
            ret, frame = cap.read()
            for _ in range(int(fps * self.timestamps[i])):
                ret, frame = cap.read()  # skip frames
            i += 1
            if ret is True:
                cur_time = str(time())
                cv2.imwrite(self.local_ftp_path + '/' + cur_time + '.jpg', frame)
                yield self.global_ftp_path + '/' + cur_time + '.jpg'
            else:
                break
        cap.release()
        cv2.destroyAllWindows()


class AudioSource(DataSource):
    def __init__(self, audio_path, pulsar_node, chunk_size_small, chunk_size_large,
                 source_id, gate=None, topic='audio-src-small', topic_extra='audio-src-large'):
        super().__init__(self.stream(), pulsar_node, source_id, gate, topic, topic_extra)
        self.audio_path = audio_path
        self.chunk_size_small = chunk_size_small
        self.chunk_size_large = chunk_size_large
        self.last_small_chunk_time = 0
        self.last_large_chunk_time = 0

    def stream(self):
        from edgeserve.util import load_audio_chunk
        import numpy as np
        while True:
            # receive new audio chunk (and e.g. wait for min_chunk_size seconds first, ...)
            audio_chunk = load_audio_chunk(self.audio_path, self.last_small_chunk_time,
                                           self.last_small_chunk_time + self.chunk_size_small)
            if len(audio_chunk) == 0:
                break
            audio_chunk = audio_chunk.tobytes()
            self.last_small_chunk_time += self.chunk_size_small

            # Send both small and large chunks when the time comes
            if self.last_large_chunk_time + self.chunk_size_large <= self.last_small_chunk_time:
                audio_chunk_large = load_audio_chunk(self.audio_path, self.last_large_chunk_time,
                                                     self.last_small_chunk_time)
                self.last_large_chunk_time += self.chunk_size_large
                if len(audio_chunk_large) > 0:
                    audio_chunk_large = audio_chunk_large.tobytes()
                    yield audio_chunk, audio_chunk_large

            # If the large chunk is not ready yet, just send the small chunk
            yield audio_chunk, None
