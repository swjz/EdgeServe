import pulsar
from pulsar.schema import AvroSchema
import time
import os
import pickle
import uuid

from edgeserve.message_format import GraphCodec
from edgeserve.util import local_to_global_path


class DataSource:
    def __init__(self, stream, pulsar_node, source_id, gate=None, topic='src', ftp_out=False, local_ftp_path='/srv/ftp/',
                 log_path=None, log_filename=None, log_payload=True):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic, schema=pulsar.schema.BytesSchema())
        self.stream = iter(stream)
        self.gate = (lambda x: x) if gate is None else gate
        assert len(source_id) <= 16, 'source_id must be at most 16 bytes long'
        self.source_id = source_id
        self.ftp_out = ftp_out
        self.local_ftp_path = local_ftp_path
        self.log_path = log_path
        self.log_filename = source_id if log_filename is None else log_filename
        self.log_payload = log_payload
        self.graph_codec = GraphCodec(msg_uuid_size=16, op_from_size=16, header_size=0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __iter__(self):
        return self

    def __next__(self):
        incoming = next(self.stream)
        if incoming is None:
            return None
        data = self.gate(incoming)
        if data is None:
            return None
        data_collection_time_ms = time.time() * 1000

        msg_uuid = uuid.uuid4()
        if self.ftp_out or (self.log_path and os.path.isdir(self.log_path) and not self.log_payload):
            local_file_path = os.path.join(self.local_ftp_path, str(msg_uuid) + '.ftp')
            with open(local_file_path, 'wb') as f:
                pickle.dump(data, f)
            global_file_path = local_to_global_path(local_file_path, self.local_ftp_path)
            # Do not modify the data variable when lazy data routing is disabled.
            if self.ftp_out:
                data = global_file_path

        message = self.graph_codec.encode(msg_uuid=msg_uuid, op_from=self.source_id, payload=data)
        self.producer.send(message)
        msg_sent_time_ms = time.time() * 1000

        # If log_path is not None, we write timestamps to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            log_file = os.path.join(self.log_path, self.log_filename + '.datasource')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    f.write('msg_uuid,payload,data_collection_time_ms,msg_sent_time_ms\n')
            with open(log_file, 'a') as f:
                if self.log_payload:
                    f.write(str(msg_uuid) + ',' + str(data) + ',' + str(data_collection_time_ms) + ',' +
                            str(msg_sent_time_ms) + '\n')
                else:
                    f.write(str(msg_uuid) + ',' + global_file_path + ',' + str(data_collection_time_ms) + ',' +
                            str(msg_sent_time_ms) + '\n')

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
