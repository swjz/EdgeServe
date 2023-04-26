import pulsar
from pulsar.schema import AvroSchema
import time
import os
import pickle

from edgeserve.message_format import MessageFormat


class DataSource:
    def __init__(self, stream, pulsar_node, source_id, gate=None, topic='src', log_path=None, log_filename=None):
        self.client = pulsar.Client(pulsar_node)
        self.producer = self.client.create_producer(topic, schema=pulsar.schema.BytesSchema())
        self.stream = iter(stream)
        self.gate = (lambda x: x) if gate is None else gate
        self.source_id = source_id
        self.log_path = log_path
        self.log_filename = str(time.time() * 1000) if log_filename is None else log_filename

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
        # message = MessageFormat(source_id=self.source_id, payload=data)
        message = data
        msg_id = self.producer.send(message)
        msg_sent_time_ms = time.time() * 1000

        # If log_path is not None, we write timestamps to a log file.
        if self.log_path and os.path.isdir(self.log_path):
            replay_log = {'msg_id': msg_id.serialize(),
                          'data_collection_time_ms': data_collection_time_ms,
                          'msg_sent_time_ms': msg_sent_time_ms}
            with open(os.path.join(self.log_path, self.log_filename + '.log'), 'ab') as f:
                pickle.dump(replay_log, f)

        return data


class CodeSource(DataSource):
    def __init__(self, stream, pulsar_node, gate=None, topic='src'):
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
