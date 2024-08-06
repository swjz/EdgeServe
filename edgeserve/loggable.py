import time
import os
import pathlib


class Loggable:
    def __init__(self, log_path, log_filename, is_overhead_logged: bool = False):
        self.log_path = log_path
        self.log_filename = log_filename
        self.is_overhead_logged = is_overhead_logged

    def overhead_log(self, msg_uuid, log_file, log_start_time_ms):
        if self.log_path:
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_end_time_ms = time.time() * 1000
            log_overhead_file = log_file + '.overhead'
            if not os.path.exists(log_overhead_file):
                with open(log_overhead_file, 'w') as f:
                    f.write('msg_uuid,log_overhead_time_ms\n')
            with open(log_overhead_file, 'a') as f:
                f.write(f'{msg_uuid},{log_end_time_ms - log_start_time_ms}\n')

    # On receive log. Note that payload is not logged here.
    def on_receive_log_to_file(self, msg_in_uuid, op_from, received_time_ms, msg_out_uuid):
        if self.log_path:
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_start_time_ms = time.time() * 1000
            log_file = os.path.join(self.log_path, self.log_filename + '.orl')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    f.write('msg_in_uuid,op_from,received_time_ms,msg_out_uuid\n')
            with open(log_file, 'a') as f:
                f.write(f'{msg_in_uuid},{op_from},{received_time_ms},{msg_out_uuid}\n')

            if self.is_overhead_logged:
                self.overhead_log(msg_in_uuid, log_file, log_start_time_ms)

    # On receive log (RocksDB version). Note that payload is not logged here.
    def on_receive_log_to_rocksdb(self, msg_in_uuid, op_from, received_time_ms, msg_out_uuid):
        import rocksdb
        if self.log_path:
            pathlib.Path(self.log_path).mkdir(parents=True, exist_ok=True)
            log_start_time_ms = time.time() * 1000
            log_file = os.path.join(self.log_path, self.log_filename + '.orl')
            log_db = rocksdb.DB(log_file + '.db', rocksdb.Options(create_if_missing=True))
            log_db.put(msg_in_uuid.bytes, f'{op_from},{received_time_ms},{msg_out_uuid}'.encode())

            if self.is_overhead_logged:
                self.overhead_log(msg_in_uuid, log_file, log_start_time_ms)

    def p2p_log(self, msg_in_uuid, op_from, local_file_path):
        if self.log_path:
            log_start_time_ms = time.time() * 1000
            log_file = os.path.join(self.log_path, self.log_filename + '.ftplog')
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    f.write('msg_in_uuid,local_file_path,op_from,fetched_time_ms\n')
            with open(log_file, 'a') as f:
                f.write(f'{msg_in_uuid},{local_file_path},{op_from},{time.time() * 1000}\n')

            if self.is_overhead_logged:
                self.overhead_log(msg_in_uuid, log_file, log_start_time_ms)
