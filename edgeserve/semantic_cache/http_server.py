import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class CacheHttpServer:
    """Serves cache blocks from `local_cache_path` over HTTP.

    Exposes `GET /cache/<block_uuid>` which streams the file
    `<local_cache_path>/<block_uuid>.bin`. Zero external deps.
    """

    def __init__(self, local_cache_path: str, port: int = 0, host: str = '0.0.0.0'):
        self.local_cache_path = local_cache_path
        os.makedirs(local_cache_path, exist_ok=True)

        cache_dir = self.local_cache_path

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not self.path.startswith('/cache/'):
                    self.send_error(404)
                    return
                block_id = self.path[len('/cache/'):]
                if '/' in block_id or '..' in block_id:
                    self.send_error(400)
                    return
                fpath = os.path.join(cache_dir, block_id + '.bin')
                if not os.path.isfile(fpath):
                    self.send_error(404)
                    return
                size = os.path.getsize(fpath)
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(size))
                self.end_headers()
                with open(fpath, 'rb') as f:
                    while True:
                        chunk = f.read(64 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)

            def log_message(self, fmt, *args):
                pass  # silence default stderr logging

        self._server = ThreadingHTTPServer((host, port), Handler)
        self.port = self._server.server_address[1]
        self.host = host
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def uri(self) -> str:
        host = socket.gethostname() if self.host in ('0.0.0.0', '') else self.host
        return f'http://{host}:{self.port}'

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def write_block(self, block_uuid: str, data: bytes) -> str:
        fpath = os.path.join(self.local_cache_path, str(block_uuid) + '.bin')
        with open(fpath, 'wb') as f:
            f.write(data)
        return fpath
