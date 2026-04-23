import os
import socket
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from edgeserve.semantic_cache.tiered_store import TieredStore


class CacheHttpServer:
    """Serves cache blocks over HTTP.

    Exposes ``GET /cache/<block_uuid>``. With a TieredStore attached, L2
    (RAM) hits are served directly from bytes; L3 (NVMe) hits fall through
    to the existing file-streaming path. Without a store the server reads
    files from ``local_cache_path`` as before.
    """

    def __init__(
        self,
        local_cache_path: str,
        port: int = 0,
        host: str = '0.0.0.0',
        tiered_store: 'Optional[TieredStore]' = None,
    ):
        self.local_cache_path = local_cache_path
        os.makedirs(local_cache_path, exist_ok=True)

        cache_dir = self.local_cache_path
        store = tiered_store

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not self.path.startswith('/cache/'):
                    self.send_error(404)
                    return
                block_id = self.path[len('/cache/'):]
                if '/' in block_id or '..' in block_id:
                    self.send_error(400)
                    return

                # L2 fast path: serve from pinned RAM without any disk I/O
                if store is not None:
                    try:
                        uid = uuid.UUID(block_id)
                    except ValueError:
                        self.send_error(400)
                        return
                    l2_data = store.peek_l2(uid)
                    if l2_data is not None:
                        self.send_response(200)
                        self.send_header('Content-Type', 'application/octet-stream')
                        self.send_header('Content-Length', str(len(l2_data)))
                        self.send_header('X-Cache-Tier', 'l2')
                        self.end_headers()
                        self.wfile.write(l2_data)
                        return

                # L3 path: stream from NVMe file
                fpath = os.path.join(cache_dir, block_id + '.bin')
                if not os.path.isfile(fpath):
                    self.send_error(404)
                    return
                size = os.path.getsize(fpath)
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(size))
                self.send_header('X-Cache-Tier', 'l3')
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
