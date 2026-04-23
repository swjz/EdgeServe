"""bench_bandwidth_throttle.py — Phase 2.2b: empirical bandwidth-vs-recompute crossover.

Simulates different network link speeds by throttling the HTTP fetch at the
Python level (no tc/root required). For each simulated bandwidth, measures
how long it takes to transfer a KV blob of a given size, then compares
against the known GPU prefill time to find the crossover empirically.

Prefill times come from Phase 2.2 mmap measurements. Blob sizes use the
same KV density observed in the mmap sweep.

Usage
-----
  python scripts/bench_bandwidth_throttle.py
  python scripts/bench_bandwidth_throttle.py --blob-mb 121 --prefill-s 0.63
  python scripts/bench_bandwidth_throttle.py --doc-repeats 128 --repeats 5
"""
from __future__ import annotations

import argparse
import http.server
import io
import os
import statistics
import tempfile
import threading
import time
import urllib.request
import uuid


# ---------------------------------------------------------------------------
# Throttled HTTP server / client
# ---------------------------------------------------------------------------

class _ThrottledResponse(io.RawIOBase):
    """Wrap a bytes buffer and rate-limit reads to simulate a WAN link."""

    def __init__(self, data: bytes, bps: float) -> None:
        self._data = data
        self._bps = bps          # simulated bytes per second
        self._pos = 0

    def readinto(self, b: bytearray) -> int:
        n = len(b)
        chunk = self._data[self._pos:self._pos + n]
        if not chunk:
            return 0
        b[:len(chunk)] = chunk
        self._pos += len(chunk)
        # Sleep to simulate the link speed
        if self._bps > 0:
            time.sleep(len(chunk) / self._bps)
        return len(chunk)


class _BlobHandler(http.server.BaseHTTPRequestHandler):
    """Serve one blob at /blob; support bandwidth throttling via query param."""

    blob: bytes = b''   # class-level; set before serving

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Length', str(len(self.__class__.blob)))
        self.end_headers()
        self.wfile.write(self.__class__.blob)

    def log_message(self, fmt, *args):
        pass  # silence


def _start_server(blob: bytes) -> tuple[http.server.HTTPServer, int]:
    """Start a bare HTTP server in a daemon thread; return (server, port)."""
    _BlobHandler.blob = blob
    server = http.server.HTTPServer(('127.0.0.1', 0), _BlobHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def fetch_throttled(url: str, bps: float, chunk_size: int = 65536) -> tuple[bytes, float]:
    """Fetch `url` with an artificial read-rate limit of `bps` bytes/sec.

    Returns (data, elapsed_s).
    """
    with urllib.request.urlopen(url, timeout=300) as resp:
        t0 = time.perf_counter()
        chunks = []
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            if bps > 0:
                time.sleep(len(chunk) / bps)
        elapsed = time.perf_counter() - t0
    return b''.join(chunks), elapsed


# ---------------------------------------------------------------------------
# Phase 2.2 reference data (from bench_bandwidth_crossover.py mmap sweep)
# ---------------------------------------------------------------------------

PHASE22_DATA = [
    # (doc_repeats, ~tokens, blob_mb, prefill_s)
    (16,  544,   15.1, 0.10),
    (32,  1089,  30.3, 0.17),
    (64,  2178,  60.6, 0.31),
    (128, 4356, 121.1, 0.63),
    (256, 8448, 234.9, 1.35),
]


def lookup_prefill(blob_mb: float) -> float:
    """Interpolate/extrapolate prefill time from Phase 2.2 reference data."""
    # Linear fit: prefill_s = a * blob_mb + b
    xs = [d[2] for d in PHASE22_DATA]
    ys = [d[3] for d in PHASE22_DATA]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    a = sum((xi - mx) * (yi - my) for xi, yi in zip(xs, ys)) / sum((xi - mx) ** 2 for xi in xs)
    b = my - a * mx
    return max(0.01, a * blob_mb + b)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--blob-mb', type=float, default=121.1,
                    help='KV blob size in MB (default 121.1 = 128 doc-repeats)')
    ap.add_argument('--prefill-s', type=float, default=None,
                    help='Measured GPU prefill time in seconds (default: interpolated)')
    ap.add_argument('--repeats', type=int, default=3,
                    help='Fetch repetitions per bandwidth point')
    args = ap.parse_args()

    blob_bytes = int(args.blob_mb * 1024 * 1024)
    prefill_s = args.prefill_s if args.prefill_s else lookup_prefill(args.blob_mb)

    print(f'KV blob size:   {args.blob_mb:.1f} MB  ({blob_bytes:,} bytes)')
    print(f'GPU prefill:    {prefill_s*1000:.0f} ms  (reference baseline)')
    print(f'Repeats:        {args.repeats} per bandwidth point')
    print()

    # Create a dummy blob (all zeros — content doesn't matter for transport timing)
    blob = bytes(blob_bytes)
    server, port = _start_server(blob)
    url = f'http://127.0.0.1:{port}/blob'

    # Bandwidth sweep: from 50 Mbps to 10 Gbps
    bw_points = [
        ('50 Mbps',   50e6 / 8),
        ('100 Mbps', 100e6 / 8),
        ('200 Mbps', 200e6 / 8),
        ('500 Mbps', 500e6 / 8),
        ('1 Gbps',  1000e6 / 8),
        ('1.5 Gbps', 1500e6 / 8),
        ('2 Gbps',  2000e6 / 8),
        ('3 Gbps',  3000e6 / 8),
        ('5 Gbps',  5000e6 / 8),
        ('10 Gbps', 10000e6 / 8),
        ('∞ (mmap)', 0),        # 0 = no throttle = loopback speed
    ]

    results = []
    for label, bps in bw_points:
        times = []
        for _ in range(args.repeats):
            _, elapsed = fetch_throttled(url, bps)
            times.append(elapsed)
        med = statistics.median(times)
        crossover = med < prefill_s
        verdict = '**faster**' if crossover else 'slower'
        results.append((label, med, crossover))
        print(f'  {label:12s}: {med*1000:7.0f} ms fetch  vs {prefill_s*1000:.0f} ms prefill  → {verdict}')

    server.shutdown()

    # Find crossover
    crossover_bw = None
    for i in range(len(results) - 1):
        if not results[i][2] and results[i+1][2]:
            crossover_bw = results[i+1][0]
            break

    print()
    print('## Phase 2.2b — Throttled HTTP fetch vs GPU prefill crossover')
    print()
    print(f'KV blob: {args.blob_mb:.1f} MB. GPU prefill: {prefill_s*1000:.0f} ms.')
    print()
    print('| Simulated link | Fetch time | GPU prefill | Verdict |')
    print('|---|---:|---:|---|')
    for label, med, faster in results:
        tag = '✓ fetch wins' if faster else '✗ prefill wins'
        print(f'| {label} | {med*1000:.0f} ms | {prefill_s*1000:.0f} ms | {tag} |')

    if crossover_bw:
        print()
        print(f'**Empirical crossover: {crossover_bw}** — '
              f'fetch beats recompute above this link speed.')
    else:
        print()
        print('(No crossover observed in sweep range.)')

    print()
    # Compute what analytic formula predicts
    analytic_gbps = args.blob_mb * 8 / prefill_s / 1000
    print(f'Analytic crossover (blob_MB × 8 / prefill_s / 1000): {analytic_gbps:.2f} Gbps')


if __name__ == '__main__':
    main()
