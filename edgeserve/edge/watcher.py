"""watcher.py — Phase 4.2: edge-side file watcher.

Monitors a directory for file changes and pushes updated content to the
context server's /ingest endpoint.  Runs on the edge device (e.g. Mac Mini).

Entity tags follow the convention:
  file:<relative/path/to/file>             (unversioned — latest)
  file:<relative/path/to/file>@sha=<sha>  (exact content version)

Usage
-----
  python -m edgeserve.edge.watcher \\
      --watch-dir ~/projects/myrepo \\
      --server http://192.168.1.214:8765 \\
      --glob "*.py" "*.md" \\
      --poll-interval 2.0

  # Or import and use programmatically:
  from edgeserve.edge.watcher import ContextWatcher
  w = ContextWatcher("http://gpu-box:8765", watch_dir="~/myrepo")
  w.push_file("src/main.py")
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _ingest(server_url: str, text: str, entities: list[str],
            sha: str, timeout: float = 60.0) -> dict:
    import json
    body = json.dumps({'text': text, 'entities': entities, 'sha': sha}).encode()
    req = urllib.request.Request(
        f'{server_url.rstrip("/")}/ingest',
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


class ContextWatcher:
    """Push file changes in a directory to a context server.

    Parameters
    ----------
    server_url:
        Base URL of the context server, e.g. ``http://192.168.1.214:8765``.
    watch_dir:
        Root directory to monitor.  Relative paths in entity tags are
        relative to this directory.
    globs:
        Filename patterns to watch (default: all files).
    poll_interval:
        Seconds between directory scans.
    on_push:
        Optional callback called with (rel_path, result_dict) after each push.
    """

    def __init__(
        self,
        server_url: str,
        watch_dir: str = '.',
        globs: Optional[list[str]] = None,
        poll_interval: float = 2.0,
        on_push=None,
    ) -> None:
        self.server_url = server_url.rstrip('/')
        self.watch_dir = Path(watch_dir).expanduser().resolve()
        self.globs = globs or ['*']
        self.poll_interval = poll_interval
        self.on_push = on_push
        self._seen: dict[str, str] = {}  # rel_path → last sha

    def push_file(self, path: str | Path) -> dict:
        """Push a single file immediately, regardless of whether it changed."""
        fpath = Path(path)
        if not fpath.is_absolute():
            fpath = self.watch_dir / fpath
        data = fpath.read_bytes()
        sha = _sha256(data)
        rel = str(fpath.relative_to(self.watch_dir))
        entities = [f'file:{rel}']
        try:
            text = data.decode('utf-8', errors='replace')
        except Exception:
            text = repr(data)
        result = _ingest(self.server_url, text, entities, sha)
        self._seen[rel] = sha
        return result

    def _matches(self, rel: str) -> bool:
        name = os.path.basename(rel)
        return any(fnmatch.fnmatch(name, g) for g in self.globs)

    def _scan_once(self) -> list[str]:
        """Scan for changed files. Returns list of rel paths pushed."""
        pushed = []
        for fpath in self.watch_dir.rglob('*'):
            if not fpath.is_file():
                continue
            try:
                rel = str(fpath.relative_to(self.watch_dir))
            except ValueError:
                continue
            if not self._matches(rel):
                continue
            try:
                data = fpath.read_bytes()
            except OSError:
                continue
            sha = _sha256(data)
            if self._seen.get(rel) == sha:
                continue  # unchanged
            self._seen[rel] = sha
            entities = [f'file:{rel}']
            try:
                text = data.decode('utf-8', errors='replace')
            except Exception:
                text = repr(data)
            try:
                result = _ingest(self.server_url, text, entities, sha)
                print(f'[watcher] pushed {rel}  sha={sha}  '
                      f'n_tokens={result.get("n_tokens")}  '
                      f'ingest_ms={result.get("ingest_ms")} ms', flush=True)
                if self.on_push:
                    self.on_push(rel, result)
                pushed.append(rel)
            except Exception as e:
                print(f'[watcher] push failed for {rel}: {e}', flush=True)
        return pushed

    def run_forever(self) -> None:
        """Poll the directory until KeyboardInterrupt."""
        print(f'[watcher] watching {self.watch_dir}  '
              f'globs={self.globs}  server={self.server_url}', flush=True)
        # Initial scan — push everything to warm the context server
        print('[watcher] initial scan ...', flush=True)
        pushed = self._scan_once()
        print(f'[watcher] initial push: {len(pushed)} file(s)', flush=True)
        while True:
            time.sleep(self.poll_interval)
            self._scan_once()

    def health(self) -> dict:
        """Check context server health."""
        import json
        with urllib.request.urlopen(f'{self.server_url}/health', timeout=5) as r:
            return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--watch-dir', default='.', help='Directory to monitor')
    ap.add_argument('--server', default='http://localhost:8765',
                    help='Context server base URL')
    ap.add_argument('--glob', nargs='+', default=['*.py', '*.md', '*.txt'],
                    dest='globs', help='Filename patterns to watch')
    ap.add_argument('--poll-interval', type=float, default=2.0)
    args = ap.parse_args()

    w = ContextWatcher(
        server_url=args.server,
        watch_dir=args.watch_dir,
        globs=args.globs,
        poll_interval=args.poll_interval,
    )
    try:
        w.run_forever()
    except KeyboardInterrupt:
        print('\n[watcher] stopped.')


if __name__ == '__main__':
    main()
