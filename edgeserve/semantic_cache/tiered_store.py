"""tiered_store.py — two-tier KV-block store: L2 (pinned CPU RAM) + L3 (NVMe).

L1 is vLLM's own paged buffer, managed entirely by vLLM.
This module manages L2 and L3 on the context server.

Tier semantics
--------------
- **L2 (hot)**: recently published/accessed blobs kept as pinned bytes in an
  OrderedDict (LRU). Capped at `l2_max_bytes`. Eviction to L3 (already there).
- **L3 (cold)**: the canonical backing store. Every published blob lands in L3
  first; L2 is a hot-read cache on top. Capped at `l3_max_bytes` (default
  unbounded). LRU eviction below L3 fires the `on_tombstone` callback so the
  caller can broadcast a deleted header to the catalog fabric.

Integration with CacheHttpServer
---------------------------------
Pass the store to `CacheHttpServer(tiered_store=store)`. The HTTP handler calls
`store.peek_l2(uuid)` for a zero-copy in-RAM serve, then falls through to the
L3 file path on a miss — no change to the HTTP wire format.

Integration with SemanticCacheClient
--------------------------------------
Pass `tiered_store=store` to the client. `publish()` routes data through
`store.put()` instead of writing the file directly; the HTTP server still
serves from the same L3 path transparently.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from collections import OrderedDict
from typing import Callable, Optional, Tuple


class TieredStore:
    """L2 (RAM) + L3 (NVMe) KV-block cache with LRU eviction and tombstoning."""

    def __init__(
        self,
        l3_path: str,
        l2_max_bytes: int = 2 * 1024 ** 3,
        l3_max_bytes: Optional[int] = None,
        on_tombstone: Optional[Callable[[uuid.UUID], None]] = None,
        promote_async: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        l3_path:
            Directory for NVMe block files (``<uuid>.bin``).  This is the same
            path as ``SemanticCacheClient.local_cache_path``.
        l2_max_bytes:
            Maximum bytes to keep in RAM.  Default 2 GB.
        l3_max_bytes:
            Maximum bytes on NVMe before LRU eviction fires tombstones.
            ``None`` (default) means unbounded.
        on_tombstone:
            Called with the block UUID after its NVMe file is deleted.
            Runs outside the internal lock; may block on Pulsar I/O.
        promote_async:
            If True, L3→L2 promotion on cache-miss reads happens in a
            background thread rather than synchronously.
        """
        self.l3_path = l3_path
        self.l2_max_bytes = l2_max_bytes
        self.l3_max_bytes = l3_max_bytes
        self.on_tombstone = on_tombstone

        os.makedirs(l3_path, exist_ok=True)

        self._l2: OrderedDict[uuid.UUID, bytes] = OrderedDict()
        self._l2_bytes = 0

        # UUID → last-access timestamp (ms); LRU = lowest value = leftmost
        self._l3_access: OrderedDict[uuid.UUID, float] = OrderedDict()
        self._l3_bytes = 0

        self._lock = threading.Lock()

        # Background promotion queue: list of (uuid, data) to absorb into L2
        self._promote_queue: list[Tuple[uuid.UUID, bytes]] = []
        self._promote_lock = threading.Lock()
        if promote_async:
            self._promote_thread = threading.Thread(
                target=self._promote_worker, daemon=True)
            self._promote_thread.start()
        else:
            self._promote_thread = None

        self._scan_l3()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def put(self, block_uuid: uuid.UUID, data: bytes) -> str:
        """Store a block. Always writes L3; also inserts into L2 if capacity permits.

        Returns ``'l2'`` if the block landed in the hot layer, ``'l3'`` otherwise.
        Tombstones for any L3 entries evicted to make room are fired after the
        lock is released.
        """
        sz = len(data)
        now_ms = time.time() * 1000

        # L3 write first — canonical backing store
        fpath = self._l3_path_for(block_uuid)
        with open(fpath, 'wb') as f:
            f.write(data)

        tombstones: list[uuid.UUID] = []
        tier = 'l3'

        with self._lock:
            # Track L3 entry — evict BEFORE incrementing so headroom is consistent
            is_new = block_uuid not in self._l3_access
            self._l3_access[block_uuid] = now_ms
            self._l3_access.move_to_end(block_uuid)

            if self.l3_max_bytes is not None:
                tombstones = self._evict_l3_locked(headroom=sz if is_new else 0)

            if is_new:
                self._l3_bytes += sz

            # Attempt L2 insert (evict LRU L2 entries first if needed)
            self._evict_l2_locked(headroom=sz)
            if self._l2_bytes + sz <= self.l2_max_bytes:
                self._l2[block_uuid] = data
                self._l2.move_to_end(block_uuid)
                self._l2_bytes += sz
                tier = 'l2'

        for uid in tombstones:
            if self.on_tombstone:
                self.on_tombstone(uid)

        return tier

    def get(self, block_uuid: uuid.UUID) -> Tuple[Optional[bytes], Optional[str]]:
        """Fetch a block, returning ``(data, tier)`` or ``(None, None)``.

        L2 hit: returns immediately from RAM and updates LRU head.
        L3 hit: reads the NVMe file, schedules async promotion to L2,
                returns data.
        """
        # L2 check (fast path, no I/O)
        with self._lock:
            if block_uuid in self._l2:
                self._l2.move_to_end(block_uuid)
                return self._l2[block_uuid], 'l2'

        # L3 check
        fpath = self._l3_path_for(block_uuid)
        if os.path.isfile(fpath):
            try:
                with open(fpath, 'rb') as f:
                    data = f.read()
                with self._lock:
                    self._l3_access[block_uuid] = time.time() * 1000
                    self._l3_access.move_to_end(block_uuid)
                # Async promotion to L2
                if self._promote_thread is not None:
                    with self._promote_lock:
                        self._promote_queue.append((block_uuid, data))
                else:
                    self._promote_to_l2(block_uuid, data)
                return data, 'l3'
            except OSError:
                pass

        return None, None

    def peek_l2(self, block_uuid: uuid.UUID) -> Optional[bytes]:
        """Return bytes if in L2 (updating LRU), else None. No disk I/O."""
        with self._lock:
            if block_uuid in self._l2:
                self._l2.move_to_end(block_uuid)
                return self._l2[block_uuid]
        return None

    def get_l3_path(self, block_uuid: uuid.UUID) -> Optional[str]:
        """Return the NVMe file path if it exists, else None."""
        fpath = self._l3_path_for(block_uuid)
        return fpath if os.path.isfile(fpath) else None

    def evict(self, block_uuid: uuid.UUID) -> bool:
        """Remove a block from all tiers and fire the tombstone callback.

        Returns True if the block was present in any tier.
        """
        found = False
        with self._lock:
            if block_uuid in self._l2:
                self._l2_bytes -= len(self._l2.pop(block_uuid))
                found = True
            if block_uuid in self._l3_access:
                del self._l3_access[block_uuid]
                found = True

        fpath = self._l3_path_for(block_uuid)
        if os.path.isfile(fpath):
            try:
                sz = os.path.getsize(fpath)
                os.remove(fpath)
                with self._lock:
                    self._l3_bytes = max(0, self._l3_bytes - sz)
            except OSError:
                pass
            found = True

        if found and self.on_tombstone:
            self.on_tombstone(block_uuid)
        return found

    @property
    def stats(self) -> dict:
        """Snapshot of tier occupancy for monitoring."""
        with self._lock:
            return {
                'l2_entries': len(self._l2),
                'l2_bytes': self._l2_bytes,
                'l2_max_bytes': self.l2_max_bytes,
                'l3_entries': len(self._l3_access),
                'l3_bytes': self._l3_bytes,
                'l3_max_bytes': self.l3_max_bytes,
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _l3_path_for(self, block_uuid: uuid.UUID) -> str:
        return os.path.join(self.l3_path, str(block_uuid) + '.bin')

    def _scan_l3(self) -> None:
        """Populate L3 tracking from existing files on disk (startup)."""
        entries = []
        for fname in os.listdir(self.l3_path):
            if not fname.endswith('.bin'):
                continue
            fpath = os.path.join(self.l3_path, fname)
            try:
                uid = uuid.UUID(fname[:-4])
                sz = os.path.getsize(fpath)
                mtime_ms = os.path.getmtime(fpath) * 1000
                entries.append((mtime_ms, uid, sz))
            except (ValueError, OSError):
                pass
        entries.sort()  # ascending mtime = LRU first
        with self._lock:
            for mtime_ms, uid, sz in entries:
                self._l3_access[uid] = mtime_ms
                self._l3_bytes += sz

    def _evict_l2_locked(self, headroom: int) -> None:
        """Evict LRU L2 entries until ``headroom`` bytes are free. Lock held."""
        while self._l2 and self._l2_bytes + headroom > self.l2_max_bytes:
            _, data = self._l2.popitem(last=False)
            self._l2_bytes -= len(data)

    def _evict_l3_locked(self, headroom: int) -> list[uuid.UUID]:
        """Evict LRU L3 entries; returns list of tombstoned UUIDs. Lock held."""
        tombstones: list[uuid.UUID] = []
        while (self._l3_access
               and self.l3_max_bytes is not None
               and self._l3_bytes + headroom > self.l3_max_bytes):
            uid, _ = self._l3_access.popitem(last=False)
            fpath = self._l3_path_for(uid)
            try:
                sz = os.path.getsize(fpath)
                os.remove(fpath)
                self._l3_bytes = max(0, self._l3_bytes - sz)
            except OSError:
                pass
            tombstones.append(uid)
        return tombstones

    def _promote_to_l2(self, block_uuid: uuid.UUID, data: bytes) -> None:
        sz = len(data)
        with self._lock:
            if block_uuid in self._l2:
                return
            self._evict_l2_locked(headroom=sz)
            if self._l2_bytes + sz <= self.l2_max_bytes:
                self._l2[block_uuid] = data
                self._l2.move_to_end(block_uuid)
                self._l2_bytes += sz

    def _promote_worker(self) -> None:
        while True:
            time.sleep(0.05)
            with self._promote_lock:
                items = self._promote_queue[:]
                self._promote_queue.clear()
            for uid, data in items:
                self._promote_to_l2(uid, data)
