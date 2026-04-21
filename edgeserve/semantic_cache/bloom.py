import hashlib
import math
from typing import Optional


class SemanticBloomFilter:
    """Fixed-size bit-array Bloom filter for semantic entity tags.

    Hashes are derived via Kirsch-Mitzenmacher (h_i = h1 + i*h2 mod m) from two
    BLAKE2b digests with distinct salts. Stdlib only.
    """

    SALT = b'edgeserv'  # blake2b salt max 16 bytes; 8 is plenty

    def __init__(self, m_bits: int = 16384, k: int = 7, _buf: Optional[bytearray] = None):
        if m_bits % 8 != 0:
            raise ValueError('m_bits must be a multiple of 8')
        self.m = m_bits
        self.k = k
        self._buf = bytearray(m_bits // 8) if _buf is None else _buf

    @classmethod
    def for_capacity(cls, n: int, fpr: float = 0.01) -> 'SemanticBloomFilter':
        m = max(8, int(-n * math.log(fpr) / (math.log(2) ** 2)))
        m = (m + 7) & ~7  # round up to byte
        k = max(1, int(round((m / n) * math.log(2))))
        return cls(m_bits=m, k=k)

    def _positions(self, entity: str):
        # Generate k independent positions by chunking enough blake2b digest bytes.
        # Kirsch-Mitzenmacher is avoided because power-of-two m + even step
        # collapses the sequence; independent hashes give uniform spread.
        data = entity.encode('utf-8')
        needed = self.k * 8
        buf = b''
        counter = 0
        while len(buf) < needed:
            buf += hashlib.blake2b(
                data + counter.to_bytes(2, 'big'),
                digest_size=min(64, needed - len(buf)),
                salt=self.SALT,
            ).digest()
            counter += 1
        for i in range(self.k):
            yield int.from_bytes(buf[i * 8:(i + 1) * 8], 'big') % self.m

    def add(self, entity: str) -> None:
        for pos in self._positions(entity):
            self._buf[pos >> 3] |= 1 << (pos & 7)

    def __contains__(self, entity: str) -> bool:
        for pos in self._positions(entity):
            if not (self._buf[pos >> 3] & (1 << (pos & 7))):
                return False
        return True

    def to_bytes(self) -> bytes:
        # 4 bytes m_bits (big-endian), 2 bytes k, then the bit buffer.
        return self.m.to_bytes(4, 'big') + self.k.to_bytes(2, 'big') + bytes(self._buf)

    @classmethod
    def from_bytes(cls, blob: bytes) -> 'SemanticBloomFilter':
        m = int.from_bytes(blob[:4], 'big')
        k = int.from_bytes(blob[4:6], 'big')
        buf = bytearray(blob[6:])
        if len(buf) * 8 != m:
            raise ValueError('bloom filter size mismatch')
        return cls(m_bits=m, k=k, _buf=buf)
