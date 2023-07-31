from typing import List, Optional

from pulsar.schema import Record, String, Bytes


class MessageFormat(Record):
    source_id = String()
    payload = Bytes(required=True)


class BaseBytesCodec:
    def __init__(self) -> None:
        pass

    def encode(self, list_of_bytes: List[bytes]) -> bytes:
        return b''.join(list_of_bytes)

    def decode(self, message_bytes: bytes) -> List[bytes]:
        pass


class NetworkCodec(BaseBytesCodec):
    def __init__(self, header_size: int) -> None:
        super().__init__()
        self.header_size = header_size

    def encode(self, list_of_bytes: List[bytes]) -> bytes:
        assert len(list_of_bytes) == 2
        return b''.join(list_of_bytes)

    def decode(self, message_bytes: bytes) -> List[bytes]:
        header_bytes = message_bytes[:self.header_size]
        data_bytes = message_bytes[self.header_size:]
        return [header_bytes, data_bytes]
