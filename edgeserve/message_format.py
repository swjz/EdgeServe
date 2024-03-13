import uuid
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
        raise NotImplementedError


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


class GraphCodec:
    def __init__(self, msg_uuid_size: int = 16, op_from_size: int = 16, header_size: int = 0) -> None:
        """The GraphCodec is a codec that is designed to encode and decode messages for a graph-based system.
        The graph-based system is a directed graph where each node is an operator (data stream / model / destination).

        Args:
            msg_uuid_size (int, optional): The size of the message id. Defaults to 16.
            op_from_size (int, optional): The size of the upstream operator id. Defaults to 16.
            header_size (int, optional): The size of the task-specific header. Defaults to 0.
        """
        super().__init__()
        self.msg_uuid_size = msg_uuid_size
        self.op_from_size = op_from_size
        self.header_size = header_size

    def encode(self, msg_uuid: uuid.UUID, op_from: str, payload: bytes, header: Optional[bytes] = None) -> bytes:
        """The encode method should return a single bytes object that represents the message.
        The length of each field is fixed and determined by the `msg_uuid_size`, `op_from_size`, and `header_size`
        parameters in the constructor.

        Args:
            msg_uuid (UUID): The message UUID.
            op_from (str): The id of the upstream op.
            payload (bytes): The actual payload.
            header (Optional[bytes]): An optional header for task-specific bookkeeping.

        Returns:
            bytes: The encoded message.
        """
        if header is None:
            header = b''
        assert len(msg_uuid.bytes) == self.msg_uuid_size
        assert len(op_from) <= self.op_from_size
        assert len(header) <= self.header_size

        op_from = op_from.encode('utf-8').ljust(self.op_from_size, b'\x00')
        header = header.ljust(self.header_size, b'\x00')
        return b''.join([msg_uuid.bytes, op_from, header, payload])

    def decode(self, message_bytes: bytes) -> List[bytes]:
        """The decode method should return a list of bytes, where the first element is the message id, the second
        element is the operation from, the third element is the header, and the fourth element is the payload.

        Args:
            message_bytes (bytes): The message bytes to decode.
        """
        msg_uuid_bytes = message_bytes[:self.msg_uuid_size]
        msg_uuid = uuid.UUID(bytes=msg_uuid_bytes)
        op_from = message_bytes[self.msg_uuid_size:self.msg_uuid_size + self.op_from_size]
        header = message_bytes[
                 self.msg_uuid_size + self.op_from_size:self.msg_uuid_size + self.op_from_size + self.header_size]
        payload = message_bytes[self.msg_uuid_size + self.op_from_size + self.header_size:]

        if op_from.find(b'\x00') != -1:
            op_from = op_from[:op_from.find(b'\x00')]
        if header.find(b'\x00') != -1:
            header = header[:header.find(b'\x00')]
        return [msg_uuid, op_from.decode('utf-8'), header, payload]
