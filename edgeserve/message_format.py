from pulsar.schema import Record, String, Bytes


class MessageFormat(Record):
    source_id = String()
    payload = Bytes(required=True)
