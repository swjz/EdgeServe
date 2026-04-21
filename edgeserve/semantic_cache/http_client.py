import uuid
from urllib.request import urlopen


def http_fetch(node_uri: str, block_uuid: uuid.UUID, timeout: float = 5.0) -> bytes:
    """Retrieve a cache block from the given node URI. Raises on non-200."""
    url = f'{node_uri.rstrip("/")}/cache/{block_uuid}'
    with urlopen(url, timeout=timeout) as resp:
        if resp.status != 200:
            raise IOError(f'cache fetch failed: {resp.status} for {url}')
        return resp.read()
