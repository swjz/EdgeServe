"""Semantic Cache Routing for EdgeServe.

Modules that talk to Pulsar (catalog, publisher, client) are lazy-imported so
that `edgeserve.semantic_cache.mock_pulsar.install()` can run before the real
`pulsar` module is loaded.
"""

import importlib

from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.header import CacheHeader
from edgeserve.semantic_cache.http_client import http_fetch
from edgeserve.semantic_cache.http_server import CacheHttpServer

_LAZY = {
    'HeaderCatalog': 'edgeserve.semantic_cache.catalog',
    'HeaderPublisher': 'edgeserve.semantic_cache.publisher',
    'SemanticCacheClient': 'edgeserve.semantic_cache.client',
}

__all__ = [
    'SemanticBloomFilter',
    'CacheHeader',
    'HeaderCatalog',
    'HeaderPublisher',
    'SemanticCacheClient',
    'CacheHttpServer',
    'http_fetch',
]


def __getattr__(name):
    mod_path = _LAZY.get(name)
    if mod_path is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    return getattr(importlib.import_module(mod_path), name)
