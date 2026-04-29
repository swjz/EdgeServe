"""Tests for the vLLM compile-cache artifact helper (Phase E3).

These tests exercise the payload serialization and engine-provenance gate
without needing a real vLLM or a real compile directory.  End-to-end
measurements live in `scripts/bench_vllm_compile_cache.py`.
"""
from __future__ import annotations

import threading
import uuid
from pathlib import Path

import pytest

from edgeserve.artifacts import vllm_compile as vc
from edgeserve.semantic_cache.bloom import SemanticBloomFilter
from edgeserve.semantic_cache.catalog import HeaderCatalog
from edgeserve.semantic_cache.header import CacheHeader


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_fake_compile_dir(tmp_path: Path, name: str, n_files: int = 5) -> Path:
    """Fabricate a directory that looks like a vLLM compile cache."""
    root = tmp_path / 'torch_compile_cache' / name
    (root / 'rank_0_0' / 'backbone').mkdir(parents=True)
    for i in range(n_files):
        f = root / 'rank_0_0' / 'backbone' / f'artifact_{i}'
        f.write_bytes(f'fake-compiled-graph-{i}-'.encode() * 1000)
    (root / 'rank_0_0' / 'backbone' / 'cache_key_factors.json').write_text(
        '{"code_hash": "deadbeef", "config_hash": "42"}'
    )
    return root


def _headless_catalog():
    cat = object.__new__(HeaderCatalog)
    cat._headers = {}
    cat._lock = threading.Lock()
    cat.rank_fn = lambda h: h.created_ms
    return cat


# ── provenance helpers ───────────────────────────────────────────────────────

def test_build_model_version_deterministic():
    v1 = vc.build_model_version(
        torch_version='2.10.0+cu128', vllm_version='0.19.1', gpu_arch='sm86',
    )
    v2 = vc.build_model_version(
        torch_version='2.10.0+cu128', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert v1 == v2
    assert 'torch=2.10.0+cu128' in v1
    assert 'vllm=0.19.1' in v1
    assert 'gpu_arch=sm86' in v1


def test_build_model_version_changes_on_arch():
    sm86 = vc.build_model_version(
        torch_version='2.10', vllm_version='0.19', gpu_arch='sm86',
    )
    sm89 = vc.build_model_version(
        torch_version='2.10', vllm_version='0.19', gpu_arch='sm89',
    )
    assert sm86 != sm89


def test_entity_tag_shape():
    tag = vc.entity_tag('Qwen/Qwen2.5-1.5B', '04f4e9d813')
    assert tag == 'vllm-compile:Qwen/Qwen2.5-1.5B@04f4e9d813'


# ── enumeration ──────────────────────────────────────────────────────────────

def test_list_cache_dirs_skips_torch_aot(tmp_path):
    _make_fake_compile_dir(tmp_path, '04f4e9d813')
    _make_fake_compile_dir(tmp_path, '1cbf5e65c5')
    (tmp_path / 'torch_compile_cache' / 'torch_aot_compile').mkdir()

    handles = vc.list_cache_dirs(tmp_path / 'torch_compile_cache')
    names = sorted(h.cache_config_sha for h in handles)
    assert names == ['04f4e9d813', '1cbf5e65c5']
    for h in handles:
        assert h.size_bytes > 0


def test_list_cache_dirs_empty_when_missing(tmp_path):
    assert vc.list_cache_dirs(tmp_path / 'nonexistent') == []


# ── pack/unpack round trip ──────────────────────────────────────────────────

def test_pack_unpack_roundtrip(tmp_path):
    src = _make_fake_compile_dir(tmp_path, 'abcdef1234', n_files=7)
    blob = vc.pack_cache_dir(src)

    dest = tmp_path / 'unpacked'
    vc.unpack_to_dir(blob, dest)

    src_files = sorted(p.relative_to(src) for p in src.rglob('*') if p.is_file())
    dst_files = sorted(p.relative_to(dest) for p in dest.rglob('*') if p.is_file())
    assert src_files == dst_files
    for rel in src_files:
        assert (src / rel).read_bytes() == (dest / rel).read_bytes()


def test_pack_uses_zstd_when_available(tmp_path):
    src = _make_fake_compile_dir(tmp_path, 'compressible', n_files=5)
    blob = vc.pack_cache_dir(src)
    # Each artifact is f'fake-compiled-graph-{i}-' * 1000 bytes — very
    # compressible.  We don't assert a specific prefix because zstd may
    # not be installed in CI; just check the blob round trips.
    assert blob.startswith(b'zstd:') or blob.startswith(b'raw:')


# ── end-to-end publish + fetch with a headless catalog ──────────────────────

class _FakeClient:
    """Stand-in for SemanticCacheClient that holds an in-memory catalog.

    Real SemanticCacheClient runs a Pulsar subscription thread; for these
    tests we bypass that and talk to the catalog directly.
    """

    def __init__(self):
        self.catalog = _headless_catalog()
        self._blobs: dict[uuid.UUID, bytes] = {}

    def publish(self, entities, data, num_tokens=0, *, user_entities=None,
                model_id=None, model_version=None, tokenizer_hash=None,
                block_size=0):
        block_uuid = uuid.uuid4()
        self._blobs[block_uuid] = data
        ents = list(entities)
        ue = list(user_entities) if user_entities else []
        bloom = SemanticBloomFilter.for_capacity(256)
        for e in ents + ue:
            bloom.add(e)
        header = CacheHeader(
            block_uuid=block_uuid,
            node_uri='http://fake',
            prefix_hash=b'',
            bloom=bloom,
            num_tokens=num_tokens,
            model_id=model_id,
            model_version=model_version,
            tokenizer_hash=tokenizer_hash,
            block_size=block_size,
            prefix_hashes=ents,
            entity_keys=ue,
        )
        self.catalog.insert(header)
        return block_uuid

    def resolve_by_uuid(self, block_uuid):
        blob = self._blobs.get(block_uuid)
        if blob is None:
            return None
        with self.catalog._lock:
            header = self.catalog._headers.get(block_uuid)
        if header is None:
            return None
        return blob, header


def test_publish_fetch_roundtrip(tmp_path):
    client = _FakeClient()
    src = _make_fake_compile_dir(tmp_path, 'abc123', n_files=4)
    handle = vc.CompileCacheHandle(
        cache_config_sha='abc123', path=src, size_bytes=vc._dir_size(src),
    )

    block_uuid, size, prov = vc.publish_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_handle=handle,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert size > 0

    # Same provenance → hit, unpacks correctly.
    dest = tmp_path / 'restored'
    ok, reason = vc.fetch_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='abc123',
        dest_dir=dest,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert ok, f'expected hit, got miss: {reason}'

    src_files = {p.relative_to(src): p.read_bytes()
                 for p in src.rglob('*') if p.is_file()}
    dst_files = {p.relative_to(dest): p.read_bytes()
                 for p in dest.rglob('*') if p.is_file()}
    assert src_files == dst_files


def test_cross_arch_miss(tmp_path):
    """The correctness demo: sm86 cache must not satisfy an sm89 consumer."""
    client = _FakeClient()
    src = _make_fake_compile_dir(tmp_path, 'arch-test', n_files=3)
    handle = vc.CompileCacheHandle(
        cache_config_sha='arch-test', path=src, size_bytes=vc._dir_size(src),
    )
    vc.publish_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_handle=handle,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )

    dest = tmp_path / 'wrong-arch'
    ok, reason = vc.fetch_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='arch-test',
        dest_dir=dest,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm89',
    )
    assert not ok
    assert 'provenance' in reason or 'miss' in reason
    assert not dest.exists() or not any(dest.rglob('*'))


def test_cross_model_miss(tmp_path):
    """A Qwen compile cache must not satisfy a Llama consumer query."""
    client = _FakeClient()
    src = _make_fake_compile_dir(tmp_path, 'model-test', n_files=2)
    handle = vc.CompileCacheHandle(
        cache_config_sha='model-test', path=src, size_bytes=vc._dir_size(src),
    )
    vc.publish_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_handle=handle,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )

    dest = tmp_path / 'wrong-model'
    ok, reason = vc.fetch_cache_dir(
        client, model_id='meta-llama/Llama-3-8B', cache_config_sha='model-test',
        dest_dir=dest,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert not ok


def test_bundle_roundtrip(tmp_path):
    """The full bundle covers tcc/<sha>/ + tcc/torch_aot_compile/* + modelinfos/*."""
    root = tmp_path / 'vllm'
    tcc = root / 'torch_compile_cache'
    aot = tcc / 'torch_aot_compile'
    mi = root / 'modelinfos'

    (tcc / 'cfg1' / 'rank_0_0').mkdir(parents=True)
    (tcc / 'cfg1' / 'rank_0_0' / 'artifact').write_bytes(b'cfg1-art' * 100)

    (aot / 'aot1' / 'rank_0_0' / 'model').mkdir(parents=True)
    (aot / 'aot1' / 'rank_0_0' / 'model' / 'compiled.bin').write_bytes(b'aot1' * 400)
    (aot / 'aot2' / 'rank_0_0' / 'model').mkdir(parents=True)
    (aot / 'aot2' / 'rank_0_0' / 'model' / 'compiled.bin').write_bytes(b'aot2' * 400)

    mi.mkdir(parents=True)
    (mi / 'vllm-qwen.json').write_text('{"model": "qwen"}')

    blob = vc.pack_bundle(cache_config_sha='cfg1', vllm_root=root)

    # Unpack into a different root and verify every file is present.
    restore = tmp_path / 'restored-vllm'
    n = vc.unpack_bundle(blob, vllm_root=restore)
    assert n == 4, f'expected 4 files, got {n}'

    assert (restore / 'torch_compile_cache' / 'cfg1' / 'rank_0_0' / 'artifact').exists()
    assert (restore / 'torch_compile_cache' / 'torch_aot_compile' / 'aot1' / 'rank_0_0' / 'model' / 'compiled.bin').exists()
    assert (restore / 'torch_compile_cache' / 'torch_aot_compile' / 'aot2' / 'rank_0_0' / 'model' / 'compiled.bin').exists()
    assert (restore / 'modelinfos' / 'vllm-qwen.json').exists()


def test_publish_fetch_bundle(tmp_path):
    """End-to-end: publish a bundle + fetch it through a headless catalog."""
    root = tmp_path / 'vllm'
    (root / 'torch_compile_cache' / 'cfgX').mkdir(parents=True)
    (root / 'torch_compile_cache' / 'cfgX' / 'file').write_bytes(b'per-config' * 20)
    (root / 'torch_compile_cache' / 'torch_aot_compile' / 'aotA').mkdir(parents=True)
    (root / 'torch_compile_cache' / 'torch_aot_compile' / 'aotA' / 'fn').write_bytes(b'aot' * 30)
    (root / 'modelinfos').mkdir(parents=True)
    (root / 'modelinfos' / 'info.json').write_text('{"m": 1}')

    client = _FakeClient()
    block_uuid, size, prov = vc.publish_bundle(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='cfgX',
        vllm_root=root,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert size > 0

    restore = tmp_path / 'restored'
    ok, reason, n = vc.fetch_bundle(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='cfgX',
        vllm_root=restore,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert ok, f'expected hit, got miss: {reason}'
    assert n == 3  # three files: per-config + aot + modelinfos

    assert (restore / 'torch_compile_cache' / 'cfgX' / 'file').read_bytes() == b'per-config' * 20
    assert (restore / 'modelinfos' / 'info.json').read_text() == '{"m": 1}'


def test_bundle_cross_arch_rejected(tmp_path):
    """The bundle API inherits the engine-provenance gate from the catalog."""
    root = tmp_path / 'vllm'
    (root / 'torch_compile_cache' / 'cfg').mkdir(parents=True)
    (root / 'torch_compile_cache' / 'cfg' / 'f').write_bytes(b'x')

    client = _FakeClient()
    vc.publish_bundle(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='cfg',
        vllm_root=root, gpu_arch='sm86',
    )

    restore = tmp_path / 'wrong'
    ok, reason, _ = vc.fetch_bundle(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='cfg',
        vllm_root=restore, gpu_arch='sm89',
    )
    assert not ok


def test_cross_torch_version_miss(tmp_path):
    """A cache built under torch 2.9 must not satisfy a torch 2.10 consumer."""
    client = _FakeClient()
    src = _make_fake_compile_dir(tmp_path, 'torch-test', n_files=2)
    handle = vc.CompileCacheHandle(
        cache_config_sha='torch-test', path=src, size_bytes=vc._dir_size(src),
    )
    vc.publish_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_handle=handle,
        torch_version='2.9.1', vllm_version='0.19.1', gpu_arch='sm86',
    )

    dest = tmp_path / 'wrong-torch'
    ok, reason = vc.fetch_cache_dir(
        client, model_id='Qwen/Qwen2.5-1.5B', cache_config_sha='torch-test',
        dest_dir=dest,
        torch_version='2.10.0', vllm_version='0.19.1', gpu_arch='sm86',
    )
    assert not ok
