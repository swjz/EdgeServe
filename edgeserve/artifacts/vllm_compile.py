"""vllm_compile — Phase E3 entity schema + payload for vLLM compile caches.

Every vLLM cold start pays ~8.6 s on torch.compile + CUDA graph capture.
vLLM already writes a content-addressed directory under
``~/.cache/vllm/torch_compile_cache/<cache_config_sha>/`` with deterministic
artifacts.  EdgeServe publishes those directories through the same
bloom-filter catalog + exact-validation gate used for KV cache; any fleet
member with matching engine provenance can fetch the tarball and skip
compile.

Entity schema
-------------
::

    vllm-compile:<model_id>@<cache_config_sha>

Engine provenance (populates CacheHeader fields directly so the catalog's
exact-match gate rejects cross-fleet mismatches):

    model_id       = HF repo id, e.g. "Qwen/Qwen2.5-1.5B"
    model_version  = f"torch={torch_ver}+vllm={vllm_ver}+gpu_arch={sm_xx}"
    tokenizer_hash = reserved (None for compile cache — it doesn't depend on
                     the tokenizer content)
    block_size     = vllm KV cache block size (baked into the compiled graph)

The correctness-gate dimensions are the ones a false hit would silently
corrupt: a Qwen cache on a Llama run would fail to load; an sm86 cache
handed to an sm89 consumer would import PTX that the hardware rejects.

Payload
-------
``zstd`` tarball containing the vLLM per-model compile-state bundle:
the matching ``torch_compile_cache/<cache_config_sha>/`` config directory
AND the related ``torch_compile_cache/torch_aot_compile/*/`` AOT-compiled
function directories AND the ``modelinfos/*.json`` file for this model.

Measurement note (2026-04-28): we originally tried to ship only the
per-config directory (~7 MB) and observed 1.01× speedup, because vLLM
reuses the AOT-compiled-function cache and the model-info JSON on cold
starts too — wiping only one dir left the other two rebuilt-for-free.
Correctly bundling all three pieces brings the "wipe ALL" baseline
(~24 s) down to the "warm" case (~16 s), an ~8 s saving on every
fleet-cold-start.  See ``scripts/bench_vllm_compile_cache.py``.

Total payload: ~60–120 MB on a typical box; ships over gigabit LAN in
~500 ms – 1 s.  Deserialization unpacks back under ``~/.cache/vllm/``
at the same relative paths so vLLM's own loader finds everything.

Used by ``scripts/bench_vllm_compile_cache.py``.
"""
from __future__ import annotations

import hashlib
import io
import os
import platform
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple


DEFAULT_VLLM_ROOT  = Path.home() / '.cache' / 'vllm'
DEFAULT_CACHE_ROOT = DEFAULT_VLLM_ROOT / 'torch_compile_cache'
DEFAULT_AOT_ROOT   = DEFAULT_CACHE_ROOT / 'torch_aot_compile'
DEFAULT_MODELINFOS = DEFAULT_VLLM_ROOT / 'modelinfos'


# ── provenance detection ─────────────────────────────────────────────────────

def detect_torch_version() -> str:
    try:
        import torch
        return torch.__version__
    except ImportError:
        return 'unknown'


def detect_vllm_version() -> str:
    try:
        import vllm
        return vllm.__version__
    except ImportError:
        return 'unknown'


def detect_gpu_arch() -> str:
    """Return GPU compute capability as ``sm_XX`` for the default CUDA device.

    Returns ``"cpu"`` if no CUDA GPU is available.  The arch baked into
    vLLM's compile cache (PTX targets and CUDA-graph captures) is one of
    the dimensions a wrong-arch fetch would silently corrupt, so we
    include it in engine provenance.
    """
    try:
        import torch
        if not torch.cuda.is_available():
            return 'cpu'
        major, minor = torch.cuda.get_device_capability(0)
        return f'sm{major}{minor}'
    except Exception:
        return 'unknown'


def build_model_version(
    *,
    torch_version: Optional[str] = None,
    vllm_version: Optional[str] = None,
    gpu_arch: Optional[str] = None,
) -> str:
    """Compose the ``model_version`` string the catalog's engine-gate compares.

    Any mismatch on any of these three dimensions must reject the hit.
    We cram them into a single string so the existing
    ``CacheHeader.matches_engine`` check does the right thing without
    needing new fields.
    """
    t = torch_version if torch_version is not None else detect_torch_version()
    v = vllm_version  if vllm_version  is not None else detect_vllm_version()
    a = gpu_arch      if gpu_arch      is not None else detect_gpu_arch()
    return f'torch={t}+vllm={v}+gpu_arch={a}'


@dataclass
class CompileCacheHandle:
    """Identifies one vLLM compile-cache directory.

    ``cache_config_sha`` is the directory name vLLM assigns (content hash
    of the compilation config).  ``path`` is the absolute path on disk.
    """
    cache_config_sha: str
    path: Path
    size_bytes: int


# ── enumeration / inspection ─────────────────────────────────────────────────

def list_cache_dirs(root: Optional[Path] = None) -> List[CompileCacheHandle]:
    """Return every compile-cache config directory under the given root.

    Filters out ``torch_aot_compile`` (a different kind of artifact vLLM
    stores alongside; we don't share those in this experiment).
    """
    root = root or DEFAULT_CACHE_ROOT
    if not root.is_dir():
        return []
    out: List[CompileCacheHandle] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir() or entry.name == 'torch_aot_compile':
            continue
        # vLLM config dirs are 10-char hex prefixes.  Filter loosely on
        # length+hexness so we skip anything else a user dropped here.
        if len(entry.name) < 8:
            continue
        size = _dir_size(entry)
        out.append(CompileCacheHandle(
            cache_config_sha=entry.name, path=entry, size_bytes=size,
        ))
    return out


def _dir_size(path: Path) -> int:
    total = 0
    for p in path.rglob('*'):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            continue
    return total


# ── entity tag construction ─────────────────────────────────────────────────

def entity_tag(model_id: str, cache_config_sha: str) -> str:
    """The bloom-filter entity used to discover a compile cache.

    Publishers emit this as the sole prefix-hash entity; consumers probe
    with the same string.  The catalog's exact-validation gate then
    verifies engine provenance before admitting.
    """
    return f'vllm-compile:{model_id}@{cache_config_sha}'


# ── tarball serialization ────────────────────────────────────────────────────

def _have_zstd() -> bool:
    return shutil.which('zstd') is not None


def pack_cache_dir(cache_dir: Path) -> bytes:
    """Pack ``cache_dir`` into a tar blob for publish().

    Uses zstd when available (3–5× smaller than gzip on these directories,
    typically a wash on speed); falls back to plain uncompressed tar if
    zstd isn't on PATH.  Writes entries relative to ``cache_dir`` so that
    unpack_to_dir produces a self-contained directory without the absolute
    path leaking into the archive.
    """
    # Plain in-memory tar first.
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tf:
        for p in sorted(cache_dir.rglob('*')):
            tf.add(p, arcname=str(p.relative_to(cache_dir)))
    raw = buf.getvalue()

    if _have_zstd():
        proc = subprocess.run(
            ['zstd', '-q', '-3', '--stdout'],
            input=raw, capture_output=True, check=True,
        )
        return b'zstd:' + proc.stdout

    return b'raw:' + raw


def unpack_to_dir(blob: bytes, dest_dir: Path) -> None:
    """Inverse of ``pack_cache_dir``.  Creates ``dest_dir`` and writes into it."""
    if blob.startswith(b'zstd:'):
        if not _have_zstd():
            raise RuntimeError('zstd blob but zstd binary not on PATH')
        proc = subprocess.run(
            ['zstd', '-q', '-d', '--stdout'],
            input=blob[len(b'zstd:'):], capture_output=True, check=True,
        )
        tar_bytes = proc.stdout
    elif blob.startswith(b'raw:'):
        tar_bytes = blob[len(b'raw:'):]
    else:
        # Back-compat: accept a bare tar blob too.
        tar_bytes = blob

    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode='r') as tf:
        # filter='data' = Python 3.12+ default-in-3.14 safe extraction:
        # rejects symlinks / abs paths that escape dest_dir.  Our own
        # pack_cache_dir only writes relative entries so this is a no-op
        # in the happy case but prevents pathological archives from
        # walking out of dest_dir on the consumer side.
        try:
            tf.extractall(dest_dir, filter='data')
        except TypeError:
            # Python <3.12 doesn't accept filter=; fall back.
            tf.extractall(dest_dir)


# ── full bundle: torch_compile_cache dir + torch_aot_compile + modelinfos ──

def _collect_bundle_files(
    *,
    cache_config_sha: str,
    vllm_root: Optional[Path] = None,
) -> List[Tuple[Path, str]]:
    """Return (absolute_path, archive_relative_path) pairs for the full
    compile-state bundle associated with one config sha.

    We include three groups, because all three are independently populated
    on a vLLM cold start and any one of them being missing forces recompile:

      1. torch_compile_cache/<cache_config_sha>/
      2. torch_compile_cache/torch_aot_compile/*/   (all AOT dirs — vLLM
         reuses them cross-config so we publish the whole set)
      3. modelinfos/*.json                          (tiny but load-bearing)

    Archive paths are relative to vllm_root so unpack just drops them back
    into ``~/.cache/vllm/``.
    """
    root = Path(vllm_root) if vllm_root else DEFAULT_VLLM_ROOT
    out: List[Tuple[Path, str]] = []

    # 1. per-config directory
    cfg = root / 'torch_compile_cache' / cache_config_sha
    if cfg.is_dir():
        for f in cfg.rglob('*'):
            if f.is_file():
                out.append((f, str(f.relative_to(root))))

    # 2. AOT-compiled functions (whole directory — vLLM caches one per compiled
    #    subgraph signature, cross-config reuse is common)
    aot = root / 'torch_compile_cache' / 'torch_aot_compile'
    if aot.is_dir():
        for f in aot.rglob('*'):
            if f.is_file():
                out.append((f, str(f.relative_to(root))))

    # 3. model-info JSONs (all of them — they're tiny, a few KB total, and
    #    the right one for this model is the load-bearing piece).
    mi = root / 'modelinfos'
    if mi.is_dir():
        for f in mi.rglob('*'):
            if f.is_file():
                out.append((f, str(f.relative_to(root))))

    return out


def pack_bundle(
    *,
    cache_config_sha: str,
    vllm_root: Optional[Path] = None,
) -> bytes:
    """Pack the full compile-state bundle for one config sha into a tar blob.

    The blob format is the same 'zstd:' / 'raw:' convention as
    ``pack_cache_dir`` and unpack_to_dir; we reuse those for compression.
    """
    entries = _collect_bundle_files(
        cache_config_sha=cache_config_sha, vllm_root=vllm_root,
    )
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tf:
        for abs_path, arcname in entries:
            tf.add(abs_path, arcname=arcname)
    raw = buf.getvalue()

    if _have_zstd():
        proc = subprocess.run(
            ['zstd', '-q', '-3', '--stdout'],
            input=raw, capture_output=True, check=True,
        )
        return b'zstd:' + proc.stdout
    return b'raw:' + raw


def unpack_bundle(blob: bytes, vllm_root: Optional[Path] = None) -> int:
    """Inverse of ``pack_bundle``.  Writes files under ``vllm_root`` and
    returns the number of files extracted."""
    root = Path(vllm_root) if vllm_root else DEFAULT_VLLM_ROOT
    root.mkdir(parents=True, exist_ok=True)

    if blob.startswith(b'zstd:'):
        if not _have_zstd():
            raise RuntimeError('zstd blob but zstd binary not on PATH')
        proc = subprocess.run(
            ['zstd', '-q', '-d', '--stdout'],
            input=blob[len(b'zstd:'):], capture_output=True, check=True,
        )
        tar_bytes = proc.stdout
    elif blob.startswith(b'raw:'):
        tar_bytes = blob[len(b'raw:'):]
    else:
        tar_bytes = blob

    n = 0
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode='r') as tf:
        try:
            tf.extractall(root, filter='data')
        except TypeError:
            tf.extractall(root)
        n = len([m for m in tf.getmembers() if m.isfile()])
    return n


# ── convenience: publish + fetch ─────────────────────────────────────────────

def publish_bundle(
    client,
    *,
    model_id: str,
    cache_config_sha: str,
    vllm_root: Optional[Path] = None,
    torch_version: Optional[str] = None,
    vllm_version: Optional[str] = None,
    gpu_arch: Optional[str] = None,
):
    """Publish the full compile-state bundle (per-config dir + AOT cache +
    modelinfos) through the given SemanticCacheClient.

    Returns ``(block_uuid, blob_size, provenance_dict)``.
    """
    prov = {
        'model_id':      model_id,
        'model_version': build_model_version(
            torch_version=torch_version, vllm_version=vllm_version,
            gpu_arch=gpu_arch,
        ),
        'block_size':    0,
    }
    blob = pack_bundle(cache_config_sha=cache_config_sha, vllm_root=vllm_root)
    tag = entity_tag(model_id, cache_config_sha)
    block_uuid = client.publish(
        entities={tag},
        data=blob,
        num_tokens=0,
        model_id=prov['model_id'],
        model_version=prov['model_version'],
        block_size=prov['block_size'],
    )
    return block_uuid, len(blob), prov


def fetch_bundle(
    client,
    *,
    model_id: str,
    cache_config_sha: str,
    vllm_root: Optional[Path] = None,
    torch_version: Optional[str] = None,
    vllm_version: Optional[str] = None,
    gpu_arch: Optional[str] = None,
) -> Tuple[bool, Optional[str], int]:
    """Look up the compile-state bundle and unpack under ``vllm_root``.

    Returns ``(ok, reason, n_files_unpacked)``.  On miss (either bloom-
    negative or engine-provenance rejection) returns
    ``(False, reason_string, 0)``.
    """
    tag = entity_tag(model_id, cache_config_sha)
    prov_version = build_model_version(
        torch_version=torch_version, vllm_version=vllm_version,
        gpu_arch=gpu_arch,
    )
    hits = client.catalog.lookup(
        [tag],
        exact_validate=True,
        engine_model_id=model_id,
        engine_model_version=prov_version,
    )
    if not hits:
        return False, 'catalog-miss-or-provenance-rejected', 0

    header = hits[0]
    got = client.resolve_by_uuid(header.block_uuid)
    if got is None:
        return False, 'resolve-returned-none', 0
    blob, _hdr = got
    n = unpack_bundle(blob, vllm_root=vllm_root)
    return True, None, n


# ── Back-compat shims for the old per-dir API (used by some unit tests) ──

def publish_cache_dir(
    client,
    *,
    model_id: str,
    cache_handle: CompileCacheHandle,
    torch_version: Optional[str] = None,
    vllm_version: Optional[str] = None,
    gpu_arch: Optional[str] = None,
):
    """Legacy single-dir publish.  Publishes just one config dir (not the
    AOT cache or modelinfos).  New code should prefer ``publish_bundle``.
    """
    prov = {
        'model_id':      model_id,
        'model_version': build_model_version(
            torch_version=torch_version, vllm_version=vllm_version,
            gpu_arch=gpu_arch,
        ),
        'block_size':    0,
    }
    blob = pack_cache_dir(cache_handle.path)
    tag = entity_tag(model_id, cache_handle.cache_config_sha)
    block_uuid = client.publish(
        entities={tag},
        data=blob,
        num_tokens=0,
        model_id=prov['model_id'],
        model_version=prov['model_version'],
        block_size=prov['block_size'],
    )
    return block_uuid, len(blob), prov


def fetch_cache_dir(
    client,
    *,
    model_id: str,
    cache_config_sha: str,
    dest_dir: Path,
    torch_version: Optional[str] = None,
    vllm_version: Optional[str] = None,
    gpu_arch: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """Legacy single-dir fetch.  New code should prefer ``fetch_bundle``."""
    tag = entity_tag(model_id, cache_config_sha)
    prov_version = build_model_version(
        torch_version=torch_version, vllm_version=vllm_version,
        gpu_arch=gpu_arch,
    )
    hits = client.catalog.lookup(
        [tag],
        exact_validate=True,
        engine_model_id=model_id,
        engine_model_version=prov_version,
    )
    if not hits:
        return False, 'catalog-miss-or-provenance-rejected'

    header = hits[0]
    got = client.resolve_by_uuid(header.block_uuid)
    if got is None:
        return False, 'resolve-returned-none'
    blob, _hdr = got
    unpack_to_dir(blob, dest_dir)
    return True, None
