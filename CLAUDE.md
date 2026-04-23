# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

EdgeServe is a distributed streaming model-serving system that coordinates data from multiple sources and models across edge nodes. It is a research prototype from University of Chicago (ChiData). The Python API is iterator-based and built on top of Apache Pulsar as the message broker.

## Setup / dependencies

```bash
pip3 install -r requirements.txt
pip3 install -e .
```

### Two virtual environments

There are **two separate venvs** in this repo — use the right one:

| venv | Purpose | torch |
|------|---------|-------|
| `.venv/` | Everything: vLLM, EdgeServe, all benchmarks | `torch==2.10.0+cu128` |
| `.venv-sglang/` | SGLang only (`sglang==0.5.10`, `sglang-kernel==0.4.1`) | `torch==2.9.1+cu128` |

**Default: always use `.venv/`.**  
`.venv-sglang/` is only used when running SGLang-specific benchmarks. sglang-kernel 0.4.1 pins `torch==2.9.1` which is incompatible with vLLM 0.19.1 (`torch==2.10.0`), hence the split.

Recreate `.venv-sglang/` from scratch:
```bash
python3 -m venv .venv-sglang
.venv-sglang/bin/pip install torch torchvision torchaudio \
  --index-url https://download.pytorch.org/whl/cu128
.venv-sglang/bin/pip install sglang==0.5.10.post1 sglang-kernel==0.4.1 \
  --extra-index-url https://download.pytorch.org/whl/cu128
```

Runtime requires a Pulsar broker reachable at e.g. `pulsar://localhost:6650`. Launch a standalone dev broker via:

```bash
docker run -it --name pulsar -p 6650:6650 -p 8080:8080 \
   --mount source=pulsardata,target=/pulsar/data \
   --mount source=pulsarconf,target=/pulsar/conf \
   apachepulsar/pulsar:3.1.0 bin/pulsar standalone
```

Some code paths optionally require `rocksdb`, `opencv-python`, `Pillow`, and numpy/audio libs; these are not in `requirements.txt` and must be installed per-test as needed.

## Running tests

Tests under `tests/` are pytest modules that assume a running Pulsar broker on `localhost:6650` plus (for FTP/lazy-routing tests) a local FTP server writable at `/srv/ftp/`. They are integration tests, not pure unit tests — running them without these services will hang on Pulsar connects.

- Run one test file: `pytest -s tests/test_multimodal_aggr.py`
- Run one test: `pytest -s tests/test_multimodal_aggr.py::test_pipeline`

Multi-process pipelines (source → compute → materialize) are orchestrated via the tmux driver scripts `tests/multimodal_log_filter.sh` and `tests/audio_log_filter.sh`, which split a window per stage and run the corresponding pytest/python command in each pane. These are the canonical way to exercise an end-to-end pipeline locally.

## Architecture

EdgeServe models a pipeline as a directed graph of operators that communicate only through Pulsar topics. Three operator classes in `edgeserve/` are the core building blocks and all three wrap a user-provided callable plus a Pulsar client:

- `DataSource` (`data_source.py`) — wraps a Python iterator; each `next()` encodes and publishes one message. Subclasses like `CameraSource`, `AudioSource`, `SimulateTimeSeries`, `SimulateVideoWithTimestamps` adapt specific sensor/file streams into the same interface. `AudioSource` emits to *two* topics (small and large chunks) and paces yields to wall-clock time.
- `Compute` (`compute.py`) — subscribes to an input topic, buffers the latest message *per upstream `op_from`*, and invokes `task(**latest_msg)` when it has one message from every parameter the task declares (introspected via `inspect.signature`). It then publishes the result on an output topic. Join semantics: `max_time_diff_ms` bounds skew between sources; `no_overlap=True` drops state after each run; `single_input=True` fires on whichever upstream produced the most recent message; `drop_if_older_than_ms` discards late arrivals on receive; `min_interval_ms` rate-limits runs.
- `Materialize` (`materialize.py`) — terminal sink; subscribes to a topic and calls a user `materialize(data)` per message.

### Message format

All inter-operator messages use `GraphCodec` in `edgeserve/message_format.py`: a fixed-width `msg_uuid` (16B) + `op_from` (16B, null-padded) + optional header + payload. `op_from` is the upstream operator id (`source_id` or `worker_id`) and is how `Compute` maps incoming messages to task parameters. Because `op_from` is padded to 16 bytes, `source_id`/`worker_id` is constrained to ≤16 bytes.

### Lazy data routing (FTP mode)

With `ftp_out=True` on a source/compute and `ftp_in=True` on the downstream consumer, large payloads are written via pickle to `local_ftp_path` (default `/srv/ftp/`) and only the `ftp://...` URL is sent through Pulsar. Downstreams fetch with `ftp_fetch` from `edgeserve/util.py` on demand. `ftp_delete` controls whether the server-side file is removed after fetch; `ftp_memory` controls whether the fetched bytes stay in memory or land on disk.

### Logging / replay / pruning

Operators all extend `Loggable` (`edgeserve/loggable.py`), which writes per-operator logs under `log_path`:

- `.wal` — write-ahead log of joins performed and outputs produced (verbose toggle via `is_log_verbose`).
- `.orl` — on-receive log (input uuid → output uuid mapping, no payload).
- `.ftplog` — records P2P fetches under FTP mode.
- `.overhead` — logging overhead timings when `is_overhead_logged=True`.

These feed downstream machinery: `log_filter.py` filters interesting records; `log_propagate.py` (`PropagateKeepLog`) walks `.orl`/`.wal` to propagate "keep" decisions back upstream so ancestors know which of their records a downstream kept, enabling log pruning (`prune_log.py`). The pipeline scripts in `tests/` run `log_propagate.py` as its own process alongside the compute stages. A RocksDB backend exists (`write_ahead_log_to_rocksdb`, `on_receive_log_to_rocksdb`) but is opt-in and not used by default.

### Other modules

- `batch_compute.py`, `batch_model.py`, `model.py`, `worker.py`, `window.py` — additional operator/variant implementations (batching, windowing, model wrapping).
- `merge_log.py` — utilities for combining per-operator log files.
- `scheduler.py`, `scheduler_complex.py` — untracked WIP scheduler components.
- `tests/beam-comp/`, `tests/wandb-comp/` — comparison baselines against Apache Beam and wandb; not part of the core test suite.

## Semantic Cache Routing (the `llm` branch)

The LLM-specific work lives in `edgeserve/semantic_cache/` and
`edgeserve/inference/`. Key entry points:

- `edgeserve/semantic_cache/` — the core library. Bloom filter
  (`bloom.py`), header + catalog (via Pulsar), HTTP block transfer, plus
  a `SemanticCacheClient` that stitches them together. `kv_io.py`
  handles safetensors round-trips of HuggingFace `past_key_values`.
- `edgeserve/inference/hf_engine.py`, `vllm_engine.py` — `InferenceEngine`
  adapters. `VLLMEngine` accepts `kv_transfer_config=...` to plug in
  `EdgeServeKVConnector`.
- `edgeserve/inference/vllm_kv_connector.py` — **`vllm.KVConnectorBase_V1`
  backed by SemanticCacheClient**. Patterns lifted from vLLM's
  `ExampleConnector` (scheduler hashes prompt prefix, worker
  gathers/scatters per-layer KV via `slot_mapping`). Registered with
  vLLM's factory via `register()` or by passing `kv_connector_module_path`
  in `KVTransferConfig`. See `KV_CONNECTOR.md` in the repo root.
- `edgeserve/inference/llm_compute.py` — Pulsar-fed LLM operator; not
  used by any live benchmark today but preserved as the
  inference-operator primitive mirroring `Compute`.

### Benchmark honesty notes (hard-won)

- **`EdgeServeKVConnector` publishes multi-boundary prefix hashes** as
  bloom entities. If a "cold baseline" consumer runs on the SAME topic
  where another process has already published — even with a different
  suffix — the consumer's prefix-boundary lookup will hit. Always use
  a UNIQUE topic per cold-baseline subprocess, or no connector at all
  for the cold reference. `scripts/demo_kvconnector_multi_agent.py`
  shipped a bug like this early; fixed to allocate one cold topic per
  consumer. See "Benchmark honesty audit" in `RESULTS.md`.

- **vLLM's `llm.generate(prompts=[...])`** batches all N prompts via
  continuous batching; comparing it to a Python for-loop on HF is NOT
  a clean "prefix cache" isolation. For a cache-only measurement, run
  the same prompt twice on ONE vLLM instance and compare warm vs cold
  (see `scripts/probe_vllm_internal_vs_connector.py`).

- **SGLang** as of v0.5.x requires `libnuma-dev` + `libibverbs-dev` +
  gcc 10+ to build `sgl_kernel` from source. Wheels on PyPI ship SM100
  binaries only and are ABI-bound to older torch; they will not load
  on Ampere or newer-torch setups.

### Benchmarks (all under `scripts/` or `tests/`)

- `scripts/demo_kvconnector_two_stage.py` — seeder + fresh consumer
  process, both vLLM + connector. Primary correctness demo.
- `scripts/demo_kvconnector_concurrent.py` — N persistent live vLLM
  workers; consumers hit seeder's cache cross-process. Strongest
  demonstration (see RESULTS.md: 2.7× at 3 workers).
- `scripts/probe_kvconnector_e2e.py`, `probe_kvconnector_multi.py`,
  `probe_kvconnector_negative.py` — targeted validation probes.
- `scripts/bench_engines.py` — single-process baseline harness across
  HF / vLLM / SGLang (SGLang path skips on Ubuntu 20.04 gcc 9 — needs
  gcc 10+ for its flashinfer C++20 headers).
- `tests/phase3_multiproc_bench.py` — legacy multi-process HF benchmark
  (from before the connector existed); still useful for pure-HF numbers
  and the `--engine vllm` flag now plumbs through to VLLMEngine.
- `tests/test_vllm_kv_connector.py`, `test_llm_compute.py`,
  `test_llm_kv_routing.py` — pytest suite for the LLM stack (requires
  torch + transformers + vllm installed to run in full).

### Benchmark reproduction boilerplate (GPU box)

```bash
# Pulsar
docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:3.1.0 bin/pulsar standalone --no-functions-worker

# Env (in venv with torch+cu128 + vllm 0.19):
python scripts/demo_kvconnector_concurrent.py \
  --num-workers 3 --doc-repeats 256 --gpu-mem 0.25
```

## Branch convention

The main branch is `pulsar` (not `master`/`main`). Target PRs at `pulsar`.
The `llm` branch holds all Semantic Cache Routing work and diverges
significantly from `pulsar`; rebase carefully.

## Running Claude Code on the GPU box (migration)

If you want to run Claude Code directly on `swjz-ubuntu` instead of ssh'ing
from Mac:

1. Install Claude Code on Ubuntu:
   `npm install -g @anthropic-ai/claude-code` (needs Node 18+)
2. The repo is already at `~/edgeserve-llm` on swjz-ubuntu.
3. Copy permissions: `scp` the Mac's
   `~/Dropbox/coding/edgeserve-llm/.claude/settings.local.json` to
   `~/edgeserve-llm/.claude/settings.local.json` on Ubuntu.
4. Copy auto-memory: `scp -r` the Mac's
   `~/.claude/projects/-Users-swjz-Dropbox-coding-edgeserve-llm/memory/`
   to `~/.claude/projects/-home-swjz-edgeserve-llm/memory/` on Ubuntu
   (Claude Code derives the path from `cwd`; starting from
   `~/edgeserve-llm` on Ubuntu gives that slug).
5. `cd ~/edgeserve-llm && claude-code` — opens a fresh session. Point it
   at `SESSION_SUMMARY.md` + `RESULTS.md` + this file to rebuild context
   in under 30 seconds.

The in-flight conversation context does NOT migrate — the session files
under `~/.claude/projects/*/sessions/` are per-machine. Lost history is
typically fine since the repo itself carries the durable state via
commits + markdown summaries.
