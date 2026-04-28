"""bench_multiturn.py — Phase 6.2: multi-turn conversation.

Extends the Phase 6 edge-inference demo to a two-turn dialogue and shows
that the KV-cache CDN compounds across turns: after turn 1, the
conversation prefix is (doc + Q1 + A1); turn 2's cold region is only
(Q1 + A1 + Q2_suffix) — a few hundred tokens, not the full document.

Compared paths for turn 2:

  B0 (cloud-API-style / Mac local):
      prefill (doc + Q1 + A1 + Q2) from scratch; N tokens prefilled.

  EdgeServe (CDN compounds across turns):
      fetch doc KV from GPU box ONCE (amortised over all turns),
      on turn 2 prefill only (Q1 + A1 + Q2_suffix) on Mac — a small
      delta — then decode the answer.

Runs on a single machine (GPU box) using same-host mmap transport so
the benchmark is self-contained and deterministic.  The wire-transfer
latency for the initial doc fetch is measured separately in Phase 6;
this script focuses on the per-turn decode story.

Usage
-----
  python scripts/bench_multiturn.py
  python scripts/bench_multiturn.py --doc-repeats 64 --turns 3
"""
from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
import time
import uuid as _uuid_mod

DOC_CHUNK = (
    "The history of artificial intelligence spans decades of research, "
    "breakthrough, and setback.  From early symbolic systems to modern "
    "deep learning, the field has transformed computing and society.  "
    "Researchers have long debated the nature of intelligence itself, "
    "whether machines can truly think, and what it means to understand "
    "language.  Large language models represent the latest chapter in "
    "this ongoing story, raising new questions about creativity, bias, "
    "and the future of human-machine collaboration.  "
)

TURN_QUESTIONS = [
    "What are the main themes discussed in this passage?",
    "Which of those themes has been most debated historically, and why?",
    "What open questions does this raise for future research?",
    "How might these concerns evolve in the next decade?",
]


def _doc_text(repeats: int) -> str:
    return DOC_CHUNK * repeats


def _boundary_hashes(token_ids, block_size: int = 16):
    import torch
    n = len(token_ids)
    boundaries = list(range(block_size, n + 1, block_size))
    return [
        hashlib.sha256(
            torch.tensor(token_ids[:b], dtype=torch.long).numpy().tobytes()
        ).hexdigest()
        for b in reversed(boundaries)
    ]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--model', default='Qwen/Qwen2.5-1.5B')
    ap.add_argument('--doc-repeats', type=int, default=64)
    ap.add_argument('--turns', type=int, default=3,
                    help='Number of conversation turns (>=2)')
    ap.add_argument('--max-new-tokens', type=int, default=40)
    ap.add_argument('--pulsar-url', default='pulsar://localhost:6650')
    args = ap.parse_args()

    assert args.turns >= 2, 'multi-turn needs at least 2 turns'
    assert args.turns <= len(TURN_QUESTIONS), f'max {len(TURN_QUESTIONS)} turns'

    import torch
    from edgeserve.inference.hf_engine import HFEngine
    from edgeserve.semantic_cache.client import SemanticCacheClient

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    dtype = torch.bfloat16 if device == 'cuda' else torch.float32

    print('=' * 60)
    print('EdgeServe multi-turn conversation benchmark  (Phase 6.2)')
    print('=' * 60)
    print(f'Model:        {args.model}')
    print(f'Device:       {device}  dtype={dtype}')
    print(f'Doc repeats:  {args.doc_repeats}')
    print(f'Turns:        {args.turns}')
    print()

    doc = _doc_text(args.doc_repeats)

    # ── Step 1: seed doc KV (one time cost, amortised across turns) ────────
    print('Loading engine + seeding doc KV ...')
    engine = HFEngine(args.model, device=device, dtype=dtype)
    doc_tokens = engine.tokenize(doc)
    print(f'  doc tokens: {len(doc_tokens)}')

    t_seed = time.perf_counter()
    doc_kv = engine.prefill(doc_tokens)
    seed_ms = (time.perf_counter() - t_seed) * 1000
    print(f'  seed prefill: {seed_ms:.0f} ms')

    # Publish via TieredStore for same-host mmap fetch
    kv_bytes = engine.serialize_cache(doc_kv)
    del doc_kv
    cache_path = tempfile.mkdtemp(prefix='edgeserve-multiturn-')
    topic = f'kvcache-multiturn-{_uuid_mod.uuid4().hex[:8]}'
    client = SemanticCacheClient(
        pulsar_node=args.pulsar_url,
        node_id='multiturn-seed',
        local_cache_path=cache_path,
        topic=topic,
    )
    entities = _boundary_hashes(doc_tokens)
    client.publish(entities, kv_bytes, num_tokens=len(doc_tokens))
    print(f'  published: {len(kv_bytes)/1e6:.1f} MB,  {len(entities)} entities')
    print()

    # ── Step 2: simulate a multi-turn conversation ─────────────────────────
    # Turn k prompt = doc + Q1 + A1 + Q2 + A2 + ... + Qk
    # B0: Mac prefills the whole thing from scratch every turn
    # ES: Mac loads doc KV from cache once; every turn prefills only
    #     (Q1 + A1 + ... + Qk) on top of the doc KV

    # We'll also use a FRESH engine for each B0 turn to reflect
    # "Mac user opens chat, sends Q, closes chat".  For ES we keep
    # the same engine because the chat session is live.

    turns_log = []

    # Running conversation history: list of (question_text, answer_tokens)
    history = []

    # Snapshot of KV we can rewind to: doc_kv for turn 1, then
    # (doc + prior Q/A) for subsequent turns.  We'll re-use the loaded
    # doc KV by loading it fresh from disk each turn (zero-copy mmap).

    for turn in range(1, args.turns + 1):
        q_text = TURN_QUESTIONS[turn - 1]
        print(f'──  Turn {turn}: {q_text!r}')

        # Build the prompt so far:  doc + [turn 1 Q1 A1 ...] + current question
        history_text = ''.join(
            f'\n\nQ: {q}\nA: {a}'
            for (q, a) in history
        )
        current_turn_suffix = f'{history_text}\n\nQ: {q_text}\nA:'
        full_prompt = doc + current_turn_suffix

        full_tokens = engine.tokenize(full_prompt)
        suffix_tokens_from_doc = engine.tokenize(current_turn_suffix)
        delta_token_count = len(full_tokens) - len(doc_tokens)
        print(f'    full prompt tokens: {len(full_tokens)}  '
              f'(doc {len(doc_tokens)} + delta {delta_token_count})')

        # ── B0: full cold prefill ───────────────────────────────────────
        t0 = time.perf_counter()
        b0_generated, _ = engine.generate(
            full_tokens, max_new_tokens=args.max_new_tokens,
        )
        b0_ms = (time.perf_counter() - t0) * 1000

        # ── EdgeServe: mmap doc KV, prefill only the delta, then decode ──
        t_es = time.perf_counter()
        # Fetch the doc KV (same-host mmap)
        import glob
        bins = sorted(glob.glob(os.path.join(cache_path, '*.bin')))
        local_path = bins[-1]
        t_fetch = time.perf_counter()
        cached_kv = engine.deserialize_cache_from_path(local_path)
        fetch_ms = (time.perf_counter() - t_fetch) * 1000

        # Prefill the delta ON TOP of the doc KV, then generate
        es_generated, _ = engine.generate(
            suffix_tokens_from_doc,
            max_new_tokens=args.max_new_tokens,
            cache=cached_kv,
        )
        es_total_ms = (time.perf_counter() - t_es) * 1000
        es_decode_ms = es_total_ms - fetch_ms

        # Record
        token_match = (b0_generated == es_generated)
        turns_log.append({
            'turn': turn,
            'q': q_text,
            'full_tokens': len(full_tokens),
            'delta_tokens': delta_token_count,
            'b0_ms': b0_ms,
            'es_fetch_ms': fetch_ms,
            'es_total_ms': es_total_ms,
            'es_decode_ms': es_decode_ms,
            'token_match': token_match,
        })

        # Pick answer to feed to the next turn (use B0's since it's
        # the reference; ES matches when bit-exact).
        answer_text = engine.detokenize(b0_generated)
        history.append((q_text, answer_text))

        print(f'    B0 total:              {b0_ms:>7.0f} ms')
        print(f'    ES fetch (mmap):       {fetch_ms:>7.0f} ms')
        print(f'    ES delta prefill+gen:  {es_decode_ms:>7.0f} ms')
        print(f'    ES total:              {es_total_ms:>7.0f} ms')
        print(f'    Speedup (B0 / ES):     {b0_ms/es_total_ms:>7.2f}×')
        print(f'    Token match:           {"✓" if token_match else "✗"}')
        print(f'    Answer: {answer_text[:120]!r}')
        print()

    # ── Summary table ─────────────────────────────────────────────────────
    print('=' * 60)
    print('Summary — multi-turn speedup compounds across turns')
    print('=' * 60)
    print()
    print('| turn | tokens (full) | delta | B0 ms | ES ms | speedup | match |')
    print('|-----:|--------------:|------:|------:|------:|--------:|:-----:|')
    for r in turns_log:
        print(f'|  {r["turn"]}   | {r["full_tokens"]:>12} '
              f'| {r["delta_tokens"]:>5} '
              f'| {r["b0_ms"]:>5.0f} | {r["es_total_ms"]:>5.0f} '
              f'| {r["b0_ms"]/r["es_total_ms"]:>6.2f}× '
              f'| {"✓" if r["token_match"] else "✗"}  |')

    # Cumulative cost
    b0_cum = sum(r['b0_ms'] for r in turns_log)
    es_cum = seed_ms + sum(r['es_total_ms'] for r in turns_log)
    es_cum_nofetch = seed_ms + sum(r['es_decode_ms'] for r in turns_log)
    print()
    print(f'Cumulative (all {args.turns} turns):')
    print(f'  B0 cold re-prefill every turn:  {b0_cum:>7.0f} ms')
    print(f'  ES (seed + turns incl fetch):   {es_cum:>7.0f} ms  '
          f'(speedup {b0_cum/es_cum:.2f}×)')
    print(f'  ES (seed + turns, mmap \"free\"): {es_cum_nofetch:>7.0f} ms  '
          f'(speedup {b0_cum/es_cum_nofetch:.2f}×)')
    print()
    print('Interpretation: the seed cost (prefilling the doc) is paid ONCE')
    print('and amortised across all subsequent turns.  B0 pays the full')
    print('doc-prefill cost every turn.  The longer the conversation, the')
    print('stronger the compounding benefit of the KV CDN.')

    client.close()
    import shutil
    shutil.rmtree(cache_path, ignore_errors=True)


if __name__ == '__main__':
    main()
