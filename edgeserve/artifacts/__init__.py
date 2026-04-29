"""edgeserve.artifacts — payload helpers for the case studies beyond KV cache.

Each module exposes an entity-tag schema and a (serialize, deserialize) pair
that fit into the existing SemanticCacheClient.publish() / resolve() flow:

  vllm_compile — Phase E3: vLLM torch.compile / CUDA-graph cache directories
  tool_result  — Phase E2: deterministic dev-tool outputs (planned)
  embedding    — Phase E1: RAG chunk embeddings (planned)

All three reuse the same bloom catalog, exact-validation gate, and HTTP
transport; only the entity schema and payload serializer differ.
"""
