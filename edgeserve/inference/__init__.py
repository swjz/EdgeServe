from edgeserve.inference.engine import CacheHandle, InferenceEngine

__all__ = ['CacheHandle', 'InferenceEngine']


def _lazy(name):
    import importlib
    if name == 'HFEngine':
        return importlib.import_module('edgeserve.inference.hf_engine').HFEngine
    if name == 'LLMCompute':
        return importlib.import_module('edgeserve.inference.llm_compute').LLMCompute
    raise AttributeError(name)


def __getattr__(name):
    return _lazy(name)
