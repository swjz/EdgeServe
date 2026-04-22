"""Probe vllm.LLM.generate() signature to find the right way to pass token ids in 0.19+."""
import inspect
from vllm import LLM
print(inspect.signature(LLM.generate))
# TokensPrompt is the current way to pass pre-tokenized input
try:
    from vllm import TokensPrompt
    print('TokensPrompt:', TokensPrompt)
except ImportError as e:
    print('no TokensPrompt:', e)
try:
    from vllm.inputs import TokensPrompt
    print('vllm.inputs.TokensPrompt:', TokensPrompt)
except ImportError as e:
    print('no vllm.inputs.TokensPrompt:', e)
