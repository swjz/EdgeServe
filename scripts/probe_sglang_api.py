"""Figure out the right sglang offline API + whether RadixAttention prefix cache is on by default."""
import sglang
print('sglang', sglang.__version__)
try:
    from sglang import Engine
    print('sglang.Engine:', Engine)
    import inspect
    print('Engine.__init__:', inspect.signature(Engine.__init__))
    # List public methods
    pub = [m for m in dir(Engine) if not m.startswith('_')]
    print('methods:', pub[:30])
except Exception as e:
    print('Engine import failed:', e)
