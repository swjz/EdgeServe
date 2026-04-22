"""Figure out the right sglang offline API + whether RadixAttention prefix cache is on by default."""
import inspect
import sglang
print('sglang', sglang.__version__)

# sglang.Engine is a LazyImport proxy; resolve it to the real class.
import importlib
real_engine_mod = importlib.import_module('sglang.srt.entrypoints.engine')
print('engine module:', real_engine_mod)
RealEngine = getattr(real_engine_mod, 'Engine', None)
print('real Engine:', RealEngine)
if RealEngine is not None:
    print('real Engine.__init__:', inspect.signature(RealEngine.__init__))
    pub = [m for m in dir(RealEngine) if not m.startswith('_')][:30]
    print('methods:', pub)
    # generate signature:
    if hasattr(RealEngine, 'generate'):
        print('Engine.generate:', inspect.signature(RealEngine.generate))
