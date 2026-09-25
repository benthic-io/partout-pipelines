from importlib import import_module

__all__ = ["DATASET", "STAGES", "Pipeline", "build"]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(name)
    module = import_module(".pipeline", __name__)
    return getattr(module, name)
