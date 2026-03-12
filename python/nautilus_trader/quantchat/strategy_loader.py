import importlib.util
from pathlib import Path
import sys
from types import ModuleType


def execute_strategy_module(
    script_path: str | Path,
    module_name: str = "__main__",
) -> ModuleType:
    """
    Execute a strategy file inside a real module namespace.

    This mirrors normal Python script/module execution closely enough that deferred
    annotations can resolve against the module globals.

    """
    resolved_path = Path(script_path)
    spec = importlib.util.spec_from_file_location(module_name, resolved_path)
    if spec is None or spec.loader is None:
        msg = f"Failed to create module spec for strategy: {resolved_path}"
        raise ImportError(msg)

    module = importlib.util.module_from_spec(spec)
    previous_module = sys.modules.get(module_name)

    try:
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        if previous_module is not None:
            sys.modules[module_name] = previous_module
        else:
            sys.modules.pop(module_name, None)
