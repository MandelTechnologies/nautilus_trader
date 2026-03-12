import importlib.util
from pathlib import Path
import sys


MODULE_PATH = (
    Path(__file__).resolve().parents[3] / "nautilus_trader" / "quantchat" / "strategy_loader.py"
)
SPEC = importlib.util.spec_from_file_location("quantchat_strategy_loader", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
STRATEGY_LOADER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STRATEGY_LOADER)
execute_strategy_module = STRATEGY_LOADER.execute_strategy_module


def test_execute_strategy_module_resolves_future_annotations(tmp_path):
    script_path = tmp_path / "strategy.py"
    script_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "from typing import get_type_hints",
                "",
                "class InstrumentId: pass",
                "class StrategyConfig: pass",
                "",
                "class DemoConfig(StrategyConfig):",
                "    instrument_id: InstrumentId",
                "",
                'resolved_name = get_type_hints(DemoConfig)["instrument_id"].__name__',
            ],
        ),
    )

    module = execute_strategy_module(script_path)

    assert module.resolved_name == "InstrumentId"


def test_execute_strategy_module_restores_main_module(tmp_path):
    original_main = sys.modules["__main__"]
    script_path = tmp_path / "strategy.py"
    script_path.write_text("marker = 'ok'\n")

    module = execute_strategy_module(script_path)

    assert module.marker == "ok"
    assert sys.modules["__main__"] is original_main
