from datetime import UTC
from datetime import datetime

from nautilus_trader.quantchat.intent_strategy import _event_active_at
from nautilus_trader.quantchat.intent_strategy import _selector_matches_event
from nautilus_trader.quantchat.intent_strategy import _validate_runtime_contract


def test_selector_matches_type_symbol_and_details_case_insensitively() -> None:
    selector = {
        "type": "MERCURY_RETROGRADE",
        "symbol": "GLOBAL",
        "details": {"planet": "mercury", "phase": ["pre_shadow", "retrograde"]},
    }
    event = {
        "type": "MERCURY_RETROGRADE",
        "symbol": "GLOBAL",
        "eventDate": "2026-02-26T12:00:00+00:00",
        "endDate": "2026-03-21T12:00:00+00:00",
        "details": {"planet": "Mercury", "phase": "Retrograde", "provider": "astro"},
    }

    assert _selector_matches_event(selector, event)


def test_event_active_uses_utc_civil_day_span() -> None:
    event = {
        "type": "MERCURY_RETROGRADE",
        "symbol": "GLOBAL",
        "eventDate": "2026-02-26T12:00:00+00:00",
        "endDate": "2026-03-21T12:00:00+00:00",
        "details": {"phase": "retrograde"},
    }

    assert _event_active_at(event, datetime(2026, 2, 26, 0, 0, tzinfo=UTC))
    assert _event_active_at(event, datetime(2026, 3, 21, 23, 59, tzinfo=UTC))
    assert not _event_active_at(event, datetime(2026, 3, 22, 0, 0, tzinfo=UTC))


def test_runtime_accepts_current_and_previous_contracts() -> None:
    _validate_runtime_contract({"runtimeContractVersion": "quantchat_strategy_intent_v4"})
    _validate_runtime_contract({"runtimeContractVersion": "quantchat_strategy_intent_v5"})
