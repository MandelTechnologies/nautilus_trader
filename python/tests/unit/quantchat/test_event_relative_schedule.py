from datetime import UTC
from datetime import datetime
import importlib.util
from pathlib import Path
import sys


_SCHEDULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "nautilus_trader"
    / "quantchat"
    / "event_relative_schedule.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "event_relative_schedule_under_test",
    _SCHEDULE_PATH,
)
assert _SPEC is not None and _SPEC.loader is not None
event_relative_schedule = importlib.util.module_from_spec(_SPEC)
# Dataclass field-type resolution requires the module to be importable by
# name, so register it before executing.
sys.modules[_SPEC.name] = event_relative_schedule
_SPEC.loader.exec_module(event_relative_schedule)

next_event_relative_fire = event_relative_schedule.next_event_relative_fire


def _xnys_sessions() -> dict:
    """
    Synthetic July 2027 weekday sessions (EDT): 09:30-16:00 ET (13:30Z-20:00Z), with Fri
    2027-07-09 an early close at 13:00 ET (17:00Z).
    """
    sessions = {}
    for day in range(5, 17):
        date = datetime(2027, 7, day, tzinfo=UTC).date()
        if date.weekday() >= 5:
            continue
        closes = "17:00" if day == 9 else "20:00"
        status = "EARLY_CLOSE" if day == 9 else "OPEN"
        sessions[date.isoformat()] = {
            "status": status,
            "opensAt": f"2027-07-{day:02d}T13:30:00Z",
            "closesAt": f"2027-07-{day:02d}T{closes}:00Z",
        }
    return sessions


def _trigger(**overrides) -> dict:
    trigger = {
        "kind": "event_relative",
        "offsetTradingDays": -1,
        "time": "13:00",
        "timezone": "America/New_York",
        "calendar": "XNYS",
        "missedFirePolicy": "skip",
    }
    trigger.update(overrides)
    return trigger


def _earnings(
    event_date: str,
    announced_at: str | None = None,
    deterministic: bool = False,
) -> dict:
    event = {
        "id": f"ev-{event_date}",
        "type": "EARNINGS",
        "symbol": "TSLA",
        "eventDate": f"{event_date}T00:00:00Z",
        "deterministic": deterministic,
    }
    if announced_at is not None:
        event["announcedAt"] = announced_at
    return event


AFTER = datetime(2027, 7, 1, tzinfo=UTC)


def test_negative_offset_skips_weekend_and_respects_early_close_boundary() -> None:
    # Earnings Mon 2027-07-12; one trading day before is Fri 2027-07-09.
    # 13:00 ET = 17:00Z sits exactly at the early close — inclusive.
    fire = next_event_relative_fire(
        _trigger(),
        [_earnings("2027-07-12", announced_at="2027-06-01T00:00:00Z")],
        _xnys_sessions(),
        AFTER,
    )
    assert fire is not None
    assert fire.fire_at == datetime(2027, 7, 9, 17, 0, tzinfo=UTC)
    assert fire.event_id == "ev-2027-07-12"


def test_fire_after_early_close_is_skipped_or_moved_to_next_open() -> None:
    # 14:00 ET (18:00Z) is past Friday's early close.
    skip = next_event_relative_fire(
        _trigger(time="14:00"),
        [_earnings("2027-07-12", announced_at="2027-06-01T00:00:00Z")],
        _xnys_sessions(),
        AFTER,
    )
    assert skip is None

    moved = next_event_relative_fire(
        _trigger(time="14:00", missedFirePolicy="next_session"),
        [_earnings("2027-07-12", announced_at="2027-06-01T00:00:00Z")],
        _xnys_sessions(),
        AFTER,
    )
    assert moved is not None
    assert moved.fire_at == datetime(2027, 7, 12, 13, 30, tzinfo=UTC)


def test_known_by_gate_skips_late_knowledge_but_not_deterministic_events() -> None:
    # knownBy 2: fire Fri 07-09 must have been knowable by Wed 07-07 13:00 ET.
    late = _earnings("2027-07-12", announced_at="2027-07-08T02:00:00Z")
    assert (
        next_event_relative_fire(
            _trigger(knownByTradingDays=2),
            [late],
            _xnys_sessions(),
            AFTER,
        )
        is None
    )

    deterministic = _earnings(
        "2027-07-12",
        announced_at="2027-07-08T02:00:00Z",
        deterministic=True,
    )
    fire = next_event_relative_fire(
        _trigger(knownByTradingDays=2),
        [deterministic],
        _xnys_sessions(),
        AFTER,
    )
    assert fire is not None
    assert fire.fire_at == datetime(2027, 7, 9, 17, 0, tzinfo=UTC)


def test_next_session_policy_fires_after_knowledge_arrives() -> None:
    # Announced Sat 07-10, after the computed Fri fire: next open is Mon.
    late = _earnings("2027-07-12", announced_at="2027-07-10T09:00:00Z")
    fire = next_event_relative_fire(
        _trigger(missedFirePolicy="next_session"),
        [late],
        _xnys_sessions(),
        AFTER,
    )
    assert fire is not None
    assert fire.fire_at == datetime(2027, 7, 12, 13, 30, tzinfo=UTC)


def test_24_7_counts_every_day_and_next_session_is_the_knowledge_instant() -> None:
    trigger = _trigger(timezone="UTC", time="09:30", calendar="24/7")
    # Offset -1 from Mon 07-12 is Sun 07-11 on a 24/7 calendar.
    fire = next_event_relative_fire(
        trigger,
        [_earnings("2027-07-12", announced_at="2027-06-01T00:00:00Z")],
        {},
        AFTER,
    )
    assert fire is not None
    assert fire.fire_at == datetime(2027, 7, 11, 9, 30, tzinfo=UTC)

    # Knowledge lands after the computed fire; 24/7 has no session to wait
    # for, so next_session fires the moment the event became knowable.
    late = _earnings("2027-07-12", announced_at="2027-07-11T12:00:00Z")
    moved = next_event_relative_fire(
        _trigger(
            timezone="UTC",
            time="09:30",
            calendar="24/7",
            missedFirePolicy="next_session",
        ),
        [late],
        {},
        AFTER,
    )
    assert moved is not None
    assert moved.fire_at == datetime(2027, 7, 11, 12, 0, tzinfo=UTC)


def test_earliest_upcoming_fire_wins_and_after_cursor_excludes_past() -> None:
    events = [
        _earnings("2027-07-12", announced_at="2027-06-01T00:00:00Z"),
        _earnings("2027-07-14", announced_at="2027-06-01T00:00:00Z"),
    ]
    first = next_event_relative_fire(_trigger(), events, _xnys_sessions(), AFTER)
    assert first is not None
    assert first.fire_at == datetime(2027, 7, 9, 17, 0, tzinfo=UTC)

    after_first = next_event_relative_fire(
        _trigger(),
        events,
        _xnys_sessions(),
        datetime(2027, 7, 9, 18, 0, tzinfo=UTC),
    )
    assert after_first is not None
    # One trading day before Wed 07-14 is Tue 07-13.
    assert after_first.fire_at == datetime(2027, 7, 13, 17, 0, tzinfo=UTC)
    assert after_first.event_id == "ev-2027-07-14"
