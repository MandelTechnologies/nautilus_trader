from datetime import UTC
from datetime import datetime
import importlib.util
from pathlib import Path


_SCHEDULE_PATH = (
    Path(__file__).resolve().parents[3] / "nautilus_trader" / "quantchat" / "wall_clock_schedule.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "wall_clock_schedule_under_test",
    _SCHEDULE_PATH,
)
assert _SPEC is not None and _SPEC.loader is not None
wall_clock_schedule = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(wall_clock_schedule)


def _always_open(_candidate_day, _candidate_utc, _trigger) -> bool:
    return True


def test_interval_schedule_fires_every_hour_at_anchor_minute() -> None:
    trigger = {
        "kind": "wall_clock",
        "recurrence": "daily",
        "time": "00:55",
        "intervalMinutes": 60,
        "untilTime": "23:59",
        "timezone": "UTC",
        "calendar": "24/7",
        "closedMarketPolicy": "skip",
    }

    assert wall_clock_schedule.next_wall_clock_fire_time(
        trigger,
        datetime(2026, 6, 1, 0, 54, 59, tzinfo=UTC),
        _always_open,
    ) == datetime(2026, 6, 1, 0, 55, tzinfo=UTC)
    assert wall_clock_schedule.next_wall_clock_fire_time(
        trigger,
        datetime(2026, 6, 1, 0, 55, tzinfo=UTC),
        _always_open,
    ) == datetime(2026, 6, 1, 1, 55, tzinfo=UTC)


def test_phase_shifted_exit_schedule_crosses_midnight() -> None:
    trigger = {
        "kind": "wall_clock",
        "recurrence": "daily",
        "time": "00:05",
        "intervalMinutes": 60,
        "untilTime": "23:59",
        "timezone": "UTC",
        "calendar": "24/7",
        "closedMarketPolicy": "skip",
    }

    assert wall_clock_schedule.next_wall_clock_fire_time(
        trigger,
        datetime(2026, 6, 1, 23, 55, tzinfo=UTC),
        _always_open,
    ) == datetime(2026, 6, 2, 0, 5, tzinfo=UTC)


def test_interval_schedule_respects_calendar_filter() -> None:
    trigger = {
        "kind": "wall_clock",
        "recurrence": "daily",
        "time": "09:30",
        "intervalMinutes": 15,
        "untilTime": "10:00",
        "timezone": "UTC",
        "calendar": "XNYS",
        "closedMarketPolicy": "skip",
    }

    def only_ten(_candidate_day, candidate_utc, _trigger) -> bool:
        return candidate_utc.hour == 10 and candidate_utc.minute == 0

    assert wall_clock_schedule.next_wall_clock_fire_time(
        trigger,
        datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
        only_ten,
    ) == datetime(2026, 6, 1, 10, 0, tzinfo=UTC)
