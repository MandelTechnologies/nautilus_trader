from __future__ import annotations

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError


def _parse_wall_clock_minute(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        hour_text, minute_text = value.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError:
        return None
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        return None
    return (hour * 60) + minute


def _wall_clock_fire_minutes(trigger: dict[str, Any]) -> list[int] | None:
    start = _parse_wall_clock_minute(trigger.get("time"))
    if start is None:
        return None

    interval_value = trigger.get("intervalMinutes")
    if interval_value is None:
        return [start]
    if isinstance(interval_value, bool):
        return None
    try:
        interval_float = float(interval_value)
    except (TypeError, ValueError):
        return None
    if not interval_float.is_integer():
        return None
    interval = int(interval_float)
    if not 1 <= interval <= 1440:
        return None

    until_value = trigger.get("untilTime")
    until = 1439 if until_value is None else _parse_wall_clock_minute(until_value)
    if until is None or until < start:
        return None

    return list(range(start, until + 1, interval))


def _date_matches_wall_clock_trigger(
    candidate_day,
    trigger: dict[str, Any],
    recurrence: str,
) -> bool:
    if recurrence == "daily":
        return True
    if recurrence == "weekly":
        days = {str(day).lower() for day in trigger.get("daysOfWeek", []) or []}
        return candidate_day.strftime("%A").lower() in days
    if recurrence == "monthly":
        days = {int(day) for day in trigger.get("daysOfMonth", []) or []}
        return candidate_day.day in days
    return False


def next_wall_clock_fire_time(
    trigger: dict[str, Any],
    after_utc: datetime,
    calendar_allows: Callable[[Any, datetime, dict[str, Any]], bool],
) -> datetime | None:
    timezone_name = str(trigger.get("timezone", "UTC"))
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return None

    fire_minutes = _wall_clock_fire_minutes(trigger)
    if fire_minutes is None:
        return None

    recurrence = str(trigger.get("recurrence", "")).lower()
    after_local = after_utc.astimezone(tz)
    start_day = after_local.date()
    for day_offset in range(400):
        candidate_day = start_day + timedelta(days=day_offset)
        if not _date_matches_wall_clock_trigger(candidate_day, trigger, recurrence):
            continue
        for minute_of_day in fire_minutes:
            candidate_local = datetime(
                candidate_day.year,
                candidate_day.month,
                candidate_day.day,
                minute_of_day // 60,
                minute_of_day % 60,
                tzinfo=tz,
            )
            candidate_utc = candidate_local.astimezone(UTC)
            if candidate_utc <= after_utc:
                continue
            if not calendar_allows(candidate_day, candidate_utc, trigger):
                continue
            return candidate_utc
    return None
