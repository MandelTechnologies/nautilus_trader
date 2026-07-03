"""
Event-relative fire computation for compiled-plan ``event_relative`` triggers.

Pure and stdlib-only (like ``wall_clock_schedule``): callers pass the
selector-matched catalyst events, the trigger, and the XNYS session map; this
module answers "when is the next fire strictly after ``after``, and for which
event?".

Semantics (the compiler contract, docs/specs/composable-signals.md §6.1):

- The event day is the UTC civil day of ``eventDate``.
- ``offsetTradingDays`` navigates tradable days (OPEN / EARLY_CLOSE for XNYS;
  every day for 24/7): negative counts backward from the event day, positive
  forward, and the event day itself is never counted. Offset 0 targets the
  event day.
- The fire instant is ``time`` in ``timezone`` on the target day, and for
  XNYS must fall inside that day's session bounds (early-close aware).
- Knowledge gate: ``deterministic`` events are knowable at any time;
  empirical events become knowable at ``announcedAt``. With
  ``knownByTradingDays`` = N, the event must have been knowable by the fire
  instant moved N tradable days earlier (same wall time).
- ``missedFirePolicy``:
  * ``skip`` — a fire that is outside session bounds, on an untradable
    target day, or whose knowledge arrived too late never happens.
  * ``next_session`` — such a fire moves to the earliest session open
    strictly after both the computed instant and the moment the event
    became knowable (for 24/7, that instant itself: the market never
    closes).

"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo


# The compiler caps offsets at 30 trading days; scanning a year of calendar
# days is more than enough to satisfy any legal offset, and bounds the walk
# when a sparse session map would otherwise never terminate.
_MAX_DAY_SCAN = 366


@dataclass(frozen=True)
class EventRelativeFire:
    fire_at: datetime  # UTC
    event_id: str


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _session_for(sessions: dict[str, Any], day: date) -> dict[str, Any] | None:
    session = sessions.get(day.isoformat())
    return session if isinstance(session, dict) else None


def _is_tradable(calendar: str, sessions: dict[str, Any], day: date) -> bool:
    if calendar == "24/7":
        return True
    session = _session_for(sessions, day)
    if session is None:
        return False
    return str(session.get("status", "")).upper() in {"OPEN", "EARLY_CLOSE"}


def _session_bounds(
    sessions: dict[str, Any],
    day: date,
) -> tuple[datetime, datetime] | None:
    session = _session_for(sessions, day)
    if session is None:
        return None
    opens_at = _parse_utc(session.get("opensAt"))
    closes_at = _parse_utc(session.get("closesAt"))
    if opens_at is None or closes_at is None:
        return None
    return opens_at, closes_at


def shift_trading_days(
    day: date,
    offset: int,
    calendar: str,
    sessions: dict[str, Any],
) -> date | None:
    """
    Return the day `offset` tradable days from `day` (the event day itself is never
    counted; offset 0 returns `day`), or None when the session map runs out.
    """
    if offset == 0:
        return day
    step = timedelta(days=1 if offset > 0 else -1)
    remaining = abs(offset)
    cursor = day
    for _ in range(_MAX_DAY_SCAN):
        cursor = cursor + step
        if _is_tradable(calendar, sessions, cursor):
            remaining -= 1
            if remaining == 0:
                return cursor
    return None


def _fire_instant(day: date, hhmm: str, timezone: str) -> datetime | None:
    try:
        hour, minute = (int(part) for part in hhmm.split(":", 1))
        local = datetime.combine(day, dt_time(hour, minute), tzinfo=ZoneInfo(timezone))
    except (ValueError, KeyError):
        return None
    return local.astimezone(UTC)


def _knowable_at(event: dict[str, Any]) -> datetime | None:
    """
    When the event became knowable; None = always knowable.
    """
    if bool(event.get("deterministic", False)):
        return None
    return _parse_utc(event.get("announcedAt"))


def _inside_session(
    fire: datetime,
    day: date,
    calendar: str,
    sessions: dict[str, Any],
) -> bool:
    if calendar == "24/7":
        return True
    bounds = _session_bounds(sessions, day)
    if bounds is None or not _is_tradable(calendar, sessions, day):
        return False
    opens_at, closes_at = bounds
    return opens_at <= fire <= closes_at


def _next_session_open(
    threshold: datetime,
    calendar: str,
    sessions: dict[str, Any],
    timezone: str,
) -> datetime | None:
    """
    Earliest session open strictly after `threshold` (24/7: `threshold`).
    """
    if calendar == "24/7":
        return threshold
    cursor = threshold.astimezone(ZoneInfo(timezone)).date()
    for _ in range(_MAX_DAY_SCAN):
        bounds = _session_bounds(sessions, cursor)
        if bounds is not None and _is_tradable(calendar, sessions, cursor):
            opens_at, _ = bounds
            if opens_at > threshold:
                return opens_at
        cursor = cursor + timedelta(days=1)
    return None


def _candidate_fire(
    trigger: dict[str, Any],
    event: dict[str, Any],
    calendar: str,
    sessions: dict[str, Any],
) -> datetime | None:
    event_date = _parse_utc(event.get("eventDate"))
    if event_date is None:
        return None
    offset = int(trigger.get("offsetTradingDays", 0))
    timezone = str(trigger.get("timezone", "UTC"))
    hhmm = str(trigger.get("time", ""))
    policy = str(trigger.get("missedFirePolicy", "skip")).lower()

    target_day = shift_trading_days(event_date.date(), offset, calendar, sessions)
    if target_day is None:
        return None
    fire = _fire_instant(target_day, hhmm, timezone)
    if fire is None:
        return None

    # Knowledge gate: the event must have been knowable early enough.
    knowable_at = _knowable_at(event)
    known_by = trigger.get("knownByTradingDays")
    deadline = fire
    if known_by is not None:
        deadline_day = shift_trading_days(
            fire.astimezone(ZoneInfo(timezone)).date(),
            -int(known_by),
            calendar,
            sessions,
        )
        if deadline_day is None:
            return None
        deadline = _fire_instant(deadline_day, hhmm, timezone) or fire
    knowledge_ok = knowable_at is None or knowable_at <= deadline

    session_ok = _inside_session(fire, target_day, calendar, sessions)
    if knowledge_ok and session_ok:
        return fire
    if policy != "next_session":
        return None
    threshold = fire
    if not knowledge_ok and knowable_at is not None:
        threshold = max(threshold, knowable_at)
    return _next_session_open(threshold, calendar, sessions, timezone)


def next_event_relative_fire(
    trigger: dict[str, Any],
    events: list[dict[str, Any]],
    sessions: dict[str, Any],
    after: datetime,
) -> EventRelativeFire | None:
    """
    Earliest fire strictly after `after` (UTC) across the selector-matched `events`, or
    None when nothing upcoming fires.
    """
    calendar = str(trigger.get("calendar", "24/7")).upper()
    best: EventRelativeFire | None = None
    for event in events:
        fire = _candidate_fire(trigger, event, calendar, sessions)
        if fire is None or fire <= after:
            continue
        if best is None or fire < best.fire_at:
            best = EventRelativeFire(fire_at=fire, event_id=str(event.get("id", "")))
    return best


def next_session_anchor_fire(
    anchor: str,
    offset_minutes: int,
    sessions: dict[str, Any],
    after: datetime,
) -> datetime | None:
    """
    Return the earliest session open/close bound plus `offset_minutes`, strictly after
    `after` (UTC), across the session map — or None when the map holds nothing upcoming.

    MARKET_CLOSED days simply do not occur; an early-close day's close bound is the
    early close, which is the whole point of anchoring to the session instead of a wall-
    clock time.

    """
    key = "opensAt" if anchor == "open" else "closesAt"
    best: datetime | None = None
    for session in sessions.values():
        if not isinstance(session, dict):
            continue
        if str(session.get("status", "")).upper() not in {"OPEN", "EARLY_CLOSE"}:
            continue
        bound = _parse_utc(session.get(key))
        if bound is None:
            continue
        fire = bound + timedelta(minutes=offset_minutes)
        if fire <= after:
            continue
        if best is None or fire < best:
            best = fire
    return best
