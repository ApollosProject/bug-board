from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

DEFAULT_LOOKBACK_DAYS = 30


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _to_linear_datetime(value: datetime) -> str:
    return _ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%S.000Z")


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime
    preset_days: int | None = None

    @classmethod
    def from_days(cls, days: int, now: datetime | None = None) -> TimeWindow:
        if days < 1:
            days = DEFAULT_LOOKBACK_DAYS
        now = _ensure_utc(now or datetime.now(timezone.utc))
        return cls(start=now - timedelta(days=days), end=now, preset_days=days)

    @classmethod
    def from_dates(cls, start_date: date, end_date: date) -> TimeWindow:
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        end = datetime(
            end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc
        ) + timedelta(days=1)
        return cls(start=start, end=end, preset_days=None)

    @classmethod
    def from_parts(
        cls,
        days: int | None,
        start: str | None,
        end: str | None,
        now: datetime | None = None,
    ) -> TimeWindow:
        start_date = _parse_iso_date(start)
        end_date = _parse_iso_date(end)
        if start_date is not None or end_date is not None:
            today = _ensure_utc(now or datetime.now(timezone.utc)).date()
            if start_date is None:
                start_date = end_date or today
            if end_date is None:
                end_date = today
            return cls.from_dates(start_date, end_date)
        return cls.from_days(days or DEFAULT_LOOKBACK_DAYS, now=now)

    @property
    def inclusive_end_date(self) -> date:
        return (self.end - timedelta(microseconds=1)).date()

    @property
    def duration_days(self) -> int:
        if self.preset_days is not None:
            return self.preset_days
        return max((self.end - self.start).days, 1)

    @property
    def label(self) -> str:
        if self.preset_days is not None:
            return f"{self.preset_days}d"
        start_date = self.start.date()
        end_date = self.inclusive_end_date
        start_text = f"{start_date.strftime('%b')} {start_date.day}"
        end_text = f"{end_date.strftime('%b')} {end_date.day}, {end_date.year}"
        if start_date.year != end_date.year:
            start_text = f"{start_text}, {start_date.year}"
        return f"{start_text} – {end_text}"

    def query_args(self) -> dict[str, str | int]:
        if self.preset_days is not None:
            return {"days": self.preset_days}
        return {
            "start": self.start.date().isoformat(),
            "end": self.inclusive_end_date.isoformat(),
        }

    def cache_parts(self) -> tuple[int | None, str | None, str | None]:
        if self.preset_days is not None:
            return self.preset_days, None, None
        return None, self.start.date().isoformat(), self.inclusive_end_date.isoformat()

    def linear_after(self) -> str:
        if self.preset_days is not None:
            return f"-P{self.preset_days}D"
        return _to_linear_datetime(self.start)

    def linear_before(self) -> str:
        return _to_linear_datetime(self.end)

    def github_merged_qualifier(self) -> str:
        start = self.start.date().isoformat()
        if self.preset_days is not None:
            return f"merged:>={start}"
        return f"merged:>={start} merged:<={self.inclusive_end_date.isoformat()}"

    def template_vars(self) -> dict[str, Any]:
        query_args = self.query_args()
        is_preset = self.preset_days is not None
        return {
            "days": self.duration_days,
            "preset_days": self.preset_days,
            "start": None if is_preset else self.start.date().isoformat(),
            "end": None if is_preset else self.inclusive_end_date.isoformat(),
            "window_label": self.label,
            "window_query": query_args,
        }


def parse_time_window(
    args: Any,
    *,
    default_days: int = DEFAULT_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> TimeWindow:
    start = _get_arg(args, "start")
    end = _get_arg(args, "end")
    start_date = _parse_iso_date(start)
    end_date = _parse_iso_date(end)
    if start_date is not None or end_date is not None:
        today = _ensure_utc(now or datetime.now(timezone.utc)).date()
        if start_date is None:
            start_date = end_date or today
        if end_date is None:
            end_date = today
        return TimeWindow.from_dates(start_date, end_date)

    days = _get_int_arg(args, "days")
    if days is None or days < 1:
        days = default_days
    return TimeWindow.from_days(days, now=now)


def _get_arg(args: Any, name: str) -> str | None:
    getter = getattr(args, "get", None)
    if getter is None:
        return None
    try:
        value = getter(name)
    except TypeError:
        return None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _get_int_arg(args: Any, name: str) -> int | None:
    getter = getattr(args, "get", None)
    if getter is None:
        return None
    try:
        value = getter(name, type=int)
    except TypeError:
        raw = _get_arg(args, name)
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            return None
    if isinstance(value, int):
        return value
    return None
