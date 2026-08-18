from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

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
    value = _ensure_utc(value)
    return f"{value:%Y-%m-%dT%H:%M:%S}.{value.microsecond // 1000:03d}Z"


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
        start = datetime.combine(start_date, datetime.min.time(), timezone.utc)
        end = datetime.combine(end_date, datetime.min.time(), timezone.utc)
        return cls(start=start, end=end + timedelta(days=1))

    @classmethod
    def resolve(
        cls,
        days: int | None = None,
        window: TimeWindow | None = None,
        start: str | None = None,
        end: str | None = None,
        now: datetime | None = None,
    ) -> TimeWindow:
        if window is not None:
            return window
        start_date = _parse_iso_date(start)
        end_date = _parse_iso_date(end)
        if start_date is not None and end_date is not None:
            return cls.from_dates(start_date, end_date)
        return cls.from_days(days or DEFAULT_LOOKBACK_DAYS, now=now)

    @property
    def inclusive_end_date(self) -> date:
        return (self.end - timedelta(microseconds=1)).date()

    def linear_bounds(self) -> dict[str, str]:
        return {"after": _to_linear_datetime(self.start), "before": _to_linear_datetime(self.end)}

    def github_merged_qualifier(self) -> str:
        start = self.start.date().isoformat()
        if self.preset_days is not None:
            return f"merged:>={start}"
        return f"merged:>={start} merged:<={self.inclusive_end_date.isoformat()}"
