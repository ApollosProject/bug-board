from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone


def parse_linear_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def format_issue_sla_text(issue: dict, now: datetime | None = None) -> str | None:
    breach_at = parse_linear_dt(issue.get("slaBreachesAt"))
    if not breach_at:
        return None

    current_time = now or datetime.now(timezone.utc)
    delta = breach_at - current_time
    delta_seconds = delta.total_seconds()
    abs_delta = abs(delta)

    if abs_delta < timedelta(days=1):
        hours = math.ceil(abs(delta_seconds) / 3600) if delta_seconds else 0
        if delta_seconds >= 0:
            return f"{hours}h"
        return f"{hours}h overdue"

    days = abs_delta.days
    if delta_seconds >= 0:
        return f"{days}d"
    return f"{days}d overdue"
