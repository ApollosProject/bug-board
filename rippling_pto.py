import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

logger = logging.getLogger(__name__)

RIPPLING_PTO_CALENDAR_ENV_VAR = "RIPPLING_PTO_CALENDAR_URL"
RIPPLING_PTO_TIMEZONE_ENV_VAR = "RIPPLING_PTO_TIMEZONE"
DEFAULT_RIPPLING_PTO_TIMEZONE = "America/New_York"
RIPPLING_PTO_TIMEOUT_SECONDS = 10
MAX_RIPPLING_PTO_CALENDAR_BYTES = 1_000_000
RIPPLING_PTO_HOST = "app.rippling.com"
RIPPLING_PTO_PATH_PREFIX = "/api/feed/calendar/pto/"


@dataclass(frozen=True)
class PTOEvent:
    summary: str
    start: date
    end: date
    is_all_day: bool


@dataclass(frozen=True)
class PTOCalendar:
    configured: bool
    available: bool
    events: tuple[PTOEvent, ...] = ()


def _https_calendar_url(value: str) -> str:
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid calendar URL port") from exc

    if (
        parsed.scheme.lower() not in {"https", "webcal"}
        or parsed.hostname != RIPPLING_PTO_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or not parsed.path.startswith(RIPPLING_PTO_PATH_PREFIX)
        or parsed.fragment
    ):
        raise ValueError("invalid Rippling PTO calendar URL")

    return urlunsplit(("https", RIPPLING_PTO_HOST, parsed.path, parsed.query, ""))


def _unfold_content_lines(calendar_data: bytes) -> list[str]:
    text = calendar_data.decode("utf-8-sig")
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    lines: list[str] = []
    for raw_line in raw_lines:
        if raw_line.startswith((" ", "\t")):
            if not lines:
                raise ValueError("invalid folded iCalendar line")
            lines[-1] += raw_line[1:]
        else:
            lines.append(raw_line)
    return lines


def _parse_content_line(line: str) -> tuple[str, dict[str, str], str] | None:
    header, separator, value = line.partition(":")
    if not separator:
        return None

    header_parts = header.split(";")
    name = header_parts[0].upper()
    parameters: dict[str, str] = {}
    for raw_parameter in header_parts[1:]:
        key, equals, parameter_value = raw_parameter.partition("=")
        if equals:
            parameters[key.upper()] = parameter_value.strip('"')
    return name, parameters, value


def _unescape_text(value: str) -> str:
    replacements = {"n": "\n", "N": "\n", ",": ",", ";": ";", "\\": "\\"}
    return re.sub(r"\\([nN,;\\])", lambda match: replacements[match.group(1)], value)


def _parse_calendar_datetime(
    value: str,
    parameters: dict[str, str],
    display_timezone: ZoneInfo,
) -> tuple[datetime, bool]:
    value_type = parameters.get("VALUE", "").upper()
    if value_type == "DATE" or re.fullmatch(r"\d{8}", value):
        parsed_date = datetime.strptime(value, "%Y%m%d")
        return parsed_date.replace(tzinfo=display_timezone), True

    if value.endswith("Z"):
        parsed_datetime = datetime.strptime(value, "%Y%m%dT%H%M%SZ")
        return parsed_datetime.replace(tzinfo=timezone.utc), False

    parsed_datetime = datetime.strptime(value, "%Y%m%dT%H%M%S")
    timezone_name = parameters.get("TZID")
    if timezone_name:
        try:
            event_timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("unknown iCalendar time zone") from exc
    else:
        event_timezone = display_timezone
    return parsed_datetime.replace(tzinfo=event_timezone), False


def _event_from_properties(
    properties: dict[str, tuple[dict[str, str], str]],
    display_timezone: ZoneInfo,
) -> PTOEvent | None:
    status_property = properties.get("STATUS")
    if status_property and status_property[1].strip().upper() == "CANCELLED":
        return None

    uid_property = properties.get("UID")
    summary_property = properties.get("SUMMARY")
    start_property = properties.get("DTSTART")
    if (
        not uid_property
        or not uid_property[1].strip()
        or not summary_property
        or not start_property
    ):
        raise ValueError("iCalendar event is missing a required property")

    summary = _unescape_text(summary_property[1]).strip()
    if not summary:
        raise ValueError("iCalendar event has an empty summary")

    start_datetime, start_is_all_day = _parse_calendar_datetime(
        start_property[1],
        start_property[0],
        display_timezone,
    )
    end_property = properties.get("DTEND")
    if end_property:
        end_datetime, end_is_all_day = _parse_calendar_datetime(
            end_property[1],
            end_property[0],
            display_timezone,
        )
        if start_is_all_day != end_is_all_day:
            raise ValueError("iCalendar event start and end types do not match")
    else:
        end_datetime = start_datetime
        end_is_all_day = start_is_all_day

    if end_datetime < start_datetime:
        raise ValueError("iCalendar event ends before it starts")

    if start_is_all_day:
        start_date = start_datetime.date()
        end_date = (
            (end_datetime - timedelta(days=1)).date()
            if end_datetime > start_datetime
            else start_date
        )
    else:
        local_start = start_datetime.astimezone(display_timezone)
        local_end = end_datetime.astimezone(display_timezone)
        inclusive_end = (
            local_end - timedelta(microseconds=1) if local_end > local_start else local_end
        )
        start_date = local_start.date()
        end_date = max(inclusive_end.date(), start_date)

    return PTOEvent(
        summary=summary,
        start=start_date,
        end=end_date,
        is_all_day=start_is_all_day,
    )


def parse_icalendar(
    calendar_data: bytes,
    timezone_name: str = DEFAULT_RIPPLING_PTO_TIMEZONE,
) -> tuple[PTOEvent, ...]:
    try:
        display_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("unknown Rippling PTO display time zone") from exc

    lines = _unfold_content_lines(calendar_data)
    normalized_lines = {line.strip().upper() for line in lines}
    if "BEGIN:VCALENDAR" not in normalized_lines or "END:VCALENDAR" not in normalized_lines:
        raise ValueError("response is not an iCalendar")

    events: list[PTOEvent] = []
    current_event: dict[str, tuple[dict[str, str], str]] | None = None
    invalid_event_count = 0
    captured_properties = {"UID", "SUMMARY", "DTSTART", "DTEND", "STATUS"}

    for line in lines:
        parsed_line = _parse_content_line(line)
        if parsed_line is None:
            continue
        name, parameters, value = parsed_line
        normalized_value = value.strip().upper()

        if name == "BEGIN" and normalized_value == "VEVENT":
            if current_event is not None:
                raise ValueError("nested iCalendar events are not supported")
            current_event = {}
            continue

        if name == "END" and normalized_value == "VEVENT":
            if current_event is None:
                continue
            try:
                event = _event_from_properties(current_event, display_timezone)
            except ValueError:
                invalid_event_count += 1
            else:
                if event is not None:
                    events.append(event)
            current_event = None
            continue

        if current_event is not None and name in captured_properties:
            current_event.setdefault(name, (parameters, value))

    if current_event is not None:
        raise ValueError("unterminated iCalendar event")
    if invalid_event_count:
        logger.warning("Skipped %d invalid Rippling PTO calendar event(s)", invalid_event_count)

    return tuple(events)


def get_rippling_pto_calendar() -> PTOCalendar:
    configured_url = os.getenv(RIPPLING_PTO_CALENDAR_ENV_VAR, "").strip()
    if not configured_url:
        return PTOCalendar(configured=False, available=False)

    try:
        calendar_url = _https_calendar_url(configured_url)
        response = requests.get(
            calendar_url,
            timeout=RIPPLING_PTO_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise ValueError("Rippling PTO calendar returned an unsuccessful status")
        if len(response.content) > MAX_RIPPLING_PTO_CALENDAR_BYTES:
            raise ValueError("Rippling PTO calendar response is too large")
        timezone_name = (
            os.getenv(RIPPLING_PTO_TIMEZONE_ENV_VAR, "").strip() or DEFAULT_RIPPLING_PTO_TIMEZONE
        )
        events = parse_icalendar(response.content, timezone_name=timezone_name)
    except (OSError, UnicodeError, ValueError, requests.RequestException) as exc:
        logger.warning(
            "Rippling PTO calendar is unavailable (%s)",
            type(exc).__name__,
        )
        return PTOCalendar(configured=True, available=False)

    return PTOCalendar(configured=True, available=True, events=events)
