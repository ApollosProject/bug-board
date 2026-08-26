from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from typing import Any, Final
from urllib.parse import quote

from config import load_config
from github import get_merged_pr_activity
from person_stats import format_stdev_label, format_stdev_tooltip, stdev_tone, z_score
from regressions import (
    EMPTY_REGRESSION_TALLY,
    RegressionRef,
    RegressionTally,
    collect_regression_attributions,
    tally_regressions_by_login,
)
from time_window import TimeWindow

PEOPLE_STATS_TTL_SECONDS: Final = 60
STDEV_COHORT_LABEL: Final = "all people"
MISSING_TEXT: Final = "—"
CURSOR_APP_LOGIN: Final = "cursor"


@dataclass(frozen=True, slots=True)
class Person:
    slug: str
    display_name: str
    team: str
    github_login: str | None

    @property
    def github_key(self) -> str:
        return (self.github_login or "").casefold()


def load_roster() -> tuple[Person, ...]:
    people = load_config().get("people") or {}
    roster: list[Person] = []
    for slug, info in people.items():
        entry = info or {}
        raw_login = entry.get("github_username")
        github_login = (
            raw_login.strip() if isinstance(raw_login, str) and raw_login.strip() else None
        )
        linear_username = entry.get("linear_username") or slug
        display_name = re.sub(r"[._-]+", " ", str(linear_username)).title()
        roster.append(
            Person(
                slug=str(slug),
                display_name=display_name,
                team=str(entry.get("team") or ""),
                github_login=github_login,
            )
        )
    return tuple(roster)


@dataclass(frozen=True, slots=True)
class AgentCredit:
    prs: int = 0
    labels: tuple[str, ...] = ()


NO_AGENT_CREDIT: Final = AgentCredit()


@dataclass(frozen=True, slots=True)
class PersonFacts:
    person: Person
    prs_merged: int | None
    prs_reviewed: int | None
    agent: AgentCredit | None
    regressions: RegressionTally | None


@dataclass(frozen=True, slots=True)
class StdevBadge:
    label: str
    tooltip: str
    tone: str | None


@dataclass(frozen=True, slots=True)
class Cell:
    text: str
    sort_key: tuple[float, str]
    href: str | None = None
    note: str | None = None
    stdev: StdevBadge | None = None


class ColumnKey(StrEnum):
    PERSON = "person"
    PRS_MERGED = "prs_merged"
    PRS_REVIEWED = "prs_reviewed"
    AGENT_PRS = "agent_prs"
    AGENT_SHARE = "agent_share"
    REGRESSIONS_AUTHORED = "regressions_authored"
    REGRESSIONS_APPROVED = "regressions_approved"


Measure = Callable[[PersonFacts], float | None]
Render = Callable[[PersonFacts, float | None], str]
Link = Callable[[PersonFacts, TimeWindow], str | None]
Note = Callable[[PersonFacts], str | None]


def render_count(_facts: PersonFacts, value: float | None) -> str:
    if value is None:
        return MISSING_TEXT
    return str(int(value))


def render_percent(_facts: PersonFacts, value: float | None) -> str:
    if value is None:
        return MISSING_TEXT
    return f"{value:.0%}"


def no_link(_facts: PersonFacts, _window: TimeWindow) -> str | None:
    return None


def no_note(_facts: PersonFacts) -> str | None:
    return None


def _github_search_url(query: str) -> str:
    return f"https://github.com/pulls?q={quote(query, safe='')}"


def _merged_prs_link(facts: PersonFacts, window: TimeWindow) -> str | None:
    login = facts.person.github_login
    if not login:
        return None
    query = f"is:closed is:pr author:{login} archived:false {window.github_merged_qualifier()}"
    return _github_search_url(query)


def _regression_list_url(refs: Sequence[RegressionRef]) -> str | None:
    identifiers = tuple(dict.fromkeys(ref.identifier for ref in refs if ref.identifier))
    if not identifiers:
        return None
    return f"https://linear.app/differential/issues/{','.join(identifiers)}"


def _authored_regressions_link(facts: PersonFacts, _window: TimeWindow) -> str | None:
    if facts.regressions is None:
        return None
    return _regression_list_url(facts.regressions.authored)


def _approved_regressions_link(facts: PersonFacts, _window: TimeWindow) -> str | None:
    if facts.regressions is None:
        return None
    return _regression_list_url(facts.regressions.approved)


def _agent_share(facts: PersonFacts) -> float | None:
    if facts.prs_merged is None or facts.agent is None or facts.prs_merged == 0:
        return None
    return facts.agent.prs / facts.prs_merged


def _agent_prs(facts: PersonFacts) -> float | None:
    if facts.agent is None:
        return None
    return facts.agent.prs


def _agent_note(facts: PersonFacts) -> str | None:
    if facts.agent is None:
        return None
    return " · ".join(facts.agent.labels) or None


def _authored_count(facts: PersonFacts) -> float | None:
    if facts.regressions is None:
        return None
    return facts.regressions.authored_count


def _approved_count(facts: PersonFacts) -> float | None:
    if facts.regressions is None:
        return None
    return facts.regressions.approved_count


@dataclass(frozen=True, slots=True)
class StatColumn:
    key: ColumnKey
    label: str
    measure: Measure
    render: Render = render_count
    link: Link = no_link
    note: Note = no_note
    stdev: bool = False


STAT_COLUMNS: Final[tuple[StatColumn, ...]] = (
    StatColumn(
        ColumnKey.PRS_MERGED,
        "PRs merged",
        measure=lambda facts: facts.prs_merged,
        link=_merged_prs_link,
        stdev=True,
    ),
    StatColumn(
        ColumnKey.PRS_REVIEWED,
        "PRs reviewed",
        measure=lambda facts: facts.prs_reviewed,
        stdev=True,
    ),
    StatColumn(
        ColumnKey.AGENT_PRS,
        "Agent PRs",
        measure=_agent_prs,
        note=_agent_note,
    ),
    StatColumn(
        ColumnKey.AGENT_SHARE,
        "Agent share",
        measure=_agent_share,
        render=render_percent,
    ),
    StatColumn(
        ColumnKey.REGRESSIONS_AUTHORED,
        "Regressions authored",
        measure=_authored_count,
        link=_authored_regressions_link,
    ),
    StatColumn(
        ColumnKey.REGRESSIONS_APPROVED,
        "Regressions approved",
        measure=_approved_count,
        link=_approved_regressions_link,
    ),
)

_COLUMN_INDEX: Final[Mapping[ColumnKey, int]] = {
    column.key: index for index, column in enumerate(STAT_COLUMNS)
}


class Direction(StrEnum):
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True, slots=True)
class SortOrder:
    key: ColumnKey
    direction: Direction

    @property
    def token(self) -> str:
        prefix = "-" if self.direction is Direction.DESC else ""
        return f"{prefix}{self.key.value}"

    @property
    def descending(self) -> bool:
        return self.direction is Direction.DESC

    def toggled(self, key: ColumnKey) -> SortOrder:
        if self.key is key:
            flipped = Direction.ASC if self.direction is Direction.DESC else Direction.DESC
            return SortOrder(key, flipped)
        natural = Direction.ASC if key is ColumnKey.PERSON else Direction.DESC
        return SortOrder(key, natural)


DEFAULT_SORT: Final = SortOrder(ColumnKey.PRS_MERGED, Direction.DESC)


def parse_sort(raw: str | None) -> SortOrder:
    if not raw:
        return DEFAULT_SORT
    descending = raw.startswith("-")
    key_raw = raw[1:] if descending else raw
    try:
        key = ColumnKey(key_raw)
    except ValueError:
        return DEFAULT_SORT
    return SortOrder(key, Direction.DESC if descending else Direction.ASC)


@dataclass(frozen=True, slots=True)
class Row:
    person: Person
    cells: tuple[Cell, ...]

    def sort_key(self, order: SortOrder) -> tuple[float, str, str]:
        name = self.person.display_name.casefold()
        if order.key is ColumnKey.PERSON:
            return (0.0, name, self.person.team)
        magnitude, _name = self.cells[_COLUMN_INDEX[order.key]].sort_key
        return (magnitude, name, self.person.team)


@dataclass(frozen=True, slots=True)
class ColumnHeader:
    key: ColumnKey
    label: str
    numeric: bool
    aria_sort: str
    sort_query: Mapping[str, str | int]


@dataclass(frozen=True, slots=True)
class PeopleStats:
    window: TimeWindow
    order: SortOrder
    headers: tuple[ColumnHeader, ...]
    rows: tuple[Row, ...]
    notes: tuple[str, ...]

    @property
    def window_query(self) -> Mapping[str, str | int]:
        return self.window.query_args()


@dataclass(frozen=True, slots=True)
class PeopleStatsInputs:
    window: TimeWindow
    roster: tuple[Person, ...]
    merged_by_login: Mapping[str, int]
    reviewed_by_login: Mapping[str, int]
    agent_by_login: Mapping[str, AgentCredit]
    regressions_by_login: Mapping[str, RegressionTally]
    notes: tuple[str, ...] = ()
    github_available: bool = True
    regressions_available: bool = True


def build_people_stats(window: TimeWindow, order: SortOrder) -> PeopleStats:
    return assemble_people_stats(_gather(*window.cache_parts(), _cache_epoch()), order)


def assemble_people_stats(inputs: PeopleStatsInputs, order: SortOrder) -> PeopleStats:
    facts = tuple(_facts_for(person, inputs) for person in inputs.roster)
    columns = tuple(_column_cells(column, facts, inputs.window) for column in STAT_COLUMNS)
    rows = tuple(Row(f.person, tuple(col[i] for col in columns)) for i, f in enumerate(facts))
    rows = tuple(sorted(rows, key=lambda row: row.sort_key(order), reverse=order.descending))
    return PeopleStats(
        window=inputs.window,
        order=order,
        headers=_headers(order, inputs.window),
        rows=rows,
        notes=inputs.notes,
    )


def _cache_epoch() -> int:
    return int(time.time() / PEOPLE_STATS_TTL_SECONDS)


@lru_cache(maxsize=8)
def _gather(
    days: int | None, start: str | None, end: str | None, _cache_epoch: int
) -> PeopleStatsInputs:
    notes: list[str] = []
    window = TimeWindow.resolve(days, start=start, end=end)
    roster = load_roster()
    github_ok = bool(os.getenv("GITHUB_TOKEN"))
    linear_ok = bool(os.getenv("LINEAR_API_KEY"))
    if not github_ok:
        notes.append("GitHub credentials are not configured.")
    if not linear_ok:
        notes.append("Linear credentials are not configured.")

    merged_by_login: dict[str, int] = {}
    reviewed_by_login: dict[str, int] = {}
    agent_by_login: dict[str, AgentCredit] = {}
    regressions_by_login: dict[str, RegressionTally] = {}
    github_available = False
    regressions_available = False

    with ThreadPoolExecutor(max_workers=2) as executor:
        github_future = (
            executor.submit(get_merged_pr_activity, window.duration_days, window)
            if github_ok
            else None
        )
        regression_future = (
            executor.submit(collect_regression_attributions, window) if linear_ok else None
        )
        if github_future is not None:
            try:
                prs_by_author, prs_by_reviewer = github_future.result()
                merged_by_login, reviewed_by_login, agent_by_login = _counts_from_activity(
                    prs_by_author, prs_by_reviewer
                )
                github_available = True
            except Exception:
                logging.exception("Failed to load GitHub PR activity for people stats")
                notes.append("Unable to load GitHub PR stats.")
        if regression_future is not None:
            try:
                records, _failed = regression_future.result()
                regressions_by_login = tally_regressions_by_login(records, window)
                regressions_available = True
            except Exception:
                logging.exception("Failed to load regression attributions for people stats")
                notes.append("Unable to load regression attributions.")

    return PeopleStatsInputs(
        window=window,
        roster=roster,
        merged_by_login=merged_by_login,
        reviewed_by_login=reviewed_by_login,
        agent_by_login=agent_by_login,
        regressions_by_login=regressions_by_login,
        notes=tuple(notes),
        github_available=github_available,
        regressions_available=regressions_available,
    )


def _counts_from_activity(
    prs_by_author: Mapping[str, Sequence[Mapping[str, Any]]],
    prs_by_reviewer: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[dict[str, int], dict[str, int], dict[str, AgentCredit]]:
    merged_by_login: dict[str, int] = {}
    agent_by_login: dict[str, AgentCredit] = {}
    for login, prs in prs_by_author.items():
        key = login.casefold()
        merged_by_login[key] = merged_by_login.get(key, 0) + len(prs)
        credit = _agent_credit(prs)
        if credit.prs:
            existing = agent_by_login.get(key, NO_AGENT_CREDIT)
            labels = tuple(dict.fromkeys((*existing.labels, *credit.labels)))
            agent_by_login[key] = AgentCredit(existing.prs + credit.prs, labels)
    reviewed_by_login: dict[str, int] = {}
    for login, prs in prs_by_reviewer.items():
        key = login.casefold()
        reviewed_by_login[key] = reviewed_by_login.get(key, 0) + len(prs)
    return merged_by_login, reviewed_by_login, agent_by_login


def _agent_credit(prs: Sequence[Mapping[str, Any]]) -> AgentCredit:
    cursor_count = sum(
        1
        for pr in prs
        if ((pr.get("author") or {}).get("login") or "").casefold() == CURSOR_APP_LOGIN
    )
    if cursor_count == 0:
        return NO_AGENT_CREDIT
    return AgentCredit(prs=cursor_count, labels=("Cursor",))


def _facts_for(person: Person, inputs: PeopleStatsInputs) -> PersonFacts:
    if person.github_login is None:
        return PersonFacts(person, 0, 0, NO_AGENT_CREDIT, EMPTY_REGRESSION_TALLY)
    key = person.github_key
    return PersonFacts(
        person,
        inputs.merged_by_login.get(key, 0) if inputs.github_available else None,
        inputs.reviewed_by_login.get(key, 0) if inputs.github_available else None,
        inputs.agent_by_login.get(key, NO_AGENT_CREDIT) if inputs.github_available else None,
        (
            inputs.regressions_by_login.get(key, EMPTY_REGRESSION_TALLY)
            if inputs.regressions_available
            else None
        ),
    )


def _column_cells(
    column: StatColumn, facts: Sequence[PersonFacts], window: TimeWindow
) -> tuple[Cell, ...]:
    values = [column.measure(fact) for fact in facts]
    present = [value for value in values if value is not None]
    badges = _stdev_badges(values, present) if column.stdev else [None] * len(values)
    cells: list[Cell] = []
    for fact, value, badge in zip(facts, values, badges, strict=True):
        magnitude = value if value is not None else float("-inf")
        cell = Cell(
            text=column.render(fact, value),
            sort_key=(magnitude, fact.person.display_name.casefold()),
            href=column.link(fact, window) if value else None,
            note=column.note(fact),
            stdev=badge,
        )
        if not value and cell.href is not None:
            raise RuntimeError("zero and unavailable cells cannot link")
        cells.append(cell)
    return tuple(cells)


def _stdev_badges(
    values: Sequence[float | None], present: Sequence[float]
) -> list[StdevBadge | None]:
    baseline = list(present)
    badges: list[StdevBadge | None] = []
    for value in values:
        if value is None:
            badges.append(None)
            continue
        z = z_score(value, baseline)
        if z is None:
            badges.append(None)
            continue
        badges.append(
            StdevBadge(
                label=format_stdev_label(z),
                tooltip=format_stdev_tooltip(baseline, cohort=STDEV_COHORT_LABEL),
                tone=stdev_tone(z),
            )
        )
    return badges


def _headers(order: SortOrder, window: TimeWindow) -> tuple[ColumnHeader, ...]:
    window_query = window.query_args()
    person = ColumnHeader(
        key=ColumnKey.PERSON,
        label="Person",
        numeric=False,
        aria_sort=_aria_sort(order, ColumnKey.PERSON),
        sort_query={**window_query, "sort": order.toggled(ColumnKey.PERSON).token},
    )
    rest = tuple(
        ColumnHeader(
            key=column.key,
            label=column.label,
            numeric=True,
            aria_sort=_aria_sort(order, column.key),
            sort_query={**window_query, "sort": order.toggled(column.key).token},
        )
        for column in STAT_COLUMNS
    )
    headers = (person, *rest)
    allowed_aria = {"none", "ascending", "descending"}
    for header in headers:
        if header.aria_sort not in allowed_aria:
            raise RuntimeError("invalid aria-sort")
        if "sort" not in header.sort_query:
            raise RuntimeError("sort query missing token")
        if header.numeric is (header.key is ColumnKey.PERSON):
            raise RuntimeError("only metric columns are numeric")
    return headers


def _aria_sort(order: SortOrder, key: ColumnKey) -> str:
    if order.key is not key:
        return "none"
    return "descending" if order.descending else "ascending"
