import os
import time
import unittest
from datetime import date
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

import github
import people_table
from people_table import (
    DEFAULT_SORT,
    AgentCredit,
    ColumnKey,
    Direction,
    PeopleStatsInputs,
    Person,
    SortOrder,
    assemble_people_stats,
    build_people_stats,
    load_roster,
    parse_sort,
)
from regressions import RegressionRef, RegressionTally
from time_window import TimeWindow


def _window() -> TimeWindow:
    return TimeWindow.from_dates(date(2026, 8, 1), date(2026, 8, 31))


def _person(
    slug: str,
    *,
    team: str = "engineering",
    github_login: str | None = "",
    display_name: str | None = None,
) -> Person:
    login = slug if github_login == "" else github_login
    return Person(slug, display_name or slug.title(), team, login)


def _tally(*, authored: int = 0, approved: int = 0) -> RegressionTally:
    authored_refs = tuple(
        RegressionRef(f"APO-A{index}", None, f"https://github.com/example/repo/pull/{index}")
        for index in range(authored)
    )
    approved_refs = tuple(
        RegressionRef(f"APO-R{index}", None, f"https://github.com/example/repo/pull/{index + 50}")
        for index in range(approved)
    )
    return RegressionTally(authored=authored_refs, approved=approved_refs)


def _inputs(
    roster: tuple[Person, ...],
    *,
    merged: dict[str, int] | None = None,
    reviewed: dict[str, int] | None = None,
    agent: dict[str, AgentCredit] | None = None,
    regressions: dict[str, RegressionTally] | None = None,
    notes: tuple[str, ...] = (),
    github_available: bool = True,
    regressions_available: bool = True,
) -> PeopleStatsInputs:
    return PeopleStatsInputs(
        window=_window(),
        roster=roster,
        merged_by_login=merged or {},
        reviewed_by_login=reviewed or {},
        agent_by_login=agent or {},
        regressions_by_login=regressions or {},
        notes=notes,
        github_available=github_available,
        regressions_available=regressions_available,
    )


def _cell(row, key: ColumnKey):
    return row.cells[people_table._COLUMN_INDEX[key]]


def _row(stats, slug: str):
    return next(row for row in stats.rows if row.person.slug == slug)


class PeopleTableTest(unittest.TestCase):
    def setUp(self):
        people_table._cached_github_counts.cache_clear()
        people_table._cached_regression_tallies.cache_clear()
        self.addCleanup(people_table._cached_github_counts.cache_clear)
        self.addCleanup(people_table._cached_regression_tallies.cache_clear)
        env = patch.dict(os.environ, {"GITHUB_TOKEN": "test-token", "LINEAR_API_KEY": "test-key"})
        env.start()
        self.addCleanup(env.stop)

    def test_every_configured_person_appears_with_no_team_filter(self):
        roster = (
            _person("eng", team="engineering"),
            _person("other", team="unassigned"),
        )
        stats = assemble_people_stats(_inputs(roster), DEFAULT_SORT)
        self.assertEqual({row.person.slug for row in stats.rows}, {"eng", "other"})
        self.assertEqual({row.person.team for row in stats.rows}, {"engineering", "unassigned"})

    def test_missing_github_login_is_zero_with_no_links(self):
        roster = (
            _person("casey", github_login=None, display_name="Casey"),
            _person("alice", display_name="Alice"),
        )
        stats = assemble_people_stats(
            _inputs(roster, merged={"alice": 4}),
            DEFAULT_SORT,
        )
        casey = _row(stats, "casey")
        self.assertEqual([cell.text for cell in casey.cells], ["0", "0", "0", "—", "0", "0"])
        self.assertTrue(all(cell.href is None for cell in casey.cells))

    def test_assemble_people_stats_never_calls_the_network(self):
        roster = (_person("alice"),)
        with (
            patch.object(people_table, "get_merged_pr_activity") as activity,
            patch.object(people_table, "collect_regression_attributions") as collect,
            patch.object(github, "get_merged_pr_counts_for_user") as counts,
        ):
            assemble_people_stats(_inputs(roster, merged={"alice": 1}), DEFAULT_SORT)
        activity.assert_not_called()
        collect.assert_not_called()
        counts.assert_not_called()

    def test_build_people_stats_uses_one_bulk_github_fetch(self):
        window = _window()
        roster = (_person("alice"),)
        with (
            patch.object(people_table, "load_roster", return_value=roster),
            patch.object(
                people_table,
                "get_merged_pr_activity",
                return_value=({"alice": [{"author": {"login": "alice"}}]}, {}),
            ) as activity,
            patch.object(
                people_table, "collect_regression_attributions", return_value=([], 0)
            ) as collect,
            patch.object(github, "get_merged_pr_counts_for_user") as counts,
        ):
            stats = build_people_stats(window, DEFAULT_SORT)
        activity.assert_called_once()
        self.assertEqual(activity.call_args.args[0], window.duration_days)
        self.assertEqual(activity.call_args.args[1].start, window.start)
        self.assertEqual(activity.call_args.args[1].end, window.end)
        collect.assert_called_once()
        self.assertEqual(collect.call_args.args[0].start, window.start)
        self.assertEqual(collect.call_args.args[0].end, window.end)
        counts.assert_not_called()
        self.assertEqual(_cell(_row(stats, "alice"), ColumnKey.PRS_MERGED).text, "1")

    def test_cursor_credited_prs_increment_agent_count_and_merged(self):
        window = _window()
        roster = (_person("alice"),)
        prs = [
            {"author": {"login": "alice"}},
            {"author": {"login": "alice"}},
            {"author": {"login": "cursor"}},
        ]
        with (
            patch.object(people_table, "load_roster", return_value=roster),
            patch.object(people_table, "get_merged_pr_activity", return_value=({"alice": prs}, {})),
            patch.object(people_table, "collect_regression_attributions", return_value=([], 0)),
        ):
            stats = build_people_stats(window, DEFAULT_SORT)
        alice = _row(stats, "alice")
        self.assertEqual(_cell(alice, ColumnKey.PRS_MERGED).text, "3")
        self.assertEqual(_cell(alice, ColumnKey.AGENT_PRS).text, "1")
        self.assertEqual(_cell(alice, ColumnKey.AGENT_PRS).note, "Cursor")
        self.assertEqual(_cell(alice, ColumnKey.AGENT_SHARE).text, "33%")

    def test_agent_share_is_missing_when_merged_is_zero(self):
        roster = (_person("alice"),)
        stats = assemble_people_stats(_inputs(roster), DEFAULT_SORT)
        self.assertEqual(_cell(_row(stats, "alice"), ColumnKey.AGENT_SHARE).text, "—")

    def test_parse_sort_is_total_and_signed(self):
        self.assertEqual(parse_sort(None), DEFAULT_SORT)
        self.assertEqual(parse_sort(""), DEFAULT_SORT)
        self.assertEqual(parse_sort("nope"), DEFAULT_SORT)
        self.assertEqual(parse_sort("-nope"), DEFAULT_SORT)
        self.assertEqual(parse_sort("prs_merged"), SortOrder(ColumnKey.PRS_MERGED, Direction.ASC))
        self.assertEqual(parse_sort("-prs_merged"), SortOrder(ColumnKey.PRS_MERGED, Direction.DESC))
        self.assertEqual(DEFAULT_SORT.token, "-prs_merged")
        self.assertEqual(parse_sort("person").token, "person")
        self.assertEqual(parse_sort("-agent_share").key, ColumnKey.AGENT_SHARE)

    def test_sort_by_each_column(self):
        roster = (
            _person("alice", display_name="Alice"),
            _person("bob", display_name="Bob"),
            _person("dana", display_name="Dana"),
        )
        inputs = _inputs(
            roster,
            merged={"alice": 10, "bob": 5, "dana": 1},
            reviewed={"alice": 1, "bob": 8, "dana": 3},
            agent={
                "alice": AgentCredit(1, ("Cursor",)),
                "bob": AgentCredit(5, ("Cursor",)),
            },
            regressions={
                "alice": _tally(approved=3),
                "bob": _tally(authored=2, approved=1),
                "dana": _tally(authored=1),
            },
        )
        expected = {
            ColumnKey.PERSON: ["Alice", "Bob", "Dana"],
            ColumnKey.PRS_MERGED: ["Alice", "Bob", "Dana"],
            ColumnKey.PRS_REVIEWED: ["Bob", "Dana", "Alice"],
            ColumnKey.AGENT_PRS: ["Bob", "Alice", "Dana"],
            ColumnKey.AGENT_SHARE: ["Bob", "Alice", "Dana"],
            ColumnKey.REGRESSIONS_AUTHORED: ["Bob", "Dana", "Alice"],
            ColumnKey.REGRESSIONS_APPROVED: ["Alice", "Bob", "Dana"],
        }
        for key, names in expected.items():
            direction = Direction.ASC if key is ColumnKey.PERSON else Direction.DESC
            stats = assemble_people_stats(inputs, SortOrder(key, direction))
            self.assertEqual(
                [row.person.display_name for row in stats.rows],
                names,
                key,
            )

    def test_header_toggle_preserves_window_args(self):
        roster = (_person("alice"),)
        stats = assemble_people_stats(_inputs(roster), DEFAULT_SORT)
        merged = next(header for header in stats.headers if header.key is ColumnKey.PRS_MERGED)
        person = next(header for header in stats.headers if header.key is ColumnKey.PERSON)
        self.assertEqual(merged.sort_query["start"], "2026-08-01")
        self.assertEqual(merged.sort_query["end"], "2026-08-31")
        self.assertEqual(merged.sort_query["sort"], "prs_merged")
        self.assertEqual(person.sort_query["sort"], "person")
        self.assertEqual(merged.aria_sort, "descending")
        self.assertEqual(person.aria_sort, "none")
        toggled = assemble_people_stats(_inputs(roster), parse_sort("prs_merged"))
        merged_asc = next(
            header for header in toggled.headers if header.key is ColumnKey.PRS_MERGED
        )
        self.assertEqual(merged_asc.sort_query["sort"], "-prs_merged")
        self.assertEqual(merged_asc.sort_query["start"], "2026-08-01")

    def test_zero_cells_have_no_href(self):
        roster = (
            _person("alice"),
            _person("bob"),
        )
        stats = assemble_people_stats(
            _inputs(
                roster,
                merged={"alice": 2},
                reviewed={"alice": 1},
                regressions={"alice": _tally(authored=1)},
            ),
            DEFAULT_SORT,
        )
        bob = _row(stats, "bob")
        self.assertTrue(all(cell.href is None for cell in bob.cells))
        alice = _row(stats, "alice")
        self.assertIsNotNone(_cell(alice, ColumnKey.PRS_MERGED).href)
        self.assertIsNone(_cell(alice, ColumnKey.PRS_REVIEWED).href)
        self.assertIsNone(_cell(alice, ColumnKey.AGENT_PRS).href)
        self.assertIsNone(_cell(alice, ColumnKey.REGRESSIONS_APPROVED).href)
        merged_query = parse_qs(urlparse(_cell(alice, ColumnKey.PRS_MERGED).href).query)["q"][0]
        self.assertIn("author:alice", unquote(merged_query))
        self.assertIn("is:closed", unquote(merged_query))
        self.assertNotIn("reviewed-by:", unquote(merged_query))
        authored_href = _cell(alice, ColumnKey.REGRESSIONS_AUTHORED).href
        self.assertIn("/issues/APO-A0", authored_href)

    def test_failed_github_load_is_unavailable_not_a_measured_zero(self):
        roster = (
            _person("casey", github_login=None, display_name="Casey"),
            _person("alice", display_name="Alice"),
            _person("bob", display_name="Bob"),
        )
        stats = assemble_people_stats(
            _inputs(
                roster,
                merged={"alice": 10, "bob": 4},
                notes=("Unable to load GitHub PR stats.",),
                github_available=False,
            ),
            DEFAULT_SORT,
        )
        alice = _row(stats, "alice")
        casey = _row(stats, "casey")
        self.assertEqual(_cell(alice, ColumnKey.PRS_MERGED).text, "—")
        self.assertEqual(_cell(alice, ColumnKey.PRS_REVIEWED).text, "—")
        self.assertEqual(_cell(alice, ColumnKey.AGENT_SHARE).text, "—")
        self.assertIsNone(_cell(alice, ColumnKey.PRS_MERGED).href)
        self.assertIsNone(_cell(alice, ColumnKey.PRS_MERGED).stdev)
        self.assertEqual(_cell(casey, ColumnKey.PRS_MERGED).text, "0")
        self.assertIsNone(_cell(casey, ColumnKey.PRS_MERGED).stdev)
        self.assertIn("Unable to load GitHub PR stats.", stats.notes)

    def test_missing_credentials_skip_fetches(self):
        roster = (_person("alice"),)
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(people_table, "load_roster", return_value=roster),
            patch.object(people_table, "get_merged_pr_activity") as activity,
            patch.object(people_table, "collect_regression_attributions") as collect,
        ):
            people_table._cached_github_counts.cache_clear()
            people_table._cached_regression_tallies.cache_clear()
            stats = build_people_stats(_window(), DEFAULT_SORT)
        activity.assert_not_called()
        collect.assert_not_called()
        alice = _row(stats, "alice")
        self.assertEqual(_cell(alice, ColumnKey.PRS_MERGED).text, "—")
        self.assertEqual(_cell(alice, ColumnKey.REGRESSIONS_AUTHORED).text, "—")
        self.assertIn("GitHub credentials are not configured.", stats.notes)
        self.assertIn("Linear credentials are not configured.", stats.notes)

    def test_slow_regressions_do_not_block_github_rows(self):
        roster = (_person("alice"),)

        def hang_regressions(*args, **kwargs):
            time.sleep(1)
            return {}

        started = time.monotonic()
        with (
            patch.object(people_table, "PEOPLE_STATS_FETCH_TIMEOUT_SECONDS", 0.05),
            patch.object(people_table, "load_roster", return_value=roster),
            patch.object(
                people_table,
                "get_merged_pr_activity",
                return_value=({"alice": [{"author": {"login": "alice"}}]}, {}),
            ),
            patch.object(people_table, "_cached_regression_tallies", side_effect=hang_regressions),
        ):
            stats = build_people_stats(_window(), DEFAULT_SORT)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.5)
        alice = _row(stats, "alice")
        self.assertEqual(_cell(alice, ColumnKey.PRS_MERGED).text, "1")
        self.assertEqual(_cell(alice, ColumnKey.REGRESSIONS_AUTHORED).text, "—")
        self.assertIn("Regression attributions took too long to load.", stats.notes)

    def test_stdev_tooltip_uses_all_people_and_includes_zeros(self):
        roster = (
            _person("alice", display_name="Alice"),
            _person("bob", display_name="Bob"),
        )
        stats = assemble_people_stats(
            _inputs(roster, merged={"alice": 10, "bob": 0}),
            DEFAULT_SORT,
        )
        alice_badge = _cell(_row(stats, "alice"), ColumnKey.PRS_MERGED).stdev
        bob_badge = _cell(_row(stats, "bob"), ColumnKey.PRS_MERGED).stdev
        self.assertIsNotNone(alice_badge)
        self.assertIsNotNone(bob_badge)
        self.assertIn("all people", alice_badge.tooltip)
        self.assertEqual(alice_badge.tooltip, bob_badge.tooltip)
        self.assertEqual(alice_badge.label, "+1.0σ")
        self.assertEqual(bob_badge.label, "−1.0σ")

    def test_blank_github_username_normalizes_to_none(self):
        config = {
            "people": {
                "casey": {
                    "team": "unassigned",
                    "linear_username": "casey",
                    "github_username": "",
                },
                "justin": {
                    "team": "unassigned",
                    "linear_username": "justin.isenhart",
                    "github_username": None,
                },
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                    "github_username": "Alice",
                },
            }
        }
        with patch("people_table.load_config", return_value=config):
            roster = load_roster()
        self.assertEqual([person.github_login for person in roster], [None, None, "Alice"])
        self.assertEqual(roster[1].display_name, "Justin Isenhart")
        self.assertEqual(roster[2].github_key, "alice")


if __name__ == "__main__":
    unittest.main()
