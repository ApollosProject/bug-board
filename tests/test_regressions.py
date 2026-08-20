import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import github_regressions
from github_regressions import deleted_line_numbers, parse_pull_request_url
from regressions import (
    _person_metrics,
    apply_regression_overrides,
    build_regression_summary,
    extract_fixing_pr_urls,
    load_regression_overrides,
    merge_issue_attributions,
)
from time_window import TimeWindow


class RegressionAttributionTest(unittest.TestCase):
    def test_parses_pull_request_urls_and_deleted_lines(self):
        self.assertEqual(
            parse_pull_request_url("https://github.com/example/repo/pull/12"),
            ("example", "repo", 12),
        )
        unified_diff = """@@ -10,4 +10,4 @@
 context
-removed one
+replacement
 context
-removed two
@@ -30,2 +31,3 @@
-removed three
+new one
+new two
 context
"""
        self.assertEqual(deleted_line_numbers(unified_diff), [11, 13, 30])

    def test_maps_blame_overlap_to_human_reviewers_and_recency_score(self):
        fixing_merged_at = datetime(2026, 8, 10, tzinfo=timezone.utc)
        inducing = {
            "url": "https://github.com/example/repo/pull/9",
            "mergedAt": "2026-07-11T00:00:00Z",
            "author": {"login": "author"},
            "reviews": {
                "nodes": [
                    {"author": {"login": "author"}},
                    {"author": {"login": "reviewer"}},
                    {"author": {"login": "Reviewer"}},
                    {"author": {"login": "copilot[bot]"}},
                ]
            },
        }
        mapped = github_regressions._original_merged_pull_request(
            {
                "committedDate": "2026-07-10T23:00:00Z",
                "associatedPullRequests": {
                    "nodes": [
                        inducing,
                        {
                            "url": "https://github.com/example/repo/pull/11",
                            "mergedAt": "2026-08-11T00:00:00Z",
                        },
                    ]
                },
            },
            fixing_merged_at,
        )
        self.assertEqual(mapped["url"], inducing["url"])
        self.assertEqual(github_regressions._reviewer_logins(mapped), ["reviewer"])
        self.assertEqual(github_regressions._line_overlap_count([11, 13, 30], 10, 13), 2)
        score, age_days = github_regressions._candidate_score(
            2, datetime(2026, 7, 11, tzinfo=timezone.utc), fixing_merged_at
        )
        self.assertEqual((score, age_days), (1.0, 30))

        with patch.object(
            github_regressions,
            "_execute",
            return_value={
                "repository": {
                    "object": {
                        "blame": {
                            "ranges": [
                                {"startingLine": 11, "endingLine": 13, "commit": {}},
                                "ignored",
                            ]
                        }
                    }
                }
            },
        ):
            ranges = github_regressions._get_blame_ranges("example", "repo", "abc", "src.py")
        self.assertEqual(ranges, [{"startingLine": 11, "endingLine": 13, "commit": {}}])

    def test_missing_blame_marks_analysis_incomplete(self):
        pull_request = {
            "merged": True,
            "merged_at": "2026-08-10T00:00:00Z",
            "merge_commit_sha": "fix",
            "changed_files": 1,
        }

        def fake_rest(path, params=None):
            if path.endswith("/pulls/10"):
                return pull_request
            if path.endswith("/commits/fix"):
                return {"parents": [{"sha": "parent"}]}
            raise AssertionError(path)

        with (
            patch.object(github_regressions, "_rest_get", side_effect=fake_rest),
            patch.object(
                github_regressions,
                "_get_pull_request_files",
                return_value=[
                    {
                        "filename": "src/example.py",
                        "status": "modified",
                        "patch": "@@ -1 +1 @@\n-old\n+new",
                    }
                ],
            ),
            patch.object(github_regressions, "_get_blame_ranges", return_value=[]),
        ):
            result = github_regressions.get_fixing_pr_attribution(
                "https://github.com/example/repo/pull/10"
            )

        self.assertIsNotNone(result)
        self.assertFalse(result["complete"])  # type: ignore[index]

    def test_attribution_metadata_filters_human_reviewers(self):
        url = "https://github.com/example/repo/pull/10"
        pull_request = {
            "merged": True,
            "merged_at": "2026-08-10T00:00:00Z",
            "user": {"login": "author"},
        }
        reviews: object = [
            {"state": "APPROVED", "user": {"login": "author"}},
            {"state": "APPROVED", "user": {"login": "Reviewer"}},
            {"state": "APPROVED", "user": {"login": "reviewer"}},
            {"state": "APPROVED", "user": {"login": "copilot[bot]"}},
        ]

        def fake_rest(path, params=None):
            if path.endswith("/pulls/10"):
                return pull_request
            if path.endswith("/reviews"):
                return reviews
            raise AssertionError(path)

        with patch.object(github_regressions, "_rest_get", side_effect=fake_rest):
            result = github_regressions.get_pull_request_attribution_metadata(url)
        self.assertEqual(result["reviewers"], ["Reviewer"])  # type: ignore[index]

        reviews = {"message": "error"}
        with patch.object(github_regressions, "_rest_get", side_effect=fake_rest):
            with self.assertRaises(github_regressions.GitHubRegressionDataError):
                github_regressions.get_pull_request_attribution_metadata(url)

    def test_extracts_merged_fixing_pr_links(self):
        issue = {
            "attachments": {
                "nodes": [
                    {
                        "metadata": {
                            "url": "https://github.com/example/repo/pull/2",
                            "status": "merged",
                            "linkKind": "closes",
                        }
                    },
                    {
                        "metadata": {
                            "url": "https://github.com/example/repo/pull/3",
                            "status": "merged",
                            "linkKind": "links",
                        }
                    },
                ]
            }
        }
        self.assertEqual(
            extract_fixing_pr_urls(issue),
            ["https://github.com/example/repo/pull/2"],
        )

    def test_loads_override_mappings_and_ignores_invalid_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            wrapped = Path(tmp) / "wrapped.yml"
            wrapped.write_text(
                "overrides:\n  APO-1:\n    ignored: true\n  APO-2: skip\n",
                encoding="utf-8",
            )
            self.assertEqual(load_regression_overrides(wrapped), {"APO-1": {"ignored": True}})

            direct = Path(tmp) / "direct.yml"
            direct.write_text(
                "APO-3:\n  inducing_pr: https://github.com/example/repo/pull/9\n",
                encoding="utf-8",
            )
            self.assertEqual(
                load_regression_overrides(direct),
                {"APO-3": {"inducing_pr": "https://github.com/example/repo/pull/9"}},
            )

            self.assertEqual(load_regression_overrides(Path(tmp) / "missing.yml"), {})
            invalid = Path(tmp) / "invalid.yml"
            invalid.write_text("- just a list\n", encoding="utf-8")
            self.assertEqual(load_regression_overrides(invalid), {})

    def test_loads_committed_overrides_independent_of_cwd(self):
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            try:
                os.chdir(tmp)
                self.assertEqual(load_regression_overrides(), {})
            finally:
                os.chdir(original)

    def test_merges_candidates_and_applies_manual_override(self):
        fixing_url = "https://github.com/example/repo/pull/10"
        other_fix = "https://github.com/example/repo/pull/11"
        winner = {
            "url": "https://github.com/example/repo/pull/5",
            "score": 2.5,
            "line_count": 3,
            "age_days": 10,
            "author": "author",
        }
        later = {
            "url": "https://github.com/example/repo/pull/5",
            "score": 1.5,
            "line_count": 2,
            "age_days": 40,
            "author": "author",
        }
        record = merge_issue_attributions(
            {"identifier": "APO-123"},
            [fixing_url, other_fix],
            {
                fixing_url: {"candidates": [winner]},
                other_fix: {"candidates": [later]},
            },
        )
        self.assertEqual(record["attribution"]["score"], 4.0)
        self.assertEqual(record["attribution"]["line_count"], 5)
        self.assertNotIn("age_days", record["attribution"])

        loaded: list[str] = []
        manual = {"url": "https://github.com/example/repo/pull/9"}
        corrected = apply_regression_overrides(
            [record, {"identifier": "APO-ignored"}],
            {
                "APO-123": {"inducing_pr": f"{manual['url']}/"},
                "APO-ignored": {"ignored": True},
            },
            metadata_loader=lambda url: loaded.append(url) or manual,
        )
        self.assertEqual(loaded, [manual["url"]])
        self.assertEqual(corrected, [{**record, "attribution": manual}])

    def test_person_metrics_use_the_inducing_pr_cohort_and_stable_links(self):
        window = TimeWindow.from_dates(date(2026, 8, 1), date(2026, 8, 31))
        people = {
            "alice": {"slug": "alice", "github_username": "alice"},
            "bob": {"slug": "bob", "github_username": "bob"},
        }
        records = [
            {
                "attribution": {
                    "url": " https://github.com/example/zeta/pull/3/ ",
                    "merged_at": "2026-08-10T00:00:00Z",
                    "author": "alice",
                    "reviewers": ["bob"],
                }
            },
            {
                "attribution": {
                    "url": "https://github.com/example/alpha/pull/12",
                    "merged_at": "2026-08-11T00:00:00Z",
                    "author": "alice",
                    "reviewers": ["bob"],
                }
            },
            {
                "attribution": {
                    "url": "https://github.com/example/repo/pull/11",
                    "merged_at": "2025-08-10T00:00:00Z",
                    "author": "alice",
                    "reviewers": ["bob"],
                }
            },
        ]
        authors, reviewers = _person_metrics(
            records,
            people,
            {"alice": 20, "bob": 5},
            {"alice": 10, "bob": 25},
            window,
        )
        self.assertEqual(authors[0]["rate"], 10.0)
        self.assertEqual(reviewers[1]["rate"], 8.0)
        expected_pull_requests = [
            {"url": "https://github.com/example/alpha/pull/12", "label": "alpha#12"},
            {"url": "https://github.com/example/zeta/pull/3", "label": "zeta#3"},
        ]
        self.assertEqual(authors[0]["pull_requests"], expected_pull_requests)
        self.assertEqual(reviewers[1]["pull_requests"], expected_pull_requests)
        self.assertEqual(authors[1]["pull_requests"], [])

    def test_unconfigured_summary_skips_external_work(self):
        with patch.dict("os.environ", {}, clear=True):
            summary = build_regression_summary()
        self.assertFalse(summary["configured"])
        self.assertEqual(summary["author_metrics"], [])


if __name__ == "__main__":
    unittest.main()
