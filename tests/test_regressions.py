import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import github_regressions
from github_regressions import deleted_line_numbers, parse_pull_request_url


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


if __name__ == "__main__":
    unittest.main()
