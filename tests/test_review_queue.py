import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import app as app_module
import github
from review_queue import (
    APPROVED,
    CI_RUNNING,
    NOT_READY,
    READY,
    build_review_queue,
    classify,
    filter_queue,
    ticket_number,
    waiting_since,
)

NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
TEAM = {"dylan-manchester", "bkraeling"}


def make_pr(number=1, **overrides):
    pr = {
        "number": number,
        "title": f"PR {number}",
        "url": f"https://github.com/apollosproject/apollos-admin/pull/{number}",
        "author": {"login": "nlewis84"},
        "createdAt": "2026-09-30T12:00:00Z",
        "headRefName": f"feature-{number}",
        "baseRefName": "main",
        "repository": {
            "nameWithOwner": "ApollosProject/apollos-admin",
            "defaultBranchRef": {"name": "main"},
        },
        "additions": 10,
        "deletions": 5,
        "mergeable": "MERGEABLE",
        "reviewDecision": "REVIEW_REQUIRED",
        "statusCheckRollup": {"state": "SUCCESS"},
        "reviews": {"nodes": []},
        "reviewRequests": {"nodes": []},
        "timelineItems": {"nodes": []},
    }
    pr.update(overrides)
    return pr


def requested(*logins):
    return {"nodes": [{"requestedReviewer": {"login": login}} for login in logins]}


class ClassifyTest(unittest.TestCase):
    def test_default_branch_passing_unapproved_pr_is_ready(self):
        self.assertEqual(classify(make_pr()), (READY, None))

    def test_pr_without_checks_is_ready(self):
        self.assertEqual(classify(make_pr(statusCheckRollup=None)), (READY, None))

    def test_pending_ci_goes_to_ci_running(self):
        pr = make_pr(statusCheckRollup={"state": "PENDING"})
        self.assertEqual(classify(pr), (CI_RUNNING, None))

    def test_approved_pr_goes_to_approved(self):
        self.assertEqual(classify(make_pr(reviewDecision="APPROVED")), (APPROVED, None))

    def test_approval_counts_when_repo_has_no_review_decision(self):
        pr = make_pr(
            reviewDecision=None,
            reviews={
                "nodes": [
                    {
                        "author": {"login": "redreceipt"},
                        "state": "APPROVED",
                        "submittedAt": "2026-09-30T13:00:00Z",
                    }
                ]
            },
        )
        self.assertEqual(classify(pr), (APPROVED, None))

    def test_not_ready_reasons(self):
        cases = {
            "Stacked (base: feature-0)": make_pr(baseRefName="feature-0"),
            "Conflicts": make_pr(mergeable="CONFLICTING"),
            "CI failing": make_pr(statusCheckRollup={"state": "FAILURE"}),
        }
        for reason, pr in cases.items():
            with self.subTest(reason=reason):
                self.assertEqual(classify(pr), (NOT_READY, reason))

    def test_change_request_waits_on_author_until_review_is_re_requested(self):
        change_request = {
            "nodes": [
                {
                    "author": {"login": "bkraeling"},
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-09-30T13:00:00Z",
                }
            ]
        }
        pr = make_pr(reviewDecision="CHANGES_REQUESTED", reviews=change_request)
        self.assertEqual(classify(pr), (NOT_READY, "Changes requested"))

        pr["reviewRequests"] = requested("bkraeling")
        self.assertEqual(classify(pr), (READY, None))


class TicketNumberTest(unittest.TestCase):
    def test_branch_wins_over_title(self):
        pr = make_pr(headRefName="apo-123-fix-thing", title="APO-456: something")
        self.assertEqual(ticket_number(pr, "APO"), 123)

    def test_falls_back_to_title(self):
        self.assertEqual(ticket_number(make_pr(title="APO-456: fix"), "APO"), 456)

    def test_no_ticket(self):
        self.assertIsNone(ticket_number(make_pr(headRefName="xapo-1x"), "APO"))


class WaitingSinceTest(unittest.TestCase):
    def test_latest_ready_or_teammate_request_wins_and_bot_requests_are_ignored(self):
        pr = make_pr(
            timelineItems={
                "nodes": [
                    {"__typename": "ReadyForReviewEvent", "createdAt": "2026-09-30T14:00:00Z"},
                    {
                        "__typename": "ReviewRequestedEvent",
                        "createdAt": "2026-09-30T16:00:00Z",
                        "requestedReviewer": {"login": "BKraeling"},
                    },
                    {
                        "__typename": "ReviewRequestedEvent",
                        "createdAt": "2026-09-30T18:00:00Z",
                        "requestedReviewer": {"login": "mary-pr-poppins"},
                    },
                ]
            }
        )
        self.assertEqual(waiting_since(pr, TEAM), datetime(2026, 9, 30, 16, tzinfo=timezone.utc))

    def test_falls_back_to_created_at(self):
        self.assertEqual(
            waiting_since(make_pr(), TEAM), datetime(2026, 9, 30, 12, tzinfo=timezone.utc)
        )


class BuildReviewQueueTest(unittest.TestCase):
    def test_orders_by_priority_then_size_then_wait(self):
        prs = [
            make_pr(1, headRefName="apo-1"),  # Low, XS
            make_pr(
                2, headRefName="apo-2", additions=300, createdAt="2026-09-28T12:00:00Z"
            ),  # High, M, older than #4
            make_pr(3, headRefName="apo-3", createdAt="2026-09-25T12:00:00Z"),  # High, XS, old
            make_pr(4, headRefName="apo-4"),  # High, XS, newer
            make_pr(5),  # no ticket
        ]
        issues = {
            1: {"identifier": "APO-1", "priority": 4},
            2: {"identifier": "APO-2", "priority": 2},
            3: {"identifier": "APO-3", "priority": 2},
            4: {"identifier": "APO-4", "priority": 2},
        }

        queue = build_review_queue(prs, issues, NOW, "APO", TEAM)

        groups = [(g["label"], [r["number"] for r in g["rows"]]) for g in queue["ready_groups"]]
        self.assertEqual(groups, [("High", [3, 4, 2]), ("Low", [1]), ("No priority", [5])])
        self.assertEqual(queue["ready_count"], 5)

    def test_row_values(self):
        pr = make_pr(
            createdAt="2026-09-27T12:00:00Z",
            reviewRequests=requested("dylan-manchester", "mary-pr-poppins"),
        )

        row = build_review_queue([pr], {}, NOW, "APO", TEAM)["ready_groups"][0]["rows"][0]

        self.assertEqual(row["repo"], "apollos-admin")
        self.assertEqual((row["size"], row["additions"], row["deletions"]), ("XS", 10, 5))
        self.assertEqual((row["waiting"], row["waiting_level"]), ("4d", "red"))
        self.assertEqual(row["reviewers"], ["dylan-manchester"])
        self.assertIsNone(row["issue"])

    def test_sections(self):
        prs = [
            make_pr(1, statusCheckRollup={"state": "PENDING"}),
            make_pr(2, mergeable="CONFLICTING"),
            make_pr(3, reviewDecision="APPROVED"),
        ]

        queue = build_review_queue(prs, {}, NOW, "APO", TEAM)

        self.assertEqual(queue["ready_groups"], [])
        self.assertEqual([r["number"] for r in queue["approved"]], [3])
        self.assertEqual([r["number"] for r in queue["running"]], [1])
        self.assertEqual(
            [(r["number"], r["reason"]) for r in queue["not_ready"]], [(2, "Conflicts")]
        )


class FilterQueueTest(unittest.TestCase):
    def setUp(self):
        prs = [
            make_pr(1, headRefName="apo-1", author={"login": "Dylan-Manchester"}),
            make_pr(2, headRefName="apo-2", reviewRequests=requested("bkraeling")),
            make_pr(
                3,
                author={"login": "dylan-manchester"},
                reviewRequests=requested("bkraeling"),
                statusCheckRollup={"state": "PENDING"},
            ),
            make_pr(4, author={"login": "dylan-manchester"}, mergeable="CONFLICTING"),
        ]
        issues = {1: {"priority": 1}, 2: {"priority": 2}}
        self.queue = build_review_queue(prs, issues, NOW, "APO", TEAM)

    def numbers(self, queue):
        ready = [row["number"] for group in queue["ready_groups"] for row in group["rows"]]
        return ready, [r["number"] for r in queue["running"] + queue["not_ready"]]

    def test_no_filters_returns_everything(self):
        self.assertIs(filter_queue(self.queue), self.queue)

    def test_author_filter_is_case_insensitive_across_sections(self):
        queue = filter_queue(self.queue, author="dylan-manchester")
        self.assertEqual(self.numbers(queue), ([1], [3, 4]))
        self.assertEqual([g["label"] for g in queue["ready_groups"]], ["Urgent"])
        self.assertEqual(queue["ready_count"], 1)

    def test_reviewer_filter_keeps_prs_awaiting_that_reviewer(self):
        queue = filter_queue(self.queue, reviewer="BKraeling")
        self.assertEqual(self.numbers(queue), ([2], [3]))

    def test_author_and_reviewer_combine(self):
        queue = filter_queue(self.queue, author="dylan-manchester", reviewer="bkraeling")
        self.assertEqual(self.numbers(queue), ([], [3]))


class ReviewsRouteTest(unittest.TestCase):
    def setUp(self):
        app_module._build_reviews_context.cache_clear()
        self.client = app_module.app.test_client()

    def test_page_renders(self):
        response = self.client.get("/reviews")
        self.assertEqual(response.status_code, 200)
        self.assertIn("/partials/reviews/content", response.get_data(as_text=True))

    def test_partial_renders_empty_state_without_prs(self):
        with patch.object(app_module, "search_open_prs", return_value=[]):
            response = self.client.get("/partials/reviews/content")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Nothing is waiting for review.", response.get_data(as_text=True))

    def test_partial_bolds_the_viewer_and_links_the_ticket(self):
        pr = make_pr(7, headRefName="apo-9", reviewRequests=requested("dylan-manchester"))
        issue = {
            "number": 9,
            "identifier": "APO-9",
            "url": "https://linear.app/differential/issue/APO-9",
            "priority": 1,
            "priorityLabel": "Urgent",
        }
        with (
            patch.dict(app_module.app.config, SECRET_KEY="test-secret-key-at-least-32-characters"),
            patch.object(app_module, "search_open_prs", return_value=[pr]),
            patch.object(app_module, "get_issues_by_number", return_value={9: issue}),
        ):
            with self.client.session_transaction() as session:
                session["github_login"] = "dylan-manchester"
            body = self.client.get("/partials/reviews/content").get_data(as_text=True)

        self.assertIn(">Urgent (1)</th>", body)
        self.assertIn("apollos-admin#7", body)
        self.assertIn(">APO-9</a>", body)
        self.assertIn("<strong>dylan-manchester</strong>", body)

    def test_approved_toggle_fetches_and_shows_approved_prs(self):
        with patch.object(
            app_module, "search_open_prs", return_value=[make_pr(8, reviewDecision="APPROVED")]
        ) as search:
            off = self.client.get("/partials/reviews/content").get_data(as_text=True)
            on = self.client.get("/partials/reviews/content?approved=1").get_data(as_text=True)

        self.assertEqual([call.args for call in search.call_args_list], [(False,), (True,)])
        self.assertNotIn("Approved, not merged", off)
        self.assertIn("Approved, not merged (1)", on)
        self.assertIn('href="/reviews?approved=1"', off)
        shell = self.client.get("/reviews?approved=1").get_data(as_text=True)
        self.assertIn("/partials/reviews/content?approved=1", shell)

    def test_filters_flow_from_the_page_to_the_partial_and_keep_each_other(self):
        prs = [
            make_pr(1, author={"login": "dylan-manchester"}),
            make_pr(2, author={"login": "bkraeling"}),
        ]
        with patch.object(app_module, "search_open_prs", return_value=prs):
            body = self.client.get("/partials/reviews/content?author=dylan-manchester").get_data(
                as_text=True
            )
            empty = self.client.get("/partials/reviews/content?reviewer=bkraeling").get_data(
                as_text=True
            )

        self.assertIn("apollos-admin#1", body)
        self.assertNotIn("apollos-admin#2", body)
        self.assertIn('<option value="dylan-manchester" selected>', body)
        self.assertIn('href="/reviews?author=dylan-manchester&amp;approved=1"', body)
        self.assertIn("No PRs match these filters.", empty)
        shell = self.client.get("/reviews?author=dylan-manchester&reviewer=").get_data(as_text=True)
        self.assertIn("/partials/reviews/content?author=dylan-manchester'", shell)

    def test_signed_in_viewer_gets_my_prs_and_my_reviews_shortcuts(self):
        with (
            patch.dict(app_module.app.config, SECRET_KEY="test-secret-key-at-least-32-characters"),
            patch.object(app_module, "search_open_prs", return_value=[]),
        ):
            with self.client.session_transaction() as session:
                session["github_login"] = "dylan-manchester"
            body = self.client.get("/partials/reviews/content?reviewer=bkraeling").get_data(
                as_text=True
            )

        self.assertIn('href="/reviews?author=dylan-manchester">My PRs waiting on review', body)
        self.assertIn('href="/reviews?reviewer=dylan-manchester">Waiting on my review', body)
        self.assertIn('href="/reviews">Everyone</a>', body)


class SearchOpenPrsTest(unittest.TestCase):
    def test_filters_drafts_and_approved_unless_asked(self):
        with (
            patch.object(github, "token", "token"),
            patch.object(github, "_search_prs", return_value=[]) as search,
        ):
            github.search_open_prs()
            github.search_open_prs(include_approved=True)

        queries = [call.args[1] for call in search.call_args_list]
        self.assertIn(
            "repo:apollosproject/apollos-admin is:pr is:open draft:false -review:approved", queries
        )
        self.assertIn("repo:apollosproject/apollos-admin is:pr is:open draft:false", queries)


if __name__ == "__main__":
    unittest.main()
