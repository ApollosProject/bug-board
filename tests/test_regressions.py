import unittest

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


if __name__ == "__main__":
    unittest.main()
