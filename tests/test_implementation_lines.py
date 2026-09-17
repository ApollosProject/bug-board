import unittest

from implementation_lines import count_implementation_additions, is_implementation_path


class IsImplementationPathTest(unittest.TestCase):
    def test_classifies_paths(self):
        cases = {
            "src/data/prayers/dataSource.js": True,
            ".github/workflows/ci.yml": True,
            "src/data/prayers/__tests__/resolver.tests.js": False,
            "src/data/prayers/__tests__/helpers.js": False,
            "tests/helpers.py": False,
            "e2e/setup.ts": False,
            "docs/index.html": False,
            "src/components/__snapshots__/notes.json": False,
            "src/Foo.Test.js": False,
            "src/utils/format.test.ts": False,
            "e2e/giving.spec.ts": False,
            "tests/test_regressions.py": False,
            "shovel/conftest.py": False,
            "pkg/sync/sync_test.go": False,
            "src/components/__snapshots__/Card.test.tsx.snap": False,
            "src/components/Card.stories.tsx": False,
            "README.md": False,
            "docs/setup.mdx": False,
            ".cursor/rules/style.mdc": False,
            "yarn.lock": False,
            "package-lock.json": False,
            "src/__generated__/graphql.ts": False,
            "src/api/types.generated.ts": False,
            "src/locales/en.json": False,
            "assets/logo.svg": False,
            "fonts/Inter.woff2": False,
            "": False,
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(is_implementation_path(path), expected)


class CountImplementationAdditionsTest(unittest.TestCase):
    def test_sums_only_implementation_files(self):
        pr = {
            "additions": 309,
            "files": {
                "pageInfo": {"hasNextPage": False},
                "nodes": [
                    {"path": "src/core/schema.js", "additions": 17},
                    {"path": "src/data/prayers/__tests__/resolver.tests.js", "additions": 191},
                    {"path": "src/data/prayers/dataSource.js", "additions": 93},
                    {"path": "src/data/prayers/resolver.js", "additions": 8},
                ],
            },
        }
        self.assertEqual(count_implementation_additions(pr), 118)

    def test_falls_back_to_total_when_file_list_is_truncated(self):
        pr = {
            "additions": 49995,
            "files": {
                "pageInfo": {"hasNextPage": True},
                "nodes": [{"path": "src/a.js", "additions": 1}],
            },
        }
        self.assertEqual(count_implementation_additions(pr), 49995)

    def test_falls_back_to_total_when_files_are_missing(self):
        self.assertEqual(count_implementation_additions({"additions": 12}), 12)
        self.assertEqual(count_implementation_additions({"additions": 12, "files": None}), 12)

    def test_returns_none_without_any_size_data(self):
        self.assertIsNone(count_implementation_additions({}))

    def test_tolerates_missing_file_fields(self):
        pr = {
            "additions": 5,
            "files": {"nodes": [{"path": None, "additions": 3}, {"path": "src/a.py"}]},
        }
        self.assertEqual(count_implementation_additions(pr), 0)


if __name__ == "__main__":
    unittest.main()
