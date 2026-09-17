from __future__ import annotations

import fnmatch
import posixpath
from typing import Any

EXCLUDED_DIRECTORIES = frozenset(
    {
        "__tests__",
        "__test__",
        "test",
        "tests",
        "spec",
        "specs",
        "__mocks__",
        "__fixtures__",
        "fixtures",
        "__snapshots__",
        "e2e",
        "cypress",
        "docs",
        "doc",
        "__generated__",
        "generated",
        "locales",
        "locale",
        "i18n",
        "translations",
        ".storybook",
        ".cursor",
        ".claude",
    }
)

EXCLUDED_FILENAME_PATTERNS = (
    "*.test.*",
    "*.tests.*",
    "*.spec.*",
    "*_test.py",
    "test_*.py",
    "conftest.py",
    "*_test.go",
    "*_spec.rb",
    "*.snap",
    "*.stories.*",
    "*.md",
    "*.mdx",
    "*.rst",
    "license*",
    "changelog*",
    "codeowners",
    ".cursorrules",
    "*.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "*.generated.*",
    "*.min.js",
    "*.min.css",
    "*.pb.go",
    "*_pb2.py",
    "*_pb2_grpc.py",
    "*.po",
    "*.pot",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.svg",
    "*.webp",
    "*.ico",
    "*.pdf",
    "*.ttf",
    "*.otf",
    "*.woff",
    "*.woff2",
    "*.mp3",
    "*.mp4",
    "*.mov",
)


def is_implementation_path(path: str) -> bool:
    normalized = path.lower()
    if not normalized:
        return False
    directory, filename = posixpath.split(normalized)
    if any(segment in EXCLUDED_DIRECTORIES for segment in directory.split("/")):
        return False
    return not any(fnmatch.fnmatchcase(filename, pattern) for pattern in EXCLUDED_FILENAME_PATTERNS)


def count_implementation_additions(pr: dict[str, Any]) -> int | None:
    """Sum additions across a PR's implementation files.

    Falls back to the PR's total additions when GitHub did not return the file
    list or truncated it, so an unclassifiable PR is never under-counted.
    """
    files = pr.get("files") or {}
    nodes = files.get("nodes")
    truncated = (files.get("pageInfo") or {}).get("hasNextPage", False)
    if nodes is None or truncated:
        total = pr.get("additions")
        return int(total) if total is not None else None
    return sum(
        int(node.get("additions") or 0)
        for node in nodes
        if is_implementation_path(node.get("path") or "")
    )
