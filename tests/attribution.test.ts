import assert from "node:assert/strict";
import test from "node:test";
import { introducingHunks, judge, removedLines } from "../lib/attribution";

const kind = (regression: number, not_regression: number, unknown: number) => ({ regression, not_regression, unknown });

test("judgement drops non-bugs, withholds blame when unsure, and guards searched suspects", () => {
  const szz = { candidates: { a: 0.6, b: 0.2 }, kind: kind(0.6, 0.1, 0.3) };
  // Confident it is not an introduced bug: the fix leaves the counts.
  assert.equal(judge({ ...szz, kind: kind(0.1, 0.6, 0.3) }, szz, new Set()).introduced, false);
  // Unsure of the cause: still a bug, nobody blamed.
  const unsure = judge({ ...szz, kind: kind(0.2, 0.05, 0.75) }, szz, new Set());
  assert.deepEqual([unsure.introduced, unsure.pick], [true, null]);
  // Default: JEV's top SZZ suspect.
  assert.deepEqual([judge(szz, szz, new Set()).pick, judge(szz, szz, new Set()).source], ["a", "szz"]);
  // A searched suspect wins only when confident and preferred over the SZZ pick.
  const all = (s: number) => ({ candidates: { a: 0.5, b: 0.1, s } });
  assert.deepEqual([judge(szz, all(0.8), new Set(["s"])).pick, judge(szz, all(0.8), new Set(["s"])).source], ["s", "search"]);
  assert.equal(judge(szz, all(0.65), new Set(["s"])).pick, "a");
  assert.equal(judge(szz, { candidates: { a: 0.9, s: 0.8 } }, new Set(["s"])).pick, "a");
});

test("evidence keeps the hunks that added lines a fix later removed", () => {
  const removed = removedLines([{ filename: "f.ts", patch: "@@ -1,3 +1,3 @@\n-  return total || null;\n+  return total ?? 0;\n-x\n" }]);
  assert.deepEqual([...removed.get("f.ts")!], ["return total || null;"]);
  const patch = "@@ -1,2 +1,2 @@\n+  return total || null;\n unchanged\n@@ -9,1 +9,1 @@\n+  other();";
  assert.deepEqual(introducingHunks(patch, removed.get("f.ts")!), ["@@ -1,2 +1,2 @@\n+  return total || null;\n unchanged"]);
});
