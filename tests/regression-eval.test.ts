import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { decisionPrediction, metrics, stateFor, szz, validateDataset, validatePrediction, type Dataset, type Prediction } from "../scripts/regression-eval";

const dataset: Dataset = JSON.parse(fs.readFileSync(path.join(process.cwd(), "tests/fixtures/regression-eval.json"), "utf8"));
const c = dataset.cases[0];
const prediction: Prediction = {
  outcome: "regression", candidate_ids: ["P0"], explanation: "Falsy zero changes to null, unlike the later rename.",
  citations: [
    { evidence_id: "F0", quote: "return total ?? 0;" },
    { evidence_id: "P0:E0", quote: "return total || null;" },
  ],
};
test("shadow model input excludes labels, people, PR URLs and SZZ ranking", () => {
  validateDataset(dataset);
  const labelled = structuredClone(c);
  labelled.candidates[0].author = "not-for-model-input";
  labelled.candidates[0].reviewers = ["also-not-for-model-input"];
  const state = JSON.stringify(stateFor(labelled));
  for (const text of ["not-for-model-input", "also-not-for-model-input", "github.com", "reviewed_by", "score", "line_count"]) assert.ok(!state.includes(text));
  assert.equal(stateFor(labelled).candidates[0].id, "P0");
  assert.deepEqual(stateFor({ ...labelled, candidates: [...labelled.candidates].reverse() }), stateFor(labelled));
});
test("dataset rejects missing redaction, duplicate groups, oversize evidence and absent adjudication", () => {
  assert.throws(() => validateDataset({ ...dataset, redacted: false }));
  assert.throws(() => validateDataset({ ...dataset, cases: [c, { ...c, id: "different" }] }));
  assert.throws(() => validateDataset({ ...dataset, cases: [{ ...c, issue: "x".repeat(16000) }] }));
  assert.throws(() => validateDataset({ ...dataset, cases: [{ ...c, gold: { ...c.gold, reviewed_by: "" } }] }));
  assert.throws(() => validateDataset({ ...dataset, cases: Array.from({ length: 51 }, (_, i) => ({ ...c, id: String(i), group: String(i) })) }));
});
test("LLM predictions require real candidates, exact quotes and fixing plus candidate evidence", () => {
  assert.equal(validatePrediction(prediction, c), prediction);
  assert.throws(() => validatePrediction({ ...prediction, candidate_ids: ["P99"] }, c));
  assert.throws(() => validatePrediction({ ...prediction, citations: [{ evidence_id: "F0", quote: "invented source code" }] }, c));
  assert.throws(() => validatePrediction({ ...prediction, citations: prediction.citations.slice(0, 1) }, c));
  assert.throws(() => validatePrediction(prediction, { ...c, complete: false }));
  assert.throws(() => validatePrediction({ ...prediction, outcome: "unknown" }, c));
});
test("multiple origins require evidence from every selected candidate", () => {
  assert.throws(() => validatePrediction({ ...prediction, candidate_ids: ["P0", "P1"] }, c));
  assert.equal(validatePrediction({ ...prediction, candidate_ids: ["P0", "P1"], citations: [...prediction.citations, { evidence_id: "P1:E0", quote: "renamed local variable" }] }, c).candidate_ids.length, 2);
});
test("JEV abstains on weak global evidence, contradictions or missing distributions", () => {
  const kind = { choice: "regression", probabilities: { regression: 0.9, not_regression: 0.05, unknown: 0.05 } };
  assert.deepEqual(decisionPrediction(kind, { P0: 0.9, P1: 0.95 }).candidate_ids, ["P0", "P1"]);
  assert.equal(decisionPrediction(kind, { P0: 0.7 }).outcome, "unknown");
  assert.equal(decisionPrediction({ choice: "regression" }, { P0: 0.99 }).outcome, "unknown");
  assert.equal(decisionPrediction({ choice: "regression", probabilities: { regression: 0.4, not_regression: 0.3, unknown: 0.3 } }, { P0: 0.99 }).outcome, "unknown");
  const nonRegression = { choice: "not_regression", probabilities: { regression: 0.01, not_regression: 0.98, unknown: 0.01 } };
  assert.equal(decisionPrediction(nonRegression, {}).outcome, "not_regression");
  assert.equal(decisionPrediction(nonRegression, { P0: 0.9 }).outcome, "unknown");
  assert.throws(() => decisionPrediction(kind, { P0: Number.NaN }));
  assert.throws(() => decisionPrediction(kind, { P0: 1.1 }));
});
test("baseline reuses production ranking even where last-touch is a misleading proxy", () => {
  assert.deepEqual(szz(c).candidate_ids, ["P1"]);
  assert.deepEqual(szz({ ...c, complete: false }).candidate_ids, ["P1"]);
  assert.equal(szz(dataset.cases[1]).outcome, "unknown");
});
test("evaluation distinguishes false blame, abstentions, missing candidates and unavailable models", () => {
  const wrong = metrics([c], [szz(c)]);
  assert.equal(wrong.falseAttributionRate, 1);
  assert.equal(wrong.candidateRecall, 0);
  const abstain = { ...prediction, outcome: "unknown" as const, candidate_ids: [], citations: [] };
  assert.equal(metrics([c], [abstain]).coverage, 0);
  assert.equal(metrics([c], [null]).errors, 1);
  assert.equal(metrics([c], [null]).exactAccuracy, null);
  const outside = { ...c, gold: { ...c.gold!, inducing_prs: ["https://github.com/example/checkout/pull/99"] } };
  assert.equal(metrics([outside], [prediction]).candidateRecall, 0);
  const canonical = { ...c, gold: { ...c.gold!, inducing_prs: ["https://github.com/EXAMPLE/CHECKOUT/pull/1/"] } };
  assert.equal(metrics([canonical], [prediction]).exactAccuracy, 1);
  assert.equal(metrics([{ ...c, gold: undefined }], [prediction]).labelled, 0);
  const uncertain = { ...c, gold: { ...c.gold!, outcome: "unknown" as const, inducing_prs: [] } };
  assert.equal(metrics([uncertain], [prediction]).unknownGoldAttributed, 1);
});
