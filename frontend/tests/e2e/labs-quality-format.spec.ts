import { expect, test } from "@playwright/test";
import { decisionGradeCoverageText } from "../../src/lib/format";

test("decision-grade coverage distinguishes a missing denominator from numeric zero", () => {
  expect(decisionGradeCoverageText(undefined)).toBe("pending — no evaluation denominator");
  expect(decisionGradeCoverageText(null)).toBe("pending — no evaluation denominator");
  expect(decisionGradeCoverageText(0)).toBe("0%");
  expect(decisionGradeCoverageText("0.975")).toBe("97.5%");
});
