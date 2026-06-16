import { expect, test } from "@playwright/test";
import { REGIME_PROFILE_COLUMNS, selectRegimeProfileRows } from "../../src/lib/reportRows";

test("regime profile selector ignores nested market_results", () => {
  const nestedMarketRows = Array.from({ length: 708 }, (_, index) => ({
    market_id: `market-${index}`,
    market_slug: "nested-detail-only",
    net_pnl: "999.99"
  }));
  const report = {
    result: {
      comparisons: [
        { profile: "static", net_pnl: "0", delta_vs_static: "0", regime_frequency: {} },
        { profile: "dynamic_safety_only", net_pnl: "1", delta_vs_static: "1", regime_frequency: { feed_risk: 10 } },
        { profile: "dynamic_quote_style", net_pnl: "2", delta_vs_static: "2", regime_frequency: { feed_risk: 10 } },
        { profile: "full_deterministic_profile", net_pnl: "3", delta_vs_static: "3", regime_frequency: { feed_risk: 10 } }
      ],
      profiles: [
        { profile: "static", fills: 1, cancels: 0, skipped_by_profile: 0, market_results: nestedMarketRows },
        { profile: "dynamic_safety_only", fills: 2, cancels: 1, skipped_by_profile: 2156 },
        { profile: "dynamic_quote_style", fills: 3, cancels: 1, skipped_by_profile: 2156 },
        { profile: "full_deterministic_profile", fills: 4, cancels: 2, skipped_by_profile: 2334 }
      ]
    }
  };

  const rows = selectRegimeProfileRows(report);

  expect(rows).toHaveLength(4);
  expect(rows.map((row) => row.profile)).toEqual([
    "static",
    "dynamic_safety_only",
    "dynamic_quote_style",
    "full_deterministic_profile"
  ]);
  expect(rows.some((row) => row.profile === undefined || row.profile === "n/a")).toBe(false);
  expect(rows.some((row) => row.market_id !== undefined || row.market_slug !== undefined)).toBe(false);
  expect(rows.some((row) => row.net_pnl === "999.99")).toBe(false);
  expect(rows.find((row) => row.profile === "dynamic_safety_only")?.skipped_orders).toBe(2156);
  expect(REGIME_PROFILE_COLUMNS.find((column) => column.key === "skipped_orders")?.label).toBe("Skipped Orders");
});
