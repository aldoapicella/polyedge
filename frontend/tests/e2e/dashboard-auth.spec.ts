import { expect, Page, test } from "@playwright/test";

test("anonymous dashboard and backend proxy requests are blocked", async ({ page, request }) => {
  await page.goto("/dashboard");
  await expect(page).toHaveURL(/\/login\?next=%2Fdashboard/);

  const response = await request.get("/api/backend/snapshot");
  expect(response.status()).toBe(401);
});

test("owner login unlocks dashboard and q/reference lines draw inside chart", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") {
      consoleErrors.push(message.text());
    }
  });
  await installApiMocks(page);
  await login(page);

  await expect(page.getByRole("heading", { name: "Operations Dashboard" })).toBeVisible();
  await expect(page.getByText("Operator Readiness")).toBeVisible();
  await expect(page.getByText("q 4", { exact: true })).toBeVisible();
  await expect(page.getByText("missing UP/DOWN book")).toBeVisible();

  const mainChart = page.getByText("Market Probability & Price", { exact: true }).locator("xpath=ancestor::section[1]");
  await mainChart.locator('path.recharts-line-curve[name="q Up"]').waitFor({ state: "attached" });
  await mainChart.locator('path.recharts-line-curve[name="reference price"]').waitFor({ state: "attached" });
  await expect(page.getByText("51.0%", { exact: true })).toBeVisible();
  const linesDrawInsidePlot = await page.evaluate(() => {
    const mainChart = Array.from(document.querySelectorAll("section")).find((section) =>
      section.textContent?.includes("Market Probability & Price")
    );
    if (!mainChart) {
      return false;
    }
    const visibleLine = (name: string) => {
      return Array.from(mainChart.querySelectorAll(`path.recharts-line-curve[name="${name}"]`)).some((path) => {
        const chart = path.closest(".recharts-wrapper");
        if (!chart || !path.getAttribute("d")?.startsWith("M")) {
          return false;
        }
        const chartRect = chart.getBoundingClientRect();
        const pathRect = path.getBoundingClientRect();
        return (
          pathRect.width > 0 &&
          pathRect.left - chartRect.left < chartRect.width / 2 &&
          pathRect.top >= chartRect.top &&
          pathRect.bottom <= chartRect.bottom
        );
      });
    };
    return Array.from(mainChart.querySelectorAll('path.recharts-line-curve[name="q Up"]')).some((qPath) => {
      return qPath.getAttribute("d")?.startsWith("M");
    }) && visibleLine("q Up") && visibleLine("reference price");
  });
  expect(linesDrawInsidePlot).toBe(true);
  expect(consoleErrors).toEqual([]);
});

test("research pages render without console errors", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") {
      consoleErrors.push(message.text());
    }
  });
  await installApiMocks(page);
  await login(page);

  for (const route of ["/reports", "/labs", "/data-quality", "/jobs"]) {
    await page.goto(route);
    await expect(page.locator("h1")).toBeVisible();
  }
  await page.goto("/labs");
  await page.getByRole("button", { name: "Regime Profiles" }).click();
  await expect(page.getByRole("cell", { name: "static", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "dynamic_safety_only", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "dynamic_quote_style", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "full_deterministic_profile", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "normal: 4, volatile: 1", exact: true })).toHaveCount(4);
  await expect(page.getByText("nested-detail-only")).toHaveCount(0);
  await expect(page.getByRole("cell", { name: "2156", exact: true })).toHaveCount(2);
  await expect(page.getByText("[object Object]")).toHaveCount(0);
  await page.goto("/jobs");
  await expect(page.getByRole("button", { name: "Manual Backfill", exact: true })).toBeDisabled();
  expect(consoleErrors).toEqual([]);
});

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel("Password").fill("test-password");
  const [response] = await Promise.all([
    page.waitForResponse((response) => response.url().includes("/api/auth/login"), { timeout: 60_000 }),
    page.getByRole("button", { name: "Sign in" }).click()
  ]);
  expect(response.ok()).toBe(true);
  await expect(page).toHaveURL(/\/dashboard/, { timeout: 60_000 });
}

async function installApiMocks(page: Page) {
  await page.route("**/api/realtime", async (route) => {
    await route.fulfill({ status: 200, contentType: "text/event-stream", body: ": ok\n\n" });
  });
  await page.route("**/api/backend/snapshot", async (route) => {
    await route.fulfill({ json: snapshotPayload() });
  });
  await page.route("**/api/backend/events/recent**", async (route) => {
    await route.fulfill({ json: { events: [] } });
  });
  await page.route("**/api/backend/markets/**/chart?range=full", async (route) => {
    await route.fulfill({ json: chartPayload() });
  });
  await page.route("**/api/backend/reports/latest", async (route) => {
    await route.fulfill({ json: { report: { report_metadata: { date: "2026-06-14" }, summary: {} } } });
  });
  await page.route("**/api/backend/labs/data-quality/latest", async (route) => {
    await route.fulfill({
      json: {
        generated_ts: "2026-06-15T21:00:00Z",
        freshness: { status: "healthy", latest_blob_last_modified: "2026-06-15T20:59:00Z" },
        recorder: { worker_alive: true, dropped_count: 0, error_count: 0 },
        exclusions: {
          version: 1,
          windows: [
            {
              id: "azure-put-bug-2026-06-11",
              start: "2026-06-11T10:00:00Z",
              end: "2026-06-12T22:00:00Z",
              reason: "Azure PUT bug",
              default_exclude: true
            }
          ]
        }
      }
    });
  });
  await page.route("**/api/backend/labs/jobs", async (route) => {
    await route.fulfill({
      json: {
        jobs: [
          {
            job_id: "daily-report",
            job_name: "polyedge-daily-research-job",
            status: "Succeeded",
            trigger: "Schedule",
            cron: "30 1 * * *",
            last_start: "2026-06-15T01:30:00Z",
            last_finish: "2026-06-15T01:31:00Z",
            duration: 60,
            research_only: true,
            live_trading_enabled: false
          }
        ]
      }
    });
  });
  await page.route("**/api/backend/labs/reports/latest", async (route) => {
    await route.fulfill({
      json: {
        date: "2026-06-14",
        report: {
          result: {
            executive_summary: {
              recommendation: "Continue collecting data unchanged",
              research_only: true,
              live_trading_enabled: false
            }
          }
        },
        baseline: { rows: [{ fill_model: "touch_after_250ms", net_pnl: -1.25 }] },
        artifacts: []
      }
    });
  });
  await page.route("**/api/backend/labs/reports/artifacts**", async (route) => {
    await route.fulfill({
      json: {
        prefix: "",
        artifacts: [
          {
            artifact_id: "daily~2026-06-14~final_report.md",
            path: "daily/2026-06-14/final_report.md",
            kind: "md",
            size_bytes: 100,
            modified_ts: "2026-06-15T01:31:00Z"
          }
        ]
      }
    });
  });
  await page.route("**/api/backend/labs/prospective", async (route) => {
    await route.fulfill({
      json: {
        result: {
          status: "collecting",
          rows: [],
          frozen_candidates: {
            candidates: [
              { name: "static_baseline" },
              { name: "dynamic_quote_style" },
              { name: "full_deterministic_profile" },
              { name: "dynamic_safety_only" }
            ]
          }
        }
      }
    });
  });
  for (const endpoint of ["calibration", "sample-size", "fill-models"]) {
    await page.route(`**/api/backend/labs/${endpoint}/latest`, async (route) => {
      await route.fulfill({ json: { date: "2026-06-14", report: {} } });
    });
  }
  await page.route("**/api/backend/labs/regimes/latest", async (route) => {
    await route.fulfill({
      json: {
        date: "2026-06-14",
        report: {
          result: {
            comparisons: [
              {
                profile: "static",
                net_pnl: "-13.35",
                delta_vs_static: "0.00",
                regime_frequency: { normal: 4, volatile: 1 },
                regime_time_share: { normal: "80%", volatile: "20%" }
              },
              {
                profile: "dynamic_safety_only",
                net_pnl: "-7.10",
                delta_vs_static: "6.25",
                regime_frequency: { normal: 4, volatile: 1 },
                regime_time_share: { normal: "80%", volatile: "20%" }
              },
              {
                profile: "dynamic_quote_style",
                net_pnl: "1.20",
                delta_vs_static: "14.55",
                regime_frequency: { normal: 4, volatile: 1 },
                regime_time_share: { normal: "80%", volatile: "20%" }
              },
              {
                profile: "full_deterministic_profile",
                net_pnl: "-2.50",
                delta_vs_static: "10.85",
                regime_frequency: { normal: 4, volatile: 1 },
                regime_time_share: { normal: "80%", volatile: "20%" }
              }
            ],
            profiles: [
              {
                profile: "static",
                fills: 440,
                cancels: 73,
                skipped_by_profile: 0,
                market_results: [
                  {
                    market_id: "nested-detail-only",
                    market_slug: "nested-detail-only",
                    net_pnl: "999.99"
                  }
                ]
              },
              {
                profile: "dynamic_safety_only",
                fills: 220,
                cancels: 31,
                skipped_by_profile: 2156
              },
              {
                profile: "dynamic_quote_style",
                fills: 218,
                cancels: 34,
                skipped_by_profile: 2156
              },
              {
                profile: "full_deterministic_profile",
                fills: 205,
                cancels: 29,
                skipped_by_profile: 2334
              }
            ]
          }
        }
      }
    });
  });
  await page.route("**/api/backend/labs/data-quality/hourly**", async (route) => {
    await route.fulfill({ json: { date: "2026-06-15", audits: [] } });
  });
  await page.route("**/api/backend/labs/data-quality/exclusions", async (route) => {
    await route.fulfill({
      json: {
        version: 1,
        windows: [
          {
            id: "azure-put-bug-2026-06-11",
            start: "2026-06-11T10:00:00Z",
            end: "2026-06-12T22:00:00Z",
            reason: "Azure PUT bug",
            default_exclude: true
          }
        ]
      }
    });
  });
  await page.route("**/api/backend/labs/data-quality/exclusions/validate", async (route) => {
    await route.fulfill({ json: { valid: true, issues: [], registry: { version: 1, windows: [] } } });
  });
}

function snapshotPayload() {
  return {
    status: {
      app: "polyedge",
      execution_mode: "paper",
      started_at: "2026-06-15T20:00:00Z",
      now: "2026-06-15T21:00:00Z",
      markets: 1,
      tradeable_markets: 1,
      books: 0,
      tracked_open_orders: 0,
      control: { paused: false },
      kill_switch: false,
      paper_fill: { paper_maker_fills: 0, paper_open_resting_orders: 0 },
      recorder: { worker_alive: true, dropped_count: 0, error_count: 0 },
      reference: {
        source: "chainlink",
        price: "66488.67",
        source_ts: "2026-06-15T21:00:00Z",
        local_ts: "2026-06-15T21:00:00Z",
        latency_ms: 0,
        stale: false,
        exact_resolution_source: true,
        quality_flags: []
      }
    },
    current_market: market(),
    markets: [market()],
    open_orders: [],
    fills: [],
    latest_decisions: [
      {
        action: "hold",
        market_id: "market-1",
        reason:
          "missing book for token 123456789012345678901234567890; missing book for token 987654321098765432109876543210"
      }
    ],
    latest_execution_reports: []
  };
}

function market() {
  return {
    market_id: "market-1",
    question: "Bitcoin Up or Down",
    condition_id: "condition-1",
    up_token_id: "up-token",
    down_token_id: "down-token",
    start_ts: "2026-06-15T21:00:00Z",
    end_ts: "2026-06-15T21:15:00Z",
    start_price: "66488.67",
    status: "tradeable",
    is_active: true,
    is_tradeable: true,
    fair_value: {
      market_id: "market-1",
      q_up: "51",
      q_down: "49",
      sigma: 0.2,
      drift_mu: 0,
      model_error: "0.01",
      computed_ts: "2026-06-15T21:00:02Z"
    }
  };
}

function chartPayload() {
  const start = Date.parse("2026-06-15T21:00:00Z");
  return {
    market_id: "market-1",
    range: "full",
    domain: [start, start + 15 * 60 * 1000],
    points: [
      { bucket: start, time: "2026-06-15T21:00:00Z", qUp: 50, qDown: 50, upBid: 49, upAsk: 51, referencePrice: 66488.67, distanceBps: 0 },
      { bucket: start + 60_000, time: "2026-06-15T21:01:00Z", qUp: 55, qDown: 45, upBid: 53, upAsk: 57, referencePrice: 66490.12, distanceBps: 1 },
      { bucket: start + 120_000, time: "2026-06-15T21:02:00Z", qUp: 52, qDown: 48, upBid: 50, upAsk: 54, referencePrice: 66485.55, distanceBps: -1 },
      { bucket: start + 180_000, time: "2026-06-15T21:03:00Z", qUp: 51, qDown: 49, upBid: 49, upAsk: 53, referencePrice: 66489, distanceBps: 0.5 }
    ],
    summary: {
      sample_count: 4,
      visible_sample_count: 4,
      q_sample_count: 4,
      book_sample_count: 0,
      first_q_ts: "2026-06-15T21:00:00Z",
      last_q_ts: "2026-06-15T21:03:00Z",
      warnings: ["no_book_quote_samples"]
    }
  };
}
