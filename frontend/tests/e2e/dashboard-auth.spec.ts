import { expect, Page, test } from "@playwright/test";

test("anonymous dashboard and backend proxy requests are blocked", async ({ page, request }) => {
  await page.goto("/dashboard");
  await expect(page).toHaveURL(/\/login\?next=%2Fdashboard/);

  const response = await request.get("/api/backend/snapshot");
  expect(response.status()).toBe(401);
});

test("owner login unlocks dashboard and q lines draw before midpoint", async ({ page }) => {
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

  const qPathStartsBeforeMidpoint = await page.evaluate(() => {
    const chart = document.querySelector(".recharts-wrapper");
    if (!chart) {
      return false;
    }
    const chartWidth = chart.getBoundingClientRect().width;
    const qPath = Array.from(chart.querySelectorAll("path.recharts-line-curve")).find(
      (path) => path.getAttribute("name") === "q Up"
    );
    if (!qPath) {
      return false;
    }
    return qPath.getBoundingClientRect().left - chart.getBoundingClientRect().left < chartWidth / 2;
  });
  expect(qPathStartsBeforeMidpoint).toBe(true);
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
  await page.goto("/jobs");
  await expect(page.getByRole("button", { name: "Backfill" })).toBeDisabled();
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
  for (const endpoint of ["regimes", "calibration", "sample-size", "fill-models"]) {
    await page.route(`**/api/backend/labs/${endpoint}/latest`, async (route) => {
      await route.fulfill({ json: { date: "2026-06-14", report: {} } });
    });
  }
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
      q_up: "0.51",
      q_down: "0.49",
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
      { bucket: start, time: "2026-06-15T21:00:00Z", qUp: 0.5, qDown: 0.5, distanceBps: 0 },
      { bucket: start + 60_000, time: "2026-06-15T21:01:00Z", qUp: 0.55, qDown: 0.45, distanceBps: 1 },
      { bucket: start + 120_000, time: "2026-06-15T21:02:00Z", qUp: 0.52, qDown: 0.48, distanceBps: -1 },
      { bucket: start + 180_000, time: "2026-06-15T21:03:00Z", qUp: 0.51, qDown: 0.49, distanceBps: 0.5 }
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
