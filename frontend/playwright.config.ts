import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  timeout: 60_000,
  expect: {
    timeout: 10_000
  },
  use: {
    baseURL: "http://127.0.0.1:3200",
    trace: "retain-on-failure"
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    },
    {
      name: "mobile",
      use: { ...devices["Pixel 7"] }
    }
  ],
  webServer: process.env.PLAYWRIGHT_SKIP_WEB_SERVER
    ? undefined
    : {
        command:
          "DASHBOARD_AUTH_PASSWORD=test-password DASHBOARD_SESSION_SECRET=test-session-secret BACKEND_API_BASE_URL=http://127.0.0.1:65535/api/v1 npm run build && DASHBOARD_AUTH_PASSWORD=test-password DASHBOARD_SESSION_SECRET=test-session-secret BACKEND_API_BASE_URL=http://127.0.0.1:65535/api/v1 npm run start -- --port 3200",
        url: "http://127.0.0.1:3200/login",
        reuseExistingServer: !process.env.CI,
        timeout: 300_000
      }
});
