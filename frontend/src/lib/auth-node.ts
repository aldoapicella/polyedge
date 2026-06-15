import { timingSafeEqual } from "crypto";

export function verifyDashboardPassword(password: unknown) {
  const expected = process.env.DASHBOARD_AUTH_PASSWORD;
  if (!expected || typeof password !== "string") {
    return false;
  }
  const expectedBytes = Buffer.from(expected);
  const actualBytes = Buffer.from(password);
  if (expectedBytes.length !== actualBytes.length) {
    return false;
  }
  return timingSafeEqual(expectedBytes, actualBytes);
}
