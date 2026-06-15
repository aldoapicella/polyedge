import type { NextRequest } from "next/server";

export const DASHBOARD_SESSION_COOKIE = "polyedge_dashboard_session";
const DEFAULT_SESSION_TTL_SECONDS = 12 * 60 * 60;

export type DashboardSession = {
  sub: "owner";
  iat: number;
  exp: number;
};

export function dashboardAuthConfigured() {
  return Boolean(process.env.DASHBOARD_AUTH_PASSWORD && process.env.DASHBOARD_SESSION_SECRET);
}

export function dashboardAuthRequired() {
  return dashboardAuthConfigured() || process.env.NODE_ENV === "production";
}

export function authConfigurationError() {
  if (dashboardAuthConfigured()) {
    return null;
  }
  if (!process.env.DASHBOARD_AUTH_PASSWORD && !process.env.DASHBOARD_SESSION_SECRET) {
    return "Dashboard auth is not configured. Set DASHBOARD_AUTH_PASSWORD and DASHBOARD_SESSION_SECRET.";
  }
  if (!process.env.DASHBOARD_AUTH_PASSWORD) {
    return "Dashboard auth password is not configured.";
  }
  return "Dashboard session secret is not configured.";
}

export function sessionTtlSeconds() {
  const parsed = Number(process.env.DASHBOARD_SESSION_TTL_SECONDS);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_SESSION_TTL_SECONDS;
}

export async function createDashboardSessionCookie(nowSeconds = Math.floor(Date.now() / 1000)) {
  const ttl = sessionTtlSeconds();
  const session: DashboardSession = {
    sub: "owner",
    iat: nowSeconds,
    exp: nowSeconds + ttl
  };
  const payload = base64UrlEncode(JSON.stringify(session));
  const signature = await signSessionPayload(payload);
  return `${payload}.${signature}`;
}

export async function validateDashboardSessionCookie(value: string | undefined | null, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (!value || !dashboardAuthConfigured()) {
    return null;
  }
  const [payload, signature] = value.split(".");
  if (!payload || !signature) {
    return null;
  }
  const expected = await signSessionPayload(payload);
  if (!constantTimeEqual(signature, expected)) {
    return null;
  }
  const session = parseSessionPayload(payload);
  if (!session || session.sub !== "owner" || session.exp <= nowSeconds) {
    return null;
  }
  return session;
}

export async function requestHasDashboardSession(request: NextRequest) {
  const cookie = request.cookies.get(DASHBOARD_SESSION_COOKIE)?.value;
  return Boolean(await validateDashboardSessionCookie(cookie));
}

async function signSessionPayload(payload: string) {
  const secret = process.env.DASHBOARD_SESSION_SECRET ?? "";
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) {
    throw new Error("Web Crypto is unavailable for dashboard session signing.");
  }
  const key = await subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return base64UrlEncode(new Uint8Array(signature));
}

function parseSessionPayload(payload: string): DashboardSession | null {
  try {
    const parsed = JSON.parse(base64UrlDecode(payload));
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    const session = parsed as Partial<DashboardSession>;
    if (session.sub !== "owner" || !Number.isFinite(session.iat) || !Number.isFinite(session.exp)) {
      return null;
    }
    return {
      sub: "owner",
      iat: Number(session.iat),
      exp: Number(session.exp)
    };
  } catch {
    return null;
  }
}

function constantTimeEqual(left: string, right: string) {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.max(leftBytes.length, rightBytes.length);
  let diff = leftBytes.length ^ rightBytes.length;
  for (let index = 0; index < length; index += 1) {
    diff |= (leftBytes[index] ?? 0) ^ (rightBytes[index] ?? 0);
  }
  return diff === 0;
}

function base64UrlEncode(value: string | Uint8Array) {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function base64UrlDecode(value: string) {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
  const binary = atob(padded);
  return new TextDecoder().decode(Uint8Array.from(binary, (char) => char.charCodeAt(0)));
}
