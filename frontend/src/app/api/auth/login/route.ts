import { NextRequest, NextResponse } from "next/server";
import { createDashboardSessionCookie, DASHBOARD_SESSION_COOKIE, dashboardAuthConfigured, sessionTtlSeconds } from "@/lib/auth";
import { verifyDashboardPassword } from "@/lib/auth-node";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  if (!dashboardAuthConfigured()) {
    return NextResponse.json({ detail: "Dashboard auth is not configured." }, { status: 503 });
  }

  let password: unknown;
  try {
    const payload = await request.json();
    password = payload?.password;
  } catch {
    return NextResponse.json({ detail: "Invalid login request." }, { status: 400 });
  }

  if (!verifyDashboardPassword(password)) {
    return NextResponse.json({ detail: "Invalid dashboard password." }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set(DASHBOARD_SESSION_COOKIE, await createDashboardSessionCookie(), {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge: sessionTtlSeconds(),
    path: "/"
  });
  return response;
}
