import { NextRequest, NextResponse } from "next/server";
import { authConfigurationError, dashboardAuthRequired, requestHasDashboardSession } from "@/lib/auth";

const PUBLIC_FILE = /\.(?:png|jpg|jpeg|gif|webp|ico|svg|css|js|map|txt|xml)$/i;

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (isPublicPath(pathname)) {
    return NextResponse.next();
  }

  if (!dashboardAuthRequired()) {
    return NextResponse.next();
  }

  const configurationError = authConfigurationError();
  if (configurationError) {
    return authFailure(request, 503, configurationError);
  }

  if (await requestHasDashboardSession(request)) {
    return NextResponse.next();
  }

  return authFailure(request, 401, "Dashboard authentication required.");
}

function authFailure(request: NextRequest, status: 401 | 503, detail: string) {
  if (request.nextUrl.pathname.startsWith("/api/")) {
    return NextResponse.json({ detail }, { status });
  }
  const login = new URL("/login", request.url);
  login.searchParams.set("next", `${request.nextUrl.pathname}${request.nextUrl.search}`);
  if (status === 503) {
    login.searchParams.set("error", "auth_not_configured");
  }
  return NextResponse.redirect(login);
}

function isPublicPath(pathname: string) {
  return (
    pathname === "/login" ||
    pathname === "/api/auth/login" ||
    pathname === "/api/auth/logout" ||
    pathname.startsWith("/_next/") ||
    pathname === "/favicon.ico" ||
    pathname === "/robots.txt" ||
    PUBLIC_FILE.test(pathname)
  );
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"]
};
