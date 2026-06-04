import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteContext = {
  params: Promise<{ path: string[] }>;
};

const METHODS_WITH_BODY = new Set(["POST", "PUT", "PATCH", "DELETE"]);

export async function GET(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function POST(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function PUT(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function PATCH(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function DELETE(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

async function proxy(request: NextRequest, context: RouteContext) {
  const params = await context.params;
  const base = process.env.BACKEND_API_BASE_URL ?? "http://127.0.0.1:8000/api/v1";
  const upstream = new URL(`${base.replace(/\/$/, "")}/${params.path.join("/")}`);
  upstream.search = request.nextUrl.search;

  const headers = new Headers();
  headers.set("Accept", "application/json");
  const contentType = request.headers.get("content-type");
  if (contentType) {
    headers.set("Content-Type", contentType);
  }
  const token = process.env.BACKEND_API_BEARER_TOKEN;
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  const init: RequestInit = {
    method: request.method,
    headers,
    cache: "no-store"
  };
  if (METHODS_WITH_BODY.has(request.method)) {
    init.body = await request.text();
  }

  try {
    const response = await fetch(upstream, init);
    const body = await response.arrayBuffer();
    return new NextResponse(body, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") ?? "application/json",
        "Cache-Control": "no-store"
      }
    });
  } catch (error) {
    return NextResponse.json(
      {
        detail: "Backend API is unavailable.",
        error: error instanceof Error ? error.message : String(error)
      },
      { status: 502 }
    );
  }
}
