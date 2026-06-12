import { NextResponse } from "next/server";
import WebSocket from "ws";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  if (process.env.BACKEND_SSE_URL) {
    return proxySse(process.env.BACKEND_SSE_URL);
  }

  const encoder = new TextEncoder();
  let ws: WebSocket | null = null;
  let heartbeat: ReturnType<typeof setInterval> | null = null;
  let fallback: ReturnType<typeof setInterval> | null = null;
  let closed = false;
  const seenEvents = new Set<string>();

  function cleanup() {
    if (closed) {
      return;
    }
    closed = true;
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
    if (fallback) {
      clearInterval(fallback);
      fallback = null;
    }
    ws?.removeAllListeners();
    ws?.close();
    ws = null;
  }

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const send = (chunk: string) => {
        if (closed) {
          return;
        }
        try {
          controller.enqueue(encoder.encode(chunk));
        } catch {
          cleanup();
        }
      };
      const sendData = (payload: string) => {
        rememberEvent(payload, seenEvents);
        send(`data: ${payload}\n\n`);
      };
      const sendSnapshot = async () => {
        const snapshot = await backendJson("snapshot");
        sendData(JSON.stringify({ type: "status_snapshot", ts: new Date().toISOString(), data: snapshot }));
      };
      const sendRecentEvents = async () => {
        const payload = await backendJson("events/recent?limit=50");
        const events = Array.isArray(payload.events) ? payload.events.slice().reverse() : [];
        for (const event of events) {
          const text = JSON.stringify(event);
          if (!seenEvents.has(eventKey(text))) {
            sendData(text);
          }
        }
      };
      const startFallback = () => {
        if (fallback || closed) {
          return;
        }
        void sendSnapshot().catch((error) => {
          send(`event: error\ndata: ${JSON.stringify({ detail: error.message })}\n\n`);
        });
        fallback = setInterval(() => {
          void sendSnapshot().catch(() => undefined);
          void sendRecentEvents().catch(() => undefined);
        }, 5000);
      };
      const wsUrl = backendWebSocketUrl();
      ws = new WebSocket(wsUrl, backendWebSocketOptions());
      heartbeat = setInterval(() => {
        send(": heartbeat\n\n");
      }, 15000);

      ws.on("message", (data) => {
        sendData(data.toString());
      });

      ws.on("open", () => {
        send("event: connected\ndata: {}\n\n");
        void sendSnapshot().catch(() => undefined);
      });

      ws.on("error", (error) => {
        send(`event: error\ndata: ${JSON.stringify({ detail: error.message })}\n\n`);
        startFallback();
      });

      ws.on("close", () => {
        ws = null;
        startFallback();
      });
    },
    cancel() {
      cleanup();
      return undefined;
    }
  });

  return new NextResponse(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    }
  });
}

async function backendJson(path: string) {
  const response = await fetch(`${backendApiBaseUrl()}/${path.replace(/^\//, "")}`, {
    headers: backendHeaders(),
    cache: "no-store"
  });
  if (!response.ok) {
    throw new Error(`Backend ${path} returned ${response.status}`);
  }
  return response.json();
}

function backendApiBaseUrl() {
  return (process.env.BACKEND_API_BASE_URL ?? "http://127.0.0.1:8081/api/v1").replace(/\/$/, "");
}

function backendHeaders() {
  const headers = new Headers();
  headers.set("Accept", "application/json");
  const token = process.env.BACKEND_API_BEARER_TOKEN;
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  return headers;
}

function backendWebSocketOptions() {
  const token = process.env.BACKEND_API_BEARER_TOKEN;
  return token ? { headers: { Authorization: `Bearer ${token}` } } : undefined;
}

async function proxySse(sseUrl: string) {
  const headers = new Headers();
  const token = process.env.BACKEND_API_BEARER_TOKEN;
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  try {
    const response = await fetch(sseUrl, {
      headers,
      cache: "no-store"
    });
    if (!response.ok || !response.body) {
      return NextResponse.json(
        {
          detail: "Backend realtime stream is unavailable.",
          status: response.status
        },
        { status: 502 }
      );
    }
    return new NextResponse(response.body, {
      headers: {
        "Content-Type": response.headers.get("content-type") ?? "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive"
      }
    });
  } catch (error) {
    return NextResponse.json(
      {
        detail: "Backend realtime stream is unavailable.",
        error: error instanceof Error ? error.message : String(error)
      },
      { status: 502 }
    );
  }
}

function backendWebSocketUrl() {
  const explicit = process.env.BACKEND_WS_URL;
  return explicit || deriveWsUrl(backendApiBaseUrl());
}

function deriveWsUrl(apiBaseUrl: string) {
  const url = new URL(apiBaseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/, "")}/ws/live`;
  return url.toString();
}

function rememberEvent(payload: string, seenEvents: Set<string>) {
  seenEvents.add(eventKey(payload));
  if (seenEvents.size <= 1000) {
    return;
  }
  const [oldest] = seenEvents;
  if (oldest) {
    seenEvents.delete(oldest);
  }
}

function eventKey(payload: string) {
  try {
    const event = JSON.parse(payload);
    return `${event.type ?? event.event_type ?? "event"}:${event.ts ?? ""}:${JSON.stringify(event.data ?? {}).slice(0, 160)}`;
  } catch {
    return payload.slice(0, 200);
  }
}
