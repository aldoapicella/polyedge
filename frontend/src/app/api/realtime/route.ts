import { NextResponse } from "next/server";
import WebSocket from "ws";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const encoder = new TextEncoder();
  let ws: WebSocket | null = null;
  let heartbeat: ReturnType<typeof setInterval> | null = null;
  let closed = false;

  function cleanup() {
    if (closed) {
      return;
    }
    closed = true;
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
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
      const wsUrl = backendWebSocketUrl();
      ws = new WebSocket(wsUrl);
      heartbeat = setInterval(() => {
        send(": heartbeat\n\n");
      }, 15000);

      ws.on("message", (data) => {
        send(`data: ${data.toString()}\n\n`);
      });

      ws.on("open", () => {
        send("event: connected\ndata: {}\n\n");
      });

      ws.on("error", (error) => {
        send(`event: error\ndata: ${JSON.stringify({ detail: error.message })}\n\n`);
      });

      ws.on("close", () => {
        cleanup();
        try {
          controller.close();
        } catch {
          return;
        }
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

function backendWebSocketUrl() {
  const explicit = process.env.BACKEND_WS_URL;
  const base = explicit || deriveWsUrl(process.env.BACKEND_API_BASE_URL ?? "http://127.0.0.1:8000/api/v1");
  const url = new URL(base);
  const token = process.env.BACKEND_API_BEARER_TOKEN;
  if (token) {
    url.searchParams.set("token", token);
  }
  return url.toString();
}

function deriveWsUrl(apiBaseUrl: string) {
  const url = new URL(apiBaseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/, "")}/ws/live`;
  return url.toString();
}
