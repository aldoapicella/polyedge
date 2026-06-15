"use client";

import { ShieldCheck } from "lucide-react";
import { useSearchParams } from "next/navigation";
import { FormEvent, useMemo, useState } from "react";
import { Button } from "@/components/ui";

export function LoginPage() {
  const params = useSearchParams();
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(
    params.get("error") === "auth_not_configured" ? "Dashboard authentication is not configured on the server." : null
  );
  const [pending, setPending] = useState(false);
  const nextPath = useMemo(() => safeNext(params.get("next")), [params]);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPending(true);
    setError(null);
    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(typeof payload?.detail === "string" ? payload.detail : "Login failed.");
      }
      window.location.assign(nextPath);
    } catch (loginError) {
      setError(loginError instanceof Error ? loginError.message : String(loginError));
    } finally {
      setPending(false);
    }
  }

  return (
    <main className="grid min-h-screen place-items-center bg-panel px-4 py-8">
      <form onSubmit={onSubmit} className="w-full max-w-sm border border-line bg-white p-5 shadow-hairline">
        <div className="flex items-center gap-3">
          <span className="grid h-10 w-10 place-items-center border border-ink bg-ink text-white">
            <ShieldCheck className="h-5 w-5" />
          </span>
          <div>
            <h1 className="text-base font-semibold text-ink">PolyEdge Dashboard</h1>
            <p className="text-xs text-ink/55">Owner access only</p>
          </div>
        </div>

        <label className="mt-5 block text-sm font-medium text-ink" htmlFor="dashboard-password">
          Password
        </label>
        <input
          id="dashboard-password"
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          className="mt-2 h-10 w-full border border-line bg-white px-3 text-sm text-ink outline-none focus:border-good"
          required
        />

        {error ? <div className="mt-3 border border-danger/20 bg-danger/10 px-3 py-2 text-sm text-danger">{error}</div> : null}

        <Button className="mt-5 w-full" tone="good" disabled={pending || !password}>
          {pending ? "Signing in" : "Sign in"}
        </Button>
      </form>
    </main>
  );
}

function safeNext(value: string | null) {
  if (!value || !value.startsWith("/") || value.startsWith("//") || value.startsWith("/api/")) {
    return "/dashboard";
  }
  return value;
}
