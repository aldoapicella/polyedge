"use client";

import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, RefreshCw } from "lucide-react";
import Link from "next/link";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { getMarketDetail } from "@/lib/api";
import { compact, dateTime, numberText, pctText } from "@/lib/format";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

export function MarketDetailPage({ marketId }: { marketId: string }) {
  const detail = useQuery({
    queryKey: ["markets", marketId],
    queryFn: () => getMarketDetail(marketId),
    refetchInterval: 10000
  });
  const data = detail.data;
  const market = data?.market;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 items-center gap-3">
          <Link
            href="/markets"
            className="grid h-9 w-9 shrink-0 place-items-center rounded-sm border border-line bg-white text-ink/70 hover:bg-panel"
            aria-label="Back to markets"
            title="Back to markets"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <div className="min-w-0">
            <h1 className="truncate text-xl font-semibold text-ink">{market?.question ?? marketId}</h1>
            <p className="truncate text-xs text-ink/50">{marketId}</p>
          </div>
        </div>
        <IconButton label="Refresh market" onClick={() => detail.refetch()}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      {market ? (
        <>
          <div className="grid gap-3 md:grid-cols-4">
            <Metric label="Status" value={market.status} tone={market.is_active ? "good" : "neutral"} />
            <Metric label="Start Price" value={compact(market.start_price)} />
            <Metric label="q Up" value={pctText(market.fair_value?.q_up)} />
            <Metric label="q Down" value={pctText(market.fair_value?.q_down)} />
          </div>

          <div className="grid gap-5 xl:grid-cols-[1fr_420px]">
            <Panel>
              <PanelHeader title="Order Books" meta={`${dateTime(market.start_ts)} → ${dateTime(market.end_ts)}`} />
              <div className="grid gap-4 p-4 md:grid-cols-2">
                <BookPanel title="Up Book" book={data.books.up ?? null} />
                <BookPanel title="Down Book" book={data.books.down ?? null} />
              </div>
            </Panel>

            <Panel>
              <PanelHeader title="Fair Value" meta={market.fair_value?.computed_ts ? dateTime(market.fair_value.computed_ts) : "n/a"} />
              <div className="h-72 p-4">
                {market.fair_value ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={[
                      { outcome: "Up", value: Number(market.fair_value.q_up) },
                      { outcome: "Down", value: Number(market.fair_value.q_down) }
                    ]}>
                      <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
                      <XAxis dataKey="outcome" tick={{ fontSize: 11 }} />
                      <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} />
                      <Tooltip />
                      <Area type="monotone" dataKey="value" stroke="#18705b" fill="#18705b" fillOpacity={0.16} />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <EmptyState label="No fair value for this market" />
                )}
              </div>
            </Panel>
          </div>

          <div className="grid gap-5 xl:grid-cols-2">
            <Timeline title="Decisions" rows={data.decisions.map((row) => [row.action, compact(row.outcome), compact(row.price), row.reason])} />
            <Timeline title="Execution Reports" rows={data.execution_reports.map((row) => [row.status, compact(row.filled_size), compact(row.avg_price), dateTime(row.local_ts)])} />
          </div>
        </>
      ) : (
        <Panel>
          <EmptyState label={detail.isLoading ? "Loading market" : detail.error?.message ?? "Market unavailable"} />
        </Panel>
      )}
    </div>
  );
}

function Metric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "good" | "warn" | "danger" }) {
  return (
    <Panel className="p-4">
      <div className="text-xs font-medium uppercase text-ink/50">{label}</div>
      <div className="mt-1 flex items-center gap-2">
        <span className="truncate text-lg font-semibold text-ink">{value}</span>
        <Pill tone={tone}>{tone}</Pill>
      </div>
    </Panel>
  );
}

function BookPanel({ title, book }: { title: string; book: { bids: { price: string; size: string }[]; asks: { price: string; size: string }[]; local_ts: string } | null }) {
  const rows = [
    ...(book?.bids ?? []).slice(0, 5).map((row) => ({ ...row, side: "Bid" })),
    ...(book?.asks ?? []).slice(0, 5).map((row) => ({ ...row, side: "Ask" }))
  ];
  return (
    <div className="border border-line bg-panel">
      <div className="flex items-center justify-between border-b border-line px-3 py-2">
        <h3 className="text-sm font-semibold text-ink">{title}</h3>
        <span className="text-xs text-ink/50">{book ? dateTime(book.local_ts) : "n/a"}</span>
      </div>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-ink/50">
          <tr>
            <th className="px-3 py-2">Side</th>
            <th className="px-3 py-2">Price</th>
            <th className="px-3 py-2">Size</th>
          </tr>
        </thead>
        <tbody>
          {rows.length ? rows.map((row, index) => (
            <tr key={`${row.side}-${index}`} className="border-t border-line">
              <td className="px-3 py-2">{row.side}</td>
              <td className="px-3 py-2">{numberText(row.price, 3)}</td>
              <td className="px-3 py-2">{numberText(row.size, 2)}</td>
            </tr>
          )) : (
            <tr><td colSpan={3}><EmptyState label="No book levels" /></td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function Timeline({ title, rows }: { title: string; rows: string[][] }) {
  return (
    <Panel>
      <PanelHeader title={title} meta={`${rows.length} rows`} />
      <div className="max-h-80 overflow-auto">
        {rows.length ? rows.slice().reverse().map((row, index) => (
          <div key={index} className="grid grid-cols-[120px_80px_80px_1fr] gap-3 border-b border-line px-4 py-3 text-sm last:border-b-0">
            {row.map((cell, cellIndex) => (
              <span key={cellIndex} className={cellIndex === 3 ? "truncate text-ink/60" : "truncate text-ink"}>{cell}</span>
            ))}
          </div>
        )) : (
          <EmptyState label={`No ${title.toLowerCase()}`} />
        )}
      </div>
    </Panel>
  );
}
