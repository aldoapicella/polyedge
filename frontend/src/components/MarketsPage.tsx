"use client";

import { useQuery } from "@tanstack/react-query";
import { ExternalLink, RefreshCw } from "lucide-react";
import Link from "next/link";
import { useMemo } from "react";
import { getHistoricalMarkets, getSnapshot } from "@/lib/api";
import { compact, dateTime, pctText } from "@/lib/format";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

export function MarketsPage() {
  const snapshot = useQuery({
    queryKey: ["snapshot"],
    queryFn: getSnapshot,
    refetchInterval: 10000
  });
  const historical = useQuery({
    queryKey: ["markets", "history"],
    queryFn: () => getHistoricalMarkets(500),
    refetchInterval: 30000
  });
  const liveMarkets = snapshot.data?.markets ?? [];
  const historicalMarkets = historical.data?.markets ?? [];
  const markets = useMemo(() => {
    const merged = new Map<string, (typeof liveMarkets)[number]>();
    for (const market of historicalMarkets) {
      merged.set(market.market_id, market);
    }
    for (const market of liveMarkets) {
      merged.set(market.market_id, market);
    }
    return [...merged.values()].sort((a, b) => new Date(b.start_ts).getTime() - new Date(a.start_ts).getTime());
  }, [historicalMarkets, liveMarkets]);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <h1 className="text-xl font-semibold text-ink">Markets</h1>
        <IconButton
          label="Refresh markets"
          onClick={() => {
            snapshot.refetch();
            historical.refetch();
          }}
        >
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <Panel>
        <PanelHeader title="Market Windows" meta={`${markets.length} loaded · ${historicalMarkets.length} historical`} />
        <div className="overflow-auto">
          <table className="w-full min-w-[960px] text-left text-sm">
            <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
              <tr>
                <th className="px-3 py-2">Market</th>
                <th className="px-3 py-2">Window</th>
                <th className="px-3 py-2">Start</th>
                <th className="px-3 py-2">q Up</th>
                <th className="px-3 py-2">q Down</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Open</th>
              </tr>
            </thead>
            <tbody>
              {markets.length ? (
                markets.map((market) => (
                  <tr key={market.market_id} className="border-b border-line last:border-b-0">
                    <td className="max-w-[360px] px-3 py-2">
                      <div className="truncate font-medium text-ink">{market.question}</div>
                      <div className="truncate text-xs text-ink/50">{market.market_id}</div>
                    </td>
                    <td className="px-3 py-2 text-ink/65">{dateTime(market.start_ts)} → {dateTime(market.end_ts)}</td>
                    <td className="px-3 py-2">{compact(market.start_price)}</td>
                    <td className="px-3 py-2">{pctText(market.fair_value?.q_up)}</td>
                    <td className="px-3 py-2">{pctText(market.fair_value?.q_down)}</td>
                    <td className="px-3 py-2">
                      <Pill tone={market.is_active ? "good" : market.is_tradeable ? "neutral" : "warn"}>
                        {market.status}
                      </Pill>
                    </td>
                    <td className="px-3 py-2">
                      <Link
                        href={`/markets/${encodeURIComponent(market.market_id)}`}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-sm border border-line bg-white text-ink/65 hover:bg-panel hover:text-ink"
                        aria-label={`Open ${market.market_id}`}
                        title="Open market detail"
                      >
                        <ExternalLink className="h-4 w-4" />
                      </Link>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={7}>
                    <EmptyState label={snapshot.isLoading ? "Loading markets" : "No markets in snapshot"} />
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
