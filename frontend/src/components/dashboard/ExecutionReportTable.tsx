"use client";

import { useMemo, useState } from "react";
import { compact, dateTime } from "@/lib/format";
import type { ExecutionReport, MarketSummary } from "@/lib/types";
import { EmptyState, Panel, PanelHeader } from "@/components/ui";
import { EXECUTION_FILTERS, filterReports, reportOutcome, sharePriceText, statusClass } from "./model";
import type { ExecutionFilter } from "./types";

export function ExecutionReportTable({ reports, active }: { reports: ExecutionReport[]; active?: MarketSummary | null }) {
  const [filter, setFilter] = useState<ExecutionFilter>("all");
  const rows = useMemo(() => filterReports(reports, filter).slice(0, 40), [filter, reports]);
  return (
    <Panel>
      <PanelHeader
        title="Execution Reports"
        meta={`${rows.length} shown`}
        help="Recent paper/live execution adapter reports. In paper mode these are simulated execution outcomes."
      >
        <div className="flex flex-wrap gap-2">
          {EXECUTION_FILTERS.map((item) => (
            <button
              key={item.value}
              className={[
                "h-8 rounded-sm border px-3 text-xs font-semibold transition",
                filter === item.value ? "border-good bg-good text-white" : "border-line bg-white text-ink/65 hover:bg-panel"
              ].join(" ")}
              onClick={() => setFilter(item.value)}
            >
              {item.label}
            </button>
          ))}
        </div>
      </PanelHeader>
      <div className="overflow-auto">
        <table className="w-full min-w-[840px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-[11px] uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Status</th>
              <th className="px-3 py-2">Outcome</th>
              <th className="px-3 py-2">Filled</th>
              <th className="px-3 py-2">Avg</th>
              <th className="px-3 py-2">Fee</th>
              <th className="px-3 py-2">Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((row, index) => (
                <tr key={`${row.order_id}-${index}`} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2">
                    <span className={statusClass(row.status)}>{row.status}</span>
                  </td>
                  <td className="px-3 py-2">{reportOutcome(row, active)}</td>
                  <td className="px-3 py-2">{compact(row.filled_size)}</td>
                  <td className="px-3 py-2">{sharePriceText(row.avg_price)}</td>
                  <td className="px-3 py-2">{compact(row.fee)}</td>
                  <td className="px-3 py-2 text-ink/60">{dateTime(row.local_ts)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={6}>
                  <EmptyState label="No execution reports match this filter" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}
