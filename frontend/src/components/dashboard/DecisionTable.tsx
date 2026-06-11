"use client";

import { useMemo } from "react";
import type { TradeDecision } from "@/lib/types";
import { EmptyState, Panel, PanelHeader } from "@/components/ui";
import { collapseDecisions } from "./model";

export function DecisionTable({ decisions }: { decisions: TradeDecision[] }) {
  const rows = useMemo(() => collapseDecisions(decisions).slice(0, 14), [decisions]);
  return (
    <Panel className="min-w-0">
      <PanelHeader
        title="Decisions"
        meta={`${rows.length} grouped rows`}
        help="Recent strategy decisions. Consecutive HOLD rows with the same reason are collapsed to reduce noise."
      />
      <div className="overflow-auto">
        <table className="w-full min-w-[760px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-[11px] uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Action</th>
              <th className="px-3 py-2">Outcome</th>
              <th className="px-3 py-2">Price</th>
              <th className="px-3 py-2">Size</th>
              <th className="px-3 py-2">Edge</th>
              <th className="px-3 py-2">Reason</th>
              <th className="px-3 py-2">Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((row) => (
                <tr key={row.key} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-semibold">{row.action}</td>
                  <td className="px-3 py-2">{row.outcome}</td>
                  <td className="px-3 py-2">{row.price}</td>
                  <td className="px-3 py-2">{row.size}</td>
                  <td className="px-3 py-2">{row.edge}</td>
                  <td className="max-w-[360px] truncate px-3 py-2 text-ink/65">{row.reason}</td>
                  <td className="px-3 py-2">{row.count > 1 ? `x${row.count}` : "1"}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={7}>
                  <EmptyState label="No decisions in snapshot" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}
