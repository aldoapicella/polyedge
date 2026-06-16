"use client";

import { useMemo, useState } from "react";
import type { JsonRecord, QueryColumn } from "@/lib/types";
import { compact, dateTime, numberText } from "@/lib/format";
import { InfoHint } from "@/components/ui";

const ROW_HEIGHT = 42;
const OVERSCAN = 8;

export function VirtualTable({
  rows,
  columns,
  maxHeight = 460,
  onRowSelect
}: {
  rows: JsonRecord[];
  columns: QueryColumn[];
  maxHeight?: number;
  onRowSelect?: (row: JsonRecord) => void;
}) {
  const [scrollTop, setScrollTop] = useState(0);
  const visible = useMemo(() => {
    const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
    const end = Math.min(rows.length, Math.ceil((scrollTop + maxHeight) / ROW_HEIGHT) + OVERSCAN);
    return {
      start,
      end,
      rows: rows.slice(start, end)
    };
  }, [maxHeight, rows, scrollTop]);
  const fields = columns.length ? columns : inferredColumns(rows);
  const topHeight = visible.start * ROW_HEIGHT;
  const bottomHeight = Math.max(0, (rows.length - visible.end) * ROW_HEIGHT);

  return (
    <div className="overflow-auto border border-line" style={{ maxHeight }} onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}>
      <table className="w-full min-w-[960px] text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            {fields.map((column) => (
              <th key={column.field} className="px-3 py-2">
                <span className="inline-flex items-center gap-1">
                  {column.label || column.field}
                  {column.help ? <InfoHint label={column.help} /> : null}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {topHeight ? (
            <tr aria-hidden>
              <td colSpan={fields.length} style={{ height: topHeight, padding: 0 }} />
            </tr>
          ) : null}
          {visible.rows.map((row, index) => (
            <tr
              key={`${visible.start + index}-${JSON.stringify(row).slice(0, 80)}`}
              className="h-[42px] border-b border-line last:border-b-0 hover:bg-panel"
            >
              {fields.map((column) => (
                <td key={column.field} className="max-w-[320px] px-3 py-2">
                  <button
                    className="block w-full truncate text-left text-ink/75"
                    onClick={() => onRowSelect?.(row)}
                    title={formatCell(row[column.field], column.kind)}
                  >
                    {formatCell(row[column.field], column.kind)}
                  </button>
                </td>
              ))}
            </tr>
          ))}
          {bottomHeight ? (
            <tr aria-hidden>
              <td colSpan={fields.length} style={{ height: bottomHeight, padding: 0 }} />
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

function inferredColumns(rows: JsonRecord[]): QueryColumn[] {
  return Object.keys(rows[0] ?? {}).map((field) => ({
    field,
    label: field.replaceAll("_", " "),
    kind: "text"
  }));
}

function formatCell(value: unknown, kind: string) {
  if (kind === "number") {
    return numberText(value);
  }
  if (kind === "datetime") {
    return dateTime(String(value ?? ""));
  }
  return compact(value);
}
