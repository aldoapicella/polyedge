import type { JsonRecord, QueryColumn } from "@/lib/types";

export function rowsToCsv(rows: JsonRecord[], columns: QueryColumn[]) {
  const fields = columns.length ? columns.map((column) => column.field) : Object.keys(rows[0] ?? {});
  const header = fields.map(csvCell).join(",");
  const body = rows.map((row) => fields.map((field) => csvCell(row[field])).join(","));
  return [header, ...body].join("\n");
}

export function downloadCsv(filename: string, csv: string) {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function csvCell(value: unknown) {
  const text = value === null || value === undefined ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}
