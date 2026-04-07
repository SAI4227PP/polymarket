"use client";

import { useState } from "react";

interface DataField {
  key: string;
  value: string;
}

interface EventPayloadVisualCardProps {
  eventType: string;
  payload: unknown;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function formatValue(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (value === null) return "null";
  if (Array.isArray(value)) return `array(${value.length})`;
  if (value && typeof value === "object") return `object(${Object.keys(value as Record<string, unknown>).length})`;
  return "-";
}

function collectFields(
  value: unknown,
  path: string,
  depth: number,
  rows: DataField[],
) {
  if (depth > 2) return;
  if (rows.length > 40) return;

  if (path && (typeof value !== "object" || value === null || Array.isArray(value))) {
    rows.push({ key: path, value: formatValue(value) });
    return;
  }

  if (Array.isArray(value)) {
    if (path) rows.push({ key: path, value: `array(${value.length})` });
    value.slice(0, 2).forEach((item, idx) => {
      const itemPath = path ? `${path}[${idx}]` : `[${idx}]`;
      if (item && typeof item === "object" && !Array.isArray(item)) {
        const obj = asRecord(item);
        Object.entries(obj)
          .slice(0, 4)
          .forEach(([k, v]) => {
            rows.push({ key: `${itemPath}.${k}`, value: formatValue(v) });
          });
      } else {
        rows.push({ key: itemPath, value: formatValue(item) });
      }
    });
    return;
  }

  if (value && typeof value === "object") {
    const obj = asRecord(value);
    for (const [k, v] of Object.entries(obj)) {
      collectFields(v, path ? `${path}.${k}` : k, depth + 1, rows);
    }
  }
}

export function EventPayloadVisualCard({ eventType, payload }: EventPayloadVisualCardProps) {
  const [activeTradeIndex, setActiveTradeIndex] = useState(0);
  const data = asRecord(payload);
  const rows: DataField[] = [];
  collectFields(data, "", 0, rows);

  const topRows = rows.slice(0, 14);
  const isCompletedTrades = eventType === "completed_trades";

  const completedTrades =
    isCompletedTrades && Array.isArray(payload)
      ? payload.filter((item): item is Record<string, unknown> => !!item && typeof item === "object" && !Array.isArray(item))
      : [];

  const boundedTradeIndex = completedTrades.length === 0 ? 0 : Math.min(activeTradeIndex, completedTrades.length - 1);
  const activeTrade = completedTrades[boundedTradeIndex] ?? null;
  const fieldCount = isCompletedTrades ? Object.keys(activeTrade ?? {}).length : rows.length;

  return (
    <article className="flex h-full min-h-0 flex-col rounded-xl border border-black/10 bg-white/85 p-2 shadow-[0_12px_24px_-20px_rgba(0,0,0,0.75)] backdrop-blur-sm">
      <div className="mb-1.5 flex items-center justify-between gap-2">
        <h3 className="truncate text-xs font-semibold uppercase tracking-[0.16em] text-black/65">{eventType}</h3>
        <span className="rounded-full border border-black/10 bg-black/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-black/60">
          fields {fieldCount}
        </span>
      </div>

      <div className="grid min-h-0 flex-1 grid-cols-1 gap-1.5">
        {!isCompletedTrades ? (
          <div className="rounded-md border border-black/10 bg-white/70 px-1.5 py-1 text-[10px]">
            <p className="mb-1 uppercase tracking-widest text-black/50">Event Data</p>
            {topRows.length === 0 ? (
              <p className="text-black/55">No visible fields</p>
            ) : (
              topRows.map((row, idx) => (
                <div key={`${row.key}-${idx}`} className="flex items-start justify-between gap-2 border-b border-black/8 py-0.5 last:border-b-0">
                  <span className="max-w-[58%] truncate text-black/60">{row.key}</span>
                  <span className="max-w-[42%] truncate text-right font-medium text-black">{row.value}</span>
                </div>
              ))
            )}
          </div>
        ) : null}

        {completedTrades.length > 0 ? (
          <div className="flex min-h-0 flex-col rounded-md border border-black/10 bg-white/70 px-1.5 py-1 text-[10px]">
            <div className="mb-1 flex items-center justify-between gap-2">
              <button
                type="button"
                onClick={() => setActiveTradeIndex((prev) => Math.max(0, prev - 1))}
                disabled={boundedTradeIndex === 0}
                className="rounded border border-black/15 bg-white px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-black/70 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Prev
              </button>
              <span className="text-[10px] font-semibold uppercase tracking-widest text-black/55">
                Trade {boundedTradeIndex + 1} / {completedTrades.length}
              </span>
              <button
                type="button"
                onClick={() => setActiveTradeIndex((prev) => Math.min(completedTrades.length - 1, prev + 1))}
                disabled={boundedTradeIndex >= completedTrades.length - 1}
                className="rounded border border-black/15 bg-white px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-black/70 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Next
              </button>
            </div>

            {activeTrade ? (
              <div className="min-h-0 flex-1 overflow-y-auto rounded-md border border-black/10 bg-white/80 px-1.5 py-1">
                {Object.entries(activeTrade).map(([key, value]) => (
                  <div key={key} className="flex items-start justify-between gap-2 border-b border-black/8 py-0.5 last:border-b-0">
                    <span className="min-w-0 max-w-[58%] truncate text-black/60">{key}</span>
                    <span className="min-w-0 max-w-[42%] truncate text-right font-medium text-black">{formatValue(value)}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  );
}
