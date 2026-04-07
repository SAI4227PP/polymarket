"use client";

import type { WebSocketSnapshot } from "@/types/ws";
import { asArray, asRecord, formatClock, formatNumber, formatPercentFromRatio } from "@/lib/format";

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function asString(value: unknown, fallback = "-"): string {
  return typeof value === "string" && value.trim().length > 0 ? value : fallback;
}

function asBoolean(value: unknown): boolean {
  return value === true;
}

function formatUsd(value: unknown, digits = 3): string {
  const n = asNumber(value, Number.NaN);
  if (!Number.isFinite(n)) return "-";
  return `$${formatNumber(n, digits)}`;
}

function formatMs(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  if (value < 1000) return `${value.toFixed(0)} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function pnlTone(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "text-black";
  if (value > 0) return "text-emerald-700";
  if (value < 0) return "text-rose-700";
  return "text-black";
}

function chipTone(value: boolean): string {
  return value
    ? "border-emerald-300 bg-emerald-50 text-emerald-700"
    : "border-amber-300 bg-amber-50 text-amber-700";
}

function KeyValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-black/8 py-1.5 text-[11px]">
      <span className="uppercase tracking-[0.14em] text-black/55">{label}</span>
      <span className="font-semibold text-black">{value}</span>
    </div>
  );
}

function SectionCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <article className="rounded-xl border border-black/10 bg-white/85 p-3 shadow-[0_10px_28px_-22px_rgba(0,0,0,0.75)]">
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-black/65">{title}</h3>
      {children}
    </article>
  );
}

export function OperationsOverview({ snapshot }: { snapshot: WebSocketSnapshot }) {
  const execution = asRecord(snapshot.latestByType.execution);
  const portfolio = asRecord(snapshot.latestByType.portfolio);
  const pastOutcomes = asRecord(snapshot.latestByType.past_outcomes);

  const completedTrades = asArray(snapshot.latestByType.completed_trades);
  const recentCompleted = completedTrades
    .filter((row): row is Record<string, unknown> => !!row && typeof row === "object" && !Array.isArray(row))
    .slice(0, 12);

  const dailyHistory = asRecord(portfolio.daily_history);
  const dailyRows = Object.entries(dailyHistory)
    .map(([day, raw]) => {
      const row = asRecord(raw);
      return {
        day,
        total: asNumber(row.current_total_pnl_usd),
        dayPnl: asNumber(row.day_pnl_usd),
        maxUp: asNumber(row.day_max_up_usd),
        maxDrop: asNumber(row.day_max_drop_usd),
      };
    })
    .sort((a, b) => b.day.localeCompare(a.day))
    .slice(0, 7);

  const totalTrades = recentCompleted.length;
  const wins = recentCompleted.filter((trade) => asNumber(trade.pnl_usd) > 0).length;
  const losses = recentCompleted.filter((trade) => asNumber(trade.pnl_usd) < 0).length;
  const winRate = totalTrades ? wins / totalTrades : 0;
  const realizedFromTable = recentCompleted.reduce((sum, trade) => sum + asNumber(trade.pnl_usd), 0);
  const avgDurationMs = totalTrades
    ? recentCompleted.reduce((sum, trade) => sum + asNumber(trade.duration_ms), 0) / totalTrades
    : 0;

  const latestPayload = JSON.stringify(snapshot.latestMessage ?? {}, null, 2);

  return (
    <section className="col-span-12 min-h-0 rounded-2xl border border-black/10 bg-white/70 p-3 shadow-[0_10px_36px_-22px_rgba(0,0,0,0.65)] backdrop-blur-sm xl:col-span-4">
      <h2 className="mb-2 text-xs font-semibold uppercase tracking-[0.2em] text-black/60">Portfolio Command</h2>

      <div className="grid gap-2">
        <SectionCard title="Portfolio Headline">
          <div className="grid grid-cols-2 gap-2">
            <div className="rounded-lg border border-black/10 bg-white/70 p-2">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Total PnL</p>
              <p className={`text-lg font-bold ${pnlTone(portfolio.total_pnl_usd)}`}>{formatUsd(portfolio.total_pnl_usd)}</p>
            </div>
            <div className="rounded-lg border border-black/10 bg-white/70 p-2">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Day PnL</p>
              <p className={`text-lg font-bold ${pnlTone(portfolio.day_pnl_usd)}`}>{formatUsd(portfolio.day_pnl_usd)}</p>
            </div>
            <div className="rounded-lg border border-black/10 bg-white/70 p-2">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Realized</p>
              <p className={`text-sm font-semibold ${pnlTone(portfolio.realized_pnl_usd)}`}>{formatUsd(portfolio.realized_pnl_usd)}</p>
            </div>
            <div className="rounded-lg border border-black/10 bg-white/70 p-2">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Unrealized</p>
              <p className={`text-sm font-semibold ${pnlTone(portfolio.unrealized_pnl_usd)}`}>{formatUsd(portfolio.unrealized_pnl_usd)}</p>
            </div>
          </div>
          <div className="mt-2">
            <KeyValue label="Source" value={asString(portfolio.source)} />
            <KeyValue label="Day" value={asString(portfolio.day)} />
            <KeyValue label="Net Qty" value={formatNumber(portfolio.net_qty, 0)} />
            <KeyValue label="Last Update" value={formatClock(snapshot.lastUpdatedAt)} />
          </div>
        </SectionCard>

        <SectionCard title="Risk and Execution">
          <KeyValue label="Execution Status" value={asString(execution.execution_status)} />
          <KeyValue label="Latest Trade Status" value={asString(execution.latest_trade_status)} />
          <KeyValue label="Open Positions" value={formatNumber(execution.open_positions, 0)} />
          <KeyValue label="Open Trades" value={formatNumber(execution.open_trades, 0)} />
          <KeyValue label="Drawdown" value={`${formatNumber(portfolio.drawdown_pct_from_peak, 2)}%`} />
          <KeyValue label="Day Max Up" value={formatUsd(portfolio.day_max_up_usd)} />
          <KeyValue label="Day Max Drop" value={formatUsd(portfolio.day_max_drop_usd)} />
          <KeyValue label="Past Outcome Prob Up" value={formatPercentFromRatio(pastOutcomes.probability_up, 2)} />

          <div className="mb-2 flex items-center justify-between text-[11px]">
            <span className="uppercase tracking-[0.14em] text-black/55">Risk Gate</span>
            <span
              className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] ${chipTone(
                asBoolean(asRecord(snapshot.latestByType.signal).risk_allowed),
              )}`}
            >
              {asBoolean(asRecord(snapshot.latestByType.signal).risk_allowed) ? "allowed" : "blocked"}
            </span>
          </div>
        </SectionCard>

        <SectionCard title="Daily Performance History">
          {dailyRows.length === 0 ? (
            <p className="text-xs text-black/60">No daily history in current portfolio payload.</p>
          ) : (
            <div className="max-h-56 overflow-auto rounded-lg border border-black/10">
              <table className="w-full border-collapse text-[11px]">
                <thead className="sticky top-0 bg-[#f6f4ee] text-black/70">
                  <tr>
                    <th className="px-2 py-1.5 text-left font-semibold uppercase tracking-[0.12em]">Day</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Total</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Day PnL</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Max Up</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Max Drop</th>
                  </tr>
                </thead>
                <tbody>
                  {dailyRows.map((row) => (
                    <tr key={row.day} className="border-t border-black/8">
                      <td className="px-2 py-1.5 text-black">{row.day}</td>
                      <td className={`px-2 py-1.5 text-right font-semibold ${pnlTone(row.total)}`}>{formatUsd(row.total)}</td>
                      <td className={`px-2 py-1.5 text-right font-semibold ${pnlTone(row.dayPnl)}`}>{formatUsd(row.dayPnl)}</td>
                      <td className="px-2 py-1.5 text-right text-emerald-700">{formatUsd(row.maxUp)}</td>
                      <td className="px-2 py-1.5 text-right text-rose-700">{formatUsd(row.maxDrop)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </SectionCard>

        <SectionCard title="Completed Trades">
          <div className="mb-2 grid grid-cols-2 gap-2">
            <div className="rounded-md border border-black/10 bg-white/70 px-2 py-1.5">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Win Rate</p>
              <p className="text-sm font-semibold text-black">{formatPercentFromRatio(winRate, 1)}</p>
            </div>
            <div className="rounded-md border border-black/10 bg-white/70 px-2 py-1.5">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Realized (buffer)</p>
              <p className={`text-sm font-semibold ${pnlTone(realizedFromTable)}`}>{formatUsd(realizedFromTable, 4)}</p>
            </div>
            <div className="rounded-md border border-black/10 bg-white/70 px-2 py-1.5">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Wins / Losses</p>
              <p className="text-sm font-semibold text-black">
                {wins} / {losses}
              </p>
            </div>
            <div className="rounded-md border border-black/10 bg-white/70 px-2 py-1.5">
              <p className="text-[10px] uppercase tracking-[0.12em] text-black/55">Avg Duration</p>
              <p className="text-sm font-semibold text-black">{formatMs(avgDurationMs)}</p>
            </div>
          </div>

          {recentCompleted.length === 0 ? (
            <p className="text-xs text-black/60">No completed trades in current buffer.</p>
          ) : (
            <div className="max-h-64 overflow-auto rounded-lg border border-black/10">
              <table className="w-full border-collapse text-[11px]">
                <thead className="sticky top-0 bg-[#f6f4ee] text-black/70">
                  <tr>
                    <th className="px-2 py-1.5 text-left font-semibold uppercase tracking-[0.12em]">Pair</th>
                    <th className="px-2 py-1.5 text-left font-semibold uppercase tracking-[0.12em]">Dir</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Qty</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Entry</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Exit</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">PnL</th>
                    <th className="px-2 py-1.5 text-left font-semibold uppercase tracking-[0.12em]">Outcome</th>
                    <th className="px-2 py-1.5 text-right font-semibold uppercase tracking-[0.12em]">Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {recentCompleted.map((trade, idx) => (
                    <tr key={`${asString(trade.id, "trade")}-${idx}`} className="border-t border-black/8">
                      <td className="px-2 py-1.5 text-black">{asString(trade.pair)}</td>
                      <td className="px-2 py-1.5 text-black">{asString(trade.direction)}</td>
                      <td className="px-2 py-1.5 text-right text-black">{formatNumber(trade.quantity, 0)}</td>
                      <td className="px-2 py-1.5 text-right text-black">{formatNumber(trade.entry_price, 4)}</td>
                      <td className="px-2 py-1.5 text-right text-black">{formatNumber(trade.exit_price, 4)}</td>
                      <td className={`px-2 py-1.5 text-right font-semibold ${pnlTone(trade.pnl_usd)}`}>
                        {formatNumber(trade.pnl_usd, 4)}
                      </td>
                      <td className="max-w-40 truncate px-2 py-1.5 text-black/75">{asString(trade.outcome)}</td>
                      <td className="px-2 py-1.5 text-right text-black">{formatMs(trade.duration_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </SectionCard>

        <SectionCard title="Latest Raw Payload">
          <pre className="max-h-56 overflow-auto rounded-lg border border-black/10 bg-[#111] p-2 text-[10px] leading-relaxed text-emerald-200">
            {latestPayload}
          </pre>
        </SectionCard>
      </div>
    </section>
  );
}