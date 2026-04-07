"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : fallback;
}

function asString(value: unknown, fallback = "-"): string {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function formatCurrency(value: number) {
  return `$${value.toFixed(2)}`;
}

function pnlColor(value: number) {
  return value >= 0 ? "text-green-600" : "text-red-600";
}

export function PortfolioOverview({
  portfolioPayload,
}: {
  portfolioPayload: unknown;
}) {
  const p = asRecord(portfolioPayload);
  const dailyHistory = asRecord(p.daily_history);

  const totalPnl = asNumber(p.total_pnl_usd);
  const dayPnl = asNumber(p.day_pnl_usd);
  const realized = asNumber(p.realized_pnl_usd);
  const drawdown = asNumber(p.drawdown_pct_from_peak);
  const maxUp = asNumber(p.day_max_up_usd);
  const maxDrop = asNumber(p.day_max_drop_usd);

  const rawDaySeries = Object.entries(dailyHistory)
    .map(([day, raw]) => {
      const v = asRecord(raw);

      return {
        day,
        total: asNumber(v.current_total_pnl_usd),
      };
    })
    .sort((a, b) => a.day.localeCompare(b.day));

  const daySeries =
    rawDaySeries.length === 1
      ? [
          {
            day: `${rawDaySeries[0].day} (start)`,
            total: rawDaySeries[0].total,
          },
          rawDaySeries[0],
        ]
      : rawDaySeries;

  return (
    <section className="h-full rounded-2xl border border-black/10 bg-white p-4 shadow-lg">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-bold uppercase tracking-[0.15em] text-black/70">
          Portfolio
        </h3>

        <span className="rounded-full bg-black/5 px-3 py-1 text-[10px] font-semibold uppercase">
          {asString(p.source, "live")}
        </span>
      </div>

      {/* Top KPI cards */}
      <div className="mt-4 grid grid-cols-2 gap-3">
        <MetricCard
          title="Total PnL"
          value={formatCurrency(totalPnl)}
          className={pnlColor(totalPnl)}
        />

        <MetricCard
          title="Day PnL"
          value={formatCurrency(dayPnl)}
          className={pnlColor(dayPnl)}
        />

        <MetricCard
          title="Realized"
          value={formatCurrency(realized)}
          className={pnlColor(realized)}
        />

        <MetricCard
          title="Drawdown"
          value={`${drawdown.toFixed(2)}%`}
          className="text-red-500"
        />

        <MetricCard
          title="Max Up"
          value={formatCurrency(maxUp)}
          className="text-green-600"
        />

        <MetricCard
          title="Max Drop"
          value={formatCurrency(maxDrop)}
          className="text-red-600"
        />
      </div>

      {/* Full width chart */}
      <div className="mt-5 h-64 rounded-xl border border-black/10 bg-white p-3 md:h-72">
        <div className="mb-3 text-xs font-semibold uppercase tracking-[0.12em] text-black/50">
          PnL Trend
        </div>

        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={daySeries}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />

            <XAxis
              dataKey="day"
              tick={{ fontSize: 10 }}
              axisLine={false}
              tickLine={false}
            />

            <YAxis
              tick={{ fontSize: 10 }}
              axisLine={false}
              tickLine={false}
            />

            <Tooltip />

            <Line
              type="monotone"
              dataKey="total"
              stroke={totalPnl >= 0 ? "#16a34a" : "#dc2626"}
              strokeWidth={3}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function MetricCard({
  title,
  value,
  className,
}: {
  title: string;
  value: string;
  className?: string;
}) {
  return (
    <div className="rounded-xl border border-black/10 bg-white px-3 py-2">
      <p className="text-[10px] uppercase tracking-[0.12em] text-black/50">
        {title}
      </p>
      <p className={`mt-1 text-sm font-bold ${className}`}>
        {value}
      </p>
    </div>
  );
}