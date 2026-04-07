"use client";

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { StreamEvent } from "@/types/ws";

function asObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asNumber(value: unknown, fallback = 0): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return fallback;
  return value;
}

function labelFromTs(ts: string): string {
  const date = new Date(ts);
  if (Number.isNaN(date.valueOf())) return "--:--:--";
  return date.toLocaleTimeString([], { hour12: false });
}

interface QuantVisualsProps {
  recentEvents: StreamEvent[];
  latestByType: Record<string, unknown>;
}

export function QuantVisuals({ recentEvents, latestByType }: QuantVisualsProps) {
  const chronological = [...recentEvents].reverse();

  const pnlSeries = chronological
    .filter((e) => e.type === "portfolio")
    .slice(-40)
    .map((e) => {
      const d = asObject(e.data);
      return {
        t: labelFromTs(e.timestamp),
        totalPnl: asNumber(d.total_pnl_usd),
        dayPnl: asNumber(d.day_pnl_usd),
        drawdown: asNumber(d.drawdown_pct_from_peak),
      };
    });

  const marketSeries = chronological
    .filter((e) => e.type === "market")
    .slice(-40)
    .map((e) => {
      const d = asObject(e.data);
      return {
        t: labelFromTs(e.timestamp),
        poly: asNumber(d.polymarket_probability),
        fair: asNumber(d.fair_value_probability),
      };
    });

  const distributionMap = recentEvents.reduce<Record<string, number>>((acc, e) => {
    acc[e.type] = (acc[e.type] ?? 0) + 1;
    return acc;
  }, {});

  const distributionSeries = Object.entries(distributionMap)
    .map(([type, count]) => ({ type, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  const signal = asObject(latestByType.signal);
  const execution = asObject(latestByType.execution);
  const portfolio = asObject(latestByType.portfolio);

  const netEdge = asNumber(signal.net_edge_bps);
  const openPositions = Math.max(0, asNumber(execution.open_positions));
  const drawdown = Math.max(0, asNumber(portfolio.drawdown_pct_from_peak));

  return (
    <section className="grid grid-cols-1 gap-2 xl:grid-cols-2">
      <article className="rounded-2xl border border-black/10 bg-white/80 p-3 shadow-[0_14px_34px_-24px_rgba(0,0,0,0.8)] backdrop-blur-sm">
        <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-black/65">PnL and Drawdown</h3>
        <div className="mt-2 h-36">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={pnlSeries}>
              <defs>
                <linearGradient id="pnlFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#0f766e" stopOpacity={0.35} />
                  <stop offset="95%" stopColor="#0f766e" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="4 4" stroke="#d5d7d4" />
              <XAxis dataKey="t" tick={{ fontSize: 11 }} minTickGap={24} />
              <YAxis yAxisId="pnl" tick={{ fontSize: 11 }} />
              <YAxis yAxisId="dd" orientation="right" tick={{ fontSize: 11 }} />
              <Tooltip />
              <Area yAxisId="pnl" type="monotone" dataKey="totalPnl" stroke="#0f766e" fill="url(#pnlFill)" />
              <Line yAxisId="pnl" type="monotone" dataKey="dayPnl" stroke="#0a4a7a" dot={false} strokeWidth={2} />
              <Line yAxisId="dd" type="monotone" dataKey="drawdown" stroke="#b45309" dot={false} strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </article>

      <article className="rounded-2xl border border-black/10 bg-white/80 p-3 shadow-[0_14px_34px_-24px_rgba(0,0,0,0.8)] backdrop-blur-sm">
        <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-black/65">Probability Spread</h3>
        <div className="mt-2 h-36">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={marketSeries}>
              <CartesianGrid strokeDasharray="4 4" stroke="#d5d7d4" />
              <XAxis dataKey="t" tick={{ fontSize: 11 }} minTickGap={24} />
              <YAxis tick={{ fontSize: 11 }} domain={[0, 1]} />
              <Tooltip />
              <Line type="monotone" dataKey="poly" stroke="#0a4a7a" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="fair" stroke="#be123c" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </article>

      <article className="rounded-2xl border border-black/10 bg-white/80 p-3 shadow-[0_14px_34px_-24px_rgba(0,0,0,0.8)] backdrop-blur-sm">
        <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-black/65">Event Throughput</h3>
        <div className="mt-2 h-32">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={distributionSeries}>
              <CartesianGrid strokeDasharray="4 4" stroke="#d5d7d4" />
              <XAxis dataKey="type" tick={{ fontSize: 9 }} interval={0} angle={-12} textAnchor="end" height={44} />
              <YAxis tick={{ fontSize: 9 }} allowDecimals={false} />
              <Tooltip />
              <Bar dataKey="count" fill="#0a4a7a" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </article>

      <article className="rounded-2xl border border-black/10 bg-white/80 p-3 shadow-[0_14px_34px_-24px_rgba(0,0,0,0.8)] backdrop-blur-sm">
        <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-black/65">Execution Pulse</h3>
        <div className="mt-3 space-y-2">
          <div>
            <div className="mb-1 flex items-center justify-between text-xs uppercase tracking-[0.16em] text-black/55">
              <span>Net Edge (bps)</span>
              <span>{netEdge.toFixed(2)}</span>
            </div>
            <div className="h-2 rounded-full bg-black/10">
              <div className="h-full rounded-full bg-[#0f766e]" style={{ width: `${Math.min(Math.abs(netEdge), 100)}%` }} />
            </div>
          </div>

          <div>
            <div className="mb-1 flex items-center justify-between text-xs uppercase tracking-[0.16em] text-black/55">
              <span>Open Positions</span>
              <span>{openPositions.toFixed(0)}</span>
            </div>
            <div className="h-2 rounded-full bg-black/10">
              <div className="h-full rounded-full bg-[#0a4a7a]" style={{ width: `${Math.min(openPositions * 20, 100)}%` }} />
            </div>
          </div>

          <div>
            <div className="mb-1 flex items-center justify-between text-xs uppercase tracking-[0.16em] text-black/55">
              <span>Drawdown (%)</span>
              <span>{drawdown.toFixed(2)}</span>
            </div>
            <div className="h-2 rounded-full bg-black/10">
              <div className="h-full rounded-full bg-[#b45309]" style={{ width: `${Math.min(drawdown, 100)}%` }} />
            </div>
          </div>
        </div>
      </article>
    </section>
  );
}
