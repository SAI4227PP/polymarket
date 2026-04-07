import type { StreamEvent } from "@/types/ws";
import {
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function compactTime(ts: string): string {
  const date = new Date(ts);
  if (Number.isNaN(date.valueOf())) return ts;
  return date.toLocaleTimeString([], { hour12: false });
}

export function EventTape({ events }: { events: StreamEvent[] }) {
  const rows = events.slice(0, 24).reverse();

  const typeCounts = events.reduce<Record<string, number>>((acc, event) => {
    acc[event.type] = (acc[event.type] ?? 0) + 1;
    return acc;
  }, {});

  const mixSeries = Object.entries(typeCounts).map(([type, value]) => ({ type, value }));

  const cadenceSeries = rows.map((event, idx) => ({
    idx,
    t: compactTime(event.timestamp),
    type: event.type,
    weight: 1,
  }));

  const typeColor = (type: string): string => {
    const palette: Record<string, string> = {
      snapshot: "#0a4a7a",
      portfolio: "#0f766e",
      market: "#1d4ed8",
      signal: "#be123c",
      execution: "#b45309",
      trades: "#52525b",
      completed_trades: "#334155",
      live_btc: "#0369a1",
      past_outcomes: "#14532d",
      error: "#dc2626",
    };
    return palette[type] ?? "#52525b";
  };

  return (
    <section className="rounded-2xl border border-black/10 bg-white/80 p-3 shadow-[0_14px_34px_-24px_rgba(0,0,0,0.8)] backdrop-blur-sm">
      <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-black/65">Event Tape</h3>
      <div className="mt-2 grid grid-cols-1 gap-2 xl:grid-cols-2">
        <div className="h-28 rounded-xl border border-black/10 bg-white/70 p-2">
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-black/55">Cadence</p>
          <ResponsiveContainer width="100%" height="90%">
            <BarChart data={cadenceSeries}>
              <XAxis dataKey="t" tick={{ fontSize: 8 }} minTickGap={18} />
              <YAxis hide domain={[0, 1]} />
              <Tooltip />
              <Bar dataKey="weight" radius={[4, 4, 0, 0]}>
                {cadenceSeries.map((row, i) => (
                  <Cell key={`cadence-${i}`} fill={typeColor(row.type)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="h-28 rounded-xl border border-black/10 bg-white/70 p-2">
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-black/55">Event Mix</p>
          <ResponsiveContainer width="100%" height="90%">
            <PieChart>
              <Pie data={mixSeries} dataKey="value" nameKey="type" innerRadius={20} outerRadius={38}>
                {mixSeries.map((row, i) => (
                  <Cell key={`mix-${i}`} fill={typeColor(row.type)} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  );
}
