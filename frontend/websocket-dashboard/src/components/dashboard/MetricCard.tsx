interface MetricCardProps {
  label: string;
  value: string;
  hint?: string;
}

export function MetricCard({ label, value, hint }: MetricCardProps) {
  return (
    <article className="rounded-2xl border border-black/10 bg-white/80 p-4 shadow-[0_8px_30px_-18px_rgba(0,0,0,0.55)] backdrop-blur-sm">
      <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-black/50">{label}</p>
      <p className="mt-2 text-2xl font-bold text-black">{value}</p>
      {hint ? <p className="mt-1 text-xs text-black/60">{hint}</p> : null}
    </article>
  );
}
