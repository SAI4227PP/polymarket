interface JsonPanelProps {
  title: string;
  data: unknown;
}

export function JsonPanel({ title, data }: JsonPanelProps) {
  const content = JSON.stringify(data ?? {}, null, 2);

  return (
    <section className="rounded-2xl border border-black/10 bg-black p-4 text-white shadow-[0_16px_40px_-22px_rgba(0,0,0,0.8)]">
      <h2 className="mb-3 text-xs font-semibold uppercase tracking-[0.2em] text-white/70">{title}</h2>
      <pre className="max-h-[22rem] overflow-auto text-xs leading-relaxed text-emerald-200">{content}</pre>
    </section>
  );
}
