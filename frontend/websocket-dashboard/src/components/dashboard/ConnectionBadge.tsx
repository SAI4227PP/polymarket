import type { ConnectionStatus } from "@/types/ws";

const statusStyles: Record<ConnectionStatus, string> = {
  connecting: "bg-amber-100 text-amber-800 border-amber-300",
  open: "bg-emerald-100 text-emerald-800 border-emerald-300",
  closed: "bg-slate-200 text-slate-700 border-slate-300",
  error: "bg-rose-100 text-rose-700 border-rose-300",
};

export function ConnectionBadge({ status }: { status: ConnectionStatus }) {
  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${statusStyles[status]}`}
    >
      {status}
    </span>
  );
}
