"use client";

import { EventPayloadVisualCard } from "@/components/dashboard/EventPayloadVisualCard";
import { PortfolioOverview } from "@/components/dashboard/PortfolioOverview";
import { useWebSocketFeed } from "@/hooks/useWebSocketFeed";

const EVENT_DISPLAY_ORDER = [
  "portfolio",
  "market",
  "execution",
  "signal",
  "live_btc",
  "trades",
  "completed_trades",
] as const;

const RIGHT_SIDE_EVENT_TYPES = ["execution", "trades", "signal", "completed_trades"] as const;

export default function Home() {
  const snapshot = useWebSocketFeed();
  const visibleEventTypes = snapshot.eventTypes.filter((type) => type !== "snapshot" && type !== "past_outcomes");
  const orderedEventTypes = [
    ...EVENT_DISPLAY_ORDER.filter((type) => visibleEventTypes.includes(type)),
    ...visibleEventTypes.filter((type) => !EVENT_DISPLAY_ORDER.includes(type as (typeof EVENT_DISPLAY_ORDER)[number])),
  ];
  const hasPortfolio = orderedEventTypes.includes("portfolio");

  return (
    <main className="mx-auto flex h-dvh w-full max-w-[99vw] flex-col gap-2 overflow-hidden px-2 py-2 md:px-3 md:py-3">
      <section className="flex min-h-0 flex-1 flex-col rounded-2xl border border-black/10 bg-white/70 p-2 shadow-[0_10px_36px_-22px_rgba(0,0,0,0.65)] backdrop-blur-sm">
        <div className="mb-3 flex items-center justify-between">
          <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-black/65">All Event Cards</h1>
          <span className="rounded-full border border-black/15 bg-black/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-black/70">
            {orderedEventTypes.length} event types
          </span>
        </div>

        <div className="grid h-full min-h-0 grid-cols-1 gap-2 xl:grid-cols-2">
          <section className="flex h-full min-h-0 flex-col rounded-xl border border-black/10 bg-white/55 p-2">
            <div className="mb-2 flex items-center justify-between">
              <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-black/60">Portfolio</h2>
              <span className="rounded-full border border-black/10 bg-black/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-black/60">
                1 card
              </span>
            </div>

            <div className="min-h-0 flex-1">
              {hasPortfolio ? (
                <PortfolioOverview portfolioPayload={snapshot.latestByType.portfolio} />
              ) : (
                <div className="h-full rounded-xl border border-dashed border-black/20 bg-white/40 p-3 text-xs uppercase tracking-[0.14em] text-black/50">
                  portfolio unavailable
                </div>
              )}
            </div>
          </section>

          <section className="flex h-full min-h-0 flex-col rounded-xl border border-black/10 bg-white/55 p-2">
            <div className="mb-2 flex items-center justify-between">
              <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-black/60">Other Event Cards</h2>
              <span className="rounded-full border border-black/10 bg-black/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-black/60">
                4 cards
              </span>
            </div>

            <div className="grid h-full min-h-0 auto-rows-fr grid-cols-1 gap-2 md:grid-cols-2">
              {RIGHT_SIDE_EVENT_TYPES.map((eventType) =>
                orderedEventTypes.includes(eventType) ? (
                  <EventPayloadVisualCard key={eventType} eventType={eventType} payload={snapshot.latestByType[eventType]} />
                ) : (
                  <div
                    key={eventType}
                    className="rounded-xl border border-dashed border-black/20 bg-white/40 p-3 text-xs uppercase tracking-[0.14em] text-black/50"
                  >
                    {eventType} unavailable
                  </div>
                ),
              )}
            </div>
          </section>
        </div>
      </section>

      {snapshot.errorMessage ? (
        <p className="rounded-xl border border-rose-300 bg-rose-50 px-3 py-2 text-xs text-rose-700">
          {snapshot.errorMessage}
        </p>
      ) : null}
    </main>
  );
}
