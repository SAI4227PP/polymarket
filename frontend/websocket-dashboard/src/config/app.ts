export const appConfig = {
  appName: "Polymarket Streamboard",
  wsUrl:
    process.env.NEXT_PUBLIC_WS_URL ?? "wss://polymarket.trackeatfit.me/ws/stream",
  reconnectDelayMs: 3000,
  maxRecentMessages: 30,
};
