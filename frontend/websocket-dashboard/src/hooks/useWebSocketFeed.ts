"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { appConfig } from "@/config/app";
import type { ConnectionStatus, JsonRecord, StreamEnvelope, StreamEvent, WebSocketSnapshot } from "@/types/ws";

function parseJsonMessage(raw: string): JsonRecord | null {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as JsonRecord;
    }
    return { value: parsed };
  } catch {
    return { raw };
  }
}

function toEnvelope(payload: JsonRecord | null): StreamEnvelope {
  if (!payload) {
    return {
      type: "unknown",
      timestamp: new Date().toISOString(),
      data: null,
    };
  }

  const type = typeof payload.type === "string" ? payload.type : "unknown";
  const timestamp = typeof payload.timestamp === "string" ? payload.timestamp : new Date().toISOString();
  const data = Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;

  return { type, timestamp, data };
}

export function useWebSocketFeed() {
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>("connecting");
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [messageCount, setMessageCount] = useState(0);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [latestMessage, setLatestMessage] = useState<JsonRecord | null>(null);
  const [eventTypes, setEventTypes] = useState<string[]>([]);
  const [latestByType, setLatestByType] = useState<Record<string, unknown>>({});
  const [recentEvents, setRecentEvents] = useState<StreamEvent[]>([]);

  const reconnectAttemptRef = useRef(0);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let disposed = false;

    const connect = () => {
      if (disposed) return;

      setConnectionStatus("connecting");
      ws = new WebSocket(appConfig.wsUrl);

      ws.onopen = () => {
        reconnectAttemptRef.current = 0;
        setReconnectAttempt(0);
        setConnectionStatus("open");
        setErrorMessage(null);
      };

      ws.onmessage = (event) => {
        const payload = parseJsonMessage(event.data);
        const envelope = toEnvelope(payload);

        setLatestMessage(payload);
        setLastUpdatedAt(Date.now());
        setMessageCount((count) => count + 1);

        setEventTypes((prev) => {
          if (prev.includes(envelope.type)) return prev;
          return [...prev, envelope.type];
        });

        setLatestByType((prev) => ({
          ...prev,
          [envelope.type]: envelope.data,
        }));

        setRecentEvents((prev) => {
          const next = [
            {
              type: envelope.type,
              timestamp: envelope.timestamp,
              data: envelope.data,
            },
            ...prev,
          ];
          return next.slice(0, appConfig.maxRecentMessages);
        });
      };

      ws.onerror = () => {
        setConnectionStatus("error");
        setErrorMessage("WebSocket error detected");
      };

      ws.onclose = () => {
        if (disposed) return;
        setConnectionStatus("closed");
        reconnectAttemptRef.current += 1;
        setReconnectAttempt(reconnectAttemptRef.current);

        reconnectTimer = setTimeout(connect, appConfig.reconnectDelayMs);
      };
    };

    connect();

    return () => {
      disposed = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (ws) ws.close();
    };
  }, []);

  const snapshot = useMemo<WebSocketSnapshot>(
    () => ({
      connectionStatus,
      lastUpdatedAt,
      reconnectAttempt,
      messageCount,
      errorMessage,
      latestMessage,
      eventTypes,
      latestByType,
      recentEvents,
    }),
    [
      connectionStatus,
      errorMessage,
      eventTypes,
      lastUpdatedAt,
      latestByType,
      latestMessage,
      messageCount,
      reconnectAttempt,
      recentEvents,
    ],
  );

  return snapshot;
}
