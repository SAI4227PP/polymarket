export type ConnectionStatus = "connecting" | "open" | "closed" | "error";

export type JsonRecord = Record<string, unknown>;

export interface StreamEnvelope {
  type: string;
  timestamp: string;
  data: unknown;
}

export interface StreamEvent {
  type: string;
  timestamp: string;
  data: unknown;
}

export interface WebSocketSnapshot {
  connectionStatus: ConnectionStatus;
  lastUpdatedAt: number | null;
  reconnectAttempt: number;
  messageCount: number;
  errorMessage: string | null;
  latestMessage: JsonRecord | null;
  eventTypes: string[];
  latestByType: Record<string, unknown>;
  recentEvents: StreamEvent[];
}
