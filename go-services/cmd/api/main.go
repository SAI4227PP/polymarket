package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"polymarket-bot/go-services/internal/monitoring"
	"polymarket-bot/go-services/internal/runner"
	"polymarket-bot/go-services/internal/storage"
)

type traderState struct {
	TSMs            uint64  `json:"ts_ms"`
	PolymarketMid   float64 `json:"polymarket_mid"`
	BinanceMid      float64 `json:"binance_mid"`
	EdgeBps         float64 `json:"edge_bps"`
	NetEdgeBps      float64 `json:"net_edge_bps"`
	Direction       string  `json:"direction"`
	RiskAllowed     bool    `json:"risk_allowed"`
	Action          string  `json:"action"`
	ExecutionStatus string  `json:"execution_status"`
	OpenPositions   int     `json:"open_positions"`
	DayPnlUSD       float64 `json:"day_pnl_usd"`
}

type streamEnvelope struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      streamFrame `json:"data"`
}

type streamFrame struct {
	Snapshot  runner.Snapshot        `json:"snapshot"`
	Market    map[string]interface{} `json:"market"`
	Signal    map[string]interface{} `json:"signal"`
	Execution map[string]interface{} `json:"execution"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(_ *http.Request) bool {
		return true
	},
}

func main() {
	logger := monitoring.New("api")
	svc := runner.NewService("paper")

	redisURL := os.Getenv("REDIS_URL")
	if strings.TrimSpace(redisURL) == "" {
		redisURL = "redis://localhost:6379"
	}
	cache, err := storage.NewRedisCache(redisURL)
	if err != nil {
		logger.Error("failed to initialize redis cache", err, nil)
		return
	}
	defer cache.Close()

	snapshotKey := getenv("REDIS_SNAPSHOT_KEY", "api:snapshot:latest")
	statusKey := getenv("REDIS_STATUS_KEY", "api:status:latest")
	metricsKey := getenv("REDIS_METRICS_KEY", "api:metrics:latest")
	tradesKey := getenv("REDIS_TRADES_KEY", "api:trades:latest")
	configKey := getenv("REDIS_CONFIG_KEY", "api:config:latest")
	traderStateKey := getenv("REDIS_TRADER_STATE_KEY", "trader:state:latest")

	go refreshRedisCache(logger, svc, cache, snapshotKey, statusKey, metricsKey, tradesKey, configKey)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		redisOK := cache.Ping(ctx) == nil
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":   "ok",
			"time":     time.Now().UTC(),
			"redis_ok": redisOK,
		})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		var report runner.RunReport
		if err := readCachedJSONWithTimeout(r.Context(), cache, statusKey, &report, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var metrics []monitoring.Metric
		if err := readCachedJSONWithTimeout(r.Context(), cache, metricsKey, &metrics, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, metrics)
	})

	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		var cfg runner.Config
		if err := readCachedJSONWithTimeout(r.Context(), cache, configKey, &cfg, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, cfg)
	})

	mux.HandleFunc("/trades", func(w http.ResponseWriter, r *http.Request) {
		var trades []storage.TradeRecord
		if err := readCachedJSONWithTimeout(r.Context(), cache, tradesKey, &trades, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		statusFilter := storage.TradeStatus(strings.TrimSpace(r.URL.Query().Get("status")))
		limit := queryIntBounded(r, "limit", 50, 5000)
		trades = filterTrades(trades, statusFilter)
		if limit > 0 && len(trades) > limit {
			trades = trades[:limit]
		}
		writeJSON(w, http.StatusOK, trades)
	})

	snapshotHandler := func(w http.ResponseWriter, r *http.Request) {
		var snap runner.Snapshot
		if err := readCachedJSONWithTimeout(r.Context(), cache, snapshotKey, &snap, 1500*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, snap)
	}
	mux.HandleFunc("/snapshot", snapshotHandler)
	mux.HandleFunc("/data", snapshotHandler)

	mux.HandleFunc("/trader-state", func(w http.ResponseWriter, r *http.Request) {
		var state traderState
		if err := readCachedJSONWithTimeout(r.Context(), cache, traderStateKey, &state, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, state)
	})

	mux.HandleFunc("/ws/stream", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Error("websocket upgrade failed", err, nil)
			return
		}
		defer conn.Close()

		intervalMs := queryIntBounded(r, "interval_ms", 1000, 60000)
		ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			var snap runner.Snapshot
			snapErr := readCachedJSONWithTimeout(r.Context(), cache, snapshotKey, &snap, 1500*time.Millisecond)

			var state traderState
			stateErr := readCachedJSONWithTimeout(r.Context(), cache, traderStateKey, &state, 1500*time.Millisecond)

			if snapErr != nil {
				_ = conn.WriteJSON(streamEnvelope{
					Type:      "error",
					Timestamp: time.Now().UTC(),
					Data: streamFrame{Signal: map[string]interface{}{"message": snapErr.Error()}},
				})
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					continue
				}
			}

			lastTradeStatus := "none"
			if len(snap.Trades) > 0 {
				lastTradeStatus = string(snap.Trades[0].Status)
			}

			marketSection := map[string]interface{}{
				"polymarket_mid": nil,
				"binance_mid":    nil,
			}
			signalSection := map[string]interface{}{
				"action":       snap.Report.Action,
				"risk_allowed": snap.Report.RiskAllowed,
				"tick_seq":     snap.Report.TickSeq,
			}
			execSection := map[string]interface{}{
				"open_trades":         snap.Report.OpenTrades,
				"latest_trade_status": lastTradeStatus,
			}

			if stateErr == nil {
				marketSection["polymarket_mid"] = state.PolymarketMid
				marketSection["binance_mid"] = state.BinanceMid
				marketSection["ts_ms"] = state.TSMs

				signalSection["edge_bps"] = state.EdgeBps
				signalSection["net_edge_bps"] = state.NetEdgeBps
				signalSection["direction"] = state.Direction
				signalSection["risk_allowed"] = state.RiskAllowed

				execSection["execution_status"] = state.ExecutionStatus
				execSection["open_positions"] = state.OpenPositions
				execSection["day_pnl_usd"] = state.DayPnlUSD
			} else {
				marketSection["warning"] = "trader state unavailable in redis"
				signalSection["warning"] = stateErr.Error()
			}

			env := streamEnvelope{
				Type:      "snapshot",
				Timestamp: time.Now().UTC(),
				Data: streamFrame{
					Snapshot:  snap,
					Market:    marketSection,
					Signal:    signalSection,
					Execution: execSection,
				},
			}

			if err := conn.WriteJSON(env); err != nil {
				logger.Warn("websocket client disconnected", map[string]interface{}{"error": err.Error()})
				return
			}

			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		logger.Info("api server starting", map[string]interface{}{"addr": server.Addr})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("api server failed", err, nil)
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	logger.Info("api server stopped", nil)
}

func refreshRedisCache(
	logger monitoring.Logger,
	svc *runner.Service,
	cache *storage.RedisCache,
	snapshotKey, statusKey, metricsKey, tradesKey, configKey string,
) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		snap, err := svc.Snapshot(ctx, 1000, "")
		cancel()
		if err != nil {
			logger.Error("snapshot refresh failed", err, nil)
			<-ticker.C
			continue
		}

		ctxw, cancelw := context.WithTimeout(context.Background(), 2*time.Second)
		if err := cache.SetJSON(ctxw, snapshotKey, snap, 30*time.Second); err != nil {
			logger.Error("redis set snapshot failed", err, nil)
		}
		if err := cache.SetJSON(ctxw, statusKey, snap.Report, 30*time.Second); err != nil {
			logger.Error("redis set status failed", err, nil)
		}
		if err := cache.SetJSON(ctxw, metricsKey, snap.Metrics, 30*time.Second); err != nil {
			logger.Error("redis set metrics failed", err, nil)
		}
		if err := cache.SetJSON(ctxw, tradesKey, snap.Trades, 30*time.Second); err != nil {
			logger.Error("redis set trades failed", err, nil)
		}
		if err := cache.SetJSON(ctxw, configKey, snap.Config, 30*time.Second); err != nil {
			logger.Error("redis set config failed", err, nil)
		}
		cancelw()

		<-ticker.C
	}
}

func filterTrades(trades []storage.TradeRecord, status storage.TradeStatus) []storage.TradeRecord {
	if status == "" {
		return trades
	}
	out := make([]storage.TradeRecord, 0, len(trades))
	for _, t := range trades {
		if t.Status == status {
			out = append(out, t)
		}
	}
	return out
}

func readCachedJSONWithTimeout(
	ctx context.Context,
	cache *storage.RedisCache,
	key string,
	dst interface{},
	timeout time.Duration,
) error {
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := cache.GetJSON(c, key, dst)
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return errors.New("data not available in redis yet")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("redis read timeout")
	}
	return err
}

func getenv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func queryIntBounded(r *http.Request, key string, fallback int, max int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	if n > max {
		return max
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
