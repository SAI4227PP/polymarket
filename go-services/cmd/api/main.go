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
	PolymarketProb  float64 `json:"polymarket_probability"`
	FairValueProb   float64 `json:"fair_value_probability"`
	BinanceMid      float64 `json:"binance_mid"`
	EdgeBps         float64 `json:"edge_bps"`
	NetEdgeBps      float64 `json:"net_edge_bps"`
	Direction       string  `json:"direction"`
	RiskAllowed     bool    `json:"risk_allowed"`
	Action          string  `json:"action"`
	PositionSignal  string  `json:"position_signal"`
	EntrySignal     bool    `json:"entry_signal"`
	ExitSignal      bool    `json:"exit_signal"`
	CloseReason     string  `json:"close_reason"`
	ExecutionStatus string  `json:"execution_status"`
	OpenPositions   int     `json:"open_positions"`
	DayPnlUSD       float64 `json:"day_pnl_usd"`
	OldestQuoteAgeMs uint64 `json:"oldest_quote_age_ms"`
	PortfolioNetQty        float64 `json:"portfolio_net_qty"`
	PortfolioAvgEntry      float64 `json:"portfolio_avg_entry"`
    PortfolioRealizedPnL        float64 `json:"portfolio_realized_pnl_usd"`
	PortfolioUnrealizedPnL float64 `json:"portfolio_unrealized_pnl_usd"`
	PortfolioTotalPnL      float64 `json:"portfolio_total_pnl_usd"`
    PortfolioAllTimeRealizedPnL float64 `json:"portfolio_all_time_realized_pnl_usd"`
	PortfolioDrawdownPctFromPeak float64 `json:"portfolio_drawdown_pct_from_peak"`
}

type completedTrade struct {
	ID               string  `json:"id"`
	Pair             string  `json:"pair"`
	Direction        string  `json:"direction"`
	EntrySide        string  `json:"entry_side"`
	ExitSide         string  `json:"exit_side"`
	Quantity         float64 `json:"quantity"`
	EntryPrice       float64 `json:"entry_price"`
	ExitPrice        float64 `json:"exit_price"`
	EntryProbability float64 `json:"entry_probability"`
	ExitProbability  float64 `json:"exit_probability"`
	EntryNotionalUSD float64 `json:"entry_notional_usd"`
	ExitNotionalUSD  float64 `json:"exit_notional_usd"`
	PnlUSD           float64 `json:"pnl_usd"`
	EntryTSMs        uint64  `json:"entry_ts_ms"`
	ExitTSMs         uint64  `json:"exit_ts_ms"`
	DurationMs       uint64  `json:"duration_ms"`
	Outcome          string  `json:"outcome"`
}
type streamEnvelope struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

type portfolioDayStats struct {
	Day              string    `json:"day"`
	StartTotalPnLUSD float64   `json:"start_total_pnl_usd"`
	CurrentTotalPnL  float64   `json:"current_total_pnl_usd"`
	DayPnLUSD        float64   `json:"day_pnl_usd"`
	DayMaxUpUSD      float64   `json:"day_max_up_usd"`
	DayMaxDropUSD    float64   `json:"day_max_drop_usd"`
    UpdatedAtMs      uint64    `json:"updated_at_ms"`
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
	runnerTradesKey := getenv("REDIS_TRADES_KEY", "api:trades:latest")
	openTradesKey := getenv("REDIS_OPEN_TRADES_KEY", "trader:open_trades:latest")
	configKey := getenv("REDIS_CONFIG_KEY", "api:config:latest")
	traderStateKey := getenv("REDIS_TRADER_STATE_KEY", "trader:state:latest")
	finalTradesKey := getenv("REDIS_FINAL_TRADES_KEY", "trader:trades:latest")
	liveBTCKey := getenv("REDIS_LIVE_BTC_KEY", "trader:live_btc:latest")
	pastOutcomesKey := getenv("REDIS_PAST_OUTCOMES_KEY", "trader:past_outcomes:latest")
	portfolioDailyKey := getenv("REDIS_PORTFOLIO_DAILY_KEY", "trader:portfolio:daily")

    postgresURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
    if postgresURL == "" {
        postgresURL = strings.TrimSpace(os.Getenv("POSTGRES_URL"))
    }

    var pgClient *storage.Client
    if postgresURL != "" {
        dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
        pg, dbErr := storage.Connect(dbCtx, storage.DBConfig{URL: postgresURL, MaxOpenConns: 5, MaxIdleConns: 2})
        dbCancel()
        if dbErr != nil {
            logger.Warn("postgres connect failed; continuing without postgres sync", map[string]interface{}{"error": dbErr.Error()})
        } else {
            ddlCtx, ddlCancel := context.WithTimeout(context.Background(), 5*time.Second)
            if err := pg.EnsurePortfolioDailyTable(ddlCtx); err != nil {
                logger.Warn("postgres portfolio_daily_stats schema init failed; continuing without postgres sync", map[string]interface{}{"error": err.Error()})
                _ = pg.Close()
            } else if err := pg.EnsureTradeRecordsTable(ddlCtx); err != nil {
                logger.Warn("postgres trade_records schema init failed; continuing without postgres sync", map[string]interface{}{"error": err.Error()})
                _ = pg.Close()
            } else if err := pg.EnsureTradeSnapshotsTable(ddlCtx); err != nil {
                logger.Warn("postgres trade_snapshots schema init failed; continuing without postgres sync", map[string]interface{}{"error": err.Error()})
                _ = pg.Close()
            } else {
                pgClient = pg
            }
            ddlCancel()
        }
    }
    if pgClient != nil {
        defer pgClient.Close()
    }

	go refreshRedisCache(logger, svc, cache, snapshotKey, statusKey, metricsKey, runnerTradesKey, configKey)
    if pgClient != nil {
        go syncPortfolioDailyToPostgres(logger, cache, pgClient, portfolioDailyKey)
        go syncTradeRecordsToPostgres(logger, cache, pgClient, finalTradesKey)
        go syncTradeSnapshotsToPostgres(logger, cache, pgClient, openTradesKey, finalTradesKey)
    }

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
		limit := queryIntBounded(r, "limit", 50, 5000)

		var openTrades []map[string]interface{}
		if err := readCachedJSONWithTimeout(r.Context(), cache, openTradesKey, &openTrades, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		if limit > 0 && len(openTrades) > limit {
			openTrades = openTrades[:limit]
		}
		writeJSON(w, http.StatusOK, openTrades)
	})

	mux.HandleFunc("/trades/completed", func(w http.ResponseWriter, r *http.Request) {
		limit := queryIntBounded(r, "limit", 50, 5000)

		var trades []completedTrade
		if err := readCachedJSONWithTimeout(r.Context(), cache, finalTradesKey, &trades, 1200*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
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

	
    mux.HandleFunc("/portfolio/daily", func(w http.ResponseWriter, r *http.Request) {
        history := make(map[string]portfolioDayStats)
        if err := readCachedJSONWithTimeout(r.Context(), cache, portfolioDailyKey, &history, 1200*time.Millisecond); err != nil {
            writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
            return
        }
        writeJSON(w, http.StatusOK, history)
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
		initialBootstrapSent := false
		lastPortfolioSig := ""
		var lastState *traderState
		var lastDailyHistory map[string]portfolioDayStats

		for {
			var snap runner.Snapshot
			snapErr := readCachedJSONWithTimeout(r.Context(), cache, snapshotKey, &snap, 1500*time.Millisecond)

			var state traderState
			stateErr := readCachedJSONWithTimeout(r.Context(), cache, traderStateKey, &state, 1500*time.Millisecond)
            var runningTrades []map[string]interface{}
            runningTradesErr := readCachedJSONWithTimeout(r.Context(), cache, openTradesKey, &runningTrades, 1200*time.Millisecond)
            runningTradesPayload := interface{}(runningTrades)
            if runningTradesErr != nil {
                runningTradesPayload = interface{}([]map[string]interface{}{})
            }
            var finalTrades []completedTrade
            finalTradesErr := readCachedJSONWithTimeout(r.Context(), cache, finalTradesKey, &finalTrades, 1200*time.Millisecond)
            completedTradesPayload := interface{}(finalTrades)
            if finalTradesErr != nil {
                completedTradesPayload = interface{}([]completedTrade{})
            }

            var liveBTCPayload interface{}
            if err := readCachedJSONWithTimeout(r.Context(), cache, liveBTCKey, &liveBTCPayload, 1200*time.Millisecond); err != nil {
                liveBTCPayload = map[string]interface{}{"warning": err.Error()}
            }

            var pastOutcomesPayload interface{}
            if err := readCachedJSONWithTimeout(r.Context(), cache, pastOutcomesKey, &pastOutcomesPayload, 1200*time.Millisecond); err != nil {
                pastOutcomesPayload = map[string]interface{}{"warning": err.Error()}
            }

            if snapErr != nil {
                _ = conn.WriteJSON(streamEnvelope{
                    Type:      "error",
                    Timestamp: time.Now().UTC(),
                    Data:      map[string]interface{}{"message": snapErr.Error()},
                })
                select {
                case <-r.Context().Done():
                    return
                case <-ticker.C:
                    continue
                }
            }

			lastTradeStatus := "none"

            marketSection := map[string]interface{}{
                "polymarket_mid": nil,
                "binance_mid":    nil,
            }
            signalSection := map[string]interface{}{
                "action":          snap.Report.Action,
                "risk_allowed":    snap.Report.RiskAllowed,
                "tick_seq":        snap.Report.TickSeq,
                "position_signal": "unknown",
                "entry_signal":    false,
                "exit_signal":     false,
                "close_reason":    "none",
            }
            execSection := map[string]interface{}{
                "open_trades":         snap.Report.OpenTrades,
                "open_trades_runner":  snap.Report.OpenTrades,
                "latest_trade_status": lastTradeStatus,
            }

            portfolioSection := map[string]interface{}{
                "net_qty":                   0.0,
                "avg_entry":                 0.0,
                "unrealized_pnl_usd":        0.0,
                "realized_pnl_usd":          0.0,
                "total_pnl_usd":             0.0,
                "day":                       time.Now().UTC().Format("2006-01-02"),
                "day_pnl_usd":               0.0,
                "day_max_up_usd":            0.0,
                "day_max_drop_usd":          0.0,
                "all_time_realized_pnl_usd": 0.0,
            }

			if stateErr == nil {
				lastState = &state
                marketSection["polymarket_mid"] = state.PolymarketMid
                marketSection["polymarket_probability"] = state.PolymarketProb
                marketSection["fair_value_probability"] = state.FairValueProb
                marketSection["binance_mid"] = state.BinanceMid
                marketSection["ts_ms"] = state.TSMs
				marketSection["quote_age_ms"] = state.OldestQuoteAgeMs

                signalSection["edge_bps"] = state.EdgeBps
                signalSection["net_edge_bps"] = state.NetEdgeBps
                signalSection["direction"] = state.Direction
                signalSection["risk_allowed"] = state.RiskAllowed
                signalSection["position_signal"] = state.PositionSignal
                signalSection["entry_signal"] = state.EntrySignal
                signalSection["exit_signal"] = state.ExitSignal
                signalSection["close_reason"] = state.CloseReason

                execSection["execution_status"] = state.ExecutionStatus
                execSection["open_positions"] = state.OpenPositions
                execSection["open_trades"] = state.OpenPositions
				if state.OpenPositions > 0 {
					lastTradeStatus = "open"
				} else {
					lastTradeStatus = "closed"
				}
				execSection["latest_trade_status"] = lastTradeStatus
                portfolioSection["net_qty"] = state.PortfolioNetQty
                portfolioSection["avg_entry"] = state.PortfolioAvgEntry
                portfolioSection["unrealized_pnl_usd"] = state.PortfolioUnrealizedPnL
                portfolioSection["realized_pnl_usd"] = state.PortfolioRealizedPnL
                portfolioSection["total_pnl_usd"] = state.PortfolioTotalPnL
                portfolioSection["all_time_realized_pnl_usd"] = state.PortfolioAllTimeRealizedPnL
				portfolioSection["drawdown_pct_from_peak"] = state.PortfolioDrawdownPctFromPeak
				portfolioSection["source"] = "redis_live"
			} else if lastState != nil {
				marketSection["polymarket_mid"] = lastState.PolymarketMid
				marketSection["polymarket_probability"] = lastState.PolymarketProb
				marketSection["fair_value_probability"] = lastState.FairValueProb
				marketSection["binance_mid"] = lastState.BinanceMid
				marketSection["ts_ms"] = lastState.TSMs
				marketSection["quote_age_ms"] = lastState.OldestQuoteAgeMs

				signalSection["edge_bps"] = lastState.EdgeBps
				signalSection["net_edge_bps"] = lastState.NetEdgeBps
				signalSection["direction"] = lastState.Direction
				signalSection["risk_allowed"] = lastState.RiskAllowed
				signalSection["position_signal"] = lastState.PositionSignal
				signalSection["entry_signal"] = lastState.EntrySignal
				signalSection["exit_signal"] = lastState.ExitSignal
				signalSection["close_reason"] = lastState.CloseReason

				execSection["execution_status"] = lastState.ExecutionStatus
				execSection["open_positions"] = lastState.OpenPositions
				execSection["open_trades"] = lastState.OpenPositions
				if lastState.OpenPositions > 0 {
					lastTradeStatus = "open"
				} else {
					lastTradeStatus = "closed"
				}
				execSection["latest_trade_status"] = lastTradeStatus

				portfolioSection["net_qty"] = lastState.PortfolioNetQty
				portfolioSection["avg_entry"] = lastState.PortfolioAvgEntry
				portfolioSection["unrealized_pnl_usd"] = lastState.PortfolioUnrealizedPnL
				portfolioSection["realized_pnl_usd"] = lastState.PortfolioRealizedPnL
				portfolioSection["total_pnl_usd"] = lastState.PortfolioTotalPnL
				portfolioSection["all_time_realized_pnl_usd"] = lastState.PortfolioAllTimeRealizedPnL
				portfolioSection["drawdown_pct_from_peak"] = lastState.PortfolioDrawdownPctFromPeak
				portfolioSection["source"] = "redis_cached"
            } else {
                marketSection["warning"] = "trader state unavailable in redis"
                signalSection["warning"] = stateErr.Error()
            }

            history := make(map[string]portfolioDayStats)
            if err := readCachedJSONWithTimeout(r.Context(), cache, portfolioDailyKey, &history, 1200*time.Millisecond); err != nil {
                if lastDailyHistory != nil {
                    portfolioSection["daily_history"] = lastDailyHistory
                } else {
                    portfolioSection["daily_history"] = map[string]portfolioDayStats{}
                }
            } else {
				lastDailyHistory = history
                portfolioSection["daily_history"] = history
                todayKey := time.Now().UTC().Format("2006-01-02")
                if todayStats, ok := history[todayKey]; ok {
                    portfolioSection["day"] = todayStats.Day
                    portfolioSection["day_pnl_usd"] = todayStats.DayPnLUSD
                    portfolioSection["day_max_up_usd"] = todayStats.DayMaxUpUSD
                    portfolioSection["day_max_drop_usd"] = todayStats.DayMaxDropUSD
                    portfolioSection["today_stats"] = todayStats
                }
            }
			execSection["day_pnl_usd"] = portfolioSection["day_pnl_usd"]
			portfolioSig := payloadSignature(portfolioSection)
			snapshotData := map[string]interface{}{
				"generated_at":     snap.GeneratedAt,
				"config":           snap.Config,
				"report":           snap.Report,
				"metrics":          snap.Metrics,
				"market":           marketSection,
				"signal":           signalSection,
				"execution":        execSection,
				"portfolio":        portfolioSection,
				"trades":           runningTradesPayload,
				"completed_trades": completedTradesPayload,
				"live_btc":         liveBTCPayload,
				"past_outcomes":    pastOutcomesPayload,
			}
			now := time.Now().UTC()
			events := make([]streamEnvelope, 0, 10)
			if !initialBootstrapSent {
				events = append(events,
					streamEnvelope{
						Type:      "snapshot",
						Timestamp: now,
						Data:      snapshotData,
					},
					streamEnvelope{
						Type:      "portfolio",
						Timestamp: now,
						Data:      portfolioSection,
                    },
				)
			}
			events = append(events,
				streamEnvelope{
					Type:      "market",
					Timestamp: now,
					Data:      marketSection,
				},
				streamEnvelope{
					Type:      "signal",
					Timestamp: now,
					Data:      signalSection,
				},
				streamEnvelope{
					Type:      "execution",
					Timestamp: now,
					Data:      execSection,
				},
                streamEnvelope{
                    Type:      "live_btc",
                    Timestamp: now,
                    Data:      liveBTCPayload,
                },
                streamEnvelope{
                    Type:      "past_outcomes",
                    Timestamp: now,
                    Data:      pastOutcomesPayload,
                },
				streamEnvelope{
					Type:      "trades",
					Timestamp: now,
					Data:      runningTradesPayload,
				},
				streamEnvelope{
					Type:      "completed_trades",
					Timestamp: now,
					Data:      completedTradesPayload,
				},
			)
            if initialBootstrapSent && portfolioSig != lastPortfolioSig {
                events = append(events, streamEnvelope{
                    Type:      "portfolio",
                    Timestamp: now,
                    Data:      portfolioSection,
                })
            }
			for _, env := range events {
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := conn.WriteJSON(env); err != nil {
					logger.Warn("websocket client disconnected", map[string]interface{}{"error": err.Error(), "event_type": env.Type})
					return
				}
			}
			initialBootstrapSent = true
            lastPortfolioSig = portfolioSig
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


func syncPortfolioDailyToPostgres(
	logger monitoring.Logger,
	cache *storage.RedisCache,
	db *storage.Client,
	portfolioDailyKey string,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		history := make(map[string]portfolioDayStats)
		if err := cache.GetJSON(ctx, portfolioDailyKey, &history); err != nil {
			cancel()
			<-ticker.C
			continue
		}

		for day, s := range history {
			updatedMs := s.UpdatedAtMs
			if updatedMs == 0 {
				updatedMs = uint64(time.Now().UTC().UnixMilli())
			}
			rec := storage.PortfolioDayRecord{
				Day:                day,
				StartTotalPnLUSD:   s.StartTotalPnLUSD,
				CurrentTotalPnLUSD: s.CurrentTotalPnL,
				DayPnLUSD:          s.DayPnLUSD,
				DayMaxUpUSD:        s.DayMaxUpUSD,
				DayMaxDropUSD:      s.DayMaxDropUSD,
				UpdatedAtMs:        updatedMs,
			}
			if err := db.UpsertPortfolioDaily(ctx, rec); err != nil {
				logger.Warn("postgres upsert portfolio daily failed", map[string]interface{}{"error": err.Error(), "day": day})
			}
		}

		cancel()
		<-ticker.C
	}
}
func syncTradeRecordsToPostgres(
	logger monitoring.Logger,
	cache *storage.RedisCache,
	db *storage.Client,
	completedTradesKey string,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		completedTrades := make([]completedTrade, 0)
		if err := cache.GetJSON(ctx, completedTradesKey, &completedTrades); err != nil {
			cancel()
			<-ticker.C
			continue
		}

		for _, t := range completedTrades {
			if strings.TrimSpace(t.ID) == "" {
				continue
			}

			createdAt := time.UnixMilli(int64(t.EntryTSMs)).UTC()
			updatedAt := time.UnixMilli(int64(t.ExitTSMs)).UTC()
			if t.EntryTSMs == 0 {
				createdAt = time.Now().UTC()
			}
			if t.ExitTSMs == 0 {
				updatedAt = createdAt
			}

			rec := storage.TradeRecord{
				ID:          t.ID,
				Pair:        t.Pair,
				Venue:       "polymarket",
				Side:        t.ExitSide,
				Quantity:    t.Quantity,
				Price:       t.ExitPrice,
				NotionalUSD: t.ExitNotionalUSD,
				Status:      storage.TradeStatusFilled,
				Metadata: map[string]string{
					"direction":          t.Direction,
					"entry_side":         t.EntrySide,
					"exit_side":          t.ExitSide,
					"entry_price":        strconv.FormatFloat(t.EntryPrice, 'f', -1, 64),
					"exit_price":         strconv.FormatFloat(t.ExitPrice, 'f', -1, 64),
					"entry_probability":  strconv.FormatFloat(t.EntryProbability, 'f', -1, 64),
					"exit_probability":   strconv.FormatFloat(t.ExitProbability, 'f', -1, 64),
					"entry_notional_usd": strconv.FormatFloat(t.EntryNotionalUSD, 'f', -1, 64),
					"exit_notional_usd":  strconv.FormatFloat(t.ExitNotionalUSD, 'f', -1, 64),
					"pnl_usd":            strconv.FormatFloat(t.PnlUSD, 'f', -1, 64),
					"entry_ts_ms":        strconv.FormatUint(t.EntryTSMs, 10),
					"exit_ts_ms":         strconv.FormatUint(t.ExitTSMs, 10),
					"duration_ms":        strconv.FormatUint(t.DurationMs, 10),
					"outcome":            t.Outcome,
				},
				CreatedAt: createdAt,
				UpdatedAt: updatedAt,
			}

			if err := db.UpsertTradeRecord(ctx, rec); err != nil {
				logger.Warn("postgres upsert completed trade failed", map[string]interface{}{"error": err.Error(), "trade_id": t.ID})
			}
		}

		cancel()
		<-ticker.C
	}
}
func syncTradeSnapshotsToPostgres(
	logger monitoring.Logger,
	cache *storage.RedisCache,
	db *storage.Client,
	runningTradesKey string,
	completedTradesKey string,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		runningTrades := make([]map[string]interface{}, 0)
		if err := cache.GetJSON(ctx, runningTradesKey, &runningTrades); err != nil && !errors.Is(err, redis.Nil) {
			logger.Warn("postgres sync running trades snapshot read failed", map[string]interface{}{"error": err.Error()})
		}
		if err := db.UpsertTradeSnapshot(ctx, storage.TradeSnapshotRecord{
			SnapshotType: "running_trades",
			Payload:      runningTrades,
			UpdatedAtMs:  uint64(time.Now().UTC().UnixMilli()),
		}); err != nil {
			logger.Warn("postgres upsert running trades snapshot failed", map[string]interface{}{"error": err.Error()})
		}

		completedTrades := make([]completedTrade, 0)
		if err := cache.GetJSON(ctx, completedTradesKey, &completedTrades); err != nil && !errors.Is(err, redis.Nil) {
			logger.Warn("postgres sync completed trades snapshot read failed", map[string]interface{}{"error": err.Error()})
		}
		nowMs := uint64(time.Now().UTC().UnixMilli())
		if err := db.UpsertTradeSnapshot(ctx, storage.TradeSnapshotRecord{
			SnapshotType: "completed_trades",
			Payload:      completedTrades,
			UpdatedAtMs:  nowMs,
		}); err != nil {
			logger.Warn("postgres upsert completed trades snapshot failed", map[string]interface{}{"error": err.Error()})
		}

		var latestCompletePayload interface{}
		if len(completedTrades) > 0 {
			latestCompletePayload = completedTrades[len(completedTrades)-1]
		}
		tradeCompleteEnvelope := streamEnvelope{
			Type:      "trade_complete",
			Timestamp: time.Now().UTC(),
			Data:      latestCompletePayload,
		}
		if err := db.UpsertTradeSnapshot(ctx, storage.TradeSnapshotRecord{
			SnapshotType: "trade_complete",
			Payload:      tradeCompleteEnvelope,
			UpdatedAtMs:  nowMs,
		}); err != nil {
			logger.Warn("postgres upsert trade_complete snapshot failed", map[string]interface{}{"error": err.Error()})
		}

		cancel()
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
func payloadSignature(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}


