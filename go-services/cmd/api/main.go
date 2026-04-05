package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"polymarket-bot/go-services/internal/monitoring"
	"polymarket-bot/go-services/internal/runner"
)

func main() {
	logger := monitoring.New("api")
	svc := runner.NewService("paper")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status": "ok",
			"time":   time.Now().UTC(),
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		report, err := svc.RunOnce(r.Context())
		if err != nil {
			logger.Error("status run failed", err, nil)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, svc.Metrics())
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

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
