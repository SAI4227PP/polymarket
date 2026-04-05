package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"polymarket-bot/go-services/internal/monitoring"
	"polymarket-bot/go-services/internal/runner"
)

func main() {
	logger := monitoring.New("supervisor")
	svc := runner.NewService("paper")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("supervisor started", nil)
	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("supervisor stopped with error", err, nil)
		return
	}
	logger.Info("supervisor stopped cleanly", nil)
}
