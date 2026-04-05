package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"polymarket-bot/go-services/internal/runner"
)

func main() {
	svc := runner.NewService("live")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	report, err := svc.RunOnce(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "live-run failed: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}
