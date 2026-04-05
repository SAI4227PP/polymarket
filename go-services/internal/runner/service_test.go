package runner

import (
	"context"
	"testing"
	"time"
)

func TestRunOnceProducesReport(t *testing.T) {
	svc := NewService("paper")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	report, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if report.Pair != "BTC" {
		t.Fatalf("expected pair BTC, got %s", report.Pair)
	}
}
