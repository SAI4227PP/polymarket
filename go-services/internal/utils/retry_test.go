package utils

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrySucceedsAfterTransientErrors(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), RetryConfig{
		Attempts:     4,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     2 * time.Millisecond,
		Multiplier:   1,
		Jitter:       0,
	}, func(_ int) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryHandlesNegativeJitter(t *testing.T) {
	err := Retry(context.Background(), RetryConfig{
		Attempts:     1,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     1 * time.Millisecond,
		Multiplier:   1,
		Jitter:       -1 * time.Millisecond,
	}, func(_ int) error { return nil })
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}
