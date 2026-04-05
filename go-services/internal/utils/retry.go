package utils

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

type RetryConfig struct {
	Attempts     int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		Attempts:     4,
		InitialDelay: 200 * time.Millisecond,
		MaxDelay:     3 * time.Second,
		Multiplier:   2,
		Jitter:       50 * time.Millisecond,
	}
}

func Retry(ctx context.Context, cfg RetryConfig, fn func(attempt int) error) error {
	if cfg.Attempts <= 0 {
		return errors.New("retry attempts must be positive")
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 2 * time.Second
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = 1
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}

	delay := cfg.InitialDelay
	var lastErr error
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		if err := fn(attempt); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == cfg.Attempts {
			break
		}

		var j time.Duration
		if cfg.Jitter > 0 {
			j = time.Duration(rand.Int63n(int64(cfg.Jitter) + 1))
		}
		sleep := delay + j
		if sleep > cfg.MaxDelay {
			sleep = cfg.MaxDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		next := time.Duration(float64(delay) * cfg.Multiplier)
		if next > cfg.MaxDelay {
			next = cfg.MaxDelay
		}
		delay = next
	}
	return lastErr
}
