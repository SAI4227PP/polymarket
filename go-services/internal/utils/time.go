package utils

import (
	"os"
	"strconv"
	"time"
)

func NowUTC() time.Time {
	return time.Now().UTC()
}

func UnixMilliNow() int64 {
	return NowUTC().UnixMilli()
}

func EnvDurationOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func EnvIntOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
