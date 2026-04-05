package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type DBConfig struct {
	URL          string
	MaxOpenConns int
	MaxIdleConns int
}

type Client struct {
	cfg         DBConfig
	connected   bool
	connectedAt time.Time
}

func Connect(ctx context.Context, cfg DBConfig) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("database url is required")
	}
	if !strings.HasPrefix(cfg.URL, "postgres://") {
		return nil, fmt.Errorf("unsupported database url: %s", cfg.URL)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return &Client{cfg: cfg, connected: true, connectedAt: time.Now().UTC()}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || !c.connected {
		return errors.New("database client is not connected")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.connected = false
	return nil
}

func (c *Client) Status() map[string]interface{} {
	if c == nil {
		return map[string]interface{}{"connected": false}
	}
	return map[string]interface{}{
		"connected":    c.connected,
		"connected_at": c.connectedAt.Format(time.RFC3339Nano),
		"dsn":          RedactDSN(c.cfg.URL),
	}
}

func RedactDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}
	at := strings.Index(dsn, "@")
	proto := strings.Index(dsn, "://")
	if at == -1 || proto == -1 || at <= proto+3 {
		return dsn
	}
	return dsn[:proto+3] + "***:***" + dsn[at:]
}
