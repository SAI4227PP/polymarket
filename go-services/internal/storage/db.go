package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBConfig struct {
	URL          string
	MaxOpenConns int
	MaxIdleConns int
}

type Client struct {
	cfg         DBConfig
	db          *sql.DB
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

	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Client{cfg: cfg, db: db, connected: true, connectedAt: time.Now().UTC()}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || !c.connected || c.db == nil {
		return errors.New("database client is not connected")
	}
	return c.db.PingContext(ctx)
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.connected = false
	if c.db != nil {
		return c.db.Close()
	}
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

func (c *Client) EnsurePortfolioDailyTable(ctx context.Context) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	const ddl = `
CREATE TABLE IF NOT EXISTS portfolio_daily_stats (
    day DATE PRIMARY KEY,
    start_total_pnl_usd DOUBLE PRECISION NOT NULL,
    current_total_pnl_usd DOUBLE PRECISION NOT NULL,
    day_pnl_usd DOUBLE PRECISION NOT NULL,
    day_max_up_usd DOUBLE PRECISION NOT NULL,
    day_max_drop_usd DOUBLE PRECISION NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
	_, err := c.db.ExecContext(ctx, ddl)
	return err
}

func (c *Client) UpsertPortfolioDaily(ctx context.Context, rec PortfolioDayRecord) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	const upsert = `
INSERT INTO portfolio_daily_stats (
    day,
    start_total_pnl_usd,
    current_total_pnl_usd,
    day_pnl_usd,
    day_max_up_usd,
    day_max_drop_usd,
    updated_at_ms,
    updated_at
) VALUES (
    $1::date, $2, $3, $4, $5, $6, $7, NOW()
)
ON CONFLICT (day) DO UPDATE SET
    start_total_pnl_usd = EXCLUDED.start_total_pnl_usd,
    current_total_pnl_usd = EXCLUDED.current_total_pnl_usd,
    day_pnl_usd = EXCLUDED.day_pnl_usd,
    day_max_up_usd = EXCLUDED.day_max_up_usd,
    day_max_drop_usd = EXCLUDED.day_max_drop_usd,
    updated_at_ms = EXCLUDED.updated_at_ms,
    updated_at = NOW();
`
	_, err := c.db.ExecContext(
		ctx,
		upsert,
		rec.Day,
		rec.StartTotalPnLUSD,
		rec.CurrentTotalPnLUSD,
		rec.DayPnLUSD,
		rec.DayMaxUpUSD,
		rec.DayMaxDropUSD,
		rec.UpdatedAtMs,
	)
	return err
}


func (c *Client) EnsureTradeRecordsTable(ctx context.Context) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	const ddl = `
CREATE TABLE IF NOT EXISTS trade_records (
    id TEXT PRIMARY KEY,
    pair TEXT NOT NULL,
    venue TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    notional_usd DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
`
	_, err := c.db.ExecContext(ctx, ddl)
	return err
}

func (c *Client) EnsureTradeSnapshotsTable(ctx context.Context) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	const ddl = `
CREATE TABLE IF NOT EXISTS trade_snapshots (
    snapshot_type TEXT PRIMARY KEY,
    payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at_ms BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
	_, err := c.db.ExecContext(ctx, ddl)
	return err
}

func (c *Client) UpsertTradeRecord(ctx context.Context, rec TradeRecord) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = now
	}
	if rec.Status == "" {
		rec.Status = TradeStatusPending
	}
	metadataJSON := []byte("{}")
	if rec.Metadata != nil {
		b, err := json.Marshal(rec.Metadata)
		if err != nil {
			return err
		}
		metadataJSON = b
	}

	const upsert = `
INSERT INTO trade_records (
    id, pair, venue, side, quantity, price, notional_usd, status, metadata, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11
)
ON CONFLICT (id) DO UPDATE SET
    pair = EXCLUDED.pair,
    venue = EXCLUDED.venue,
    side = EXCLUDED.side,
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price,
    notional_usd = EXCLUDED.notional_usd,
    status = EXCLUDED.status,
    metadata = EXCLUDED.metadata,
    updated_at = EXCLUDED.updated_at;
`

	_, err := c.db.ExecContext(
		ctx,
		upsert,
		rec.ID,
		rec.Pair,
		rec.Venue,
		rec.Side,
		rec.Quantity,
		rec.Price,
		rec.NotionalUSD,
		string(rec.Status),
		string(metadataJSON),
		rec.CreatedAt,
		rec.UpdatedAt,
	)
	return err
}
func (c *Client) UpsertTradeSnapshot(ctx context.Context, rec TradeSnapshotRecord) error {
	if c == nil || c.db == nil {
		return errors.New("database client is not connected")
	}
	if strings.TrimSpace(rec.SnapshotType) == "" {
		return errors.New("snapshot type is required")
	}
	if rec.UpdatedAtMs == 0 {
		rec.UpdatedAtMs = uint64(time.Now().UTC().UnixMilli())
	}
	payloadJSON := []byte("[]")
	if rec.Payload != nil {
		b, err := json.Marshal(rec.Payload)
		if err != nil {
			return err
		}
		payloadJSON = b
	}

	const upsert = `
INSERT INTO trade_snapshots (
    snapshot_type, payload, updated_at_ms, updated_at
) VALUES (
    $1, $2::jsonb, $3, NOW()
)
ON CONFLICT (snapshot_type) DO UPDATE SET
    payload = EXCLUDED.payload,
    updated_at_ms = EXCLUDED.updated_at_ms,
    updated_at = NOW();
`
	_, err := c.db.ExecContext(
		ctx,
		upsert,
		rec.SnapshotType,
		string(payloadJSON),
		rec.UpdatedAtMs,
	)
	return err
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
