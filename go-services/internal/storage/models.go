package storage

import "time"

type TradeStatus string

const (
	TradeStatusPending   TradeStatus = "pending"
	TradeStatusSubmitted TradeStatus = "submitted"
	TradeStatusFilled    TradeStatus = "filled"
	TradeStatusCanceled  TradeStatus = "canceled"
	TradeStatusRejected  TradeStatus = "rejected"
	TradeStatusFailed    TradeStatus = "failed"
)

type TradeRecord struct {
	ID          string            `json:"id"`
	Pair        string            `json:"pair"`
	Venue       string            `json:"venue"`
	Side        string            `json:"side"`
	Quantity    float64           `json:"quantity"`
	Price       float64           `json:"price"`
	NotionalUSD float64           `json:"notional_usd"`
	Status      TradeStatus       `json:"status"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

func (t *TradeRecord) Touch(now time.Time) {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
}
