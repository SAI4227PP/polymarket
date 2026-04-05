package storage

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type TradeRepository interface {
	SaveTrade(TradeRecord) error
	GetTrade(id string) (TradeRecord, bool)
	ListTradesByStatus(status TradeStatus, limit int) []TradeRecord
	UpdateTradeStatus(id string, status TradeStatus) error
}

type InMemoryTradeRepository struct {
	mu     sync.RWMutex
	trades map[string]TradeRecord
}

func NewInMemoryTradeRepository() *InMemoryTradeRepository {
	return &InMemoryTradeRepository{trades: map[string]TradeRecord{}}
}

func (r *InMemoryTradeRepository) SaveTrade(t TradeRecord) error {
	if t.ID == "" {
		return errors.New("trade id is required")
	}
	now := time.Now().UTC()
	t.Touch(now)
	if t.Status == "" {
		t.Status = TradeStatusPending
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.trades[t.ID] = t
	return nil
}

func (r *InMemoryTradeRepository) GetTrade(id string) (TradeRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.trades[id]
	return t, ok
}

func (r *InMemoryTradeRepository) ListTradesByStatus(status TradeStatus, limit int) []TradeRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TradeRecord, 0)
	for _, t := range r.trades {
		if status == "" || t.Status == status {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func (r *InMemoryTradeRepository) UpdateTradeStatus(id string, status TradeStatus) error {
	if id == "" {
		return errors.New("trade id is required")
	}
	if status == "" {
		return errors.New("trade status is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.trades[id]
	if !ok {
		return errors.New("trade not found")
	}
	t.Status = status
	t.Touch(time.Now().UTC())
	r.trades[id] = t
	return nil
}
