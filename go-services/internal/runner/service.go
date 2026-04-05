package runner

import (
	"context"
	"fmt"
	"time"

	"polymarket-bot/go-services/internal/monitoring"
	"polymarket-bot/go-services/internal/storage"
)

type Mode string

const (
	ModePaper Mode = "paper"
	ModeLive  Mode = "live"
)

type Config struct {
	Mode          Mode
	Pair          string
	Interval      time.Duration
	MaxOpenTrades int
}

type RunReport struct {
	Mode        Mode      `json:"mode"`
	Pair        string    `json:"pair"`
	TickSeq     int64     `json:"tick_seq"`
	TickAt      time.Time `json:"tick_at"`
	Action      string    `json:"action"`
	OpenTrades  int       `json:"open_trades"`
	RiskAllowed bool      `json:"risk_allowed"`
}

type Service struct {
	cfg       Config
	scheduler *Scheduler
	repo      storage.TradeRepository
	logger    monitoring.Logger
	metrics   *monitoring.Registry
	alerts    monitoring.Dispatcher
}

func NewService(mode string) *Service {
	m := Mode(mode)
	if m != ModeLive {
		m = ModePaper
	}
	logger := monitoring.New("runner")
	cfg := Config{
		Mode:          m,
		Pair:          "BTC",
		Interval:      time.Second,
		MaxOpenTrades: 3,
	}
	return &Service{
		cfg:       cfg,
		scheduler: NewScheduler(cfg.Interval),
		repo:      storage.NewInMemoryTradeRepository(),
		logger:    logger,
		metrics:   monitoring.NewRegistry(),
		alerts:    monitoring.NewDispatcher(logger),
	}
}

func (s *Service) Run(ctx context.Context) error {
	return s.scheduler.Run(ctx, func(t Tick) error {
		_, err := s.iteration(t)
		return err
	})
}

func (s *Service) RunOnce(ctx context.Context) (RunReport, error) {
	select {
	case <-ctx.Done():
		return RunReport{}, ctx.Err()
	default:
	}

	var report RunReport
	err := s.scheduler.RunOnce(func(t Tick) error {
		r, err := s.iteration(t)
		report = r
		return err
	})
	return report, err
}

func (s *Service) iteration(t Tick) (RunReport, error) {
	openTrades := len(s.repo.ListTradesByStatus(storage.TradeStatusSubmitted, 0))
	riskAllowed := openTrades < s.cfg.MaxOpenTrades
	action := "hold"

	if riskAllowed {
		action = "scan"
		if t.Seq%5 == 0 {
			action = "prepare-order"
			rec := storage.TradeRecord{
				ID:          fmt.Sprintf("%s-%d", s.cfg.Pair, t.Seq),
				Pair:        s.cfg.Pair,
				Venue:       "control-plane",
				Side:        "BUY",
				Quantity:    1,
				Price:       0.5,
				NotionalUSD: 100,
				Status:      storage.TradeStatusSubmitted,
				Metadata: map[string]string{
					"mode": string(s.cfg.Mode),
				},
			}
			if err := s.repo.SaveTrade(rec); err != nil {
				s.metrics.IncCounter("runner_iteration_errors_total", 1, nil)
				return RunReport{}, err
			}
		}
	} else {
		s.alerts.Send(monitoring.Alert{
			Code:     "OPEN_TRADE_LIMIT",
			Message:  "open trade limit reached",
			Severity: monitoring.SeverityWarning,
			Labels:   map[string]string{"mode": string(s.cfg.Mode)},
		})
	}

	s.metrics.IncCounter("runner_iterations_total", 1, map[string]string{"mode": string(s.cfg.Mode)})
	s.metrics.SetGauge("runner_open_trades", float64(openTrades), map[string]string{"pair": s.cfg.Pair})

	report := RunReport{
		Mode:        s.cfg.Mode,
		Pair:        s.cfg.Pair,
		TickSeq:     t.Seq,
		TickAt:      t.At,
		Action:      action,
		OpenTrades:  openTrades,
		RiskAllowed: riskAllowed,
	}

	s.logger.Info("runner iteration", map[string]interface{}{
		"tick":         report.TickSeq,
		"action":       report.Action,
		"risk_allowed": report.RiskAllowed,
		"open_trades":  report.OpenTrades,
	})

	return report, nil
}

func (s *Service) Metrics() []monitoring.Metric {
	return s.metrics.Snapshot()
}
