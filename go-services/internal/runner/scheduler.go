package runner

import (
	"context"
	"errors"
	"time"
)

type Tick struct {
	Seq int64
	At  time.Time
}

type Scheduler struct {
	interval time.Duration
}

func NewScheduler(interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = time.Second
	}
	return &Scheduler{interval: interval}
}

func (s *Scheduler) Interval() time.Duration {
	return s.interval
}

func (s *Scheduler) Run(ctx context.Context, fn func(Tick) error) error {
	if fn == nil {
		return errors.New("scheduler callback is nil")
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-ticker.C:
			seq++
			if err := fn(Tick{Seq: seq, At: t.UTC()}); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) RunOnce(fn func(Tick) error) error {
	if fn == nil {
		return errors.New("scheduler callback is nil")
	}
	return fn(Tick{Seq: 1, At: time.Now().UTC()})
}
