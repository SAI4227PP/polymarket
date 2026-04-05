package runner

import "fmt"

type Scheduler struct {
	IntervalMs int
}

func NewScheduler(intervalMs int) Scheduler {
	return Scheduler{IntervalMs: intervalMs}
}

func (s Scheduler) Tick() string {
	return fmt.Sprintf("tick interval=%dms", s.IntervalMs)
}
