package runner

import "fmt"

type Service struct {
	Mode      string
	Scheduler Scheduler
}

func NewService(mode string) Service {
	return Service{Mode: mode, Scheduler: NewScheduler(1000)}
}

func (s Service) RunOnce() string {
	return fmt.Sprintf("mode=%s %s", s.Mode, s.Scheduler.Tick())
}
