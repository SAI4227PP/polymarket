package monitoring

import "time"

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Alert struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Severity  Severity          `json:"severity"`
	Labels    map[string]string `json:"labels,omitempty"`
	Triggered time.Time         `json:"triggered"`
}

type Dispatcher struct {
	logger Logger
}

func NewDispatcher(logger Logger) Dispatcher {
	return Dispatcher{logger: logger}
}

func (d Dispatcher) Send(a Alert) {
	if a.Triggered.IsZero() {
		a.Triggered = time.Now().UTC()
	}
	d.logger.Warn("alert emitted", map[string]interface{}{
		"code":      a.Code,
		"severity":  a.Severity,
		"message":   a.Message,
		"labels":    a.Labels,
		"triggered": a.Triggered.Format(time.RFC3339Nano),
	})
}
