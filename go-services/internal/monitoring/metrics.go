package monitoring

import (
	"sort"
	"strings"
	"sync"
	"time"
)

type MetricKind string

const (
	Counter MetricKind = "counter"
	Gauge   MetricKind = "gauge"
)

type Metric struct {
	Name      string
	Kind      MetricKind
	Value     float64
	Labels    map[string]string
	UpdatedAt time.Time
}

type Registry struct {
	mu      sync.RWMutex
	metrics map[string]Metric
}

func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]Metric)}
}

func (r *Registry) IncCounter(name string, by float64, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels)
	m := r.metrics[k]
	m.Name = name
	m.Kind = Counter
	m.Labels = cloneLabels(labels)
	m.Value += by
	m.UpdatedAt = time.Now().UTC()
	r.metrics[k] = m
}

func (r *Registry) SetGauge(name string, value float64, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels)
	r.metrics[k] = Metric{
		Name:      name,
		Kind:      Gauge,
		Value:     value,
		Labels:    cloneLabels(labels),
		UpdatedAt: time.Now().UTC(),
	}
}

func (r *Registry) Snapshot() []Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Metric, 0, len(r.metrics))
	for _, m := range r.metrics {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].UpdatedAt.Before(out[j].UpdatedAt)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}
	cp := make(map[string]string, len(labels))
	for k, v := range labels {
		cp[k] = v
	}
	return cp
}

func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(labels)+1)
	parts = append(parts, name)
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, "|")
}
