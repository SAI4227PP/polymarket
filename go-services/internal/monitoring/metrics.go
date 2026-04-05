package monitoring

func EmitMetric(name string, value float64) map[string]float64 {
	return map[string]float64{name: value}
}
