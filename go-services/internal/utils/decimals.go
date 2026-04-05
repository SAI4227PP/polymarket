package utils

import "math"

func RoundTo(value float64, decimals int) float64 {
	if decimals < 0 {
		decimals = 0
	}
	pow := math.Pow(10, float64(decimals))
	return math.Round(value*pow) / pow
}

func Clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func Bps(delta, base float64) float64 {
	if base == 0 {
		return 0
	}
	return (delta / base) * 10_000
}
