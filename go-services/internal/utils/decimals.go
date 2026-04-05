package utils

func RoundTo(value float64, decimals int) float64 {
	pow := 1.0
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int(value*pow+0.5)) / pow
}
