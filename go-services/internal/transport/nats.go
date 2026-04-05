package transport

func NewNATS(url string) string {
	return "nats://" + url
}
