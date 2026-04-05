package transport

import (
	"context"
	"errors"
	"strings"
)

type KafkaClient struct {
	Brokers   []string
	connected bool
}

func NewKafka(brokers string) (*KafkaClient, error) {
	parts := strings.Split(brokers, ",")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			clean = append(clean, p)
		}
	}
	if len(clean) == 0 {
		return nil, errors.New("at least one kafka broker is required")
	}
	return &KafkaClient{Brokers: clean}, nil
}

func (c *KafkaClient) Connect(ctx context.Context) error {
	if c == nil {
		return errors.New("kafka client is nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.connected = true
		return nil
	}
}

func (c *KafkaClient) Publish(topic string, key string, value []byte) error {
	if c == nil || !c.connected {
		return errors.New("kafka client not connected")
	}
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic is required")
	}
	if len(value) == 0 {
		return errors.New("message value is empty")
	}
	_ = key
	return nil
}

func (c *KafkaClient) Close() error {
	if c != nil {
		c.connected = false
	}
	return nil
}
