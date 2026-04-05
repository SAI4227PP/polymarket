package transport

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type NATSClient struct {
	URL       string
	connected bool
}

func NewNATS(url string) (*NATSClient, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, errors.New("nats url is required")
	}
	if !strings.HasPrefix(url, "nats://") {
		url = "nats://" + url
	}
	return &NATSClient{URL: url}, nil
}

func (c *NATSClient) Connect(ctx context.Context) error {
	if c == nil {
		return errors.New("nats client is nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.connected = true
		return nil
	}
}

func (c *NATSClient) Publish(subject string, payload []byte, headers map[string]string) error {
	if c == nil || !c.connected {
		return errors.New("nats client not connected")
	}
	if strings.TrimSpace(subject) == "" {
		return errors.New("subject is required")
	}
	_ = fmt.Sprintf("publishing subject=%s payload_bytes=%d headers=%d", subject, len(payload), len(headers))
	return nil
}

func (c *NATSClient) Close() error {
	if c != nil {
		c.connected = false
	}
	return nil
}
