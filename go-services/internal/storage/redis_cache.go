package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(redisURL string) (*RedisCache, error) {
	if redisURL == "" {
		return nil, errors.New("redis url is required")
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opt)
	return &RedisCache{client: client}, nil
}

func (c *RedisCache) Ping(ctx context.Context) error {
	if c == nil || c.client == nil {
		return errors.New("redis client is nil")
	}
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) SetJSON(ctx context.Context, key string, v interface{}, ttl time.Duration) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, b, ttl).Err()
}

func (c *RedisCache) GetRaw(ctx context.Context, key string) ([]byte, error) {
	s, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func (c *RedisCache) GetJSON(ctx context.Context, key string, dst interface{}) error {
	b, err := c.GetRaw(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

func (c *RedisCache) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}
