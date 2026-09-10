// Package redis implements Redis Streams dispatch and Session leases.
package redis

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Client owns the Redis connection shared by dispatch and lease operations.
type Client struct {
	client *goredis.Client
}

// NewClient opens and verifies a Redis connection from a URL.
func NewClient(ctx context.Context, rawURL string) (*Client, error) {
	if rawURL == "" {
		return nil, errors.New("redis url is required")
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := goredis.NewClient(options)
	wrapped := &Client{client: client}
	if err := wrapped.Ping(ctx); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("close redis client after ping failure: %w", closeErr))
		}
		return nil, err
	}
	return wrapped, nil
}

// Ping verifies that Redis is available.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.client == nil {
		return errors.New("redis client is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

// Close releases the Redis connection. It is safe to call on nil.
func (c *Client) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}
