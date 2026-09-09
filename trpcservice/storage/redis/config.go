package redis

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	URL             string
	KeyPrefix       string
	ClaimTTL        time.Duration
	SessionLeaseTTL time.Duration
	RenewInterval   time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

func (c Config) withDefaults() (Config, error) {
	if c.URL == "" {
		return Config{}, fmt.Errorf("redis: URL is required")
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "trpc:v1"
	}
	if c.ClaimTTL == 0 {
		c.ClaimTTL = 30 * time.Second
	}
	if c.SessionLeaseTTL == 0 {
		c.SessionLeaseTTL = 30 * time.Second
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = c.SessionLeaseTTL / 3
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 5 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.ClaimTTL <= 0 || c.SessionLeaseTTL <= 0 || c.RenewInterval <= 0 || c.RenewInterval >= c.SessionLeaseTTL {
		return Config{}, fmt.Errorf("redis: invalid ttl or renewal interval")
	}
	if c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		return Config{}, fmt.Errorf("redis: timeouts must be positive")
	}
	for _, r := range c.KeyPrefix {
		if r == ':' || r == '{' || r == '}' || r <= 0x1f {
			return Config{}, fmt.Errorf("redis: invalid key prefix")
		}
	}
	return c, nil
}

func NewClient(c Config) (*redis.Client, error) {
	c, err := c.withDefaults()
	if err != nil {
		return nil, err
	}
	opt, err := redis.ParseURL(c.URL)
	if err != nil {
		return nil, fmt.Errorf("redis: parse URL: %w", err)
	}
	opt.DialTimeout, opt.ReadTimeout, opt.WriteTimeout = c.DialTimeout, c.ReadTimeout, c.WriteTimeout
	return redis.NewClient(opt), nil
}
