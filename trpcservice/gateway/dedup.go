package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const dedupPrefix = "dedup:"

var (
	markScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			redis.call("PSETEX", KEYS[1], ARGV[2], "done")
			return 1
		end
		return 0
	`)
	releaseScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
)

// RedisDeduper stores in-flight and completed inbound idempotency keys.
type RedisDeduper struct {
	client      redis.Cmdable
	inflightTTL time.Duration
	doneTTL     time.Duration
}

// NewRedisDeduper constructs a two-phase Redis deduper.
func NewRedisDeduper(client redis.Cmdable, inflightTTL, doneTTL time.Duration) (*RedisDeduper, error) {
	if client == nil {
		return nil, errors.New("deduper Redis client is required")
	}
	if inflightTTL <= 0 || doneTTL <= 0 {
		return nil, errors.New("deduper TTLs must be positive")
	}
	return &RedisDeduper{client: client, inflightTTL: inflightTTL, doneTTL: doneTTL}, nil
}

// Claim claims a message as in-flight and returns its fencing token.
func (d *RedisDeduper) Claim(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("deduplication key is required")
	}
	token, err := newOwnerToken()
	if err != nil {
		return "", err
	}
	claimed, err := d.client.SetNX(ctx, dedupPrefix+key, token, d.inflightTTL).Result()
	if err != nil {
		return "", fmt.Errorf("claim deduplication key: %w", err)
	}
	if !claimed {
		return "", ErrDuplicate
	}
	return token, nil
}

// Mark transitions an owned in-flight claim to the long-lived done state.
func (d *RedisDeduper) Mark(ctx context.Context, key, token string) error {
	if key == "" || token == "" {
		return errors.New("deduplication key and token are required")
	}
	if _, err := markScript.Run(
		ctx, d.client, []string{dedupPrefix + key}, token, d.doneTTL.Milliseconds(),
	).Result(); err != nil {
		return fmt.Errorf("mark deduplication key done: %w", err)
	}
	return nil
}

// Release removes an owned in-flight claim so the platform may redeliver it.
func (d *RedisDeduper) Release(ctx context.Context, key, token string) error {
	if key == "" || token == "" {
		return errors.New("deduplication key and token are required")
	}
	if _, err := releaseScript.Run(
		ctx, d.client, []string{dedupPrefix + key}, token,
	).Result(); err != nil {
		return fmt.Errorf("release deduplication key: %w", err)
	}
	return nil
}

func newOwnerToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("create owner token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
