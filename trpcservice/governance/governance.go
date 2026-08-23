// Package governance enforces tenant concurrency, token and daily cost limits.
package governance

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
)

var (
	ErrConcurrencyLimit = errors.New("tenant concurrency limit exceeded")
	ErrInputTokenLimit  = errors.New("tenant input token limit exceeded")
	ErrDailyCostLimit   = errors.New("tenant daily cost limit exceeded")
	ErrToolDenied       = errors.New("tool denied by tenant policy")
)

type Controller interface {
	Reserve(context.Context, tenant.Tenant, string, string) error
	Finalize(context.Context, tenant.Tenant, string, int, int) (float64, error)
	Cancel(context.Context, tenant.Tenant, string) error
	Close() error
}

func New(redisAddr string) (Controller, error) {
	if strings.TrimSpace(redisAddr) == "" {
		return NewMemoryController(), nil
	}
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping governance redis: %w", err)
	}
	return &RedisController{client: client}, nil
}

func EstimateInputTokens(value string) int {
	if value == "" {
		return 0
	}
	var ascii, nonASCII int
	for _, r := range value {
		if r <= 127 {
			ascii++
		} else {
			nonASCII++
		}
	}
	estimate := nonASCII + (ascii+3)/4
	if estimate == 0 {
		return utf8.RuneCountInString(value)
	}
	return estimate
}

func CostUSD(policy tenant.BudgetPolicy, promptTokens, completionTokens int) float64 {
	return float64(promptTokens)*policy.InputCostPerMillionUSD/1_000_000 +
		float64(completionTokens)*policy.OutputCostPerMillionUSD/1_000_000
}

func reserveMicros(policy tenant.BudgetPolicy, inputTokens int) int64 {
	return int64(math.Ceil(float64(inputTokens)*policy.InputCostPerMillionUSD +
		float64(policy.MaxOutputTokens)*policy.OutputCostPerMillionUSD))
}

func actualMicros(policy tenant.BudgetPolicy, promptTokens, completionTokens int) int64 {
	return int64(math.Ceil(float64(promptTokens)*policy.InputCostPerMillionUSD +
		float64(completionTokens)*policy.OutputCostPerMillionUSD))
}

func validateInput(policy tenant.BudgetPolicy, value string) (int, error) {
	tokens := EstimateInputTokens(value)
	if policy.MaxInputTokens > 0 && tokens > policy.MaxInputTokens {
		return tokens, ErrInputTokenLimit
	}
	return tokens, nil
}

type reservation struct {
	tenantID string
	day      string
	reserved int64
}

type MemoryController struct {
	mu           sync.Mutex
	concurrent   map[string]int
	dailyMicros  map[string]int64
	reservations map[string]reservation
}

func NewMemoryController() *MemoryController {
	return &MemoryController{
		concurrent: make(map[string]int), dailyMicros: make(map[string]int64),
		reservations: make(map[string]reservation),
	}
}

func (c *MemoryController) Reserve(_ context.Context, t tenant.Tenant, requestID, input string) error {
	tokens, err := validateInput(t.Budget, input)
	if err != nil {
		return err
	}
	key := t.ID + "|" + requestID
	day := time.Now().UTC().Format("2006-01-02")
	dailyKey := t.ID + "|" + day
	reserved := reserveMicros(t.Budget, tokens)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.reservations[key]; exists {
		return nil
	}
	if t.Budget.MaxConcurrent > 0 && c.concurrent[t.ID] >= t.Budget.MaxConcurrent {
		return ErrConcurrencyLimit
	}
	limit := int64(math.Round(t.Budget.DailyCostUSD * 1_000_000))
	if limit > 0 && c.dailyMicros[dailyKey]+reserved > limit {
		return ErrDailyCostLimit
	}
	c.concurrent[t.ID]++
	c.dailyMicros[dailyKey] += reserved
	c.reservations[key] = reservation{tenantID: t.ID, day: dailyKey, reserved: reserved}
	return nil
}

func (c *MemoryController) Finalize(_ context.Context, t tenant.Tenant, requestID string, promptTokens, completionTokens int) (float64, error) {
	key := t.ID + "|" + requestID
	c.mu.Lock()
	defer c.mu.Unlock()
	item, exists := c.reservations[key]
	if !exists {
		return CostUSD(t.Budget, promptTokens, completionTokens), nil
	}
	actual := actualMicros(t.Budget, promptTokens, completionTokens)
	c.dailyMicros[item.day] += actual - item.reserved
	if c.concurrent[item.tenantID] > 0 {
		c.concurrent[item.tenantID]--
	}
	delete(c.reservations, key)
	return float64(actual) / 1_000_000, nil
}

func (c *MemoryController) Cancel(ctx context.Context, t tenant.Tenant, requestID string) error {
	_, err := c.Finalize(ctx, t, requestID, 0, 0)
	return err
}

func (*MemoryController) Close() error { return nil }

type RedisController struct{ client *redis.Client }

func (c *RedisController) Reserve(ctx context.Context, t tenant.Tenant, requestID, input string) error {
	tokens, err := validateInput(t.Budget, input)
	if err != nil {
		return err
	}
	day := time.Now().UTC().Format("2006-01-02")
	keys := []string{
		"trpc:governance:concurrent:" + t.ID,
		"trpc:governance:cost:" + t.ID + ":" + day,
		"trpc:governance:reservation:" + t.ID + ":" + requestID,
	}
	limit := int64(math.Round(t.Budget.DailyCostUSD * 1_000_000))
	code, err := reserveScript.Run(ctx, c.client, keys,
		t.Budget.MaxConcurrent, limit, reserveMicros(t.Budget, tokens), 172800).Int()
	if err != nil {
		return err
	}
	switch code {
	case 1:
		return nil
	case -1:
		return ErrConcurrencyLimit
	case -2:
		return ErrDailyCostLimit
	default:
		return fmt.Errorf("unexpected governance reserve result %d", code)
	}
}

func (c *RedisController) Finalize(ctx context.Context, t tenant.Tenant, requestID string, promptTokens, completionTokens int) (float64, error) {
	day := time.Now().UTC().Format("2006-01-02")
	keys := []string{
		"trpc:governance:concurrent:" + t.ID,
		"trpc:governance:cost:" + t.ID + ":" + day,
		"trpc:governance:reservation:" + t.ID + ":" + requestID,
	}
	actual := actualMicros(t.Budget, promptTokens, completionTokens)
	if _, err := finalizeScript.Run(ctx, c.client, keys, actual, 172800).Result(); err != nil {
		return 0, err
	}
	return float64(actual) / 1_000_000, nil
}

func (c *RedisController) Cancel(ctx context.Context, t tenant.Tenant, requestID string) error {
	_, err := c.Finalize(ctx, t, requestID, 0, 0)
	return err
}

func (c *RedisController) Close() error { return c.client.Close() }

var reserveScript = redis.NewScript(`
if redis.call('exists', KEYS[3]) == 1 then return 1 end
local concurrent = redis.call('incr', KEYS[1])
if tonumber(ARGV[1]) > 0 and concurrent > tonumber(ARGV[1]) then
  redis.call('decr', KEYS[1]); return -1
end
local cost = redis.call('incrby', KEYS[2], ARGV[3])
if tonumber(ARGV[2]) > 0 and cost > tonumber(ARGV[2]) then
  redis.call('incrby', KEYS[2], -tonumber(ARGV[3])); redis.call('decr', KEYS[1]); return -2
end
redis.call('hset', KEYS[3], 'reserved', ARGV[3], 'cost_key', KEYS[2])
redis.call('expire', KEYS[1], ARGV[4]); redis.call('expire', KEYS[2], ARGV[4]); redis.call('expire', KEYS[3], ARGV[4])
return 1`)

var finalizeScript = redis.NewScript(`
if redis.call('exists', KEYS[3]) == 0 then return 0 end
local reserved = tonumber(redis.call('hget', KEYS[3], 'reserved') or '0')
local cost_key = redis.call('hget', KEYS[3], 'cost_key') or KEYS[2]
local concurrent = tonumber(redis.call('get', KEYS[1]) or '0')
if concurrent > 0 then redis.call('decr', KEYS[1]) end
redis.call('incrby', cost_key, tonumber(ARGV[1]) - reserved)
redis.call('expire', cost_key, ARGV[2]); redis.call('del', KEYS[3])
return 1`)
