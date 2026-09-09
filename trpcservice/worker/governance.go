package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const quotaKeyPrefix = "quota:"

var authorizeQuotaScript = redis.NewScript(`
	local rate = tonumber(ARGV[1])
	local daily_limit = tonumber(ARGV[2])
	if rate > 0 then
		local requests = redis.call("INCR", KEYS[1])
		if requests == 1 then
			redis.call("PEXPIRE", KEYS[1], ARGV[3])
		end
		if requests > rate then
			return 0
		end
	end
	if daily_limit > 0 then
		local used = tonumber(redis.call("GET", KEYS[2]) or "0")
		if used >= daily_limit then
			return 0
		end
	end
	return 1
`)

// NopGovernor allows all runs and discards usage.
type NopGovernor struct{}

// Authorize implements Governor.
func (NopGovernor) Authorize(context.Context, tenant.Snapshot, []storage.UserEvent) error {
	return nil
}

// RecordUsage implements Governor.
func (NopGovernor) RecordUsage(context.Context, string, int) error {
	return nil
}

// QuotaCounter stores distributed rate and daily token counters.
type QuotaCounter interface {
	Authorize(context.Context, string, tenant.Quota) (bool, error)
	AddTokens(context.Context, string, int) error
}

// PolicyGovernor enforces binding users and distributed tenant quotas.
type PolicyGovernor struct {
	counter QuotaCounter
}

// NewPolicyGovernor constructs tenant policy governance.
func NewPolicyGovernor(counter QuotaCounter) *PolicyGovernor {
	return &PolicyGovernor{counter: counter}
}

// Authorize checks all senders in a debounced batch before model invocation.
func (g *PolicyGovernor) Authorize(
	ctx context.Context,
	snapshot tenant.Snapshot,
	messages []storage.UserEvent,
) error {
	allowed := allowedUsers(snapshot.Binding.Config["allowed_users"])
	if len(allowed) > 0 {
		for _, message := range messages {
			if _, ok := allowed[message.SenderID]; !ok {
				return fmt.Errorf("%w: user %q", ErrPermissionDenied, message.SenderID)
			}
		}
	}
	if g.counter == nil {
		return nil
	}
	ok, err := g.counter.Authorize(ctx, snapshot.Tenant.ID, snapshot.Tenant.Quota)
	if err != nil {
		return fmt.Errorf("authorize tenant quota: %w", err)
	}
	if !ok {
		return ErrBudgetDenied
	}
	return nil
}

// RecordUsage adds model token usage to the tenant's daily counter.
func (g *PolicyGovernor) RecordUsage(ctx context.Context, tenantID string, tokens int) error {
	if g.counter == nil || tokens <= 0 {
		return nil
	}
	return g.counter.AddTokens(ctx, tenantID, tokens)
}

func allowedUsers(value string) map[string]struct{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	result := make(map[string]struct{})
	for _, user := range strings.Split(value, ",") {
		if user = strings.TrimSpace(user); user != "" {
			result[user] = struct{}{}
		}
	}
	return result
}

// RedisQuotaCounter implements a fixed-window request rate and daily token
// budget. Keys are tenant-scoped and expire automatically.
type RedisQuotaCounter struct {
	client redis.Cmdable
	now    func() time.Time
}

// NewRedisQuotaCounter constructs a distributed quota counter.
func NewRedisQuotaCounter(client redis.Cmdable) (*RedisQuotaCounter, error) {
	if client == nil {
		return nil, errors.New("quota Redis client is required")
	}
	return &RedisQuotaCounter{client: client, now: time.Now}, nil
}

// Authorize atomically checks and increments the current minute request window.
func (c *RedisQuotaCounter) Authorize(
	ctx context.Context,
	tenantID string,
	quota tenant.Quota,
) (bool, error) {
	if tenantID == "" {
		return false, errors.New("quota tenant ID is required")
	}
	now := c.now().UTC()
	minuteKey := quotaKeyPrefix + "requests:" + tenantID + ":" + now.Format("200601021504")
	dailyKey := quotaKeyPrefix + "tokens:" + tenantID + ":" + now.Format("20060102")
	result, err := authorizeQuotaScript.Run(
		ctx,
		c.client,
		[]string{minuteKey, dailyKey},
		quota.RatePerMinute,
		quota.DailyTokenLimit,
		(2 * time.Minute).Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("check Redis quota: %w", err)
	}
	return result == 1, nil
}

// AddTokens increments the current UTC day's token counter.
func (c *RedisQuotaCounter) AddTokens(ctx context.Context, tenantID string, tokens int) error {
	if tenantID == "" || tokens <= 0 {
		return errors.New("quota tenant ID and positive token count are required")
	}
	key := quotaKeyPrefix + "tokens:" + tenantID + ":" + c.now().UTC().Format("20060102")
	pipeline := c.client.TxPipeline()
	pipeline.IncrBy(ctx, key, int64(tokens))
	pipeline.Expire(ctx, key, 48*time.Hour)
	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("record Redis token usage: %w", err)
	}
	return nil
}
