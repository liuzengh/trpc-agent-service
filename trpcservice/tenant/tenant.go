// Package tenant models multi-tenant isolation, quotas and cost budgets.
package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/redis/go-redis/v9"
)

var (
	ErrRateLimited        = errors.New("tenant request rate exceeded")
	ErrConcurrencyLimited = errors.New("tenant concurrent run limit exceeded")
	ErrBudgetExceeded     = errors.New("tenant daily budget exceeded")
	ErrPricingRequired    = errors.New("nonzero model pricing required when a monetary budget is enabled")
)

type QuotaPolicy struct {
	RequestsPerMinute     int     `json:"requests_per_minute"`
	ConcurrentRuns        int     `json:"concurrent_runs"`
	DailyPromptTokens     int64   `json:"daily_prompt_tokens"`
	DailyCompletionTokens int64   `json:"daily_completion_tokens"`
	DailyCostUSD          float64 `json:"daily_cost_usd"`
}

func ParseQuotaPolicy(raw json.RawMessage) (QuotaPolicy, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var policy QuotaPolicy
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || decoder.Decode(new(any)) != io.EOF || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return QuotaPolicy{}, errors.New("invalid tenant quota policy")
	}
	if policy.RequestsPerMinute < 0 || policy.ConcurrentRuns < 0 ||
		policy.DailyPromptTokens < 0 || policy.DailyCompletionTokens < 0 ||
		policy.DailyCostUSD < 0 || math.IsInf(policy.DailyCostUSD, 0) || math.IsNaN(policy.DailyCostUSD) || policy.DailyCostUSD > 1_000_000 || policy.DailyPromptTokens > 1_000_000_000 || policy.DailyCompletionTokens > 1_000_000_000 {
		return QuotaPolicy{}, errors.New("tenant quotas must not be negative")
	}
	return policy, nil
}

type Guard struct {
	repository   controlplane.Repository
	redis        *redis.Client
	prefix       string
	mu           sync.Mutex
	local        map[string]*localQuota
	usageSeen    map[string]struct{}
	reservations map[string]reservationState
	runMembers   map[string]map[string]time.Time
	runLeases    map[*RunLease]struct{}
	closed       bool
}

type localQuota struct {
	minute     string
	requests   int
	day        string
	prompt     int64
	completion int64
	cost       float64
}

func NewGuard(
	ctx context.Context,
	repository controlplane.Repository,
	cfg config.QuotaConfig,
) (*Guard, error) {
	if repository == nil {
		return nil, errors.New("quota guard repository is required")
	}
	guard := &Guard{
		repository: repository, prefix: cfg.KeyPrefix + ":quota",
		local: make(map[string]*localQuota), usageSeen: make(map[string]struct{}),
		runMembers: map[string]map[string]time.Time{}, runLeases: map[*RunLease]struct{}{},
	}
	if cfg.Backend == config.QuotaBackendRedis {
		options, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, err
		}
		guard.redis = redis.NewClient(options)
		if err := guard.redis.Ping(ctx).Err(); err != nil {
			_ = guard.redis.Close()
			return nil, fmt.Errorf("probe quota Redis: %w", err)
		}
	}
	return guard, nil
}

func (g *Guard) AllowInbound(ctx context.Context, tenantID string, userID string) error {
	policy, err := g.policy(ctx, tenantID)
	if err != nil || policy.RequestsPerMinute == 0 {
		return err
	}
	minute := time.Now().UTC().Format("200601021504")
	if g.redis != nil {
		key := g.prefix + ":rate:" + tenantID + ":" + userID + ":" + minute
		allowed, err := redis.NewScript(`
local value=redis.call('INCR',KEYS[1])
if value==1 then redis.call('EXPIRE',KEYS[1],120) end
if value>tonumber(ARGV[1]) then return 0 end
return 1`).Run(ctx, g.redis, []string{key}, policy.RequestsPerMinute).Int()
		if err != nil {
			return err
		}
		if allowed == 0 {
			return ErrRateLimited
		}
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	entry := g.local[tenantID+"\x00"+userID]
	if entry == nil {
		entry = &localQuota{}
		g.local[tenantID+"\x00"+userID] = entry
	}
	if entry.minute != minute {
		entry.minute, entry.requests = minute, 0
	}
	entry.requests++
	if entry.requests > policy.RequestsPerMinute {
		return ErrRateLimited
	}
	return nil
}

// AcquireRun preserves the older release-only API for embedded callers. New
// execution paths must use AcquireRunLease and its cancellation context.
func (g *Guard) AcquireRun(ctx context.Context, tenantID string) (func(), error) {
	l, err := g.AcquireRunLease(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return l.Release, nil
}
func (g *Guard) RecordUsage(
	ctx context.Context,
	tenantID string,
	requestID string,
	promptTokens int,
	completionTokens int,
	cost float64,
) error {
	day := time.Now().UTC().Format("20060102")
	if g.redis != nil {
		base := g.prefix + ":usage:" + tenantID + ":" + day
		_, err := redis.NewScript(`
if not redis.call('SET',KEYS[1],1,'NX','EX',172800) then return 0 end
redis.call('INCRBY',KEYS[2],ARGV[1]); redis.call('EXPIRE',KEYS[2],172800)
redis.call('INCRBY',KEYS[3],ARGV[2]); redis.call('EXPIRE',KEYS[3],172800)
redis.call('INCRBYFLOAT',KEYS[4],ARGV[3]); redis.call('EXPIRE',KEYS[4],172800)
return 1`).Run(ctx, g.redis, []string{
			base + ":request:" + requestID,
			base + ":prompt", base + ":completion", base + ":cost",
		}, promptTokens, completionTokens, strconv.FormatFloat(cost, 'f', 8, 64)).Result()
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	marker := tenantID + "\x00" + day + "\x00" + requestID
	if _, exists := g.usageSeen[marker]; exists {
		return nil
	}
	g.usageSeen[marker] = struct{}{}
	entry := g.local[tenantID]
	if entry == nil {
		entry = &localQuota{}
		g.local[tenantID] = entry
	}
	if entry.day != day {
		entry.day, entry.prompt, entry.completion, entry.cost = day, 0, 0, 0
	}
	entry.prompt += int64(promptTokens)
	entry.completion += int64(completionTokens)
	entry.cost += cost
	return nil
}

func (g *Guard) checkBudget(ctx context.Context, tenantID string, policy QuotaPolicy) error {
	if policy.DailyPromptTokens == 0 && policy.DailyCompletionTokens == 0 && policy.DailyCostUSD == 0 {
		return nil
	}
	day := time.Now().UTC().Format("20060102")
	var prompt, completion int64
	var cost float64
	if g.redis != nil {
		base := g.prefix + ":usage:" + tenantID + ":" + day
		values, err := g.redis.MGet(ctx, base+":prompt", base+":completion", base+":cost").Result()
		if err != nil {
			return err
		}
		prompt = parseInt(values[0])
		completion = parseInt(values[1])
		cost = parseFloat(values[2])
	} else {
		g.mu.Lock()
		entry := g.local[tenantID]
		if entry != nil && entry.day == day {
			prompt, completion, cost = entry.prompt, entry.completion, entry.cost
		}
		g.mu.Unlock()
	}
	if (policy.DailyPromptTokens > 0 && prompt >= policy.DailyPromptTokens) ||
		(policy.DailyCompletionTokens > 0 && completion >= policy.DailyCompletionTokens) ||
		(policy.DailyCostUSD > 0 && cost >= policy.DailyCostUSD) {
		return ErrBudgetExceeded
	}
	return nil
}

func (g *Guard) policy(ctx context.Context, tenantID string) (QuotaPolicy, error) {
	tenant, err := g.repository.GetTenant(ctx, tenantID)
	if err != nil {
		return QuotaPolicy{}, err
	}
	return ParseQuotaPolicy(tenant.QuotaConfig)
}

func (g *Guard) Ready(ctx context.Context) error {
	if g.redis != nil {
		return g.redis.Ping(ctx).Err()
	}
	return g.repository.Ready(ctx)
}

func (g *Guard) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	g.closed = true
	leases := make([]*RunLease, 0, len(g.runLeases))
	for lease := range g.runLeases {
		leases = append(leases, lease)
	}
	g.mu.Unlock()
	for _, lease := range leases {
		lease.Release()
	}
	if g.redis == nil {
		return nil
	}
	return g.redis.Close()
}

func parseInt(value any) int64 {
	parsed, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	return parsed
}

func parseFloat(value any) float64 {
	parsed, _ := strconv.ParseFloat(fmt.Sprint(value), 64)
	return parsed
}
