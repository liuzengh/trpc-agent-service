package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/redis/go-redis/v9"
)

var (
	ErrRateLimitBackendUnavailable = errors.New("rate limit backend unavailable")
	ErrRateLimited                 = errors.New("rate limited")
	ErrInvalidArgument             = storage.ErrInvalidArgument
)

const (
	ScopeTenant  = "tenant"
	ScopeBinding = "channel-binding"
	ScopeChat    = "external-chat"
)

type LimitRequest struct {
	TenantID, BindingID, Channel, ExternalChatID string
	Cost                                         int64
}

type LimitDecision struct {
	Allowed    bool
	RetryAfter time.Duration
	Scope      string
	Remaining  int64
	ResetAfter time.Duration
}

type LimitPolicy struct {
	TenantLimit, BindingLimit, ChatLimit int64
	Window                               time.Duration
}

type scriptExecutor interface {
	Eval(context.Context, string, []string, ...interface{}) (interface{}, error)
}

type redisExecutor struct{ client *redis.Client }

func (e redisExecutor) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return e.client.Eval(ctx, script, keys, args...).Result()
}

type RateLimiter struct {
	executor     scriptExecutor
	prefix       string
	policy       LimitPolicy
	windowSecond int64
}

func New(client *redis.Client, prefix string, policy LimitPolicy) (*RateLimiter, error) {
	if client == nil || prefix == "" || policy.Window < time.Second || policy.Window%time.Second != 0 ||
		policy.TenantLimit <= 0 || policy.BindingLimit <= 0 || policy.ChatLimit <= 0 {
		return nil, fmt.Errorf("%w: invalid rate limiter configuration", ErrInvalidArgument)
	}
	return &RateLimiter{executor: redisExecutor{client: client}, prefix: prefix, policy: policy, windowSecond: int64(policy.Window / time.Second)}, nil
}

func validatePart(name, value string) error {
	if value == "" || len(value) > 256 || strings.ContainsAny(value, ":{}\x00\r\n") {
		return fmt.Errorf("%w: invalid %s", ErrInvalidArgument, name)
	}
	return nil
}

// encodeIdentity uses length-prefixed fields so scope identity cannot be
// confused by separators. All dimensions are tenant-qualified.
func encodeIdentity(values ...string) string {
	var b strings.Builder
	for _, value := range values {
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte('=')
		b.WriteString(value)
		b.WriteByte('|')
	}
	return b.String()
}

func (l *RateLimiter) keys(r LimitRequest) []string {
	base := l.prefix + ":rate-limit:"
	return []string{
		base + ScopeTenant + ":" + encodeIdentity(r.TenantID),
		base + ScopeBinding + ":" + encodeIdentity(r.TenantID, r.Channel, r.BindingID),
		base + ScopeChat + ":" + encodeIdentity(r.TenantID, r.Channel, r.BindingID, r.ExternalChatID),
	}
}

func (l *RateLimiter) validateRequest(r LimitRequest) error {
	if err := validatePart("tenant", r.TenantID); err != nil {
		return err
	}
	if err := validatePart("channel", r.Channel); err != nil {
		return err
	}
	if err := validatePart("binding", r.BindingID); err != nil {
		return err
	}
	if err := validatePart("external chat", r.ExternalChatID); err != nil {
		return err
	}
	if r.Cost <= 0 {
		return fmt.Errorf("%w: cost must be positive", ErrInvalidArgument)
	}
	return nil
}

func asInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func (l *RateLimiter) Allow(ctx context.Context, request LimitRequest) (LimitDecision, error) {
	if err := l.validateRequest(request); err != nil {
		return LimitDecision{}, err
	}
	result, err := l.executor.Eval(ctx, threeDimensionalFixedWindowScript, l.keys(request), l.windowSecond, request.Cost,
		l.policy.TenantLimit, l.policy.BindingLimit, l.policy.ChatLimit)
	if err != nil {
		return LimitDecision{}, fmt.Errorf("%w: %v", ErrRateLimitBackendUnavailable, err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 4 {
		return LimitDecision{}, ErrRateLimitBackendUnavailable
	}
	allowedCode, ok1 := asInt64(values[0])
	retrySeconds, ok2 := asInt64(values[1])
	remaining, ok3 := asInt64(values[2])
	scopeCode, ok4 := asInt64(values[3])
	if !ok1 || !ok2 || !ok3 || !ok4 || (allowedCode != 0 && allowedCode != 1) || scopeCode < 0 || scopeCode > 3 || retrySeconds < 0 || remaining < 0 {
		return LimitDecision{}, ErrRateLimitBackendUnavailable
	}
	decision := LimitDecision{Allowed: allowedCode == 1, Remaining: remaining, RetryAfter: time.Duration(retrySeconds) * time.Second, ResetAfter: time.Duration(retrySeconds) * time.Second}
	if !decision.Allowed {
		decision.Scope = []string{"", ScopeTenant, ScopeBinding, ScopeChat}[scopeCode]
		return decision, errors.Join(ErrRateLimited, storage.ErrRateLimited)
	}
	return decision, nil
}
