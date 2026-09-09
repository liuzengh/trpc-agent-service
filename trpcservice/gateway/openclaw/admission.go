package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

var ErrAdmissionLimited = errors.New("gateway: tenant admission rate exceeded")

type AdmissionLimiter interface {
	Allow(context.Context, gateway.InboundMessage) error
}

type RedisEvaler interface {
	Eval(context.Context, string, []string, ...any) (any, error)
}

type RedisAdmissionLimiter struct {
	Redis  RedisEvaler
	Prefix string
	Limit  int
	Window time.Duration
}

const admissionLua = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
if count > tonumber(ARGV[2]) then return 0 end
return 1`

func (limiter *RedisAdmissionLimiter) Allow(ctx context.Context, inbound gateway.InboundMessage) error {
	if limiter == nil || limiter.Redis == nil || ctx == nil || inbound.TenantID == "" || inbound.BindingID == "" {
		return errors.New("gateway: admission limiter is unavailable")
	}
	window := limiter.Window
	if window <= 0 {
		window = time.Second
	}
	limit := limiter.Limit
	if limit <= 0 {
		limit = 100
	}
	digest := sha256.Sum256([]byte(inbound.TenantID + "\x00" + inbound.BindingID))
	prefix := limiter.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	result, err := limiter.Redis.Eval(ctx, admissionLua, []string{prefix + ":gateway-rate:" + hex.EncodeToString(digest[:])}, window.Milliseconds(), limit)
	if err != nil {
		return errors.New("gateway: admission backend unavailable")
	}
	allowed, err := admissionResult(result)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrAdmissionLimited
	}
	return nil
}

func admissionResult(value any) (bool, error) {
	var number int64
	var err error
	switch typed := value.(type) {
	case int64:
		number = typed
	case string:
		number, err = strconv.ParseInt(typed, 10, 64)
	case []byte:
		number, err = strconv.ParseInt(string(typed), 10, 64)
	default:
		return false, errors.New("gateway: invalid admission response")
	}
	if err != nil || (number != 0 && number != 1) {
		return false, errors.New("gateway: invalid admission response")
	}
	return number == 1, nil
}

var _ AdmissionLimiter = (*RedisAdmissionLimiter)(nil)
