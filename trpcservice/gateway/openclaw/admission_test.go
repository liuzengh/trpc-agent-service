package openclaw

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type admissionEvalFunc func(context.Context, string, []string, ...any) (any, error)

func (eval admissionEvalFunc) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return eval(ctx, script, keys, args...)
}

func TestRedisAdmissionLimiter(t *testing.T) {
	var calls int
	limiter := &RedisAdmissionLimiter{Redis: admissionEvalFunc(func(_ context.Context, _ string, keys []string, args ...any) (any, error) {
		calls++
		if len(keys) != 1 || len(args) != 2 || args[0] != int64(time.Second/time.Millisecond) || args[1] != 2 {
			t.Fatalf("keys=%v args=%v", keys, args)
		}
		if calls > 2 {
			return int64(0), nil
		}
		return int64(1), nil
	}), Limit: 2, Window: time.Second}
	inbound := gateway.InboundMessage{TenantID: "tenant", BindingID: "binding"}
	if err := limiter.Allow(context.Background(), inbound); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Allow(context.Background(), inbound); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Allow(context.Background(), inbound); !errors.Is(err, ErrAdmissionLimited) {
		t.Fatalf("third admission error=%v", err)
	}
}
