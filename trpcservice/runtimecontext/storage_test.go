package runtimecontext

import (
	"context"
	"errors"
	"testing"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestStorageScopeRequiresIndependentIdentity(t *testing.T) {
	const a = "t/tenant-a/a/app"
	const b = "t/tenant-b/a/app"
	inv := func(app string) context.Context {
		return agentcore.NewInvocationContext(context.Background(), &agentcore.Invocation{
			RunOptions: agentcore.RunOptions{AppName: app}, Session: session.NewSession(app, "user", "session"),
		})
	}
	for name, ctx := range map[string]context.Context{
		"missing": context.Background(), "nil": nil,
		"foreign platform":        WithStorageScope(context.Background(), b),
		"foreign invocation":      inv(b),
		"conflicting identities":  WithStorageScope(inv(b), a),
		"nested widening":         WithStorageScope(WithStorageScope(context.Background(), b), a),
		"without cancel widening": WithStorageScope(context.WithoutCancel(WithStorageScope(context.Background(), b)), a),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ValidateStorageScope(ctx, a); !errors.Is(err, ErrStorageScope) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	for _, ctx := range []context.Context{WithStorageScope(context.Background(), a), inv(a), WithStorageScope(inv(a), a)} {
		if tenant, app, err := ValidateStorageScope(ctx, a); err != nil || tenant != "tenant-a" || app != "app" {
			t.Fatalf("%s %s %v", tenant, app, err)
		}
	}
}
