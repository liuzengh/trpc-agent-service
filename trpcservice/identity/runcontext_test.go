package identity

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

func TestRunContextRoundTrip(t *testing.T) {
	scope := RunContext{
		RequestID:   "request-1",
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: testPrincipal,
		SessionID:   "conversation-1",
		RevisionID:  "revision-1",
	}
	ctx, err := WithRunContext(context.Background(), scope)
	require.NoError(t, err)

	restored, err := RunContextFrom(ctx)
	require.NoError(t, err)
	require.Equal(t, scope, restored)
	require.Equal(t, "u/"+testPrincipal, restored.UserID())
}

func TestRunContextFromRejectsUntrustedContexts(t *testing.T) {
	_, err := RunContextFrom(context.Background())
	require.ErrorIs(t, err, ErrNoRunContext)

	var missingContext context.Context
	_, err = RunContextFrom(missingContext)
	require.ErrorIs(t, err, ErrNoRunContext)

	// A value stored under the package key still has to validate, so a partial
	// scope can never be read back as a trusted one.
	partial := context.WithValue(context.Background(), runContextKey{}, RunContext{
		TenantID: "tenant-a", AppID: "assistant",
	})
	_, err = RunContextFrom(partial)
	require.ErrorIs(t, err, ErrNoRunContext)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	// A value of another type under the same key is not a scope either.
	wrongType := context.WithValue(context.Background(), runContextKey{}, "tenant-a")
	_, err = RunContextFrom(wrongType)
	require.ErrorIs(t, err, ErrNoRunContext)
}

func TestWithRunContextRejectsIncompleteScope(t *testing.T) {
	complete := RunContext{
		RequestID:   "request-1",
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: testPrincipal,
		SessionID:   "conversation-1",
		RevisionID:  "revision-1",
	}
	for name, mutate := range map[string]func(*RunContext){
		// The request id is required like every other field: a scope that
		// carries none is a run nothing downstream can be correlated with, and a
		// correlation id that is only sometimes there correlates nothing.
		"request":   func(c *RunContext) { c.RequestID = "" },
		"tenant":    func(c *RunContext) { c.TenantID = "" },
		"app":       func(c *RunContext) { c.AppID = "" },
		"principal": func(c *RunContext) { c.PrincipalID = "" },
		"session":   func(c *RunContext) { c.SessionID = "not a session" },
		"revision":  func(c *RunContext) { c.RevisionID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			scope := complete
			mutate(&scope)
			ctx, err := WithRunContext(context.Background(), scope)
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.Nil(t, ctx)
		})
	}

	var missingContext context.Context
	ctx, err := WithRunContext(missingContext, complete)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.Nil(t, ctx)
}
