package runtimecontext

import (
	"context"
	"errors"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
)

var ErrStorageScope = errors.New("storage scope is missing or does not match the trusted caller")

type storageScopeKey struct{}
type storageScope struct {
	appName string
	invalid bool
}

// WithStorageScope is for trusted ingress, authorized admin operations and
// durable job consumers, never for binding a caller-supplied storage key.
// A nested call cannot widen an existing scope, even through WithoutCancel.
func WithStorageScope(ctx context.Context, appName string) context.Context {
	_, _, err := ParseStorageScope(appName)
	prior, exists := ctx.Value(storageScopeKey{}).(storageScope)
	invalid := err != nil || (exists && (prior.invalid || prior.appName != appName))
	return context.WithValue(ctx, storageScopeKey{}, storageScope{appName, invalid})
}

// ValidateStorageScope checks both the platform identity (available before
// Runner creates an Invocation) and every available framework identity. A
// syntactically valid key alone is not authorization. Internal callers must
// explicitly install a scope rather than relying on an unscoped Background.
func ValidateStorageScope(ctx context.Context, appName string) (string, string, error) {
	tenantID, appID, err := ParseStorageScope(appName)
	if err != nil {
		return "", "", err
	}
	if ctx == nil {
		return "", "", ErrStorageScope
	}
	authorized := false
	if bound, ok := ctx.Value(storageScopeKey{}).(storageScope); ok {
		if bound.invalid || bound.appName != appName {
			return "", "", ErrStorageScope
		}
		authorized = true
	}
	if inv, ok := agentcore.InvocationFromContext(ctx); ok {
		if inv == nil || inv.RunOptions.AppName != appName ||
			(inv.Session != nil && inv.Session.AppName != appName) {
			return "", "", ErrStorageScope
		}
		authorized = true
	}
	if !authorized {
		return "", "", ErrStorageScope
	}
	return tenantID, appID, nil
}
