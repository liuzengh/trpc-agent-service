package runtimecontext

import (
	"context"
	"time"
)

// MessageLifetime is stamped by authenticated IM ingress, never HTTP JSON.
// ExpiresAt is an admission deadline, not cancellation of a started operation.
type MessageLifetime struct {
	Mode       string    `json:"mode,omitempty"`
	OccurredAt time.Time `json:"occurred_at,omitempty"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}
type messageLifetimeKey struct{}

func WithMessageLifetime(ctx context.Context, lifetime MessageLifetime) context.Context {
	return context.WithValue(ctx, messageLifetimeKey{}, lifetime)
}
func MessageLifetimeFromContext(ctx context.Context) MessageLifetime {
	value, _ := ctx.Value(messageLifetimeKey{}).(MessageLifetime)
	return value
}
