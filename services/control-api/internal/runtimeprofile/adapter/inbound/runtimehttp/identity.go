// Package runtimehttp exposes the Profile owner's internal credential resolution
// boundary. It requires trusted Worker authentication and online execution authorization.
package runtimehttp

import "context"

type workerIdentityKey struct{}

// WithWorkerIdentity is for trusted workload-authentication middleware only.
// HTTP headers and request bodies are never interpreted as Worker identity here.
func WithWorkerIdentity(ctx context.Context, workerID string) context.Context {
	return context.WithValue(ctx, workerIdentityKey{}, workerID)
}

func workerIdentity(ctx context.Context) (string, bool) {
	workerID, ok := ctx.Value(workerIdentityKey{}).(string)
	return workerID, ok && workerID != ""
}
