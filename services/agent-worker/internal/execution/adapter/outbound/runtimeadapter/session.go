package runtimeadapter

import (
	"context"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"time"
)

func validateSessionPlan(p domain.Plan) error {
	u := p.SessionCredential
	if u.CredentialID == "" {
		return application.ErrManifestInvalid
	}
	if p.SessionBackend == nil {
		if u.Purpose != "dsn" {
			return application.ErrManifestInvalid
		}
		return nil
	}
	b := p.SessionBackend
	d, err := b.Digest()
	if err != nil || b.ValidateForRole("session") != nil || b.Kind != datav1.Redis || b.TenantID != p.TenantID || b.Redis.Username != "session_runtime" || b.Limits.MaxBytes > int64(int(b.Limits.MaxBytes)) || p.SessionTarget != (domain.StorageTarget{}) || u.Purpose != "dsn_password" || u.AudienceDigest != d {
		return application.ErrManifestInvalid
	}
	return nil
}
func sessionContext(ctx context.Context, p domain.Plan) (context.Context, context.CancelFunc) {
	if p.SessionBackend != nil {
		return context.WithTimeout(ctx, time.Duration(p.SessionBackend.Limits.TimeoutMS)*time.Millisecond)
	}
	return ctx, func() {}
}
func (f *Factory) prepareSession(ctx context.Context, p domain.Plan, password string, capacity int) (candidateStore, error) {
	if p.SessionBackend != nil {
		parent := ctx
		ctx, cancel := sessionContext(ctx, p)
		defer cancel()
		b := p.SessionBackend
		t := b.Redis
		store, err := f.openRedisSession(ctx, sessionstore.RedisTarget{Host: t.Host, Port: uint16(t.Port), Database: int(t.Database), Username: t.Username, TLS: t.TLS, MaxConcurrency: int(b.Limits.MaxConcurrency)}, password, capacity)
		if err != nil && parent.Err() == nil && ctx.Err() != nil {
			return nil, application.ErrDependency
		}
		return store, err
	}
	t := p.SessionTarget
	target := sessionstore.Target{Host: t.Host, Port: t.Port, Database: t.Database, Username: t.Username, SSLMode: t.SSLMode}
	dsn, err := sessionstore.CredentialDSN(target, password)
	if err != nil {
		return nil, err
	}
	return f.openStore(ctx, dsn, target, capacity)
}

// A backend operation budget is not the entire Attempt's execution deadline.
// Local timeout remains retryable while caller cancellation retains ownership.
func sessionOperationError(parent, operation context.Context, err error) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	if operation.Err() != nil {
		return application.ErrDependency
	}
	return sessionError(parent, err)
}
