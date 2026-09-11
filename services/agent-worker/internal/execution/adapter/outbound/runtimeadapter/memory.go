package runtimeadapter

import (
	"context"
	"errors"
	"time"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type memoryStore interface {
	Load(context.Context, memorystore.Scope) (memorystore.Snapshot, error)
	ApplyAccepted(context.Context, memorystore.Accepted, memorystore.Candidate) (memorystore.Snapshot, error)
	Close()
}

func validateMemoryPlan(p domain.Plan) error {
	m := p.Memory
	if m == nil {
		return nil
	}
	b := m.Backend
	digest, err := b.Digest()
	if err != nil || b.ValidateForRole("memory") != nil || b.TenantID != p.TenantID || m.AgentID == "" || b.Limits.MaxBytes > int64(int(b.Limits.MaxBytes)) || p.MaxToolCalls < 1 {
		return application.ErrManifestInvalid
	}
	if (b.Kind == datav1.PostgreSQL && b.PostgreSQL.Username != "memory_runtime") || (b.Kind == datav1.Redis && b.Redis.Username != "memory_runtime") {
		return application.ErrManifestInvalid
	}
	if m.Credential.CredentialID == "" || m.Credential.Purpose != "dsn_password" || m.Credential.AudienceDigest != digest {
		return application.ErrManifestInvalid
	}
	cfg := protocol.ManifestMemory{Resource: "memory", Tools: m.Tools}
	limit := int64(m.PreloadLimit)
	cfg.PreloadLimit = &limit
	if cfg.Validate() != nil {
		return application.ErrManifestInvalid
	}
	return nil
}
func (f *Factory) prepareMemory(ctx context.Context, p domain.Plan, password string) (memoryStore, error) {
	if err := validateMemoryPlan(p); err != nil {
		return nil, err
	}
	b := p.Memory.Backend
	ctx, cancel := context.WithTimeout(ctx, time.Duration(b.Limits.TimeoutMS)*time.Millisecond)
	defer cancel()
	ctx, span := telemetrytrace.Start(f.options.Tracer, ctx, "worker.memory.open")
	var ms memoryStore
	var err error
	switch b.Kind {
	case datav1.PostgreSQL:
		t := b.PostgreSQL
		target := memorystore.Target{Host: t.Host, Port: uint16(t.Port), Database: t.Database, Username: t.Username, SSLMode: t.SSLMode, MaxConcurrency: int32(b.Limits.MaxConcurrency)}
		dsn, e := memorystore.CredentialDSN(target, password)
		if e != nil {
			err = memorystore.ErrIdentity
		} else {
			ms, err = f.openMemory(ctx, dsn, target, int(b.Limits.MaxBytes))
		}
	case datav1.Redis:
		t := b.Redis
		target := memorystore.RedisTarget{Host: t.Host, Port: uint16(t.Port), Database: int(t.Database), Username: t.Username, TLS: t.TLS, MaxConcurrency: int(b.Limits.MaxConcurrency)}
		ms, err = f.openRedisMemory(ctx, target, password, int(b.Limits.MaxBytes))
	default:
		err = memorystore.ErrIdentity
	}
	telemetrytrace.End(span, memoryError(err))
	return ms, memoryError(err)
}
func memoryError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, memorystore.ErrIdentity) {
		return application.ErrCredentialDenied
	}
	if errors.Is(err, memorystore.ErrCorrupt) || errors.Is(err, memorystore.ErrConflict) || errors.Is(err, memorystore.ErrCapacity) {
		return application.ErrRuntimeFailed
	}
	return application.ErrDependency
}

// ApplyAccepted uses the confirmed immutable Completion, not a now-ended lease.
// It does not release Final; the application calls the existing ledger after it.
func (a *attempt) ApplyAccepted(ctx context.Context, c domain.Completion) (err error) {
	ctx, span := telemetrytrace.Start(a.tracer, ctx, "worker.memory.apply_accepted")
	defer func() { telemetrytrace.End(span, err) }()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.memoryStore == nil || a.memoryCandidate == nil || a.staged.Ref == "" {
		return application.ErrRuntimeFailed
	}
	f := domain.Finish{Grant: a.grant, Status: domain.Succeeded, Candidate: a.staged, FinalText: a.finalText, MemoryDigest: a.memoryDigest}
	if c.TenantID != a.plan.TenantID || c.RunID != a.grant.Run.Request.RunID || c.AttemptID != a.grant.AttemptID || c.CompletionID != domain.StableID("cmp", c.RunID) || c.Kind != "ATTEMPT" || c.Status != domain.Succeeded || c.Candidate.Ref != a.staged.Ref || c.Candidate.Digest != a.staged.Digest || c.MemoryDigest != a.memoryDigest || c.ResultDigest != domain.FinishDigest(f) {
		return application.ErrRuntimeFailed
	}
	_, err = a.memoryStore.ApplyAccepted(ctx, memorystore.Accepted{CompletionID: c.CompletionID, RunID: c.RunID, AttemptID: c.AttemptID, CandidateDigest: a.memoryDigest}, *a.memoryCandidate)
	return memoryError(err)
}
