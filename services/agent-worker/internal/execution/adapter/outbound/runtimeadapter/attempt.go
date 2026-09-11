package runtimeadapter

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/mcptoolset"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type attempt struct {
	nodeModelKeys     map[string]string
	sequenceKnowledge map[string]*knowledgestore.Store
	mcpServices       []*mcptoolset.Service
	knowledgeStore    *knowledgestore.Store
	artifactStore     *artifactstore.Store
	memoryStore       memoryStore
	memoryCandidate   *memorystore.Candidate
	memoryDigest      string
	finalText         string
	staged            domain.Candidate
	tracer            trace.Tracer
	grant             domain.Grant
	plan              domain.Plan
	store             candidateStore
	check             func(context.Context) error
	modelKey          string
	summaryKey        string
	executor          trpcagent.Executor
	capacity          int
	mu                sync.Mutex
	closed            bool
	executed          bool
	loaded            bool
	loadedDigest      string
	resultDigest      string
}

func (a *attempt) Load(ctx context.Context, head domain.Head) (history []byte, resultErr error) {
	ctx, span := telemetrytrace.Start(a.tracer, ctx, "worker.session.load")
	defer func() {
		if resultErr == nil {
			span.SetAttributes(attribute.Int("app.storage.bytes", len(history)))
		}
		telemetrytrace.End(span, resultErr)
	}()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || head != a.grant.Parent {
		return nil, application.ErrSessionInvalid
	}
	if err := a.check(ctx); err != nil {
		return nil, err
	}
	if head == (domain.Head{}) {
		a.loaded = true
		a.loadedDigest = domain.Digest(nil)
		return nil, nil
	}
	parentCtx := ctx
	ctx, cancel := sessionContext(ctx, a.plan)
	defer cancel()
	c, err := a.store.Load(ctx, a.plan.TenantID, a.grant.Run.SessionID, sessionstore.Head{Ref: head.Ref, Digest: head.Digest})
	if err != nil {
		return nil, sessionOperationError(parentCtx, ctx, err)
	}
	a.loaded = true
	a.loadedDigest = domain.Digest(c.Snapshot)
	return append([]byte(nil), c.Snapshot...), nil
}
func (a *attempt) Execute(ctx context.Context, history []byte) (domain.RuntimeResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.executed || !a.loaded || domain.Digest(history) != a.loadedDigest {
		return domain.RuntimeResult{}, application.ErrRuntimeFailed
	}
	a.executed = true
	if err := a.check(ctx); err != nil {
		return domain.RuntimeResult{}, err
	}
	p := a.plan
	g := a.grant
	request := trpcagent.Request{TenantID: p.TenantID, SessionID: g.Run.SessionID, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID, NodeID: p.NodeID, Instruction: p.Instruction, InputText: g.Run.Request.Input.Text, Model: trpcagent.Model{Endpoint: p.ModelEndpoint, Name: p.ModelName, APIKey: a.modelKey, Temperature: p.Temperature, MaxOutputTokens: p.NodeMaxOutputTokens}, MaxOutputTokens: p.MaxOutputTokens, MaxToolCalls: p.MaxToolCalls, AcceptedSnapshot: history}
	if len(a.mcpServices) != len(p.Tools) {
		return domain.RuntimeResult{}, application.ErrRuntimeFailed
	}
	for i, t := range p.Tools {
		request.Tools = append(request.Tools, trpcagent.MCPToolConfig{Resource: t.Resource, Capability: t.Capability, Tool: a.mcpServices[i].Tool()})
	}
	if p.Knowledge != nil {
		if a.knowledgeStore == nil {
			return domain.RuntimeResult{}, application.ErrRuntimeFailed
		}
		request.Knowledge = &trpcagent.KnowledgeConfig{Resource: p.Knowledge.Resource, Service: a.knowledgeStore}
	}
	if p.Artifact != nil {
		if a.artifactStore == nil {
			return domain.RuntimeResult{}, application.ErrRuntimeFailed
		}
		request.Artifact = &trpcagent.ArtifactConfig{Service: a.artifactStore, MaxBytes: p.Artifact.Backend.Limits.MaxBytes}
	}
	if p.Summary != nil {
		request.Summary = &trpcagent.SummaryConfig{Model: trpcagent.Model{Endpoint: p.Summary.ModelEndpoint, Name: p.Summary.ModelName, APIKey: a.summaryKey}, EventThreshold: p.Summary.EventThreshold, AddSessionSummary: p.Summary.AddSessionSummary}
	}
	if p.Memory != nil {
		scopeID, err := g.Run.Request.MemoryScopeID(p.Memory.AgentID)
		if err != nil || a.memoryStore == nil {
			return domain.RuntimeResult{}, application.ErrManifestInvalid
		}
		scope := memorystore.Scope{TenantID: p.TenantID, ID: scopeID}
		readCtx, cancel := context.WithTimeout(ctx, time.Duration(p.Memory.Backend.Limits.TimeoutMS)*time.Millisecond)
		readCtx, span := telemetrytrace.Start(a.tracer, readCtx, "worker.memory.load")
		saved, err := a.memoryStore.Load(readCtx, scope)
		telemetrytrace.End(span, memoryError(err))
		cancel()
		if err != nil {
			return domain.RuntimeResult{}, memoryError(err)
		}
		request.Memory = &trpcagent.MemoryConfig{BoundKey: scope.Key(), Entries: saved.Entries, BaseRevision: saved.Revision, Tools: p.Memory.Tools, PreloadLimit: p.Memory.PreloadLimit}
	}
	if err := a.populateSequence(&request); err != nil {
		return domain.RuntimeResult{}, err
	}
	result, err := a.executeWorkspace(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return domain.RuntimeResult{}, ctx.Err()
		}
		if errors.Is(err, trpcagent.ErrSnapshot) || errors.Is(err, trpcagent.ErrCapacity) || errors.Is(err, trpcagent.ErrOverlay) {
			return domain.RuntimeResult{}, application.ErrSessionInvalid
		}
		if errors.Is(err, trpcagent.ErrMCPAuthentication) {
			return domain.RuntimeResult{}, application.ErrCredentialDenied
		}
		if errors.Is(err, trpcagent.ErrRetryableModel) || errors.Is(err, trpcagent.ErrMCPDependency) {
			return domain.RuntimeResult{}, application.ErrDependency
		}
		return domain.RuntimeResult{}, application.ErrRuntimeFailed
	}
	var memoryTimeout time.Duration
	if p.Memory != nil {
		if result.Memory == nil {
			return domain.RuntimeResult{}, application.ErrRuntimeFailed
		}
		c := result.Memory
		a.memoryCandidate = &memorystore.Candidate{Scope: memorystore.Scope{TenantID: c.Scope.AppName, ID: c.Scope.UserID}, BaseRevision: c.BaseRevision, Entries: c.Entries}
		a.memoryDigest, err = a.memoryCandidate.Digest()
		if err != nil {
			return domain.RuntimeResult{}, application.ErrRuntimeFailed
		}
		memoryTimeout = time.Duration(p.Memory.Backend.Limits.TimeoutMS) * time.Millisecond
	}
	a.finalText = result.FinalText
	a.resultDigest = domain.Digest(result.Snapshot)
	return domain.RuntimeResult{Attachments: append([]domain.Attachment(nil), result.Attachments...), MemoryDigest: a.memoryDigest, MemoryTimeout: memoryTimeout, FinalText: result.FinalText, Snapshot: result.Snapshot, InputTokens: int64(result.Usage.InputTokens), OutputTokens: int64(result.Usage.OutputTokens), TotalTokens: int64(result.Usage.TotalTokens), UsageKnown: result.UsageKnown}, nil
}
func (a *attempt) Stage(ctx context.Context, snapshot []byte) (staged domain.Candidate, resultErr error) {
	ctx, span := telemetrytrace.Start(a.tracer, ctx, "worker.session.stage", trace.WithAttributes(attribute.Int("app.storage.bytes", len(snapshot))))
	defer func() {
		if resultErr == nil {
			span.SetAttributes(attribute.String("app.outcome", "candidate"))
		}
		telemetrytrace.End(span, resultErr)
	}()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || !a.executed || a.resultDigest == "" || domain.Digest(snapshot) != a.resultDigest {
		return domain.Candidate{}, application.ErrRuntimeFailed
	}
	if err := a.check(ctx); err != nil {
		return domain.Candidate{}, err
	}
	g := a.grant
	candidate := sessionstore.Candidate{Identity: sessionstore.Identity{TenantID: a.plan.TenantID, SessionID: g.Run.SessionID, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID}, Parent: sessionstore.Head{Ref: g.Parent.Ref, Digest: g.Parent.Digest}, ContentVersion: sessionstore.ContentVersion, Snapshot: snapshot}
	parentCtx := ctx
	ctx, cancel := sessionContext(ctx, a.plan)
	defer cancel()
	head, err := a.store.Put(ctx, candidate)
	if err != nil {
		if ctx.Err() != nil {
			return domain.Candidate{}, sessionOperationError(parentCtx, ctx, err)
		}
		if errors.Is(err, sessionstore.ErrConflict) || errors.Is(err, sessionstore.ErrCapacity) || errors.Is(err, sessionstore.ErrCorrupt) {
			return domain.Candidate{}, sessionError(ctx, err)
		}
		span.SetAttributes(attribute.String("app.outcome", "UNKNOWN"))
		// An insert may commit before its response is lost. Read only the exact
		// deterministic key/digest under the same still-current authorization batch.
		if e := a.check(ctx); e != nil {
			return domain.Candidate{}, e
		}
		_, expected, e := candidate.Encode(a.capacity)
		if e != nil {
			return domain.Candidate{}, application.ErrSessionInvalid
		}
		verifyCtx, verifySpan := telemetrytrace.Start(a.tracer, ctx, "worker.session.verify")
		_, e = a.store.Load(verifyCtx, a.plan.TenantID, g.Run.SessionID, expected)
		telemetrytrace.End(verifySpan, e)
		if e != nil {
			return domain.Candidate{}, sessionError(ctx, err)
		}
		head = expected
	}
	a.staged = domain.Candidate{Ref: head.Ref, Digest: head.Digest, Parent: g.Parent}
	return a.staged, nil
}
func (a *attempt) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	for _, s := range a.mcpServices {
		_ = s.Close()
	}
	a.mcpServices = nil
	for _, store := range a.sequenceKnowledge {
		store.Close()
	}
	a.sequenceKnowledge = nil
	a.nodeModelKeys = nil
	if a.knowledgeStore != nil {
		a.knowledgeStore.Close()
		a.knowledgeStore = nil
	}
	if a.artifactStore != nil {
		a.artifactStore.Close()
		a.artifactStore = nil
	}
	a.modelKey = ""
	a.summaryKey = ""
	if a.memoryStore != nil {
		a.memoryStore.Close()
		a.memoryStore = nil
	}
	a.memoryCandidate = nil
	a.store.Close()
	a.store = nil
}

var _ application.AttemptRuntime = (*attempt)(nil)

func sessionError(ctx context.Context, err error) error {
	if errors.Is(err, sessionstore.ErrIdentity) {
		return application.ErrSessionInvalid
	}
	return storeError(ctx, err)
}
