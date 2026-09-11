// Package runtimeadapter composes the credential protocol, immutable Session
// store and pinned SDK behind Execution-owned application interfaces.
package runtimeadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var ErrAlreadyPrepared = errors.New("attempt credential initialization already started")

type Options struct {
	ArtifactPool  *pgxpool.Pool
	ApprovalStore *toolapproval.Store
	Tracer        trace.Tracer
	BaseURL       string
	// Client is a borrowed mTLS client configured by bootstrap with trust roots
	// and the Workload certificate. The factory never closes the shared transport.
	Client                *http.Client
	RequestTimeout        time.Duration
	MaxResponseBytes      int64
	SnapshotCapacityBytes int
	DrainTimeout          time.Duration
	MaxTrackedAttempts    int
	Observer              application.Observer
}
type Factory struct {
	approvals        *toolapproval.Store
	openRedisSession func(context.Context, sessionstore.RedisTarget, string, int) (candidateStore, error)
	options          Options
	client           *http.Client
	mu               sync.Mutex
	started          map[attemptKey]time.Time
	openRedisMemory  func(context.Context, memorystore.RedisTarget, string, int) (memoryStore, error)
	openMemory       func(context.Context, string, memorystore.Target, int) (memoryStore, error)
	openStore        func(context.Context, string, sessionstore.Target, int) (candidateStore, error)
}
type attemptKey struct {
	TenantID, RunID, AttemptID string
	LeaseEpoch                 int64
}
type candidateStore interface {
	sessionstore.Store
	Close()
}

func New(o Options) (*Factory, error) {
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || o.Client == nil || o.RequestTimeout <= 0 || o.MaxResponseBytes <= 0 || o.SnapshotCapacityBytes <= 0 || o.DrainTimeout <= 0 || o.MaxTrackedAttempts <= 0 {
		return nil, domain.ErrInvalid
	}
	client := *o.Client
	// Never forward the execution token or an initialized credential batch to a
	// redirect target, including same-origin redirects.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	f := &Factory{options: o, client: &client, started: make(map[attemptKey]time.Time)}
	f.approvals = o.ApprovalStore
	f.openStore = func(ctx context.Context, dsn string, t sessionstore.Target, capacity int) (candidateStore, error) {
		return sessionstore.Open(ctx, dsn, t, capacity)
	}
	f.openMemory = func(ctx context.Context, dsn string, target memorystore.Target, capacity int) (memoryStore, error) {
		return memorystore.Open(ctx, dsn, target, capacity)
	}
	f.openRedisMemory = func(ctx context.Context, target memorystore.RedisTarget, password string, capacity int) (memoryStore, error) {
		return memorystore.OpenRedis(ctx, target, password, capacity)
	}
	f.openRedisSession = func(ctx context.Context, target sessionstore.RedisTarget, password string, capacity int) (candidateStore, error) {
		return sessionstore.OpenRedis(ctx, target, password, capacity)
	}
	return f, nil
}

func (f *Factory) Prepare(ctx context.Context, g domain.Grant, p domain.Plan, check func(context.Context) error) (prepared application.AttemptRuntime, resultErr error) {
	ctx, span := telemetrytrace.Start(f.options.Tracer, ctx, "worker.runtime.prepare", trace.WithAttributes(attribute.String("app.run.id", g.Run.Request.RunID), attribute.String("app.attempt.id", g.AttemptID), attribute.String("app.session.id", g.Run.SessionID)))
	defer func() { telemetrytrace.End(span, resultErr) }()
	if check == nil || g.Run.ExecutionDeadline == nil || g.Token == "" || g.AttemptID == "" || g.WorkerID == "" || g.LeaseEpoch <= 0 || p.TenantID != g.Run.Request.Route.TenantID || p.ManifestID != g.Run.Request.Route.ManifestRef || p.ManifestDigest != g.Run.Request.Route.ManifestDigest || p.DeploymentRevisionID != g.Run.Request.Route.DeploymentRevisionID || p.ProfileID == "" || p.ProfileRevision <= 0 {
		return nil, application.ErrManifestInvalid
	}
	p = cloneSequencePlan(p)
	p.Tools = append([]domain.ToolPlan(nil), p.Tools...)
	if p.Knowledge != nil {
		fixed := *p.Knowledge
		fixed.Backend = fixed.Backend.Clone()
		p.Knowledge = &fixed
	}
	if p.Artifact != nil {
		fixed := *p.Artifact
		fixed.Backend = fixed.Backend.Clone()
		p.Artifact = &fixed
	}
	if p.SessionBackend != nil {
		fixed := p.SessionBackend.Clone()
		p.SessionBackend = &fixed
	}
	if p.Memory != nil {
		fixed := *p.Memory
		fixed.Backend = fixed.Backend.Clone()
		fixed.Tools = append([]string(nil), fixed.Tools...)
		p.Memory = &fixed
	}
	if p.Summary != nil {
		fixed := *p.Summary
		p.Summary = &fixed
	}
	if err := check(ctx); err != nil {
		return nil, err
	}
	if err := f.start(g); err != nil {
		return nil, err
	}
	uses, err := requiredUses(p)
	if err != nil {
		return nil, err
	}
	resolveStart := time.Now()
	resolveCtx, resolveSpan := telemetrytrace.Start(f.options.Tracer, ctx, "worker.credential.resolve")
	batch, err := f.resolve(resolveCtx, g, p, uses)
	telemetrytrace.End(resolveSpan, err)
	f.observe(ctx, "credential_resolve", g, resolveStart, err)
	if err != nil {
		return nil, err
	}
	// This map only lives during initialization. Retain just the initialized
	// model value in the attempt; Session credentials live in its owned pool.
	defer func() {
		for key := range batch {
			delete(batch, key)
		}
	}()
	if err = check(ctx); err != nil {
		return nil, err
	}
	storeStart := time.Now()
	ctx, openSpan := telemetrytrace.Start(f.options.Tracer, ctx, "worker.session.open")
	defer func() { telemetrytrace.End(openSpan, resultErr) }()
	capacity := f.options.SnapshotCapacityBytes
	if p.SessionBackend != nil && int64(capacity) > p.SessionBackend.Limits.MaxBytes {
		capacity = int(p.SessionBackend.Limits.MaxBytes)
	}
	store, err := f.prepareSession(ctx, p, batch[p.SessionCredential], capacity)
	if err != nil {
		mapped := storeError(ctx, err)
		stage, _, _ := sessionstore.OpenFailure(err)
		f.observe(ctx, "session_open", g, storeStart, mapped, stage)
		return nil, mapped
	}
	f.observe(ctx, "session_open", g, storeStart, nil)
	summaryKey := ""
	if p.Summary != nil {
		summaryKey = batch[p.Summary.ModelCredential]
	}
	var ms memoryStore
	if p.Memory != nil {
		ms, err = f.prepareMemory(ctx, p, batch[p.Memory.Credential])
		if err != nil {
			store.Close()
			return nil, err
		}
	}
	var as *artifactstore.Store
	if p.Artifact != nil {
		as, err = f.prepareArtifact(ctx, g, p, batch)
		if err != nil {
			store.Close()
			if ms != nil {
				ms.Close()
			}
			return nil, err
		}
	}
	var ks *knowledgestore.Store
	if p.Knowledge != nil {
		ks, err = f.prepareKnowledge(ctx, p, batch)
		if err != nil {
			store.Close()
			if ms != nil {
				ms.Close()
			}
			if as != nil {
				as.Close()
			}
			return nil, err
		}
	}
	sequenceKnowledge, err := f.prepareSequenceKnowledge(ctx, p, batch)
	if err != nil {
		store.Close()
		if ms != nil {
			ms.Close()
		}
		if as != nil {
			as.Close()
		}
		if ks != nil {
			ks.Close()
		}
		return nil, err
	}
	nodeModelKeys := map[string]string{}
	for id, node := range p.Nodes {
		if node.Kind == "llm" {
			nodeModelKeys[id] = batch[node.ModelCredential]
		}
	}
	toolServices, err := f.prepareTools(ctx, g, p, batch)
	if err != nil {
		for _, s := range sequenceKnowledge {
			s.Close()
		}
		store.Close()
		if ms != nil {
			ms.Close()
		}
		if as != nil {
			as.Close()
		}
		if ks != nil {
			ks.Close()
		}
		return nil, err
	}
	return &attempt{nodeModelKeys: nodeModelKeys, sequenceKnowledge: sequenceKnowledge, mcpServices: toolServices, knowledgeStore: ks, artifactStore: as, memoryStore: ms, summaryKey: summaryKey, tracer: f.options.Tracer, grant: g, plan: p, store: store, check: check, modelKey: batch[p.ModelCredential], executor: trpcagent.Executor{Tracer: f.options.Tracer, Approvals: f.approvals, CapacityBytes: capacity, DrainTimeout: f.options.DrainTimeout}, capacity: capacity}, nil
}
func (f *Factory) observe(ctx context.Context, operation string, g domain.Grant, start time.Time, err error, stages ...string) {
	if f.options.Observer != nil {
		stage := ""
		if len(stages) == 1 {
			stage = stages[0]
		}
		f.options.Observer.Observe(ctx, application.Observation{Stage: stage, Operation: operation, Result: application.ObservationResult(err), TenantID: g.Run.Request.Route.TenantID, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID, Duration: time.Since(start)})
	}
}
func (f *Factory) start(g domain.Grant) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	for key, until := range f.started {
		if now.After(until) {
			delete(f.started, key)
		}
	}
	key := attemptKey{g.Run.Request.Route.TenantID, g.Run.Request.RunID, g.AttemptID, g.LeaseEpoch}
	if _, ok := f.started[key]; ok {
		return ErrAlreadyPrepared
	}
	if len(f.started) >= f.options.MaxTrackedAttempts {
		return application.ErrDependency
	}
	if !g.Run.ExecutionDeadline.After(now) {
		return context.DeadlineExceeded
	}
	// Keep the tombstone on failure and on Close. A new Attempt is required after
	// response loss; the original Attempt must not silently mix credential values.
	f.started[key] = *g.Run.ExecutionDeadline
	return nil
}

func requiredUses(p domain.Plan) ([]domain.CredentialUse, error) {
	if err := validateSequencePlan(p); err != nil {
		return nil, err
	}
	if err := validateToolPlans(p); err != nil {
		return nil, err
	}
	if err := validateKnowledgePlan(p); err != nil {
		return nil, err
	}
	if err := validateArtifactPlan(p); err != nil {
		return nil, err
	}
	if err := validateSessionPlan(p); err != nil {
		return nil, err
	}
	validModelUse := func(use domain.CredentialUse) bool {
		if use.CredentialID == "" {
			return use.Purpose == "" && use.AudienceDigest == ""
		}
		return use.Purpose == "api_key" && domain.DigestValid(use.AudienceDigest)
	}
	if !validModelUse(p.ModelCredential) {
		return nil, application.ErrManifestInvalid
	}
	if p.Summary != nil {
		summary := p.Summary
		endpoint, err := url.Parse(summary.ModelEndpoint)
		if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || strings.Contains(summary.ModelEndpoint, "#") || strings.TrimSpace(summary.ModelName) == "" || summary.EventThreshold < 1 || summary.EventThreshold > 9007199254740991 || int64(int(summary.EventThreshold)) != summary.EventThreshold || !validModelUse(summary.ModelCredential) {
			return nil, application.ErrManifestInvalid
		}
		if summary.ModelCredential.CredentialID != "" && summary.ModelCredential.AudienceDigest != protocol.CredentialAudienceDigest("openai_compatible", summary.ModelEndpoint) {
			return nil, application.ErrManifestInvalid
		}
	}
	if p.Memory != nil {
		if err := validateMemoryPlan(p); err != nil {
			return nil, err
		}
	}
	uses := p.Uses()
	seen := map[string]domain.CredentialUse{}
	for _, use := range uses {
		if !domain.DigestValid(use.AudienceDigest) {
			return nil, application.ErrManifestInvalid
		}
		if previous, ok := seen[use.CredentialID]; ok && previous != use {
			return nil, application.ErrManifestInvalid
		}
		seen[use.CredentialID] = use
	}
	return uses, nil
}

type useWire struct {
	CredentialID   string `json:"credential_id"`
	Purpose        string `json:"purpose"`
	AudienceDigest string `json:"audience_digest"`
}
type resolveRequest struct {
	ExecutionToken string    `json:"execution_token"`
	ManifestID     string    `json:"manifest_id"`
	ManifestDigest string    `json:"manifest_digest"`
	Uses           []useWire `json:"uses"`
}
type credentialWire struct {
	CredentialID       string `json:"credential_id"`
	Purpose            string `json:"purpose"`
	AudienceDigest     string `json:"audience_digest"`
	CredentialRevision int64  `json:"credential_revision"`
	Value              string `json:"value"`
}
type batchWire struct {
	TenantID        string           `json:"tenant_id"`
	ProfileID       string           `json:"profile_id"`
	ProfileRevision int64            `json:"profile_revision_number"`
	RunID           string           `json:"run_id"`
	AttemptID       string           `json:"attempt_id"`
	WorkerID        string           `json:"worker_id"`
	LeaseEpoch      int64            `json:"lease_epoch"`
	ManifestID      string           `json:"manifest_id"`
	ManifestDigest  string           `json:"manifest_digest"`
	Credentials     []credentialWire `json:"credentials"`
}

func (f *Factory) resolve(ctx context.Context, g domain.Grant, p domain.Plan, uses []domain.CredentialUse) (map[domain.CredentialUse]string, error) {
	values := make([]useWire, 0, len(uses))
	for _, use := range uses {
		values = append(values, useWire{use.CredentialID, use.Purpose, use.AudienceDigest})
	}
	body, err := json.Marshal(resolveRequest{g.Token, p.ManifestID, p.ManifestDigest, values})
	if err != nil {
		return nil, application.ErrManifestInvalid
	}
	defer clear(body)
	requestCtx, cancel := context.WithTimeout(ctx, f.options.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(f.options.BaseURL, "/")+"/internal/v1/runtime-profiles/credentials/resolve", bytes.NewReader(body))
	if err != nil {
		return nil, application.ErrDependency
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := f.client.Do(req)
	if err != nil {
		return nil, dependencyError(ctx)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return nil, dependencyError(ctx)
		}
		return nil, application.ErrCredentialDenied
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, application.ErrCredentialDenied
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, f.options.MaxResponseBytes+1))
	defer clear(raw)
	if err != nil {
		return nil, dependencyError(ctx)
	}
	if int64(len(raw)) > f.options.MaxResponseBytes {
		return nil, application.ErrCredentialDenied
	}
	var batch batchWire
	if err = decodeBatch(raw, &batch); err != nil {
		return nil, application.ErrCredentialDenied
	}
	if batch.TenantID != p.TenantID || batch.ProfileID != p.ProfileID || batch.ProfileRevision != p.ProfileRevision || batch.RunID != g.Run.Request.RunID || batch.AttemptID != g.AttemptID || batch.WorkerID != g.WorkerID || batch.LeaseEpoch != g.LeaseEpoch || batch.ManifestID != p.ManifestID || batch.ManifestDigest != p.ManifestDigest || len(batch.Credentials) != len(uses) {
		return nil, application.ErrCredentialDenied
	}
	expected := map[domain.CredentialUse]bool{}
	for _, use := range uses {
		expected[use] = true
	}
	out := make(map[domain.CredentialUse]string, len(uses))
	for i, c := range batch.Credentials {
		use := domain.CredentialUse{CredentialID: c.CredentialID, Purpose: c.Purpose, AudienceDigest: c.AudienceDigest}
		if !expected[use] || c.CredentialRevision <= 0 || strings.TrimSpace(c.Value) == "" || strings.ContainsAny(c.Value, "\x00\r\n") {
			return nil, application.ErrCredentialDenied
		}
		if _, exists := out[use]; exists {
			return nil, application.ErrCredentialDenied
		}
		out[use] = c.Value
		batch.Credentials[i].Value = ""
	}
	return out, nil
}
func dependencyError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return application.ErrDependency
}
func storeError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch {
	case errors.Is(err, sessionstore.ErrPreparation):
		return application.ErrSessionPreparation
	case errors.Is(err, sessionstore.ErrIdentity):
		return application.ErrCredentialDenied
	case errors.Is(err, sessionstore.ErrNotFound), errors.Is(err, sessionstore.ErrCorrupt), errors.Is(err, sessionstore.ErrConflict), errors.Is(err, sessionstore.ErrCapacity):
		return application.ErrSessionInvalid
	default:
		return application.ErrDependency
	}
}

var _ application.RuntimeFactory = (*Factory)(nil)
