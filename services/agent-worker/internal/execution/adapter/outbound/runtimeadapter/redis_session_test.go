package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/http"
	"strings"
	"testing"
)

func redisSessionFactoryFixture() (domain.Grant, domain.Plan) {
	g, p := runtimeFixture()
	p.SessionTarget = domain.StorageTarget{}
	p.SessionBackend = &datav1.Snapshot{SchemaVersion: "v1", TenantID: p.TenantID, BackendID: "redis-session", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.SessionIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 2, MaxBytes: 2048}, Redis: &datav1.RedisTarget{Host: "session.invalid", Port: 6379, Username: "session_runtime", Database: 2, TLS: true}}
	p.SessionCredential.Purpose = "dsn_password"
	p.SessionCredential.AudienceDigest, _ = p.SessionBackend.Digest()
	return g, p
}

type missingRedisSession struct{ fakeStore }

func (s *missingRedisSession) Load(ctx context.Context, _ string, _ string, _ sessionstore.Head) (sessionstore.Candidate, error) {
	s.loads++
	if _, ok := ctx.Deadline(); !ok {
		return sessionstore.Candidate{}, errors.New("missing operation timeout")
	}
	return sessionstore.Candidate{}, sessionstore.ErrNotFound
}
func TestRedisSessionFactoryFixedTargetMissingParentAndClose(t *testing.T) {
	g, p := redisSessionFactoryFixture()
	g.Parent = domain.Head{Ref: "sc1_" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("b", 64)}
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		var req resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if len(req.Uses) != 2 || req.Uses[1].Purpose != "dsn_password" || req.Uses[1].AudienceDigest != p.SessionCredential.AudienceDigest {
			t.Error("wrong credential closure")
		}
		b := responseFixture(g, p)
		b.Credentials[1].Value = "redis://evil.invalid/0"
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b)
	})
	store := &missingRedisSession{}
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
		t.Error("PG fallback")
		return nil, errors.New("wrong store")
	}
	f.openRedisSession = func(ctx context.Context, target sessionstore.RedisTarget, password string, capacity int) (candidateStore, error) {
		if target.Host != "session.invalid" || target.Port != 6379 || target.Database != 2 || target.Username != "session_runtime" || !target.TLS || target.MaxConcurrency != 2 || password != "redis://evil.invalid/0" || capacity != 2048 {
			t.Error("fixed target/capacity changed")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing open timeout")
		}
		return store, nil
	}
	rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p.SessionBackend.Redis.Host = "changed"
	a := rt.(*attempt)
	if a.plan.SessionBackend.Redis.Host != "session.invalid" || a.capacity != 2048 {
		t.Fatal("mutable backend")
	}
	if out, err := rt.Load(context.Background(), g.Parent); out != nil || !errors.Is(err, application.ErrSessionInvalid) {
		t.Fatalf("missing accepted Parent: %s %v", out, err)
	}
	if _, err := rt.Execute(context.Background(), nil); !errors.Is(err, application.ErrRuntimeFailed) {
		t.Fatal("failed Load reached SDK")
	}
	if store.loads != 1 || store.puts != 0 || a.loaded {
		t.Fatal("missing Parent became empty/latest session")
	}
	rt.Close()
	rt.Close()
	if !store.closed {
		t.Fatal("leak")
	}
}
func TestRedisSessionPlanRejectBeforeResolve(t *testing.T) {
	for name, change := range map[string]func(*domain.Plan){
		"host": func(p *domain.Plan) { p.SessionBackend.Redis.Host = "changed" },
		"db":   func(p *domain.Plan) { p.SessionBackend.Redis.Database = 0 },
		"tls":  func(p *domain.Plan) { p.SessionBackend.Redis.TLS = false },
		"user": func(p *domain.Plan) {
			p.SessionBackend.Redis.Username = "memory_runtime"
			p.SessionCredential.AudienceDigest, _ = p.SessionBackend.Digest()
		},
		"legacy_target": func(p *domain.Plan) { p.SessionTarget.Host = "old.pg" },
		"nil_redis":     func(p *domain.Plan) { p.SessionBackend.Redis = nil },
		"tenant":        func(p *domain.Plan) { p.SessionBackend.TenantID = "other" },
		"role":          func(p *domain.Plan) { p.SessionBackend.Isolation = datav1.MemoryIsolation },
		"purpose":       func(p *domain.Plan) { p.SessionCredential.Purpose = "dsn" },
	} {
		t.Run(name, func(t *testing.T) {
			g, p := redisSessionFactoryFixture()
			change(&p)
			f, _ := newFixtureFactory(t, func(http.ResponseWriter, *http.Request) { t.Error("invalid plan resolved") })
			if rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); rt != nil || !errors.Is(err, application.ErrManifestInvalid) {
				t.Fatalf("%v %v", rt, err)
			}
		})
	}
}
func TestRedisSessionOperationDeadlineClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(sessionOperationError(context.Background(), ctx, context.Canceled), application.ErrDependency) {
		t.Fatal("operation timeout not retryable")
	}
	if !errors.Is(sessionOperationError(ctx, ctx, context.Canceled), context.Canceled) {
		t.Fatal("outer cancellation lost")
	}
	if !errors.Is(sessionError(context.Background(), sessionstore.ErrNotFound), application.ErrSessionInvalid) {
		t.Fatal("missing parent classified transient")
	}
}

type slowRedisSession struct{ fakeStore }

func (s *slowRedisSession) Load(ctx context.Context, _ string, _ string, _ sessionstore.Head) (sessionstore.Candidate, error) {
	<-ctx.Done()
	return sessionstore.Candidate{}, ctx.Err()
}
func (s *slowRedisSession) Put(ctx context.Context, _ sessionstore.Candidate) (sessionstore.Head, error) {
	<-ctx.Done()
	return sessionstore.Head{}, ctx.Err()
}
func TestRedisSessionSlowOperationsRemainRetryable(t *testing.T) {
	g, p := redisSessionFactoryFixture()
	p.SessionBackend.Limits.TimeoutMS = 1
	p.SessionCredential.AudienceDigest, _ = p.SessionBackend.Digest()
	f := &Factory{openRedisSession: func(ctx context.Context, _ sessionstore.RedisTarget, _ string, _ int) (candidateStore, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	if _, err := f.prepareSession(context.Background(), p, "password", 2048); !errors.Is(err, application.ErrDependency) {
		t.Fatalf("open timeout: %v", err)
	}
	g.Parent = domain.Head{Ref: "sc1_" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("b", 64)}
	a := &attempt{plan: p, grant: g, store: &slowRedisSession{}, check: func(ctx context.Context) error { return ctx.Err() }, capacity: 2048}
	if _, err := a.Load(context.Background(), g.Parent); !errors.Is(err, application.ErrDependency) {
		t.Fatalf("load timeout: %v", err)
	}
	snapshot := []byte(`{"events":[]}`)
	a.executed = true
	a.resultDigest = domain.Digest(snapshot)
	if _, err := a.Stage(context.Background(), snapshot); !errors.Is(err, application.ErrDependency) {
		t.Fatalf("stage timeout: %v", err)
	}
}
