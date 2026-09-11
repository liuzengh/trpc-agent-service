package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func memoryFactoryFixture() (domain.Grant, domain.Plan) {
	g, p := runtimeFixture()
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: p.TenantID, BackendID: "pg-memory", BackendRevision: 1, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1 << 20}, PostgreSQL: &datav1.PostgresTarget{Host: "memory.invalid", Port: 5432, Database: "memory", Username: "memory_runtime", SSLMode: "require"}}
	digest, _ := b.Digest()
	p.Memory = &domain.MemoryPlan{AgentID: "agent-a", Backend: b, Credential: domain.CredentialUse{CredentialID: "crd_memory", Purpose: "dsn_password", AudienceDigest: digest}, Tools: []string{"memory_add", "memory_load"}}
	p.MaxToolCalls = 16
	return g, p
}

type fakeMemoryStore struct {
	closed  bool
	applied int
	err     error
}

func (*fakeMemoryStore) Load(context.Context, memorystore.Scope) (memorystore.Snapshot, error) {
	return memorystore.Snapshot{}, nil
}
func (s *fakeMemoryStore) ApplyAccepted(context.Context, memorystore.Accepted, memorystore.Candidate) (memorystore.Snapshot, error) {
	s.applied++
	return memorystore.Snapshot{}, s.err
}
func (s *fakeMemoryStore) Close() { s.closed = true }
func TestMemoryFactoryFixedCredentialAndLifecycle(t *testing.T) {
	g, p := memoryFactoryFixture()
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		var request resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if len(request.Uses) != 3 || request.Uses[2].Purpose != "dsn_password" || request.Uses[2].AudienceDigest != p.Memory.Credential.AudienceDigest {
			t.Error("wrong immutable credential closure")
		}
		b := responseFixture(g, p)
		u := p.Memory.Credential
		b.Credentials = append(b.Credentials, credentialWire{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest, CredentialRevision: 1, Value: "postgres://not-a-destination"})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b)
	})
	ss := &fakeStore{}
	ms := &fakeMemoryStore{}
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return ss, nil }
	f.openMemory = func(ctx context.Context, dsn string, target memorystore.Target, capacity int) (memoryStore, error) {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		password, _ := u.User.Password()
		if u.Hostname() != "memory.invalid" || target.Username != "memory_runtime" || password != "postgres://not-a-destination" || capacity != 1<<20 {
			t.Error("secret altered fixed backend target")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing fixed timeout")
		}
		return ms, nil
	}
	rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p.Memory.Tools[0] = "other"
	p.Memory.Backend.PostgreSQL.Host = "changed.invalid"
	a := rt.(*attempt)
	if a.plan.Memory.Tools[0] != "memory_add" || a.plan.Memory.Backend.PostgreSQL.Host != "memory.invalid" {
		t.Fatal("mutable Plan escaped")
	}
	rt.Close()
	rt.Close()
	if !ss.closed || !ms.closed || a.memoryStore != nil {
		t.Fatal("resources retained")
	}
}
func TestMemoryFactoryRejectBeforeResolve(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Plan){
		"audience": func(p *domain.Plan) { p.Memory.Credential.AudienceDigest = domain.Digest(nil) },
		"purpose":  func(p *domain.Plan) { p.Memory.Credential.Purpose = "dsn" },
		"tenant":   func(p *domain.Plan) { p.Memory.Backend.TenantID = "different" },
		"role":     func(p *domain.Plan) { p.Memory.Backend.PostgreSQL.Username = "session_runtime" },
		"agent":    func(p *domain.Plan) { p.Memory.AgentID = "" },
		"disabled": func(p *domain.Plan) { p.Memory.Tools = nil },
		"tool":     func(p *domain.Plan) { p.Memory.Tools = []string{"web.search"} },
		"budget":   func(p *domain.Plan) { p.MaxToolCalls = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			g, p := memoryFactoryFixture()
			mutate(&p)
			f, _ := newFixtureFactory(t, func(http.ResponseWriter, *http.Request) { t.Error("invalid plan resolved credentials") })
			if rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); rt != nil || !errors.Is(err, application.ErrManifestInvalid) {
				t.Fatalf("%v %v", rt, err)
			}
		})
	}
}
func TestMemoryApplyOnlyExactAcceptedCompletion(t *testing.T) {
	g, p := memoryFactoryFixture()
	s := &fakeMemoryStore{}
	candidate := memorystore.Candidate{Scope: memorystore.Scope{TenantID: p.TenantID, ID: "scope"}}
	digest, err := candidate.Digest()
	if err != nil {
		t.Fatal(err)
	}
	staged := domain.Candidate{Ref: "candidate", Digest: domain.Digest([]byte("session")), Parent: g.Parent}
	a := &attempt{grant: g, plan: p, memoryStore: s, memoryCandidate: &candidate, memoryDigest: digest, staged: staged, finalText: "saved"}
	f := domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: staged, FinalText: a.finalText, MemoryDigest: digest}
	c := domain.Completion{TenantID: p.TenantID, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID, CompletionID: domain.StableID("cmp", g.Run.Request.RunID), Kind: "ATTEMPT", Status: domain.Succeeded, Candidate: domain.Head{Ref: staged.Ref, Digest: staged.Digest}, MemoryDigest: digest, ResultDigest: domain.FinishDigest(f)}
	for name, mutate := range map[string]func(*domain.Completion){"attempt": func(c *domain.Completion) { c.AttemptID = "other" }, "tenant": func(c *domain.Completion) { c.TenantID = "other" }, "failed": func(c *domain.Completion) { c.Status = domain.Failed }, "digest": func(c *domain.Completion) { c.MemoryDigest = domain.Digest(nil) }, "result": func(c *domain.Completion) { c.ResultDigest = domain.Digest(nil) }} {
		t.Run(name, func(t *testing.T) {
			bad := c
			mutate(&bad)
			if a.ApplyAccepted(context.Background(), bad) == nil {
				t.Fatal("accepted wrong completion")
			}
		})
	}
	if s.applied != 0 {
		t.Fatal("unaccepted attempt applied memory")
	}
	if err := a.ApplyAccepted(context.Background(), c); err != nil || s.applied != 1 {
		t.Fatalf("valid apply %v %d", err, s.applied)
	}
	s.err = memorystore.ErrConflict
	if err := a.ApplyAccepted(context.Background(), c); !errors.Is(err, application.ErrRuntimeFailed) {
		t.Fatalf("persistence failure swallowed: %v", err)
	}
}
