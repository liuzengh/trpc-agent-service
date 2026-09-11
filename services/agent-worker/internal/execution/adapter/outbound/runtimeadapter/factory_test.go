package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func runtimeFixture() (domain.Grant, domain.Plan) {
	digest := domain.Digest([]byte("manifest"))
	deadline := time.Now().Add(time.Minute)
	p := domain.Plan{TenantID: "tenant", ManifestID: "manifest", ManifestDigest: digest, DeploymentRevisionID: "revision", ProfileID: "profile", ProfileRevision: 3, NodeID: "root", Instruction: "instruction", ModelEndpoint: "https://model.invalid/v1", ModelName: "fixture-model", MaxOutputTokens: 20000, MaxRunSeconds: 30, ModelCredential: domain.CredentialUse{CredentialID: "crd_model", Purpose: "api_key", AudienceDigest: domain.Digest([]byte("model"))}, SessionCredential: domain.CredentialUse{CredentialID: "crd_session", Purpose: "dsn", AudienceDigest: domain.Digest([]byte("session"))}, SessionTarget: domain.StorageTarget{Host: "127.0.0.1", Port: 5432, Database: "agent_platform", Username: "session_runtime", SSLMode: "disable"}}
	g := domain.Grant{Run: domain.Run{Request: domain.Requested{RunID: "run", Route: domain.Route{TenantID: p.TenantID, ManifestRef: p.ManifestID, ManifestDigest: digest, DeploymentRevisionID: p.DeploymentRevisionID}, Input: domain.Input{Text: "Hello"}}, SessionID: "session", ExecutionDeadline: &deadline}, AttemptID: "attempt", WorkerID: "worker", LeaseEpoch: 2, Token: "execution-proof"}
	return g, p
}
func responseFixture(g domain.Grant, p domain.Plan) batchWire {
	return batchWire{TenantID: p.TenantID, ProfileID: p.ProfileID, ProfileRevision: p.ProfileRevision, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID, WorkerID: g.WorkerID, LeaseEpoch: g.LeaseEpoch, ManifestID: p.ManifestID, ManifestDigest: p.ManifestDigest, Credentials: []credentialWire{{CredentialID: p.ModelCredential.CredentialID, Purpose: p.ModelCredential.Purpose, AudienceDigest: p.ModelCredential.AudienceDigest, CredentialRevision: 4, Value: "fixture-api-key"}, {CredentialID: p.SessionCredential.CredentialID, Purpose: p.SessionCredential.Purpose, AudienceDigest: p.SessionCredential.AudienceDigest, CredentialRevision: 8, Value: "fixture-only"}}}
}
func newFixtureFactory(t *testing.T, handler http.HandlerFunc) (*Factory, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	f, err := New(Options{BaseURL: server.URL, Client: server.Client(), RequestTimeout: time.Second, MaxResponseBytes: 1 << 20, SnapshotCapacityBytes: 1 << 20, DrainTimeout: time.Second, MaxTrackedAttempts: 10})
	if err != nil {
		t.Fatal(err)
	}
	return f, server
}

type fakeStore struct {
	closed    bool
	candidate sessionstore.Candidate
	head      sessionstore.Head
	putErr    error
	puts      int
	loads     int
}

func (s *fakeStore) Load(context.Context, string, string, sessionstore.Head) (sessionstore.Candidate, error) {
	s.loads++
	return s.candidate, nil
}
func (s *fakeStore) Put(_ context.Context, c sessionstore.Candidate) (sessionstore.Head, error) {
	s.puts++
	s.candidate = c
	_, s.head, _ = c.Encode(1 << 20)
	return s.head, s.putErr
}
func (s *fakeStore) Close() { s.closed = true }
func TestFactoryExactCredentialBatchBeforeStore(t *testing.T) {
	g, p := runtimeFixture()
	var requests atomic.Int32
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/internal/v1/runtime-profiles/credentials/resolve" {
			t.Error(r.URL.Path)
		}
		var req resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.ExecutionToken != g.Token || req.ManifestID != p.ManifestID || len(req.Uses) != 2 {
			t.Errorf("invalid request=%+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseFixture(g, p))
	})
	store := &fakeStore{}
	checks := 0
	f.openStore = func(ctx context.Context, dsn string, target sessionstore.Target, capacity int) (candidateStore, error) {
		if checks != 2 {
			t.Errorf("constructor before post-batch fence: checks=%d", checks)
		}
		if !strings.Contains(dsn, "fixture-only") || target.Username != "session_runtime" || capacity != 1<<20 {
			t.Error("lost store configuration")
		}
		return store, nil
	}
	rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { checks++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	rt.Close()
	if !store.closed {
		t.Fatal("attempt-owned pool leaked")
	}
	if _, err = f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); !errors.Is(err, ErrAlreadyPrepared) {
		t.Fatalf("repeat=%v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("transparent repeat Resolve=%d", requests.Load())
	}
}
func TestFactoryRejectsWholeMalformedBatch(t *testing.T) {
	mutations := map[string]func(*batchWire){
		"tenant": func(b *batchWire) { b.TenantID = "other" }, "profile": func(b *batchWire) { b.ProfileID = "other" }, "profile revision": func(b *batchWire) { b.ProfileRevision++ }, "run": func(b *batchWire) { b.RunID = "other" }, "attempt": func(b *batchWire) { b.AttemptID = "other" }, "worker": func(b *batchWire) { b.WorkerID = "other" }, "epoch": func(b *batchWire) { b.LeaseEpoch++ }, "manifest": func(b *batchWire) { b.ManifestID = "other" }, "digest": func(b *batchWire) { b.ManifestDigest = domain.Digest([]byte("other")) }, "missing": func(b *batchWire) { b.Credentials = b.Credentials[:1] }, "extra": func(b *batchWire) {
			extra := b.Credentials[0]
			extra.CredentialID = "crd_extra"
			b.Credentials = append(b.Credentials, extra)
		}, "duplicate": func(b *batchWire) { b.Credentials[1] = b.Credentials[0] }, "purpose": func(b *batchWire) { b.Credentials[0].Purpose = "dsn" }, "audience": func(b *batchWire) { b.Credentials[0].AudienceDigest = domain.Digest([]byte("other")) }, "revision": func(b *batchWire) { b.Credentials[0].CredentialRevision = 0 }, "blank value": func(b *batchWire) { b.Credentials[0].Value = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			g, p := runtimeFixture()
			batch := responseFixture(g, p)
			mutate(&batch)
			var calls atomic.Int32
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(batch)
			})
			var opened bool
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
				opened = true
				return &fakeStore{}, nil
			}
			runtime, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if runtime != nil || !errors.Is(err, application.ErrCredentialDenied) || opened {
				t.Fatalf("runtime=%v opened=%t err=%v", runtime, opened, err)
			}
			if rt, repeat := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); rt != nil || !errors.Is(repeat, ErrAlreadyPrepared) {
				t.Fatalf("same rejected Attempt reused: runtime=%v err=%v", rt, repeat)
			}
			if calls.Load() != 1 || opened {
				t.Fatalf("batch rejection initialized a Store or retried Resolve: calls=%d opened=%t", calls.Load(), opened)
			}
			t.Log("WHOLE_BATCH_REJECTED=PASS zero_store_constructor=true one_resolve=true same_attempt_not_reused=true")
		})
	}
}
func TestFactoryResponseLossSingleFlightAndErrorClasses(t *testing.T) {
	for _, status := range []int{400, 401, 403, 409, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			g, p := runtimeFixture()
			var calls atomic.Int32
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"message":"credential-canary-must-not-return"}}`)
			})
			_, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			expected := application.ErrCredentialDenied
			if status == 429 || status >= 500 {
				expected = application.ErrDependency
			}
			if !errors.Is(err, expected) || strings.Contains(err.Error(), "canary") {
				t.Fatalf("err=%v", err)
			}
			if _, err = f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); !errors.Is(err, ErrAlreadyPrepared) {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("failed attempt transparently resolved again")
			}
		})
	}
	t.Run("lost response", func(t *testing.T) {
		g, p := runtimeFixture()
		var calls atomic.Int32
		f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		})
		f.options.RequestTimeout = 20 * time.Millisecond
		_, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
		if !errors.Is(err, application.ErrDependency) {
			t.Fatal(err)
		}
		_, err = f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
		if !errors.Is(err, ErrAlreadyPrepared) || calls.Load() != 1 {
			t.Fatalf("calls=%d err=%v", calls.Load(), err)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		g, p := runtimeFixture()
		var calls atomic.Int32
		f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) })
		var wg sync.WaitGroup
		for range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			}()
		}
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("single-flight calls=%d", calls.Load())
		}
	})
}
func TestFactoryNoConstructorAfterLostFenceAndNoRedirect(t *testing.T) {
	g, p := runtimeFixture()
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseFixture(g, p))
	})
	opened := false
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
		opened = true
		return &fakeStore{}, nil
	}
	checks := 0
	_, err := f.Prepare(context.Background(), g, p, func(context.Context) error {
		checks++
		if checks == 2 {
			return domain.ErrFenced
		}
		return nil
	})
	if !errors.Is(err, domain.ErrFenced) || opened {
		t.Fatalf("opened=%t err=%v", opened, err)
	}
	var redirected atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	f, _ = newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) })
	_, err = f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if err == nil || redirected.Load() != 0 {
		t.Fatal("credential request followed redirect")
	}
}
func TestBatchStrictJSON(t *testing.T) {
	g, p := runtimeFixture()
	body, _ := json.Marshal(responseFixture(g, p))
	for _, raw := range [][]byte{append(append([]byte(nil), body...), []byte(` {}`)...), []byte(strings.Replace(string(body), `"tenant_id":"tenant"`, `"tenant_id":"tenant","tenant_id":"tenant"`, 1)), []byte(strings.Replace(string(body), `"tenant_id"`, `"Tenant_ID"`, 1)), []byte(strings.Replace(string(body), `"value":"fixture-api-key"`, `"value":"fixture-api-key","value":"fixture-api-key"`, 1))} {
		var b batchWire
		if decodeBatch(raw, &b) == nil {
			t.Fatalf("invalid JSON accepted: %s", raw)
		}
	}
}
func TestAttemptCandidateUncertainWriteRechecksExactIdentity(t *testing.T) {
	g, p := runtimeFixture()
	body := []byte(`{"version":"fixture"}`)
	store := &fakeStore{putErr: errors.New("uncertain write credential-canary")}
	a := &attempt{grant: g, plan: p, store: store, check: func(context.Context) error { return nil }, capacity: 1 << 20, executed: true, resultDigest: domain.Digest(body)}
	candidate, err := a.Stage(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Ref == "" || !domain.DigestValid(candidate.Digest) || candidate.Parent != g.Parent || store.puts != 1 || store.loads != 1 {
		t.Fatalf("candidate=%+v store=%+v", candidate, store)
	}
	if _, err = a.Stage(context.Background(), []byte(`{"changed":true}`)); err == nil {
		t.Fatal("accepted arbitrary replacement snapshot")
	}
	a.Close()
}

func TestFactoryPreparationErrorIsExplicitAndSanitized(t *testing.T) {
	g, p := runtimeFixture()
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseFixture(g, p))
	})
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
		return nil, fmt.Errorf("credential-canary: %w", sessionstore.ErrPreparation)
	}
	_, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if !errors.Is(err, application.ErrSessionPreparation) || strings.Contains(err.Error(), "canary") {
		t.Fatalf("preparation=%v", err)
	}
}

type phaseObserver struct{ events []application.Observation }

func (o *phaseObserver) Observe(_ context.Context, event application.Observation) {
	o.events = append(o.events, event)
}

func TestFactoryObservesResolveAndSessionOpenWithoutCredentialMaterial(t *testing.T) {
	for _, failStore := range []bool{false, true} {
		t.Run(fmt.Sprint(failStore), func(t *testing.T) {
			g, p := runtimeFixture()
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(responseFixture(g, p))
			})
			observer := &phaseObserver{}
			f.options.Observer = observer
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
				if failStore {
					return nil, errors.New("postgres://session_runtime:private-password@private-host")
				}
				return &fakeStore{}, nil
			}
			rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if failStore && !errors.Is(err, application.ErrDependency) {
				t.Fatal("changed Session error classification")
			}
			if !failStore {
				if err != nil {
					t.Fatal(err)
				}
				rt.Close()
			}
			if len(observer.events) != 2 || observer.events[0].Operation != "credential_resolve" || observer.events[0].Result != "ok" || observer.events[1].Operation != "session_open" {
				t.Fatal("missing precise initialization phases")
			}
			want := "ok"
			if failStore {
				want = "dependency"
			}
			if observer.events[1].Result != want || observer.events[1].RunID != g.Run.Request.RunID {
				t.Fatal("incorrect stage classification")
			}
			b, err := json.Marshal(observer.events)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{g.Token, "fixture-api-key", "fixture-only", "private-password", "private-host", "postgres://"} {
				if strings.Contains(string(b), secret) {
					t.Fatal("credential material entered observation")
				}
			}
		})
	}
}
