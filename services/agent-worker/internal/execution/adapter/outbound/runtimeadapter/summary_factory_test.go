package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func summaryFactoryFixture() (domain.Grant, domain.Plan) {
	g, p := runtimeFixture()
	p.Summary = &domain.SummaryPlan{ModelEndpoint: "https://summary.invalid/v1", ModelName: "summary-model", ModelCredential: domain.CredentialUse{CredentialID: "crd_summary", Purpose: "api_key", AudienceDigest: protocol.CredentialAudienceDigest("openai_compatible", "https://summary.invalid/v1")}, EventThreshold: 3, AddSessionSummary: true}
	return g, p
}
func summaryBatchFixture(g domain.Grant, p domain.Plan) batchWire {
	b := responseFixture(g, p)
	if p.Summary != nil && p.Summary.ModelCredential.CredentialID != "" && p.Summary.ModelCredential != p.ModelCredential {
		use := p.Summary.ModelCredential
		b.Credentials = append(b.Credentials, credentialWire{CredentialID: use.CredentialID, Purpose: use.Purpose, AudienceDigest: use.AudienceDigest, CredentialRevision: 2, Value: "fixture-summary-key"})
	}
	return b
}
func TestSummaryFactoryExactResolveAndClose(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "separate", true: "shared"}[shared], func(t *testing.T) {
			g, p := summaryFactoryFixture()
			if shared {
				p.ModelCredential.AudienceDigest = protocol.CredentialAudienceDigest("openai_compatible", p.ModelEndpoint)
				p.Summary.ModelEndpoint = p.ModelEndpoint
				p.Summary.ModelCredential = p.ModelCredential
			}
			expectedUses, err := requiredUses(p)
			if err != nil {
				t.Fatal(err)
			}
			if len(expectedUses) != map[bool]int{false: 3, true: 2}[shared] {
				t.Fatal("wrong dedupe")
			}
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				var request resolveRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				actual := make([]domain.CredentialUse, len(request.Uses))
				for i, use := range request.Uses {
					actual[i] = domain.CredentialUse{CredentialID: use.CredentialID, Purpose: use.Purpose, AudienceDigest: use.AudienceDigest}
				}
				if !reflect.DeepEqual(actual, expectedUses) || request.ManifestID != p.ManifestID || request.ExecutionToken != g.Token {
					t.Errorf("wrong summary credential batch %+v", request)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(summaryBatchFixture(g, p))
			})
			store := &fakeStore{}
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return store, nil }
			runtime, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			prepared := runtime.(*attempt)
			want := "fixture-summary-key"
			if shared {
				want = "fixture-api-key"
			}
			if prepared.summaryKey != want || prepared.plan.Summary == p.Summary {
				t.Fatal("summary key/binding not privately captured")
			}
			p.Summary.ModelEndpoint = "https://changed.invalid"
			p.Summary.ModelName = "changed"
			if prepared.plan.Summary.ModelName != "summary-model" {
				t.Fatal("mutable summary plan escaped")
			}
			runtime.Close()
			runtime.Close()
			if prepared.summaryKey != "" || prepared.modelKey != "" || prepared.store != nil || !store.closed {
				t.Fatal("Close retained keys/store")
			}
		})
	}
}
func TestSummaryFactoryRejectsInvalidPlanBeforeHTTP(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.Plan)
	}{
		{"threshold zero", func(p *domain.Plan) { p.Summary.EventThreshold = 0 }},
		{"threshold unsafe", func(p *domain.Plan) { p.Summary.EventThreshold = 9007199254740992 }},
		{"endpoint scheme", func(p *domain.Plan) { p.Summary.ModelEndpoint = "file:///tmp/key" }},
		{"endpoint userinfo", func(p *domain.Plan) { p.Summary.ModelEndpoint = "https://user:password@summary.invalid/v1" }},
		{"endpoint query", func(p *domain.Plan) { p.Summary.ModelEndpoint += "?" }},
		{"endpoint fragment", func(p *domain.Plan) { p.Summary.ModelEndpoint += "#" }},
		{"model blank", func(p *domain.Plan) { p.Summary.ModelName = " " }},
		{"purpose", func(p *domain.Plan) { p.Summary.ModelCredential.Purpose = "dsn" }},
		{"audience", func(p *domain.Plan) { p.Summary.ModelCredential.AudienceDigest = domain.Digest([]byte("other")) }},
		{"incomplete use", func(p *domain.Plan) { p.Summary.ModelCredential.CredentialID = "" }},
		{"same id conflicting binding", func(p *domain.Plan) { p.Summary.ModelCredential.CredentialID = p.ModelCredential.CredentialID }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, p := summaryFactoryFixture()
			tc.mutate(&p)
			var calls atomic.Int32
			f, _ := newFixtureFactory(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
				t.Error("opened store for invalid plan")
				return &fakeStore{}, nil
			}
			runtime, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if runtime != nil || !errors.Is(err, application.ErrManifestInvalid) || calls.Load() != 0 {
				t.Fatalf("invalid plan resolved: %v %v calls=%d", runtime, err, calls.Load())
			}
		})
	}
}
func TestSummaryFactoryRejectsMalformedBatchBeforeStore(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*batchWire)
	}{
		{"summary missing", func(b *batchWire) { b.Credentials = b.Credentials[:2] }},
		{"summary audience", func(b *batchWire) { b.Credentials[2].AudienceDigest = domain.Digest([]byte("other")) }},
		{"summary purpose", func(b *batchWire) { b.Credentials[2].Purpose = "dsn" }},
		{"summary id", func(b *batchWire) { b.Credentials[2].CredentialID = "other" }},
		{"summary empty key", func(b *batchWire) { b.Credentials[2].Value = "" }},
		{"summary revision", func(b *batchWire) { b.Credentials[2].CredentialRevision = 0 }},
		{"tenant", func(b *batchWire) { b.TenantID = "other" }},
		{"profile revision", func(b *batchWire) { b.ProfileRevision++ }},
		{"attempt", func(b *batchWire) { b.AttemptID = "other" }},
		{"worker", func(b *batchWire) { b.WorkerID = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, p := summaryFactoryFixture()
			batch := summaryBatchFixture(g, p)
			tc.mutate(&batch)
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(batch)
			})
			opened := false
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) {
				opened = true
				return &fakeStore{}, nil
			}
			runtime, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if runtime != nil || opened || !errors.Is(err, application.ErrCredentialDenied) {
				t.Fatalf("partial credential initialization: %v %v opened=%v", runtime, err, opened)
			}
		})
	}
}
func TestSummaryFactoryAnonymousSummaryAndLegacyUses(t *testing.T) {
	_, p := runtimeFixture()
	legacy := p.Uses()
	uses, err := requiredUses(p)
	if err != nil || !reflect.DeepEqual(legacy, uses) || len(uses) != 2 {
		t.Fatal("legacy changed", err)
	}
	_, p = summaryFactoryFixture()
	p.Summary.ModelCredential = domain.CredentialUse{}
	uses, err = requiredUses(p)
	if err != nil || len(uses) != 2 {
		t.Fatal("anonymous summary must not inherit model key", err)
	}
}

// Real HTTP model + real TLS credential resolution + SDK execution. Only the
// Session candidate store is a fake; this is not PostgreSQL acceptance.
func TestSummaryFactoryAttemptProviderErrorClassification(t *testing.T) {
	for _, code := range []int{http.StatusServiceUnavailable, http.StatusUnauthorized} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			g, p := summaryFactoryFixture()
			var summaryCalls, primaryCalls, resolves atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request["model"] == "summary-model" {
					summaryCalls.Add(1)
					if r.Header.Get("Authorization") != "Bearer fixture-summary-key" {
						t.Error("summary used wrong resolved credential")
					}
					if request["max_completion_tokens"] != float64(p.MaxOutputTokens) {
						t.Error("summary lost fixed per-output ceiling")
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(code)
					fmt.Fprint(w, `{"error":{"message":"private-summary-provider-body","type":"provider_error"}}`)
					return
				}
				primaryCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer fixture-api-key" {
					t.Error("primary used wrong resolved credential")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, `data: {"id":"response","object":"chat.completion.chunk","created":1,"model":"fixture-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Seed answer"},"finish_reason":null}]}`+"\n\n")
				fmt.Fprint(w, `data: {"id":"response","object":"chat.completion.chunk","created":1,"model":"fixture-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`+"\n\ndata: [DONE]\n\n")
			}))
			defer provider.Close()
			p.ModelEndpoint = provider.URL + "/v1"
			p.ModelCredential.AudienceDigest = protocol.CredentialAudienceDigest("openai_compatible", p.ModelEndpoint)
			p.Summary.ModelEndpoint = provider.URL + "/v1"
			p.Summary.ModelCredential.AudienceDigest = protocol.CredentialAudienceDigest("openai_compatible", p.Summary.ModelEndpoint)
			p.Summary.EventThreshold = 1
			executor := trpcagent.Executor{CapacityBytes: 1 << 20, DrainTimeout: time.Second}
			seed, err := executor.Execute(context.Background(), trpcagent.Request{TenantID: p.TenantID, SessionID: g.Run.SessionID, RunID: "seed-run", AttemptID: "seed-attempt", NodeID: p.NodeID, Instruction: p.Instruction, InputText: "Remember tea", Model: trpcagent.Model{Endpoint: p.ModelEndpoint, Name: p.ModelName, APIKey: "fixture-api-key"}, MaxOutputTokens: p.MaxOutputTokens})
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Session struct {
					Events []json.RawMessage `json:"events"`
				} `json:"session"`
			}
			if err = json.Unmarshal(seed.Snapshot, &decoded); err != nil || len(decoded.Session.Events) < 2 {
				t.Fatalf("legacy SDK did not produce accepted-history seed: events=%d err=%v", len(decoded.Session.Events), err)
			}
			g.Parent = domain.Head{Ref: "fixed-seed", Digest: domain.Digest(seed.Snapshot)}
			store := &fakeStore{candidate: sessionstore.Candidate{Snapshot: seed.Snapshot}}
			factory, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				resolves.Add(1)
				var request resolveRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if len(request.Uses) != 3 {
					t.Error("summary missing from initialization batch")
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(summaryBatchFixture(g, p))
			})
			factory.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return store, nil }
			runtime, err := factory.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			history, err := runtime.Load(context.Background(), g.Parent)
			if err != nil {
				t.Fatal(err)
			}
			result, err := runtime.Execute(context.Background(), history)
			want := application.ErrRuntimeFailed
			if code == http.StatusServiceUnavailable {
				want = application.ErrDependency
			}
			if !errors.Is(err, want) || errors.Is(err, application.ErrSessionInvalid) {
				t.Fatalf("provider HTTP %d classified as %v, want %v", code, err, want)
			}
			if len(result.Snapshot) != 0 || result.FinalText != "" || strings.Contains(err.Error(), "private-summary-provider-body") {
				t.Fatal("failure returned candidate/content/provider secrets")
			}
			if _, err = runtime.Stage(context.Background(), seed.Snapshot); !errors.Is(err, application.ErrRuntimeFailed) {
				t.Fatal("failed execution allowed candidate stage", err)
			}
			if summaryCalls.Load() != 1 || resolves.Load() != 1 || primaryCalls.Load() < 1 || store.loads != 1 || store.puts != 0 {
				t.Fatalf("unexpected execution: summary=%d resolver=%d primary=%d loads=%d puts=%d", summaryCalls.Load(), resolves.Load(), primaryCalls.Load(), store.loads, store.puts)
			}
		})
	}
}
