package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	controlevents "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/manifestadapter"
	ledgerpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/runtimeadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifestwire "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/inbound/wire"
	projectionpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// The authenticated publication/Profile HTTP providers are fixtures. Reader,
// Processor, Factory, SDK HTTP, Stage, Complete and next Claim/Load are real.
// Neither Plan nor accepted candidate selection is supplied by this test.
func TestWorkerSummaryManifestFactoryPostgresSDK(t *testing.T) {
	keys := []string{"WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SUMMARY_TEST_MIGRATION_URL", "WORKER_SUMMARY_TEST_RUNTIME_URL"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("requires isolated Worker/Session PostgreSQL fixture")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	open := func(key string) *pgxpool.Pool {
		pool, err := pgxpool.New(ctx, os.Getenv(key))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	wm, worker, sm := open(keys[0]), open(keys[1]), open(keys[2])
	if err := migrations.ApplyForRuntime(ctx, wm, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	if err := sessionmigrations.Apply(ctx, sm); err != nil {
		t.Fatal(err)
	}
	sessionConfig, err := pgxpool.ParseConfig(os.Getenv(keys[3]))
	if err != nil {
		t.Fatal(err)
	}
	ledger := ledgerpg.New(worker)
	var failSummary atomic.Bool
	var mu sync.Mutex
	var mainRequests, summaryRequests []map[string]any
	var resolveAttempts []string
	mainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer main-summary-vertical-key" {
			t.Error("main endpoint/credential mismatch")
			http.Error(w, "denied", 403)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req["model"] != "vertical-main" || req["max_completion_tokens"] != float64(20000) {
			t.Error("main published model/generation changed")
		}
		mu.Lock()
		mainRequests = append(mainRequests, req)
		ordinal := len(mainRequests)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"id\":\"main\",\"object\":\"chat.completion.chunk\",\"model\":\"vertical-main\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%q},\"finish_reason\":null}]}\n\n", fmt.Sprintf("Main answer %d", ordinal))
		fmt.Fprint(w, "data: {\"id\":\"main\",\"object\":\"chat.completion.chunk\",\"model\":\"vertical-main\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer mainServer.Close()
	summaryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer separate-summary-vertical-key" {
			t.Error("summary endpoint/credential mismatch")
			http.Error(w, "denied", 403)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req["model"] != "vertical-summarizer" || req["stream"] == true || req["max_completion_tokens"] != float64(20000) {
			t.Error("summary fixed model/generation changed")
		}
		mu.Lock()
		summaryRequests = append(summaryRequests, req)
		ordinal := len(summaryRequests)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if failSummary.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":{"message":"UNACCEPTED summary provider failure","type":"authentication_error"}}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "summary", "object": "chat.completion", "model": "vertical-summarizer", "choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": fmt.Sprintf("Accepted summary preference marker %d", ordinal)}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}})
	}))
	defer summaryServer.Close()
	publication, content := summaryVerticalManifest(t, mainServer.URL, summaryServer.URL, sessionConfig)
	projection := projectionpg.New(worker)
	wire, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := manifestwire.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err = projection.Apply(ctx, decoded, 10000); err != nil {
		t.Fatal(err)
	}
	reader := manifestadapter.Reader{Projection: projection, ContractDigest: content.PlatformContract.Digest}
	route := domain.Route{TenantID: publication.TenantID, Provider: "telegram", AccountID: "summary-account", BindingID: "summary-binding", DeploymentRevisionID: publication.DeploymentRevisionID, ManifestRef: publication.Manifest.ID, ManifestDigest: publication.Manifest.ContentDigest, Generation: 1}
	// This is the exact persisted Manifest Reader contract, not a hand-made Plan.
	readPlan, err := reader.Resolve(ctx, route)
	if err != nil {
		t.Fatalf("real Reader must accept published Session Summary: %v", err)
	}
	if readPlan.Summary == nil || !readPlan.Summary.AddSessionSummary || readPlan.Summary.EventThreshold != 1 || readPlan.Summary.ModelEndpoint != summaryServer.URL+"/v1" {
		t.Fatal("Reader lost fixed Summary selection")
	}
	uses := []protocol.CredentialUse{content.Resources.Models["primary"].Credential, content.Resources.Models["summarizer"].Credential, content.Resources.Storage["session"].Credential}
	values := map[string]string{uses[0].CredentialID: "main-summary-vertical-key", uses[1].CredentialID: "separate-summary-vertical-key", uses[2].CredentialID: sessionConfig.ConnConfig.Password}
	if uses[0].CredentialID == uses[1].CredentialID || values[uses[0].CredentialID] == values[uses[1].CredentialID] {
		t.Fatal("fixture did not separate main/summary credentials")
	}
	profile := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ExecutionToken string                   `json:"execution_token"`
			ManifestID     string                   `json:"manifest_id"`
			ManifestDigest string                   `json:"manifest_digest"`
			Uses           []protocol.CredentialUse `json:"uses"`
		}
		if r.URL.Path != "/internal/v1/runtime-profiles/credentials/resolve" || r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Error("unexpected actual credential request")
			http.Error(w, "invalid", 400)
			return
		}
		grant, err := ledger.ActiveToken(r.Context(), "summary-vertical-worker", req.ExecutionToken, req.ManifestID, req.ManifestDigest)
		if err != nil {
			t.Error("credential fixture received nonlive actual Grant")
			http.Error(w, "denied", 403)
			return
		}
		if len(req.Uses) != 3 {
			t.Errorf("actual Resolve use count=%d", len(req.Uses))
			http.Error(w, "invalid", 400)
			return
		}
		seen := map[string]bool{}
		for _, got := range req.Uses {
			valid := false
			for _, expected := range uses {
				if reflect.DeepEqual(got, expected) {
					valid = true
				}
			}
			if !valid || seen[got.CredentialID] {
				t.Error("actual Resolve partial/duplicate/wrong binding")
				http.Error(w, "invalid", 400)
				return
			}
			seen[got.CredentialID] = true
		}
		mu.Lock()
		resolveAttempts = append(resolveAttempts, grant.AttemptID)
		mu.Unlock()
		credentials := []map[string]any{}
		for _, use := range uses {
			credentials = append(credentials, map[string]any{"credential_id": use.CredentialID, "purpose": use.Purpose, "audience_digest": use.AudienceDigest, "credential_revision": 1, "value": values[use.CredentialID]})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": route.TenantID, "profile_id": content.Sources.Profile.ProfileID, "profile_revision_number": content.Sources.Profile.RevisionNumber, "run_id": grant.Run.Request.RunID, "attempt_id": grant.AttemptID, "worker_id": grant.WorkerID, "lease_epoch": grant.LeaseEpoch, "manifest_id": req.ManifestID, "manifest_digest": req.ManifestDigest, "credentials": credentials})
	}))
	defer profile.Close()
	factory, err := runtimeadapter.New(runtimeadapter.Options{BaseURL: profile.URL, Client: profile.Client(), RequestTimeout: 3 * time.Second, MaxResponseBytes: 1 << 20, SnapshotCapacityBytes: 1 << 20, DrainTimeout: time.Second, MaxTrackedAttempts: 100})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := application.NewProcessor(ledger, reader, factory, "summary-vertical-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.Policy{Version: "summary-vertical", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Minute, LeaseTTL: 5 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 2}
	var accepted domain.Head
	var acceptedBytes []byte
	var acceptedSummary string
	successes := 0
	for index, text := range []string{"warmup preference", "generate summary", "consume accepted summary", "FAILED_INPUT_NOT_ACCEPTED", "fresh follower after failure"} {
		failed := index == 3
		failSummary.Store(failed)
		id := fmt.Sprintf("summary-vertical-%d", index)
		req := domain.Requested{EventID: "evt_" + id, RunID: "run_" + id, AdmissionID: "adm_" + id, EventDigest: domain.Digest([]byte("event-" + id)), RunDigest: domain.Digest([]byte("run-" + id)), Route: route, Input: domain.Input{ConversationID: "same-summary-chat", SenderID: "sender", Text: text, ReceivedAt: time.Now().UTC()}}
		if _, err = ledger.Accept(ctx, req, policy, domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
			t.Fatal(err)
		}
		run, err := ledger.FindRun(ctx, route.TenantID, req.RunID)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		primaryBefore, summariesBefore := len(mainRequests), len(summaryRequests)
		mu.Unlock()
		advanceErr := processor.Advance(ctx, run)
		if failed && !errors.Is(advanceErr, application.ErrRuntimeFailed) {
			t.Fatalf("summary401 error=%v", advanceErr)
		}
		if !failed && advanceErr != nil {
			t.Fatal(advanceErr)
		}
		completion, err := ledger.FindCompletion(ctx, route.TenantID, req.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var parent domain.Head
		if err = worker.QueryRow(ctx, `SELECT parent_ref,parent_digest FROM execution_attempts WHERE tenant_id=$1 AND attempt_id=$2`, route.TenantID, completion.AttemptID).Scan(&parent.Ref, &parent.Digest); err != nil || parent != accepted {
			t.Fatalf("actual Claim parent=%+v expected=%+v err=%v", parent, accepted, err)
		}
		mu.Lock()
		actualMain := mainRequests[primaryBefore:]
		summaryDelta := len(summaryRequests) - summariesBefore
		summaryOrdinal := len(summaryRequests)
		mu.Unlock()
		if len(actualMain) != 1 {
			t.Fatalf("primary HTTP calls for one Attempt=%d", len(actualMain))
		}
		messages, _ := json.Marshal(actualMain[0]["messages"])
		if acceptedSummary != "" && !strings.Contains(string(messages), acceptedSummary) {
			t.Fatal("real SDK next Run did not consume previously accepted summary")
		}
		if index == 4 && strings.Contains(string(messages), "FAILED_INPUT_NOT_ACCEPTED") {
			t.Fatal("failed summary input polluted fresh accepted context")
		}
		var actualHead domain.Head
		if err = worker.QueryRow(ctx, `SELECT accepted_ref,accepted_digest FROM execution_sessions WHERE tenant_id=$1 AND session_id=$2`, route.TenantID, run.SessionID).Scan(&actualHead.Ref, &actualHead.Digest); err != nil {
			t.Fatal(err)
		}
		var candidateCount int
		if err = sm.QueryRow(ctx, `SELECT count(*) FROM runtime_session.session_candidates WHERE tenant_id=$1 AND run_id=$2`, route.TenantID, req.RunID).Scan(&candidateCount); err != nil {
			t.Fatal(err)
		}
		if failed {
			if summaryDelta != 1 || completion.Status != domain.Failed || completion.Reason != "RUNTIME_FAILED" || candidateCount != 0 || actualHead != accepted {
				t.Fatalf("failed summary accepted: completion=%+v candidates=%d summaryCalls=%d", completion, candidateCount, summaryDelta)
			}
			var old []byte
			if err = sm.QueryRow(ctx, `SELECT content FROM runtime_session.session_candidates WHERE tenant_id=$1 AND candidate_ref=$2`, route.TenantID, accepted.Ref).Scan(&old); err != nil || !bytes.Equal(old, acceptedBytes) {
				t.Fatal("failed summary changed accepted candidate", err)
			}
		} else {
			if completion.Status != domain.Succeeded || candidateCount != 1 || actualHead != completion.Candidate {
				t.Fatalf("successful Summary not formally accepted %+v candidates=%d", completion, candidateCount)
			}
			accepted = completion.Candidate
			successes++
			if err = sm.QueryRow(ctx, `SELECT content FROM runtime_session.session_candidates WHERE tenant_id=$1 AND candidate_ref=$2 AND attempt_id=$3`, route.TenantID, accepted.Ref, completion.AttemptID).Scan(&acceptedBytes); err != nil {
				t.Fatal(err)
			}
			var stored struct {
				Snapshot struct {
					Session *session.Session `json:"session"`
				} `json:"snapshot"`
			}
			if err = json.Unmarshal(acceptedBytes, &stored); err != nil || stored.Snapshot.Session == nil {
				t.Fatal("actual stored Session snapshot missing", err)
			}
			if index >= 1 && (summaryDelta != 1 || len(stored.Snapshot.Session.Summaries) != 1) {
				t.Fatalf("Summary not persisted: delta=%d summaries=%d", summaryDelta, len(stored.Snapshot.Session.Summaries))
			}
			for _, summary := range stored.Snapshot.Session.Summaries {
				acceptedSummary = summary.Summary
			}
			if index >= 1 && acceptedSummary != fmt.Sprintf("Accepted summary preference marker %d", summaryOrdinal) {
				t.Fatal("stored summary is not this Attempt's actual provider output")
			}
		}
		var commits, finals int
		if err = worker.QueryRow(ctx, `SELECT count(*) FROM execution_session_commits WHERE tenant_id=$1`, route.TenantID).Scan(&commits); err != nil || commits != successes {
			t.Fatalf("accepted commits=%d want=%d err=%v", commits, successes, err)
		}
		if err = worker.QueryRow(ctx, `SELECT count(*) FROM execution_reply_outbox WHERE tenant_id=$1 AND run_id=$2`, route.TenantID, req.RunID).Scan(&finals); err != nil || finals != 1 {
			t.Fatalf("unique Completion Final outbox=%d err=%v", finals, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(mainRequests) != 5 || len(resolveAttempts) != 5 {
		t.Fatalf("main calls=%d resolves=%d", len(mainRequests), len(resolveAttempts))
	}
	seen := map[string]bool{}
	for _, attempt := range resolveAttempts {
		if seen[attempt] {
			t.Fatal("same Attempt resolved twice")
		}
		seen[attempt] = true
	}
	t.Logf("SUMMARY_MANIFEST_FACTORY_PG=PASS strict_manifest_reader=true real_factory=true separate_model_credentials=true exact_batch_uses=3 actual_sdk_http=true processor_claim_load_stage_complete=true next_run_summary_consumed=true failed_summary_zero_candidate=true failed_summary_head_unchanged=true fresh_follower_clean=true runs=5 successful_session_commits=4 model_http=%d summary_http=%d resolve_http=%d", len(mainRequests), len(summaryRequests), len(resolveAttempts))
}

func summaryVerticalManifest(t *testing.T, mainURL, summaryURL string, config *pgxpool.Config) (controlevents.RuntimeManifestPublishedEvent, protocol.ManifestContent) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("repository root missing")
		}
		dir = next
	}
	raw, err := os.ReadFile(filepath.Join(dir, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	publication, err := controlevents.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.VerifyRuntimeManifest(publication.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	main := content.Resources.Models["primary"]
	main.Model = "vertical-main"
	main.BaseURL = mainURL + "/v1"
	main.Credential.CredentialID = "crd_00000000000000000000000000000011"
	main.Credential.AudienceDigest = protocol.CredentialAudienceDigest(main.Kind, main.BaseURL)
	content.Resources.Models["primary"] = main
	summary := main
	summary.Model = "vertical-summarizer"
	summary.BaseURL = summaryURL + "/v1"
	summary.Credential.CredentialID = "crd_00000000000000000000000000000012"
	summary.Credential.AudienceDigest = protocol.CredentialAudienceDigest(summary.Kind, summary.BaseURL)
	content.Resources.Models["summarizer"] = summary
	content.ResolvedRequirements.Models["summarizer"] = "summarizer"
	storage := content.Resources.Storage["session"]
	storage.Destination = protocol.StorageDestination{Host: config.ConnConfig.Host, Port: int64(config.ConnConfig.Port), Database: config.ConnConfig.Database, Username: config.ConnConfig.User, SSLMode: "disable"}
	storage.Credential.AudienceDigest = protocol.CredentialAudienceDigest(storage.Kind, storage.Destination)
	content.Resources.Storage["session"] = storage
	content.Runtime = &protocol.ManifestRuntime{Summary: &protocol.ManifestSummary{Enabled: true, ModelResource: "summarizer", EventThreshold: 1}}
	yes := true
	node := content.AgentPlan.Nodes[content.AgentPlan.Root]
	node.AddSessionSummary = &yes
	content.AgentPlan.Nodes[content.AgentPlan.Root] = node
	content.Execution.AllowedEndpointHosts = []string{"127.0.0.1"}
	content.Execution.MaxOutputTokens = 20000
	b, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	b, err = jcs.Transform(b)
	if err != nil {
		t.Fatal(err)
	}
	publication.Manifest.Content = b
	publication.Manifest.ContentDigest = domain.Digest(b)
	publication.OccurredAt = time.Now().UTC()
	publication.Manifest.PublishedAt = publication.OccurredAt
	return publication, content
}
