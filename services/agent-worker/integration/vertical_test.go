// Package integration exercises the real Worker pipeline with real PostgreSQL,
// NATS, SDK and Session storage. Profile/model HTTP fixtures are explicitly not
// evidence of a live Control publication or a real model/Telegram acceptance.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	controlevents "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	executionwire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/manifestadapter"
	ledgerpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/runtimeadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	projectionpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWorkerVerticalPGNATSSDK(t *testing.T) {
	keys := []string{"WORKER_TEST_ADMIN_URL", "WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SESSION_TEST_MIGRATION_URL", "WORKER_SESSION_TEST_RUNTIME_URL", "WORKER_TEST_NATS_URL"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("requires dedicated Worker vertical fixtures")
		}
	}
	if os.Getenv("WORKER_TEST_ALLOW_RESET") != "1" {
		t.Fatal("explicit fixture reset required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	open := func(key string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, os.Getenv(key))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	admin, migration, runtime, sessionMigration := open(keys[0]), open(keys[1]), open(keys[2]), open(keys[3])
	if err := migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	if err := sessionmigrations.Apply(ctx, sessionMigration); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `TRUNCATE worker.execution_sessions CASCADE;TRUNCATE worker.execution_rejections;TRUNCATE worker.runtime_manifests CASCADE;TRUNCATE worker.manifest_conflicts;TRUNCATE worker.manifest_conflict_identities;TRUNCATE runtime_session.session_candidates`); err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(os.Getenv(keys[5]), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name, subject, durable string
		retention              jetstream.RetentionPolicy
	}{
		{natsadapter.RunStream, natsadapter.RunSubject, natsadapter.RunDurable, jetstream.WorkQueuePolicy},
		{natsadapter.ManifestStream, natsadapter.ManifestSubject, natsadapter.ManifestDurable, jetstream.LimitsPolicy},
		{natsadapter.ReplyStream, natsadapter.ReplySubject, "channel-gateway-replies-v1", jetstream.WorkQueuePolicy},
	} {
		if _, err = js.Stream(ctx, item.name); err == nil {
			t.Fatalf("dedicated broker already has %s", item.name)
		}
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: item.name, Subjects: []string{item.subject}, Retention: item.retention, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew, MaxBytes: 16 << 20, MaxMsgSize: 1 << 20, DenyDelete: true, DenyPurge: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cc := context.WithTimeout(context.Background(), 5*time.Second)
			defer cc()
			nc, e := nats.Connect(os.Getenv(keys[5]), nats.NoReconnect())
			if e == nil {
				defer nc.Close()
				j, e := jetstream.New(nc)
				if e == nil {
					_ = j.DeleteStream(cleanup, item.name)
				}
			}
		})
		if _, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: item.durable, FilterSubject: item.subject, AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy, ReplayPolicy: jetstream.ReplayInstantPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 64}); err != nil {
			t.Fatal(err)
		}
	}
	var modelCalls atomic.Int32
	var requestsMu sync.Mutex
	var requests []map[string]any
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		requestsMu.Lock()
		requests = append(requests, req)
		requestsMu.Unlock()
		if r.Header.Get("Authorization") != "Bearer fixture-model-key" {
			http.Error(w, "bad key", 403)
			return
		}
		answer := "First answer"
		if modelCalls.Add(1) > 1 {
			answer = "Second answer"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%q},\"finish_reason\":null}]}\n\n", answer)
		fmt.Fprint(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer model.Close()
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
			t.Fatal("root missing")
		}
		dir = next
	}
	raw, err := os.ReadFile(filepath.Join(dir, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := controlevents.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.VerifyRuntimeManifest(event.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(os.Getenv(keys[4]))
	if err != nil {
		t.Fatal(err)
	}
	modelResource := content.Resources.Models["primary"]
	modelResource.BaseURL = model.URL + "/v1"
	modelResource.Credential.AudienceDigest = protocol.CredentialAudienceDigest(modelResource.Kind, modelResource.BaseURL)
	content.Resources.Models["primary"] = modelResource
	storage := content.Resources.Storage["session"]
	storage.Destination = protocol.StorageDestination{Host: config.ConnConfig.Host, Port: int64(config.ConnConfig.Port), Database: config.ConnConfig.Database, Username: config.ConnConfig.User, SSLMode: "disable"}
	storage.Credential.AudienceDigest = protocol.CredentialAudienceDigest(storage.Kind, storage.Destination)
	content.Resources.Storage["session"] = storage
	content.Execution.AllowedEndpointHosts = []string{"127.0.0.1"}
	content.Execution.MaxOutputTokens = 20000
	contentBytes, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	contentBytes, err = jcs.Transform(contentBytes)
	if err != nil {
		t.Fatal(err)
	}
	event.Manifest.Content = contentBytes
	event.Manifest.ContentDigest = domain.Digest(contentBytes)
	event.OccurredAt = time.Now().UTC()
	event.Manifest.PublishedAt = event.OccurredAt
	manifestWire, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = controlevents.DecodeRuntimeManifestPublishedEvent(manifestWire); err != nil {
		t.Fatal(err)
	}
	ledger := ledgerpg.New(runtime)
	projection := projectionpg.New(runtime)
	reader := manifestadapter.Reader{Projection: projection, ContractDigest: content.PlatformContract.Digest}
	var resolves atomic.Int32
	profile := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			ExecutionToken string           `json:"execution_token"`
			ManifestID     string           `json:"manifest_id"`
			ManifestDigest string           `json:"manifest_digest"`
			Uses           []map[string]any `json:"uses"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		grant, e := ledger.ActiveToken(r.Context(), "worker-fixture", input.ExecutionToken, input.ManifestID, input.ManifestDigest)
		if e != nil {
			http.Error(w, "denied", 403)
			return
		}
		resolves.Add(1)
		credentials := []map[string]any{}
		for _, u := range []protocol.CredentialUse{modelResource.Credential, storage.Credential} {
			value := "fixture-model-key"
			if u.Purpose == "dsn" {
				value = config.ConnConfig.Password
			}
			credentials = append(credentials, map[string]any{"credential_id": u.CredentialID, "purpose": u.Purpose, "audience_digest": u.AudienceDigest, "credential_revision": 1, "value": value})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": grant.Run.Request.Route.TenantID, "profile_id": content.Sources.Profile.ProfileID, "profile_revision_number": content.Sources.Profile.RevisionNumber, "run_id": grant.Run.Request.RunID, "attempt_id": grant.AttemptID, "worker_id": grant.WorkerID, "lease_epoch": grant.LeaseEpoch, "manifest_id": input.ManifestID, "manifest_digest": input.ManifestDigest, "credentials": credentials})
	}))
	defer profile.Close()
	factory, err := runtimeadapter.New(runtimeadapter.Options{BaseURL: profile.URL, Client: profile.Client(), RequestTimeout: 3 * time.Second, MaxResponseBytes: 1 << 20, SnapshotCapacityBytes: 1 << 20, DrainTimeout: time.Second, MaxTrackedAttempts: 100})
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.Policy{Version: "integration-v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 5 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	acceptor, err := application.NewAcceptor(ledger, policy, domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 100000})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := application.NewProcessor(ledger, reader, factory, "worker-fixture", 4)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := natsadapter.BindRun(ctx, js, acceptor, ledger)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := natsadapter.BindManifest(ctx, js, projection, ledger, 100)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := natsadapter.NewReplyRelay(ledger, js, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"Hello", "Continue"} {
		request := dto.RunRequested{SchemaVersion: 1, EventID: fmt.Sprintf("evt_vertical_%d", i), AdmissionID: fmt.Sprintf("adm_vertical_%d", i), RunID: fmt.Sprintf("run_vertical_%d", i), Route: dto.RouteSnapshot{Provider: "telegram", AccountID: "account_fixture", Generation: 1, TenantID: event.TenantID, BindingID: "binding_fixture", DeploymentRevisionID: event.DeploymentRevisionID, ManifestRef: event.Manifest.ID, ManifestDigest: event.Manifest.ContentDigest}, Input: dto.Inbound{Key: dto.EventKey{Provider: "telegram", AccountID: "account_fixture", EventID: fmt.Sprint(100 + i)}, Kind: "text", ConversationID: "42", SenderID: "43", Text: text, ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), ReplyContext: &dto.ReplyContext{ChatID: "42"}, SourceDigest: strings.Repeat("a", 64)}}
		b, e := executionwire.EncodeRunRequested(request)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = js.Publish(ctx, natsadapter.RunSubject, b); e != nil {
			t.Fatal(e)
		}
		if got, e := runs.Poll(ctx); e != nil || !got {
			t.Fatal(got, e)
		}
		run, e := ledger.FindRun(ctx, event.TenantID, request.RunID)
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			if e = processor.Advance(ctx, run); !errors.Is(e, application.ErrManifestMissing) {
				t.Fatal(e)
			}
			if modelCalls.Load() != 0 || resolves.Load() != 0 {
				t.Fatal("model/credentials called before fixed Manifest")
			}
			if _, e = js.Publish(ctx, natsadapter.ManifestSubject, manifestWire); e != nil {
				t.Fatal(e)
			}
			if got, e := manifests.Poll(ctx); e != nil || !got {
				t.Fatal(got, e)
			}
		}
		if e = processor.Advance(ctx, run); e != nil {
			t.Fatal(e)
		}
		completion, e := ledger.FindCompletion(ctx, event.TenantID, request.RunID)
		if e != nil || completion.Status != domain.Succeeded || completion.Candidate.Ref == "" || completion.FinalIntentID == "" {
			t.Fatalf("completion=%+v err=%v", completion, e)
		}
		if count, e := relay.Tick(ctx); e != nil || count != 1 {
			t.Fatal(count, e)
		}
		// Redelivery after completion replays intake; Advance does not call SDK.
		if _, e = js.Publish(ctx, natsadapter.RunSubject, b); e != nil {
			t.Fatal(e)
		}
		if _, e = runs.Poll(ctx); e != nil {
			t.Fatal(e)
		}
		done, e := ledger.FindRun(ctx, event.TenantID, request.RunID)
		if e != nil {
			t.Fatal(e)
		}
		if e = processor.Advance(ctx, done); e != nil && !errors.Is(e, domain.ErrNotReady) {
			t.Fatal(e)
		}
	}
	if modelCalls.Load() != 2 || resolves.Load() != 2 {
		t.Fatalf("model=%d resolve=%d", modelCalls.Load(), resolves.Load())
	}
	requestsMu.Lock()
	history, _ := json.Marshal(requests[1]["messages"])
	firstLimit, secondLimit := requests[0]["max_completion_tokens"], requests[1]["max_completion_tokens"]
	requestsMu.Unlock()
	if !strings.Contains(string(history), "First answer") || !strings.Contains(string(history), "Hello") || !strings.Contains(string(history), "Continue") {
		t.Fatal("accepted history did not reach next model request")
	}
	if firstLimit != float64(20000) || secondLimit != float64(20000) {
		t.Fatal("published per-response cap changed", firstLimit, secondLimit)
	}
	var sessions, completions, candidates, finals int
	for query, target := range map[string]*int{`SELECT count(*) FROM worker.execution_sessions`: &sessions, `SELECT count(*) FROM worker.execution_completions`: &completions, `SELECT count(*) FROM runtime_session.session_candidates`: &candidates, `SELECT count(*) FROM worker.execution_reply_outbox WHERE published_at IS NOT NULL`: &finals} {
		if e := admin.QueryRow(ctx, query).Scan(target); e != nil {
			t.Fatal(e)
		}
	}
	if sessions != 1 || completions != 2 || candidates != 2 || finals != 2 {
		t.Fatal(sessions, completions, candidates, finals)
	}
	t.Log("WORKER_VERTICAL_FIXTURE=PASS real NATS intake/Manifest projection + live grant check + SDK model HTTP + PostgreSQL accepted Session + unique Completion/Final + Reply PubAck; two rounds; duplicate intake; 2 model calls, 2 Resolve calls; model/Profile are explicit HTTP fixtures, not live-provider acceptance")
}
