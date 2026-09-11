package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	ledgerpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This bounded WV-32 gate uses the actual SDK, Session store, ledger and relay.
// The model HTTP server and opaque broker-filling message are explicit fixtures;
// no Gateway or live external Provider/Telegram delivery is claimed. App-loop
// retry intervals are independently covered by the bootstrap unit gate.
func TestWorkerReplyCapacityPGNATSSDK(t *testing.T) {
	keys := []string{"WORKER_TEST_ADMIN_URL", "WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SESSION_TEST_MIGRATION_URL", "WORKER_SESSION_TEST_RUNTIME_URL", "WORKER_TEST_NATS_URL"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("requires dedicated Worker PostgreSQL/NATS capacity fixtures")
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
	if _, err := admin.Exec(ctx, `TRUNCATE worker.execution_sessions CASCADE; TRUNCATE worker.execution_rejections; TRUNCATE runtime_session.session_candidates`); err != nil {
		t.Fatal(err)
	}

	conn, err := nats.Connect(os.Getenv(keys[5]), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Stream(ctx, natsadapter.ReplyStream); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("capacity test requires its own initially absent Reply stream", err)
	}
	config := jetstream.StreamConfig{Name: natsadapter.ReplyStream, Subjects: []string{natsadapter.ReplySubject},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew,
		MaxBytes: 64 << 10, MaxMsgSize: 32 << 10, DenyDelete: true, DenyPurge: true}
	stream, err := js.CreateStream(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cc()
		// Only remove this test's entire private stream; never delete/purge a
		// retained message or lower the production deny_delete protection.
		if err := js.DeleteStream(cleanup, natsadapter.ReplyStream); err != nil {
			t.Error("remove owned capacity fixture stream:", err)
		}
	})
	info := func() *jetstream.StreamInfo {
		t.Helper()
		v, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v.Config.Retention != jetstream.WorkQueuePolicy || v.Config.Storage != jetstream.FileStorage ||
			v.Config.Discard != jetstream.DiscardNew || !v.Config.DenyDelete || !v.Config.DenyPurge || v.Config.MaxAge != 0 {
			t.Fatal("capacity fixture weakened retained-source invariants")
		}
		return v
	}
	// This is deliberately opaque capacity occupancy, not forged product state.
	filler := bytes.Repeat([]byte("explicit-private-broker-capacity-fixture\n"), 512)
	if len(filler) >= int(config.MaxMsgSize) {
		t.Fatal("occupancy fixture must fit below the individual message-size limit")
	}
	fillerAck, err := js.Publish(ctx, natsadapter.ReplySubject, filler, jetstream.WithMsgID("capacity-fixture-occupancy"))
	if err != nil || fillerAck == nil || fillerAck.Duplicate {
		t.Fatal("publish explicit capacity occupancy", err)
	}
	full := info()
	if full.State.Msgs != 1 || full.State.Bytes == 0 {
		t.Fatal("broker did not retain capacity occupancy")
	}
	// Use the broker's measured encoded size rather than guessing protocol cost.
	// The per-message cap still admits the original Final individually.
	config = full.Config
	config.MaxBytes = int64(full.State.Bytes)
	config.MaxMsgSize = int32(len(filler))
	if stream, err = js.UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	full = info()
	if full.State.Bytes != uint64(full.Config.MaxBytes) {
		t.Fatal("fixture is not actually at its explicit broker byte capacity")
	}

	ledger := ledgerpg.New(runtime)
	r := domain.Requested{EventID: "evt_capacity", EventDigest: domain.Digest([]byte("capacity-event")),
		RunID: "run_capacity", RunDigest: domain.Digest([]byte("capacity-run")), AdmissionID: "adm_capacity",
		Route: domain.Route{TenantID: "tenant_capacity", Provider: "telegram", AccountID: "account_capacity", BindingID: "binding_capacity",
			DeploymentRevisionID: "revision_capacity", ManifestRef: "manifest_capacity", ManifestDigest: domain.Digest([]byte("capacity-manifest")), Generation: 1},
		Input: domain.Input{ConversationID: "42", SenderID: "sender_capacity", Text: "Capacity test input", ReceivedAt: time.Now().UTC()}}
	policy := domain.Policy{Version: "explicit-capacity-fixture-v1", MaxRunAge: time.Minute, MaxReplyAge: time.Minute,
		MaxFutureSkew: time.Second, LeaseTTL: 30 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Second, MaxAttempts: 2}
	if _, err = ledger.Accept(ctx, r, policy, domain.IntakeLimits{MaxQueuedRuns: 10, MaxRetainedRuns: 100}); err != nil {
		t.Fatal(err)
	}
	grant, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: "worker-capacity", MaxRunSeconds: 45, MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.MarkExecuting(ctx, grant); err != nil {
		t.Fatal(err)
	}
	var modelCalls atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		var request struct {
			Model string `json:"model"`
		}
		if req.Method != http.MethodPost || req.URL.Path != "/v1/chat/completions" || req.Header.Get("Authorization") != "Bearer capacity-fixture-key" ||
			json.NewDecoder(req.Body).Decode(&request) != nil || request.Model != "capacity-fixture-model" {
			http.Error(w, "invalid explicit model fixture request", http.StatusBadRequest)
			return
		}
		modelCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"capacity\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"capacity-fixture-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Capacity final\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"capacity\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"capacity-fixture-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(model.Close)
	result, err := (trpcagent.Executor{CapacityBytes: 1 << 20, DrainTimeout: time.Second}).Execute(ctx, trpcagent.Request{
		TenantID: r.Route.TenantID, SessionID: grant.Run.SessionID, RunID: r.RunID, AttemptID: grant.AttemptID,
		NodeID: "answer", Instruction: "Return the requested text", InputText: r.Input.Text, MaxOutputTokens: 4096,
		Model: trpcagent.Model{Endpoint: model.URL + "/v1", Name: "capacity-fixture-model", APIKey: "capacity-fixture-key"}})
	if err != nil || result.FinalText != "Capacity final" || modelCalls.Load() != 1 {
		t.Fatal("actual SDK did not finish exactly one valid response", err)
	}
	sessionConfig, err := pgxpool.ParseConfig(os.Getenv(keys[4]))
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv(keys[4]))
	if err != nil {
		t.Fatal(err)
	}
	cc := sessionConfig.ConnConfig
	store, err := sessionstore.Open(ctx, os.Getenv(keys[4]), sessionstore.Target{Host: cc.Host, Port: cc.Port,
		Database: cc.Database, Username: cc.User, SSLMode: u.Query().Get("sslmode")}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	head, err := store.Put(ctx, sessionstore.Candidate{Identity: sessionstore.Identity{TenantID: r.Route.TenantID,
		SessionID: grant.Run.SessionID, RunID: r.RunID, AttemptID: grant.AttemptID},
		Parent: sessionstore.Head{Ref: grant.Parent.Ref, Digest: grant.Parent.Digest}, ContentVersion: sessionstore.ContentVersion, Snapshot: result.Snapshot})
	if err != nil {
		t.Fatal(err)
	}
	completion, err := ledger.Complete(ctx, domain.Finish{Grant: grant, Status: domain.Succeeded, FinalText: result.FinalText,
		Candidate: domain.Candidate{Ref: head.Ref, Digest: head.Digest, Parent: grant.Parent}})
	if err != nil || completion.FinalIntentID == "" {
		t.Fatal("atomic Completion did not create durable Final", err)
	}
	items, err := ledger.PendingReplies(ctx, 1)
	if err != nil || len(items) != 1 || items[0].IntentID != completion.FinalIntentID || len(items[0].Payload) >= int(config.MaxMsgSize) {
		t.Fatal("expected one valid, individually admissible, unpublished Final", err)
	}
	original := items[0]
	event, err := codec.DecodeReplyIntent(original.Payload)
	if err != nil || event.Execution.AttemptID != grant.AttemptID || event.Execution.CompletionID != completion.CompletionID {
		t.Fatal("durable original Final does not identify committed execution", err)
	}
	immutable, published := capacityFacts(t, ctx, admin, r.RunID)
	if published {
		t.Fatal("Final marked before any relay publication")
	}
	observed := &capacityPublisher{next: js}
	relay, err := natsadapter.NewReplyRelay(ledger, observed, 1)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if n, err := relay.Tick(ctx); n != 0 || !errors.Is(err, natsadapter.ErrUnavailable) {
			t.Fatalf("full broker publish %d: n=%d err=%v", attempt, n, err)
		}
		var apiErr *jetstream.APIError
		if !errors.As(observed.err, &apiErr) || !strings.Contains(strings.ToLower(apiErr.Description), "maximum bytes exceeded") {
			t.Fatalf("rejection was not actual broker byte-capacity error: %v", observed.err)
		}
		actual, marked := capacityFacts(t, ctx, admin, r.RunID)
		if marked || !bytes.Equal(actual, immutable) || modelCalls.Load() != 1 {
			t.Fatal("capacity rejection changed durable execution/candidate/Outbox or reran SDK")
		}
		retained := info()
		if retained.Created != full.Created || retained.State.Bytes != full.State.Bytes || retained.State.Msgs != 1 || retained.State.LastSeq != fillerAck.Sequence {
			t.Fatal("full stream did not retain its original occupancy unchanged")
		}
		t.Logf("WORKER_REPLY_CAPACITY_REJECTION=%d actual_nats_code=%d err_code=%d result=%q outbox_published=false immutable_facts=equal sdk_calls=1", attempt, apiErr.Code, apiErr.ErrorCode, apiErr.Description)
	}
	// The only recovery mutation increases this private fixture's explicit byte
	// capacity. It does not ACK/delete filler, change source identity, or touch PG.
	config.MaxBytes = 1 << 20
	if stream, err = js.UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.Tick(ctx); err != nil || n != 1 || observed.ack == nil || observed.ack.Duplicate {
		t.Fatal("original retained Final did not obtain a fresh real PubAck", n, err)
	}
	msg, err := stream.GetMsg(ctx, observed.ack.Sequence)
	if err != nil || !bytes.Equal(msg.Data, original.Payload) || msg.Header.Get(nats.MsgIdHdr) != original.IntentID || msg.Subject != natsadapter.ReplySubject {
		t.Fatal("broker bytes/MsgId differ from original durable Final", err)
	}
	after, published := capacityFacts(t, ctx, admin, r.RunID)
	if !published || !bytes.Equal(after, immutable) || modelCalls.Load() != 1 {
		t.Fatal("recovery changed more than published_at or reran execution")
	}
	if n, err := relay.Tick(ctx); n != 0 || err != nil || observed.calls != 3 {
		t.Fatal("already published Final was sent again", n, err)
	}
	recovered := info()
	if recovered.Created != full.Created || recovered.State.Msgs != 2 || recovered.Config.MaxBytes != 1<<20 {
		t.Fatal("recovery did not preserve incarnation and exactly filler plus one original Final")
	}
	if _, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: "worker-capacity", MaxRunSeconds: 45, MaxActive: 1}); !errors.Is(err, domain.ErrNotReady) {
		t.Fatal("completed Run became claimable after reply recovery", err)
	}
	t.Logf("WORKER_REPLY_CAPACITY=PASS stream_created=%s capacity_before=%d capacity_after=%d original_final_seq=%d original_digest=%s original_bytes=%d publish_calls=3 puback_success=1 pending=0 sdk_calls=1 attempt=1 candidate=1 completion=1 accepted_head=unchanged", recovered.Created.Format(time.RFC3339Nano), full.Config.MaxBytes, recovered.Config.MaxBytes, observed.ack.Sequence, original.Digest, len(original.Payload))
}

// capacityPublisher forwards every call and every result to/from real JetStream.
// It only records the raw broker error hidden by Relay's bounded error mapping.
type capacityPublisher struct {
	next  natsadapter.Publisher
	calls int
	ack   *jetstream.PubAck
	err   error
}

func (p *capacityPublisher) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.calls++
	p.ack, p.err = p.next.PublishMsg(ctx, msg, opts...)
	return p.ack, p.err
}

func capacityFacts(t *testing.T, ctx context.Context, admin *pgxpool.Pool, runID string) ([]byte, bool) {
	t.Helper()
	var raw []byte
	var marked bool
	// Include every row and original bytea, so an extra Attempt/candidate or a
	// rewritten payload cannot be hidden behind a count or selected-column check.
	err := admin.QueryRow(ctx, `SELECT jsonb_build_object(
 'run', (SELECT jsonb_agg(to_jsonb(r)) FROM worker.execution_runs r WHERE run_id=$1),
 'attempt', (SELECT jsonb_agg(to_jsonb(a) ORDER BY attempt_id) FROM worker.execution_attempts a WHERE run_id=$1),
 'completion', (SELECT jsonb_agg(to_jsonb(c)) FROM worker.execution_completions c WHERE run_id=$1),
 'commit', (SELECT jsonb_agg(to_jsonb(c)) FROM worker.execution_session_commits c WHERE run_id=$1),
 'candidate', (SELECT jsonb_agg(to_jsonb(c) ORDER BY candidate_ref) FROM runtime_session.session_candidates c WHERE run_id=$1),
 'session', (SELECT jsonb_agg(to_jsonb(s)) FROM worker.execution_sessions s JOIN worker.execution_runs r USING(tenant_id,session_id) WHERE r.run_id=$1),
 'outbox', (SELECT jsonb_agg(to_jsonb(o)-'published_at') FROM worker.execution_reply_outbox o WHERE run_id=$1)
 ), (SELECT published_at IS NOT NULL FROM worker.execution_reply_outbox WHERE run_id=$1)`, runID).Scan(&raw, &marked)
	if err != nil {
		t.Fatal(err)
	}
	var facts map[string][]json.RawMessage
	if err := json.Unmarshal(raw, &facts); err != nil || len(facts) != 7 {
		t.Fatal("invalid durable fact snapshot", err)
	}
	for kind, rows := range facts {
		if len(rows) != 1 {
			t.Fatalf("expected exactly one actual %s row, got %d", kind, len(rows))
		}
	}
	return raw, marked
}
