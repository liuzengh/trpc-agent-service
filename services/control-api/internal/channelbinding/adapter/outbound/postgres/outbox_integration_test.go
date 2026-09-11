package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	natsadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/nats"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	"github.com/nats-io/nats.go"
)

func seededRoute(t *testing.T, service *application.Service) application.CommandResult {
	t.Helper()
	created := createAccount(t, service)
	result, err := service.CreateBinding(context.Background(), testActor, "bind", application.CreateBindingInput{AccountID: created.Account.ID, Target: domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type publishRecorder struct {
	calls int
	err   error
	body  []byte
	id    string
}

func (p *publishRecorder) PublishRoute(_ context.Context, id string, raw []byte) error {
	p.calls++
	p.body = append([]byte(nil), raw...)
	p.id = id
	return p.err
}
func TestRouteRelayClaimFenceAndRecoveryAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	seedTargets(t, pool)
	route := seededRoute(t, service)
	ctx := context.Background()
	first, found, err := store.ClaimRoute(ctx)
	if err != nil || !found {
		t.Fatal("first claim missing", err)
	}
	if _, found, err := store.ClaimRoute(ctx); err != nil || found {
		t.Fatal("live lease was re-claimed", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET claimed_until=clock_timestamp()-interval '1 second' WHERE id=$1`, route.EventID); err != nil {
		t.Fatal(err)
	}
	second, found, err := store.ClaimRoute(ctx)
	if err != nil || !found || second.Attempt != 2 || second.ClaimToken == first.ClaimToken {
		t.Fatal("expired lease not fenced", err)
	}
	if err := store.FinishRoute(ctx, first, true, "", 0); !errors.Is(err, application.ErrOutboxLeaseLost) {
		t.Fatal("late claimant completed replacement lease", err)
	}
	if err := store.FinishRoute(ctx, second, false, "CHANNEL_ROUTE_PUBLISH_UNAVAILABLE", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ClaimRoute(ctx); err != nil || found {
		t.Fatal("retry delay ignored", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1`, route.EventID); err != nil {
		t.Fatal(err)
	}
	publisher := &publishRecorder{}
	relay, err := application.NewRouteRelay(store, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := relay.Step(ctx); err != nil || !found {
		t.Fatal(err)
	}
	var state string
	var attempt int
	var published bool
	if err := pool.QueryRow(ctx, `SELECT status,attempt_count,published_at IS NOT NULL FROM control_outbox WHERE id=$1`, route.EventID).Scan(&state, &attempt, &published); err != nil {
		t.Fatal(err)
	}
	if state != "PUBLISHED" || attempt != 3 || !published || publisher.calls != 1 || publisher.id != route.EventID {
		t.Fatal("publication completion mismatch")
	}
	projection, err := domain.ValidateStoredRoute(first.Payload, first.PayloadDigest)
	if err != nil {
		t.Fatal(err)
	}
	expected, _, err := projection.Encode()
	if err != nil || !bytes.Equal(expected, publisher.body) {
		t.Fatal("retry changed canonical body")
	}
}
func TestRouteRelayCorruptionAndFailureRetainOutboxAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	seedTargets(t, pool)
	route := seededRoute(t, service)
	ctx := context.Background()
	publisher := &publishRecorder{err: errors.New("transport error must not persist")}
	relay, _ := application.NewRouteRelay(store, publisher)
	if found, err := relay.Step(ctx); err != nil || !found {
		t.Fatal(err)
	}
	var state, code string
	if err := pool.QueryRow(ctx, `SELECT status,last_error FROM control_outbox WHERE id=$1`, route.EventID).Scan(&state, &code); err != nil {
		t.Fatal(err)
	}
	if state != "PENDING" || code != "CHANNEL_ROUTE_PUBLISH_UNAVAILABLE" {
		t.Fatal("broker failure was not sanitized and retained")
	}
	// The normal database immutability guard rejects rewrites; corruption is
	// injected as a separate malformed insert to exercise the Relay boundary.
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET payload_jsonb=jsonb_set(payload_jsonb,'{schema_version}','2') WHERE id=$1`, route.EventID); err == nil {
		t.Fatal("outbox content update bypassed immutability")
	}
	_, err := pool.Exec(ctx, `INSERT INTO control_outbox(tenant_id,id,aggregate_type,aggregate_id,aggregate_revision,event_type,schema_version,payload_jsonb,payload_digest,status,available_at,created_at,updated_at)
    SELECT tenant_id,'evt_corrupt',aggregate_type,aggregate_id,aggregate_revision+1,event_type,schema_version,jsonb_set(payload_jsonb,'{schema_version}','2'),payload_digest,'PENDING',clock_timestamp(),clock_timestamp(),clock_timestamp() FROM control_outbox WHERE id=$1`, route.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := relay.Step(ctx); err != nil || !found {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status,last_error FROM control_outbox WHERE id=$1`, "evt_corrupt").Scan(&state, &code); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" || code != "CHANNEL_OUTBOX_INTEGRITY" || publisher.calls != 1 {
		t.Fatal("corrupt event reached broker or was deleted")
	}
}
func TestRouteRelayRealJetStreamPubAckAndDuplicateAgainstPostgreSQL(t *testing.T) {
	configFile := os.Getenv("CONTROL_TEST_NATS_CONFIG_FILE")
	if configFile == "" {
		t.Skip("CONTROL_TEST_NATS_CONFIG_FILE is not set")
	}
	var cfg struct {
		URL             string `json:"url"`
		AdminUser       string `json:"admin_user"`
		AdminPassword   string `json:"admin_password"`
		ControlUser     string `json:"control_user"`
		ControlPassword string `json:"control_password"`
	}
	raw, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatal("read test NATS config")
	}
	err = json.Unmarshal(raw, &cfg)
	clear(raw)
	if err != nil {
		t.Fatal("decode test NATS config")
	}
	admin, err := nats.Connect(cfg.URL, nats.UserInfo(cfg.AdminUser, cfg.AdminPassword))
	if err != nil {
		t.Fatal("connect test NATS admin")
	}
	defer admin.Close()
	js, err := admin.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	// This environment is an explicitly isolated disposable test server, never the joint Gateway server.
	if _, err := js.StreamInfo(domain.RouteStream); err == nil {
		if err = js.DeleteStream(domain.RouteStream); err != nil {
			t.Fatal(err)
		}
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: domain.RouteStream, Subjects: []string{domain.RouteSubject}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, Discard: nats.DiscardNew, MaxBytes: 64 * 1024 * 1024, MaxMsgSize: domain.MaxRouteEventBytes, Duplicates: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer js.DeleteStream(domain.RouteStream)
	nc, err := nats.Connect(cfg.URL, nats.UserInfo(cfg.ControlUser, cfg.ControlPassword), nats.CustomInboxPrefix("_INBOX.control"))
	if err != nil {
		t.Fatal("connect limited control producer")
	}
	defer nc.Close()
	publisher, err := natsadapter.NewPublisher(nc)
	if err != nil {
		t.Fatal(err)
	}
	store, service, pool := channelPG(t)
	seedTargets(t, pool)
	route := seededRoute(t, service)
	ctx := context.Background()
	claim, found, err := store.ClaimRoute(ctx)
	if err != nil || !found {
		t.Fatal(err)
	}
	projection, err := domain.ValidateStoredRoute(claim.Payload, claim.PayloadDigest)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := projection.Encode()
	if err != nil {
		t.Fatal(err)
	}
	// Publish really succeeds but the simulated process dies before recording completion.
	if err := publisher.PublishRoute(ctx, claim.EventID, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET claimed_until=clock_timestamp()-interval '1 second' WHERE id=$1`, route.EventID); err != nil {
		t.Fatal(err)
	}
	relay, _ := application.NewRouteRelay(store, publisher)
	if found, err := relay.Step(ctx); err != nil || !found {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(domain.RouteStream)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatal("stable event ID was not deduplicated")
	}
	msg, err := js.GetMsg(domain.RouteStream, info.State.LastSeq)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Get(nats.MsgIdHdr) != route.EventID || !bytes.Equal(msg.Data, payload) {
		t.Fatal("JetStream payload identity changed")
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM control_outbox WHERE id=$1`, route.EventID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" {
		t.Fatal("PubAck did not complete outbox")
	}
	// A stream-less publish must never fabricate success.
	if err := js.DeleteStream(domain.RouteStream); err != nil {
		t.Fatal(err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := publisher.PublishRoute(publishCtx, "evt_missing_stream", payload); err == nil {
		t.Fatal("missing stream accepted as durable")
	}
}
