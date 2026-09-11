package bootstrap

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	deliverypg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// Default maintenance operates on previously committed ledger facts, independently
// of any current Sender/owner. No fixture verifier is installed in App.New.
func TestGatewayDefaultRuntimeExpiresPendingWithoutOwner(t *testing.T) {
	natsURL := os.Getenv("GATEWAY_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("dedicated NATS required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated NATS reset required")
	}
	pool := deliveryDB(t)
	ctx, stop := context.WithTimeout(context.Background(), 12*time.Second)
	defer stop()
	topology, err := transport.LoadTopology("../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	broker, err := transport.Connect(natsURL, topology, transport.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
		if err = broker.JS.DeleteStream(ctx, name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
			_ = broker.JS.DeleteStream(cleanup, name)
		}
	})
	if err = broker.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	ledger, err := deliverypg.NewStore(pool, nil, deliverypg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	intent := d.Intent{ID: "orphan-final", AdmissionID: "old-admission", RunID: "old-run", AttemptID: "old-execution", CompletionID: "old-completion", ExecutionGeneration: 1, Sequence: 1, Text: "expired before any owner can send", Deadline: now.Add(500 * time.Millisecond)}
	target := d.Target{TenantID: "tenant", Provider: "wecom", AccountID: "missing-account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "chat", SourceEventID: "event", CallbackRequestID: "callback", ReceivedAt: now, Origin: &d.ReplyOrigin{InstanceID: "old-owner", Epoch: 1, Revision: 1, SocketGeneration: 1}}
	digest, err := d.IntentDigest(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.Accept(ctx, d.Prepared{Intent: intent, Digest: digest, Target: target, Parts: []string{intent.Text}}); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("GATEWAY_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	u.RawQuery = q.Encode()
	database := fixtureDatabaseConfig(t, u.String())
	config := Config{HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL, MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology}
	// Two independently constructed App instances in this process compete on the same persistent fact.
	for range 2 {
		gateway, err := newWithDatabaseTarget(ctx, config, database.target)
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- gateway.Run(runCtx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(4 * time.Second):
				t.Error("maintenance prevented bounded App shutdown")
			}
			gateway.Close()
		})
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		state, err := ledger.Get(ctx, intent.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Parts) != 1 {
			t.Fatal(state)
		}
		if state.Parts[0].State == d.Expired {
			receipt, gotDigest, found, err := ledger.Find(ctx, intent.ID)
			if err != nil || !found || gotDigest != digest || receipt.IntentID != intent.ID {
				t.Fatalf("expiry erased identity: %+v %v", receipt, err)
			}
			t.Log("DEFAULT_MAINTENANCE_VERIFIED: two App.Run instances; no accounts/owner/Sender; expired PENDING; original receipt retained; bounded shutdown")
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("default App runtime left expired no-owner Final in %s", state.Parts[0].State)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
