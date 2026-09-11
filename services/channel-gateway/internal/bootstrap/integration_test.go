package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// Requires a dedicated broker: the test explicitly replaces only the two
// Gateway streams. Never enable the reset flag against a shared environment.
func TestGatewayVerticalIntegration(t *testing.T) {
	dsn, natsURL := os.Getenv("GATEWAY_TEST_DATABASE_URL"), os.Getenv("GATEWAY_TEST_NATS_URL")
	if dsn == "" || natsURL == "" {
		t.Skip("GATEWAY_TEST_DATABASE_URL and GATEWAY_TEST_NATS_URL required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated broker requires explicit GATEWAY_TEST_ALLOW_NATS_RESET=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema := fmt.Sprintf("gateway_vertical_%d", time.Now().UnixNano())
	if _, err = pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	}()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	topology, err := transport.LoadTopology("../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := transport.Connect(natsURL, topology, transport.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
		if err = admin.JS.DeleteStream(ctx, name); err != nil && err != jetstream.ErrStreamNotFound {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
			_ = admin.JS.DeleteStream(context.Background(), name)
		}
	}()
	if err = admin.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err = admin.Reconcile(ctx); err != nil {
		t.Fatalf("idempotent reconcile: %v", err)
	}
	runstream, err := admin.JS.Stream(ctx, transport.RunStream)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := runstream.Consumer(ctx, transport.RunConsumer)
	if err != nil {
		t.Fatal(err)
	}
	database := fixtureDatabaseConfig(t, u.String())
	config := Config{HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL, MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology, Accounts: []Account{{ID: "acct-1", SecretEnv: "TEST_SECRET", Secret: "test-secret-1234567890"}}}
	var apps []*App
	var servers []*httptest.Server
	var healthServers []*httptest.Server
	var cancels []context.CancelFunc
	var exits []chan error
	for i := 0; i < 2; i++ {
		app, err := newWithDatabaseTarget(ctx, config, database.target)
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, app)
		servers = append(servers, httptest.NewServer(app.Handler()))
		healthServers = append(healthServers, httptest.NewServer(app.AdminHandler()))
		runCtx, stop := context.WithCancel(ctx)
		cancels = append(cancels, stop)
		done := make(chan error, 1)
		exits = append(exits, done)
		go func() { done <- app.Run(runCtx) }()
	}
	defer func() {
		for _, stop := range cancels {
			stop()
		}
		for i, done := range exits {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("shutdown: %v", err)
				}
			case <-time.After(12 * time.Second):
				t.Error("shutdown timeout")
			}
			servers[i].Close()
			healthServers[i].Close()
			apps[i].Close()
		}
	}()
	getStatus := func(endpoint string) int {
		resp, err := http.Get(endpoint)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	eventually(t, func() bool { return getStatus(healthServers[0].URL+"/readyz") == 204 }, "empty stream initializes")
	if got := getStatus(servers[0].URL + "/readyz"); got != 404 {
		t.Fatalf("public probe exposed: %d", got)
	}
	if got := getStatus(healthServers[0].URL + "/v1/telegram/acct-1"); got != 404 {
		t.Fatalf("admin webhook exposed: %d", got)
	}
	publishRoute := func(eventID string, generation int, enabled bool) {
		route := map[string]any{"provider": "telegram", "account_id": "acct-1", "generation": generation}
		if enabled {
			route["tenant_id"] = "tenant-1"
			route["binding_id"] = "binding-1"
			route["deployment_revision_id"] = "drev-1"
			route["manifest_ref"] = "manifests/revisions/manifest-1"
			route["manifest_digest"] = "sha256:" + strings.Repeat("a", 64)
		}
		b, _ := json.Marshal(map[string]any{"schema_version": 1, "event_id": eventID, "enabled": enabled, "route": route})
		if _, err := admin.JS.Publish(ctx, transport.RouteSubject, b); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool {
			var g int
			return db.QueryRow(ctx, `SELECT generation FROM gateway_route_projections WHERE provider='telegram' AND account_id='acct-1'`).Scan(&g) == nil && g == generation
		}, "route projection generation")
	}
	publishRoute("route-1", 1, true)
	body := func(update int, text string) []byte {
		b, _ := json.Marshal(map[string]any{"update_id": update, "message": map[string]any{"message_id": 77, "date": 1, "from": map[string]any{"id": 123, "is_bot": false, "first_name": "Test"}, "chat": map[string]any{"id": 123, "type": "private"}, "text": text}})
		return b
	}
	send := func(server int, b []byte, secret string) int {
		req, _ := http.NewRequest(http.MethodPost, servers[server].URL+"/v1/telegram/acct-1", bytes.NewReader(b))
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := send(0, body(500, "hello"), "wrong"); got != 401 {
		t.Fatalf("authentication=%d", got)
	}
	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); codes <- send(i%2, body(500, "hello"), config.Accounts[0].Secret) }(i)
	}
	wg.Wait()
	close(codes)
	for code := range codes {
		if code != 200 {
			t.Fatalf("concurrent webhook status=%d", code)
		}
	}
	var inbox, admissions, outbox int
	if err = db.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_inbox),(SELECT count(*) FROM gateway_admissions),(SELECT count(*) FROM gateway_outbox)`).Scan(&inbox, &admissions, &outbox); err != nil {
		t.Fatal(err)
	}
	if inbox != 1 || admissions != 1 || outbox != 1 {
		t.Fatalf("atomic dedup: inbox=%d admissions=%d outbox=%d", inbox, admissions, outbox)
	}
	msg, err := worker.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	event, err := wire.DecodeRunRequested(msg.Data())
	if err != nil {
		t.Fatalf("published event: %v", err)
	}
	if event.Route.TenantID != "tenant-1" || event.Input.Text != "hello" || event.Input.Key.EventID != "500" {
		t.Fatal("unexpected normalized execution event")
	}
	if err = msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		var published bool
		return db.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM gateway_outbox`).Scan(&published) == nil && published
	}, "outbox acknowledged")
	eventually(t, func() bool { info, err := runstream.Info(ctx); return err == nil && info.State.Msgs == 0 }, "Worker ACK reclaims WorkQueue message")
	publishRoute("route-disabled", 2, false)
	if got := send(1, body(500, "hello"), config.Accounts[0].Secret); got != 200 {
		t.Fatalf("replay after disable=%d", got)
	}
	if got := send(1, body(500, "different"), config.Accounts[0].Secret); got != 409 {
		t.Fatalf("content conflict=%d", got)
	}
	if got := send(1, body(501, "new"), config.Accounts[0].Secret); got != 503 {
		t.Fatalf("new on disabled=%d", got)
	}
	callback := []byte(`{"update_id":502,"callback_query":{"id":"query-1","from":{"id":123,"is_bot":false,"first_name":"Test"},"chat_instance":"chat-1","data":"ack"}}`)
	if got := send(0, callback, config.Accounts[0].Secret); got != 200 {
		t.Fatalf("interaction=%d", got)
	}
	if err = db.QueryRow(ctx, `SELECT count(*) FROM gateway_admissions`).Scan(&admissions); err != nil || admissions != 1 {
		t.Fatalf("interaction created run: %d %v", admissions, err)
	}
	apps[0].admission.Stop()
	if got := send(0, body(500, "hello"), config.Accounts[0].Secret); got != 200 {
		t.Fatalf("stopped replay=%d", got)
	}
	if got := send(0, body(503, "new"), config.Accounts[0].Secret); got != 503 {
		t.Fatalf("stopped new=%d", got)
	}
	t.Log("VERIFIED: 2 replicas; empty replay readiness; separate listeners; authenticated HTTP; 20 duplicate requests produce 1 Admission/Outbox; real JetStream schema-valid RunRequested; replay after disable; content conflict; interaction creates no Run; stopping gate; graceful drain")
}
func eventually(t *testing.T, predicate func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout: " + what)
}
