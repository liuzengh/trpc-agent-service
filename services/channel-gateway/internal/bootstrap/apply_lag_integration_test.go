package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	routeconsumer "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/inbound/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// Fault injection uses the existing consumer seam: only Fetch fails. StreamInfo
// and all actual HTTP, PostgreSQL, and JetStream operations remain live.
type failingFetch struct {
	inner    routeconsumer.MessageConsumer
	paused   atomic.Bool
	active   atomic.Int64
	rejected atomic.Int64
}

func (f *failingFetch) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	f.active.Add(1)
	defer f.active.Add(-1)
	if f.paused.Load() {
		f.rejected.Add(1)
		return nil, errors.New("injected route fetch failure")
	}
	return f.inner.Next(opts...)
}

func TestGatewayApplyLagAdmissionGate(t *testing.T) {
	dsn, natsURL := os.Getenv("GATEWAY_TEST_DATABASE_URL"), os.Getenv("GATEWAY_TEST_NATS_URL")
	if dsn == "" || natsURL == "" {
		t.Skip("GATEWAY_TEST_DATABASE_URL and GATEWAY_TEST_NATS_URL required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated broker requires GATEWAY_TEST_ALLOW_NATS_RESET=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	adminDB, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()
	schema := fmt.Sprintf("gateway_lag_http_%d", time.Now().UnixNano())
	if _, err = adminDB.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, e := adminDB.Exec(c, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
			t.Error(e)
		}
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
	broker, err := transport.Connect(natsURL, topology, transport.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
		if err = broker.JS.DeleteStream(ctx, name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatal(err)
		}
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
			_ = broker.JS.DeleteStream(c, name)
		}
	}()
	if err = broker.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	database := fixtureDatabaseConfig(t, u.String())
	cfg := Config{HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL, MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology, Accounts: []Account{{ID: "acct-1", SecretEnv: "FIXTURE_SECRET", Secret: "fixture-webhook-secret"}}}
	var apps []*App
	var public, health []*httptest.Server
	var faults []*failingFetch
	for range 2 {
		app, e := newWithDatabaseTarget(ctx, cfg, database.target)
		if e != nil {
			t.Fatal(e)
		}
		// Each replica owns its own SDK stream handle, as in New; sharing a
		// handle here races the SDK internal Stream.Info cache across replicas.
		stream, e := app.transport.JS.Stream(ctx, transport.RouteStream)
		if e != nil {
			app.Close()
			t.Fatal(e)
		}
		consumer, e := stream.Consumer(ctx, transport.RouteConsumer)
		if e != nil {
			app.Close()
			t.Fatal(e)
		}
		fault := &failingFetch{inner: consumer}
		faults = append(faults, fault)
		app.consumer = routeconsumer.New(fault, stream, app.routes)
		if e = app.consumer.Initialize(ctx); e != nil {
			app.Close()
			t.Fatal(e)
		}
		pub, adm := httptest.NewServer(app.Handler()), httptest.NewServer(app.AdminHandler())
		apps = append(apps, app)
		public = append(public, pub)
		health = append(health, adm)
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- app.Run(runCtx) }()
		defer func() {
			stop()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(12 * time.Second):
				t.Error("Gateway shutdown timeout")
			}
			pub.Close()
			adm.Close()
			app.Close()
		}()
	}
	get := func(endpoint string) int {
		r, e := http.Get(endpoint)
		if e != nil {
			t.Fatal(e)
		}
		defer r.Body.Close()
		return r.StatusCode
	}
	publish := func(generation int, enabled bool) {
		route := map[string]any{"provider": "telegram", "account_id": "acct-1", "generation": generation}
		if enabled {
			route["tenant_id"] = "tenant"
			route["binding_id"] = "binding"
			route["deployment_revision_id"] = "revision"
			route["manifest_ref"] = "manifest/revision"
			route["manifest_digest"] = "sha256:" + strings.Repeat("a", 64)
		}
		body, e := json.Marshal(map[string]any{"schema_version": 1, "event_id": fmt.Sprintf("route-%d", generation), "enabled": enabled, "route": route})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = broker.JS.Publish(ctx, transport.RouteSubject, body); e != nil {
			t.Fatal(e)
		}
	}
	send := func(replica, update int) int {
		body := fmt.Sprintf(`{"update_id":%d,"message":{"message_id":1,"date":1,"from":{"id":7,"is_bot":false,"first_name":"Fixture"},"chat":{"id":7,"type":"private"},"text":"fixture"}}`, update)
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, public[replica].URL+"/v1/telegram/acct-1", bytes.NewBufferString(body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", cfg.Accounts[0].Secret)
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode
	}
	publish(1, true)
	eventually(t, func() bool {
		var g int
		return db.QueryRow(ctx, `SELECT generation FROM gateway_route_projections WHERE account_id='acct-1'`).Scan(&g) == nil && g == 1
	}, "initial live route")
	if code := send(0, 100); code != 200 {
		t.Fatalf("initial acceptance=%d", code)
	}
	for _, f := range faults {
		f.paused.Store(true)
	}
	eventually(t, func() bool {
		for _, f := range faults {
			if f.active.Load() != 0 || f.rejected.Load() == 0 {
				return false
			}
		}
		return true
	}, "both route fetch loops fail independently of source observation")
	publish(2, false)
	// Let the actual Consumer.Run timer discover the new watermark. Calling
	// ObserveSource here would hide a broken periodic-observation scheduling path.
	observed := false
	discoveryDeadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(discoveryDeadline) {
		h, e := apps[0].routes.QueryProjectionHealth(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if h.HighestSequence == 2 && h.ContiguousSequence == 1 && h.ApplyLagSince != nil {
			if time.Since(h.LastObservedAt) > 5*time.Second {
				t.Fatal("source observer is not fresh")
			}
			observed = true
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	if !observed {
		t.Fatal("Consumer.Run did not observe backlog while Fetch was failing")
	}
	if code := get(health[0].URL + "/readyz"); code != 204 {
		t.Fatalf("transient backlog denied before grace=%d", code)
	}
	// SQL is only a test fixture for elapsed time, not evidence of wall-clock grace.
	// The Store regression separately waits the real 61 seconds before rejecting.
	if _, err = db.Exec(ctx, `UPDATE gateway_route_replay_state SET apply_lag_since=clock_timestamp()-interval '61 seconds' WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	for i := range apps {
		if code := get(health[i].URL + "/livez"); code != 204 {
			t.Fatalf("livez replica %d=%d", i, code)
		}
		if code := get(health[i].URL + "/readyz"); code != 503 {
			t.Fatalf("readyz during persistent apply lag replica %d=%d", i, code)
		}
		if code := send(i, 200+i); code != 503 {
			t.Fatalf("new input bypassed lag gate replica %d=%d", i, code)
		}
		if code := send(i, 100); code != 200 {
			t.Fatalf("old receipt blocked replica %d=%d", i, code)
		}
	}
	var count int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM gateway_admissions`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("lagging input mutated durable admission: count=%d err=%v", count, err)
	}
	for _, f := range faults {
		f.paused.Store(false)
	}
	eventually(t, func() bool {
		var g int
		return db.QueryRow(ctx, `SELECT generation FROM gateway_route_projections WHERE account_id='acct-1'`).Scan(&g) == nil && g == 2
	}, "resume fetch applies disabled route")
	eventually(t, func() bool { return get(health[0].URL+"/readyz") == 204 && get(health[1].URL+"/readyz") == 204 }, "catch-up restores both readiness probes")
	if code := send(0, 300); code != 503 {
		t.Fatalf("caught up to disabled route but admitted=%d", code)
	}
	publish(3, true)
	eventually(t, func() bool {
		var g int
		return db.QueryRow(ctx, `SELECT generation FROM gateway_route_projections WHERE account_id='acct-1'`).Scan(&g) == nil && g == 3
	}, "reactivate account with greater route generation")
	if code := send(1, 301); code != 200 {
		t.Fatalf("reactivation did not restore acceptance=%d", code)
	}
	t.Log("APPLY_LAG_HTTP_VERIFIED: real HTTP/PG/NATS; two replicas; injected Fetch-only failure; production observer discovers backlog despite Fetch failure; source fresh; grace accepted; aged episode rejects new Run but replays old receipt; livez stays 204; catch-up applies disable; generation 3 reactivation admits")
}
