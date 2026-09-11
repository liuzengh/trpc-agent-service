package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	controlwire "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	connapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type wecomPeer struct {
	conn   *websocket.Conn
	ctx    context.Context
	secret string
}

func (p wecomPeer) send(t *testing.T, msgID, reqID, text, cmd, eventType string) {
	t.Helper()
	body := map[string]any{"msgid": msgID, "aibotid": "fixture-bot", "msgtype": "text", "chattype": "single", "from": map[string]string{"userid": "fixture-user"}, "text": map[string]string{"content": text}}
	if eventType != "" {
		body = map[string]any{"msgid": msgID, "aibotid": "fixture-bot", "msgtype": "event", "event": map[string]string{"eventtype": eventType}, "from": map[string]string{"userid": "fixture-user"}}
	}
	frame, _ := json.Marshal(map[string]any{"cmd": cmd, "headers": map[string]string{"req_id": reqID}, "body": body})
	if err := p.conn.Write(p.ctx, websocket.MessageText, frame); err != nil {
		t.Fatal(err)
	}
}
func TestWeComConnectionGatewayIntegration(t *testing.T) {
	dsn, natsURL := os.Getenv("GATEWAY_TEST_DATABASE_URL"), os.Getenv("GATEWAY_TEST_NATS_URL")
	if dsn == "" || natsURL == "" {
		t.Skip("dedicated PG/NATS required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated NATS reset required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	adminDB, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()
	schema := fmt.Sprintf("gateway_wecom_%d", time.Now().UnixNano())
	if _, err = adminDB.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = adminDB.Exec(c, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	}()
	u, _ := url.Parse(dsn)
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
	publishRoute := func(generation int, enabled bool) {
		t.Helper()
		snapshot := map[string]any{"provider": "wecom", "account_id": "wecom-account", "generation": generation}
		if enabled {
			for k, v := range map[string]string{"tenant_id": "tenant-1", "binding_id": "binding-1", "deployment_revision_id": "drev-1", "manifest_ref": "manifests/revisions/manifest-1", "manifest_digest": "sha256:" + strings.Repeat("a", 64)} {
				snapshot[k] = v
			}
		}
		route, _ := json.Marshal(map[string]any{"schema_version": 1, "event_id": fmt.Sprintf("wecom-route-%d", generation), "enabled": enabled, "route": snapshot})
		if _, e := controlwire.DecodeRouteProjectionEvent(route); e != nil {
			t.Fatal(e)
		}
		if _, err = broker.JS.Publish(ctx, transport.RouteSubject, route); err != nil {
			t.Fatal(err)
		}
	}
	publishRoute(1, true)
	peers := make(chan wecomPeer, 12)
	var active, total atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		sc, stop := context.WithCancel(r.Context())
		defer stop()
		_, b, e := c.Read(sc)
		if e != nil {
			return
		}
		var frame struct {
			Cmd     string            `json:"cmd"`
			Headers map[string]string `json:"headers"`
			Body    map[string]string `json:"body"`
		}
		if json.Unmarshal(b, &frame) != nil || frame.Cmd != "aibot_subscribe" || frame.Body["bot_id"] != "fixture-bot" {
			return
		}
		secret := frame.Body["secret"]
		if secret != "fixture-secret-one" && secret != "fixture-secret-two" {
			return
		}
		ack, _ := json.Marshal(map[string]any{"headers": frame.Headers, "errcode": 0})
		if c.Write(sc, websocket.MessageText, ack) != nil {
			return
		}
		active.Add(1)
		defer active.Add(-1)
		total.Add(1)
		select {
		case peers <- wecomPeer{c, sc, secret}:
		case <-ctx.Done():
			return
		}
		for {
			_, b, e = c.Read(sc)
			if e != nil {
				return
			}
			if json.Unmarshal(b, &frame) != nil {
				return
			}
			if frame.Cmd != "ping" {
				return
			}
			ack, _ = json.Marshal(map[string]any{"headers": frame.Headers, "errcode": 0})
			if c.Write(sc, websocket.MessageText, ack) != nil {
				return
			}
		}
	}))
	defer server.Close()
	t.Setenv("CONNECTION_SECRET_ONE", "fixture-secret-one")
	t.Setenv("CONNECTION_SECRET_TWO", "fixture-secret-two")
	path := filepath.Join(t.TempDir(), "wecom.json")
	writeAccounts := func(revision int, enabled bool, ref string) {
		t.Helper()
		b, _ := json.Marshal([]map[string]any{{"account_id": "wecom-account", "bot_id": "fixture-bot", "revision": revision, "enabled": enabled, "secret_env": ref}})
		temp := path + ".new"
		if e := os.WriteFile(temp, b, 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.Rename(temp, path); e != nil {
			t.Fatal(e)
		}
	}
	writeAccounts(1, true, "CONNECTION_SECRET_ONE")
	database := fixtureDatabaseConfig(t, u.String())
	apps := []*App{}
	stops := []context.CancelFunc{}
	exits := []chan error{}
	closed := []bool{}
	for i := range 2 {
		cfg := Config{HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL, MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology, WeComAccountsFile: path, WeComURL: "ws" + strings.TrimPrefix(server.URL, "http"), InstanceID: fmt.Sprintf("replica-%d", i), ConnectionOptions: connapp.Options{LeaseTTL: 900 * time.Millisecond, PollInterval: 20 * time.Millisecond, OperationTimeout: 100 * time.Millisecond, DrainTimeout: 600 * time.Millisecond}}
		app, e := newWithDatabaseTarget(ctx, cfg, database.target)
		if e != nil {
			t.Fatal(e)
		}
		apps = append(apps, app)
		runCtx, stop := context.WithCancel(ctx)
		stops = append(stops, stop)
		done := make(chan error, 1)
		exits = append(exits, done)
		closed = append(closed, false)
		go func() { done <- app.Run(runCtx) }()
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
		for i, done := range exits {
			if !closed[i] {
				select {
				case e := <-done:
					if e != nil {
						t.Error(e)
					}
				case <-time.After(8 * time.Second):
					t.Error("Gateway shutdown exceeded bound")
				}
			}
			apps[i].Close()
		}
	}()
	nextPeer := func() wecomPeer {
		t.Helper()
		select {
		case p := <-peers:
			return p
		case <-ctx.Done():
			t.Fatal("missing authenticated connection")
			return wecomPeer{}
		}
	}
	first := nextPeer()
	eventually(t, func() bool { return apps[0].connections.Ready() && apps[1].connections.Ready() }, "one active plus standby ready")
	time.Sleep(100 * time.Millisecond)
	if active.Load() != 1 || total.Load() != 1 {
		t.Fatal("two replicas opened duplicate bot sockets")
	}
	eventually(t, func() bool { _, e := apps[0].routes.Resolve(ctx, "wecom", "wecom-account"); return e == nil }, "WeCom route applied")
	stream, e := broker.JS.Stream(ctx, transport.RunStream)
	if e != nil {
		t.Fatal(e)
	}
	consumer, e := stream.Consumer(ctx, transport.RunConsumer)
	if e != nil {
		t.Fatal(e)
	}
	count := func(n int64) bool {
		var got int64
		return db.QueryRow(ctx, "SELECT count(*) FROM gateway_admissions").Scan(&got) == nil && got == n
	}
	verifyRun := func(msgID string) {
		t.Helper()
		m, e := consumer.Next(jetstream.FetchMaxWait(3 * time.Second))
		if e != nil {
			t.Fatal(e)
		}
		event, e := wire.DecodeRunRequested(m.Data())
		if e != nil {
			t.Fatal(e)
		}
		if event.Input.Key.EventID != msgID || event.Input.Key.Provider != "wecom" {
			t.Fatalf("wrong durable event: %s", m.Data())
		}
		if strings.Contains(string(m.Data()), "fixture-secret") || strings.Contains(string(m.Data()), "instance_id") || strings.Contains(string(m.Data()), "owner_epoch") {
			t.Fatal("private material leaked into execution event")
		}
		if e = m.DoubleAck(ctx); e != nil {
			t.Fatal(e)
		}
	}
	first.send(t, "message-1", "request-1", "hello", "aibot_msg_callback", "")
	eventually(t, func() bool { return count(1) }, "first WeCom admission commits")
	verifyRun("message-1")
	first.send(t, "message-1", "request-2", "hello", "aibot_msg_callback", "")
	time.Sleep(100 * time.Millisecond)
	if !count(1) {
		t.Fatal("transport request ID changed dedup identity")
	}
	first.send(t, "notice-1", "notice-request", "", "aibot_event_callback", "enter_chat")
	eventually(t, func() bool {
		var decision string
		return db.QueryRow(ctx, "SELECT receipt->>'decision' FROM gateway_inbox WHERE event_id='notice-1'").Scan(&decision) == nil && decision == "interaction"
	}, "chatless notice records interaction without Run")
	if !count(1) {
		t.Fatal("chatless notice created Run")
	}
	publishRoute(2, false)
	eventually(t, func() bool {
		_, a := apps[0].routes.Resolve(ctx, "wecom", "wecom-account")
		_, b := apps[1].routes.Resolve(ctx, "wecom", "wecom-account")
		return a != nil && b != nil
	}, "route disabled through real publisher")
	first.send(t, "message-transient", "request-transient", "retain across outage", "aibot_msg_callback", "")
	time.Sleep(150 * time.Millisecond)
	if !count(1) || active.Load() != 1 || total.Load() != 1 {
		t.Fatal("unavailable route admitted or terminated socket")
	}
	publishRoute(3, true)
	eventually(t, func() bool { return count(2) }, "same callback retained until route recovers at same account revision")
	verifyRun("message-transient")
	if total.Load() != 1 {
		t.Fatal("brief Admission outage unnecessarily recreated Client")
	}
	publishRoute(4, false)
	eventually(t, func() bool {
		_, a := apps[0].routes.Resolve(ctx, "wecom", "wecom-account")
		_, b := apps[1].routes.Resolve(ctx, "wecom", "wecom-account")
		return a != nil && b != nil
	}, "longer outage visible")
	first.send(t, "message-not-admitted", "request-long-outage", "expires locally", "aibot_msg_callback", "")
	first = nextPeer()
	if total.Load() != 2 || !count(2) {
		t.Fatal("bounded failure did not recreate client at same revision")
	}
	publishRoute(5, true)
	eventually(t, func() bool {
		_, a := apps[0].routes.Resolve(ctx, "wecom", "wecom-account")
		_, b := apps[1].routes.Resolve(ctx, "wecom", "wecom-account")
		return a == nil && b == nil
	}, "route restored without changing account configuration")
	first.send(t, "message-recovered", "request-recovered", "new callback after recovery", "aibot_msg_callback", "")
	eventually(t, func() bool { return count(3) }, "same revision new Client resumes durable admission")
	verifyRun("message-recovered")
	var orphan int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM gateway_inbox WHERE event_id='message-not-admitted'").Scan(&orphan); err != nil || orphan != 0 {
		t.Fatal("unadmitted expired callback was fabricated as durable")
	}
	owner := 0
	s0, _ := apps[0].connections.Status("wecom-account")
	if !s0.Owned {
		owner = 1
	}
	before, _ := apps[owner].connections.Status("wecom-account")
	stops[owner]()
	select {
	case e := <-exits[owner]:
		closed[owner] = true
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("owner failed to drain")
	}
	second := nextPeer()
	other := 1 - owner
	eventually(t, func() bool {
		s, _ := apps[other].connections.Status("wecom-account")
		return s.Owned && s.Ready && s.Epoch > before.Epoch
	}, "standby takes higher epoch")
	second.send(t, "message-1", "request-3", "hello", "aibot_msg_callback", "")
	second.send(t, "message-1", "request-conflict", "changed", "aibot_msg_callback", "")
	second.send(t, "message-2", "request-4", "next", "aibot_msg_callback", "")
	eventually(t, func() bool { return count(4) }, "old receipt replay and conflict do not create new Run")
	verifyRun("message-2")
	second.send(t, "replacement", "replacement-req", "", "aibot_event_callback", "disconnected_event")
	eventually(t, func() bool {
		s, _ := apps[other].connections.Status("wecom-account")
		return !s.Ready && s.Phase == connapp.PhaseBlocked && s.IsolationPersisted && !s.IsolationError
	}, "replacement fenced")
	time.Sleep(1100 * time.Millisecond)
	if total.Load() != 3 || active.Load() != 0 {
		t.Fatal("replaced revision was automatically reclaimed")
	}
	writeAccounts(2, true, "CONNECTION_SECRET_TWO")
	third := nextPeer()
	if third.secret != "fixture-secret-two" {
		t.Fatal("credential revision did not rotate")
	}
	third.send(t, "message-3", "request-5", "rotated", "aibot_msg_callback", "")
	eventually(t, func() bool { return count(5) }, "higher revision restores admission")
	verifyRun("message-3")
	writeAccounts(3, false, "CONNECTION_SECRET_TWO")
	eventually(t, func() bool {
		s, _ := apps[other].connections.Status("wecom-account")
		return s.Phase == connapp.PhaseDisabled && active.Load() == 0
	}, "disabled account drains socket")
	writeAccounts(2, true, "CONNECTION_SECRET_TWO")
	time.Sleep(150 * time.Millisecond)
	if active.Load() != 0 {
		t.Fatal("stale configuration revived disabled account")
	}
	writeAccounts(4, true, "CONNECTION_SECRET_TWO")
	fourth := nextPeer()
	fourth.send(t, "message-4", "request-6", "enabled", "aibot_msg_callback", "")
	eventually(t, func() bool { return count(6) }, "higher revision enables account")
	verifyRun("message-4")
	t.Log("WECOM_GATEWAY_VERIFIED: real WS/PG/NATS; two replicas one socket; durable RunRequested; req_id and owner handover preserve dedup; conflict does not reconnect; higher epoch takeover; replaced no reclaim; credential revision; disable/stale/re-enable; chatless notice; same-event temporary route recovery without socket restart; budget exhaustion recreates client at same revision; expired callback not fabricated; no secret/fence on wire")
}
