package wecomclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	managed "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/wecomclient"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

func writeJSON(ctx context.Context, c *websocket.Conn, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}
func textFrame(id string) map[string]any {
	return map[string]any{"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": "request-" + id}, "body": map[string]any{"msgid": id, "aibotid": "bot-1", "chattype": "single", "from": map[string]string{"userid": "user-1"}, "msgtype": "text", "text": map[string]string{"content": "hello"}}}
}
func replacementFrame() map[string]any {
	return map[string]any{"cmd": "aibot_event_callback", "headers": map[string]string{"req_id": "request-replaced"}, "body": map[string]any{"msgid": "replaced", "aibotid": "bot-1", "msgtype": "event", "event": map[string]string{"eventtype": "disconnected_event"}}}
}
func wireServer(t *testing.T, serve func(context.Context, *websocket.Conn, map[string]any)) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		connections.Add(1)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var subscribe map[string]any
		if err = json.Unmarshal(data, &subscribe); err != nil {
			t.Error(err)
			return
		}
		if subscribe["cmd"] != "aibot_subscribe" {
			t.Error("missing subscribe")
			return
		}
		serve(ctx, c, subscribe)
	}))
	t.Cleanup(server.Close)
	return server, &connections
}
func acknowledge(ctx context.Context, c *websocket.Conn, subscribe map[string]any, code int64) error {
	return writeJSON(ctx, c, map[string]any{"headers": subscribe["headers"], "errcode": code})
}
func protocolClient(t *testing.T, url string, modify func(*wecom.Config)) *wecom.Client {
	t.Helper()
	cfg := wecom.Config{BotID: "bot-1", Secret: "fixture-secret", URL: "ws" + strings.TrimPrefix(url, "http"), AckTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, HeartbeatInterval: 10 * time.Second, CloseTimeout: 200 * time.Millisecond, ReconnectBackoff: time.Millisecond}
	if modify != nil {
		modify(&cfg)
	}
	c, err := wecom.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func policy() managed.ErrorPolicy {
	return managed.ErrorPolicy{Temporary: func(error) bool { return false }, Rejected: func(error) bool { return false }}
}
func wrap(t *testing.T, c *wecom.Client, h wecom.Handler, p managed.ErrorPolicy) *managed.Client {
	t.Helper()
	result, err := managed.NewClient(c, h, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = result.Close(ctx)
	})
	return result
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
func eventually(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("state did not become ready")
}

func TestProtocolTerminalCannotInheritTemporaryHandlerCandidate(t *testing.T) {
	peer := make(chan *websocket.Conn, 1)
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if err := acknowledge(ctx, c, s, 0); err != nil {
			return
		}
		peer <- c
		_ = writeJSON(ctx, c, textFrame("one"))
		_, _, _ = c.Read(ctx)
	})
	predicateEntered, resumePredicate := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releasePredicate := func() { releaseOnce.Do(func() { close(resumePredicate) }) }
	defer releasePredicate()
	handlerContext := make(chan context.Context, 1)
	transient := errors.New("temporary admission failure")
	p := policy()
	p.Temporary = func(err error) bool { close(predicateEntered); <-resumePredicate; return errors.Is(err, transient) }
	client := wrap(t, protocolClient(t, server.URL, nil), func(ctx context.Context, _ wecom.Event) error { handlerContext <- ctx; return transient }, p)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	c := receive(t, peer)
	eventCtx := receive(t, handlerContext)
	receive(t, predicateEntered)
	// The classification callback is a public dependency; pausing it models a
	// scheduler preemption between the managed adapter's ctx check and flag write.
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"headers":`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-eventCtx.Done():
	case <-ctx.Done():
		t.Fatal("protocol failure did not cancel callback")
	}
	releasePredicate()
	if err := receive(t, done); !errors.Is(err, wecom.ErrProtocol) {
		t.Fatalf("terminal cause=%v", err)
	}
	if status := client.Status(); !status.Terminal || status.Retryable || status.Replaced {
		t.Fatalf("protocol terminal polluted by old temporary candidate: %+v", status)
	}
}

func TestTerminalCauseAndSDKReconnectBudget(t *testing.T) {
	for _, tc := range []struct {
		name                string
		want                error
		replaced, retryable bool
		dials               int32
	}{
		{"auth", wecom.ErrAuth, false, false, 1},
		{"protocol", wecom.ErrProtocol, false, false, 1},
		{"replaced", wecom.ErrReplaced, true, false, 1},
		{"disconnect budget", wecom.ErrDisconnected, false, false, 3},
		{"temporary handler", wecom.ErrHandler, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, dials := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
				if tc.name == "auth" {
					_ = acknowledge(ctx, c, s, 40001)
					_, _, _ = c.Read(ctx)
					return
				}
				if acknowledge(ctx, c, s, 0) != nil {
					return
				}
				switch tc.name {
				case "protocol":
					_ = c.Write(ctx, websocket.MessageText, []byte(`{"headers":`))
				case "replaced":
					_ = writeJSON(ctx, c, replacementFrame())
				case "disconnect budget":
					return
				case "temporary handler":
					_ = writeJSON(ctx, c, textFrame("temporary"))
				}
				_, _, _ = c.Read(ctx)
			})
			transient := errors.New("temporary admission failure")
			p := policy()
			p.Temporary = func(err error) bool { return errors.Is(err, transient) }
			client := wrap(t, protocolClient(t, server.URL, func(cfg *wecom.Config) { cfg.MaxReconnects = 2 }), func(context.Context, wecom.Event) error { return transient }, p)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Run(ctx); !errors.Is(err, tc.want) {
				t.Fatalf("result=%v want %v", err, tc.want)
			}
			status := client.Status()
			if !status.Terminal || status.Ready || status.Replaced != tc.replaced || status.Retryable != tc.retryable {
				t.Fatalf("terminal classification=%+v", status)
			}
			if dials.Load() != tc.dials {
				t.Fatalf("dials=%d want %d", dials.Load(), tc.dials)
			}
		})
	}
}

func TestQuiesceDrainsAcceptedCallbackAndDropsSubsequentIngress(t *testing.T) {
	peer := make(chan *websocket.Conn, 1)
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		peer <- c
		_, _, _ = c.Read(ctx)
	})
	entered, release := make(chan struct{}), make(chan struct{})
	afterQuiesce := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	var calls atomic.Int32
	client := wrap(t, protocolClient(t, server.URL, nil), func(ctx context.Context, _ wecom.Event) error {
		if calls.Add(1) == 1 {
			close(entered)
		} else {
			close(afterQuiesce)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, policy())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	c := receive(t, peer)
	eventually(t, func() bool { return client.Status().Ready })
	if err := writeJSON(ctx, c, textFrame("first")); err != nil {
		t.Fatal(err)
	}
	receive(t, entered)
	client.Quiesce()
	drainCtx, drainCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if err := client.Drain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active callback not retained: %v", err)
	}
	drainCancel()
	releaseHandler()
	if err := client.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(ctx, c, textFrame("second")); err != nil {
		t.Fatal(err)
	}
	// A protocol ping proves the reader advanced beyond the second callback;
	// quiescing does not close a socket or interrupt its protocol loop.
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-afterQuiesce:
		t.Fatal("quiesced client accepted a callback while socket was live")
	case <-time.After(25 * time.Millisecond):
	}
	if !client.Status().Ready {
		t.Fatal("quiesce closed the authenticated protocol loop")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("callback accepted after quiesce: %d", calls.Load())
	}
	if status := client.Status(); !status.Terminal || status.Retryable || status.Ready {
		t.Fatalf("closed status=%+v", status)
	}
}

func TestCloseCancelsActiveCallbackAndDoubleRunPreservesLifecycle(t *testing.T) {
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		_, _, _ = c.Read(ctx)
	})
	entered := make(chan struct{})
	canceled := make(chan struct{})
	client := wrap(t, protocolClient(t, server.URL, nil), func(ctx context.Context, _ wecom.Event) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}, policy())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	receive(t, entered)
	if err := client.Run(ctx); !errors.Is(err, wecom.ErrAlreadyRun) {
		t.Fatalf("second Run=%v", err)
	}
	if status := client.Status(); status.Terminal || !status.Ready {
		t.Fatalf("second Run corrupted first lifecycle: %+v", status)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	receive(t, canceled)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if err := client.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOverflowPublishesTerminalWithFinalRetryClassification(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		entered := make(chan struct{})
		server, dials := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
			if acknowledge(ctx, c, s, 0) != nil {
				return
			}
			_ = writeJSON(ctx, c, textFrame("first"))
			select {
			case <-entered:
			case <-ctx.Done():
				return
			}
			_ = writeJSON(ctx, c, textFrame("second"))
			_ = writeJSON(ctx, c, textFrame("third"))
			_, _, _ = c.Read(ctx)
		})
		client := wrap(t, protocolClient(t, server.URL, func(cfg *wecom.Config) { cfg.EventBuffer = 1; cfg.MaxReconnects = 2 }), func(ctx context.Context, _ wecom.Event) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}, policy())
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		done := make(chan error, 1)
		go func() { done <- client.Run(ctx) }()
		for !client.Status().Terminal {
			select {
			case <-ctx.Done():
				cancel()
				t.Fatal("overflow did not terminate")
			default:
				runtime.Gosched()
			}
		}
		if status := client.Status(); !status.Retryable || status.Replaced {
			cancel()
			t.Fatalf("terminal observed before final overflow classification: %+v", status)
		}
		if err := receive(t, done); !errors.Is(err, wecom.ErrEventOverflow) {
			cancel()
			t.Fatalf("overflow result=%v", err)
		}
		if dials.Load() != 1 {
			cancel()
			t.Fatalf("SDK rebuilt after overflow: %d", dials.Load())
		}
		cancel()
	}
}

// These fixtures implement the Supervisor's external persistence/configuration
// ports; protocol, managed lifecycle, and Supervisor recovery are real code.
// Lease fencing itself is covered by separate PostgreSQL integration tests.
type availableLeaseStore struct{ epoch atomic.Int64 }

func (s *availableLeaseStore) ApplyAndAcquire(_ context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
	now := time.Now()
	return domain.OwnerGrant{AccountID: a.ID, BotID: a.BotID, InstanceID: id, Revision: a.Revision, Epoch: s.epoch.Add(1), ObservedAt: now, LeaseUntil: now.Add(ttl)}, nil
}
func (s *availableLeaseStore) Renew(_ context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
	g.ObservedAt = time.Now()
	g.LeaseUntil = g.ObservedAt.Add(ttl)
	return g, nil
}
func (*availableLeaseStore) Release(context.Context, domain.OwnerGrant) error      { return nil }
func (*availableLeaseStore) Check(context.Context, domain.OwnerGrant) error        { return nil }
func (*availableLeaseStore) MarkReplaced(context.Context, domain.OwnerGrant) error { return nil }

type accountSourceFunc func(context.Context) ([]domain.Account, error)

func (f accountSourceFunc) List(ctx context.Context) ([]domain.Account, error) { return f(ctx) }

type credentialFunc func(context.Context, domain.Account) (connection.CredentialMaterial, error)

func (f credentialFunc) Resolve(ctx context.Context, a domain.Account) (connection.CredentialMaterial, error) {
	return f(ctx, a)
}

type factoryFunc func(context.Context, domain.Account, domain.OwnerGrant, connection.CredentialMaterial) (connection.Client, error)

func (f factoryFunc) New(ctx context.Context, a domain.Account, g domain.OwnerGrant, m connection.CredentialMaterial) (connection.Client, error) {
	return f(ctx, a, g, m)
}

func TestOverflowRecoveryUsesSupervisorBackoffButCannotResetAuthenticationBudget(t *testing.T) {
	entered := make(chan struct{})
	var sequence atomic.Int32
	authTimes := make(chan time.Time, 3)
	server, dials := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		authTimes <- time.Now()
		if sequence.Add(1) == 1 {
			if acknowledge(ctx, c, s, 0) != nil {
				return
			}
			_ = writeJSON(ctx, c, textFrame("first"))
			select {
			case <-entered:
			case <-ctx.Done():
				return
			}
			_ = writeJSON(ctx, c, textFrame("second"))
			_ = writeJSON(ctx, c, textFrame("third"))
		} else {
			_ = acknowledge(ctx, c, s, 40001)
		}
		_, _, _ = c.Read(ctx)
	})
	a := domain.Account{ID: "account-1", BotID: "bot-1", CredentialRef: "FIXTURE_SECRET", Revision: 1, Enabled: true}
	source := accountSourceFunc(func(context.Context) ([]domain.Account, error) { return []domain.Account{a}, nil })
	resolver := credentialFunc(func(context.Context, domain.Account) (connection.CredentialMaterial, error) {
		return connection.CredentialMaterial{Secret: "fixture-secret"}, nil
	})
	factory := factoryFunc(func(_ context.Context, a domain.Account, _ domain.OwnerGrant, m connection.CredentialMaterial) (connection.Client, error) {
		sdk, err := wecom.NewClient(wecom.Config{BotID: a.BotID, Secret: m.Secret, URL: "ws" + strings.TrimPrefix(server.URL, "http"), EventBuffer: 1, MaxReconnects: 2, AckTimeout: 200 * time.Millisecond, HeartbeatInterval: 10 * time.Second, CloseTimeout: 200 * time.Millisecond, ReconnectBackoff: time.Millisecond})
		if err != nil {
			return nil, err
		}
		return managed.NewClient(sdk, func(ctx context.Context, _ wecom.Event) error { close(entered); <-ctx.Done(); return ctx.Err() }, policy(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	const backoff = 40 * time.Millisecond
	supervisor, err := connection.NewSupervisor(&availableLeaseStore{}, source, resolver, factory, connection.Options{InstanceID: "instance-1", LeaseTTL: 300 * time.Millisecond, PollInterval: 5 * time.Millisecond, OperationTimeout: 20 * time.Millisecond, DrainTimeout: 100 * time.Millisecond, MaxAccounts: 2, MaxRestarts: 1, RestartBackoff: backoff, RestartCooldown: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	first, second := receive(t, authTimes), receive(t, authTimes)
	if second.Sub(first) < backoff {
		t.Fatalf("source polling bypassed backoff: %v", second.Sub(first))
	}
	eventually(t, func() bool {
		status, ok := supervisor.Status(a.ID)
		return ok && status.Phase == connection.PhaseBlocked && status.Reason == connection.ReasonClient
	})
	// Exceed a cooldown and many source polls: auth failure is not half-opened,
	// and its SDK MaxReconnects budget is not reset by constructing new Clients.
	select {
	case <-authTimes:
		t.Fatal("authentication failure incorrectly rebuilt a new Client")
	case <-time.After(150 * time.Millisecond):
	}
	if dials.Load() != 2 {
		t.Fatalf("expected overflow and one bounded retry, dials=%d", dials.Load())
	}
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}
