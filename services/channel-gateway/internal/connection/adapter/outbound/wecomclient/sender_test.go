package wecomclient_test

import (
	"context"
	"encoding/json"
	"errors"
	managed "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/wecomclient"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
)

func TestReserveFinalHasNoSideEffectAndDrainIncludesPersistence(t *testing.T) {
	frames := make(chan map[string]any, 4)
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			if json.Unmarshal(raw, &f) != nil {
				return
			}
			frames <- f
			_ = acknowledge(ctx, c, f, 0)
		}
	})
	events := make(chan wecom.Event, 1)
	client := wrap(t, protocolClient(t, server.URL, nil), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	e := receive(t, events)
	sender, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	select {
	case <-frames:
		t.Fatal("Reserve sent Provider frame")
	case <-time.After(20 * time.Millisecond):
	}
	result := sender.SendFinal(ctx, connection.FinalCommand{StreamID: "stream-one", Content: "final"})
	if result.Certainty != connection.Accepted || result.ProviderCode == nil || *result.ProviderCode != 0 {
		t.Fatalf("result=%+v", result)
	}
	frame := receive(t, frames)
	if frame["cmd"] != "aibot_respond_msg" {
		t.Fatal(frame)
	}
	client.Quiesce()
	drain := make(chan error, 1)
	go func() { drain <- client.Drain(ctx) }()
	select {
	case <-drain:
		t.Fatal("Drain returned before evidence persistence released reservation")
	case <-time.After(20 * time.Millisecond):
	}
	if again := sender.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "again"}); again.Certainty != connection.NotSent {
		t.Fatal(again)
	}
	sender.Release()
	sender.Release()
	if err = receive(t, drain); err != nil {
		t.Fatal(err)
	}
	cancel()
	receive(t, done)
}

func TestReservationRejectsOriginalSocketAfterReconnect(t *testing.T) {
	peers := make(chan *websocket.Conn, 4)
	events := make(chan wecom.Event, 4)
	var finals atomic.Int32
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		peers <- c
		_ = writeJSON(ctx, c, textFrame("same"))
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(raw, &f)
			finals.Add(1)
			_ = acknowledge(ctx, c, f, 0)
		}
	})
	client := wrap(t, protocolClient(t, server.URL, func(c *wecom.Config) { c.MaxReconnects = 1 }), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	first := receive(t, events)
	old, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: first.RequestID, SocketGeneration: first.Generation})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	c := receive(t, peers)
	_ = c.CloseNow()
	second := receive(t, events)
	if second.Generation != 2 {
		t.Fatalf("generation=%d", second.Generation)
	}
	if result := old.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "old"}); result.Certainty != connection.NotSent || result.Code != connection.SendStale {
		t.Fatal(result)
	}
	if _, err = client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: first.RequestID, SocketGeneration: first.Generation}); !errors.Is(err, connection.ErrStaleOrigin) {
		t.Fatal(err)
	}
	fresh, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: second.RequestID, SocketGeneration: second.Generation})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if result := fresh.SendFinal(ctx, connection.FinalCommand{StreamID: "fresh", Content: "current"}); result.Certainty != connection.Accepted {
		t.Fatal(result)
	}
	if finals.Load() != 1 {
		t.Fatalf("finals=%d", finals.Load())
	}
	cancel()
	receive(t, done)
}

func TestReservationConcurrentOnceAndReleaseWhileCalling(t *testing.T) {
	written, allowAck := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseAck := func() { once.Do(func() { close(allowAck) }) }
	defer releaseAck()
	var finals atomic.Int32
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		finals.Add(1)
		var f map[string]any
		_ = json.Unmarshal(raw, &f)
		close(written)
		select {
		case <-allowAck:
		case <-ctx.Done():
			return
		}
		_ = acknowledge(ctx, c, f, 0)
		_, _, _ = c.Read(ctx)
	})
	events := make(chan wecom.Event, 1)
	client := wrap(t, protocolClient(t, server.URL, func(c *wecom.Config) { c.AckTimeout = time.Second }), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	e := receive(t, events)
	sender, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	results := make(chan connection.SendResult, 20)
	for range 20 {
		go func() { results <- sender.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "once"}) }()
	}
	receive(t, written)
	sender.Release()
	client.Quiesce()
	drain := make(chan error, 1)
	go func() { drain <- client.Drain(ctx) }()
	select {
	case <-drain:
		t.Fatal("Release erased active network call")
	case <-time.After(20 * time.Millisecond):
	}
	releaseAck()
	accepted := 0
	for range 20 {
		r := receive(t, results)
		if r.Certainty == connection.Accepted {
			accepted++
		} else if r.Certainty != connection.NotSent {
			t.Fatal(r)
		}
	}
	if accepted != 1 || finals.Load() != 1 {
		t.Fatalf("accepted=%d finals=%d", accepted, finals.Load())
	}
	if err = receive(t, drain); err != nil {
		t.Fatal(err)
	}
	cancel()
	receive(t, done)
}

func TestReservationCancelAfterWriteLateAckAndPoison(t *testing.T) {
	written := make(chan struct{})
	lateAck := make(chan struct{})
	var once sync.Once
	releaseAck := func() { once.Do(func() { close(lateAck) }) }
	defer releaseAck()
	var finals atomic.Int32
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		finals.Add(1)
		var f map[string]any
		_ = json.Unmarshal(raw, &f)
		close(written)
		select {
		case <-lateAck:
		case <-ctx.Done():
			return
		}
		_ = acknowledge(ctx, c, f, 0)
		_, _, _ = c.Read(ctx)
	})
	events := make(chan wecom.Event, 1)
	client := wrap(t, protocolClient(t, server.URL, nil), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	e := receive(t, events)
	target := connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation}
	sender, err := client.ReserveFinal(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	call, stop := context.WithCancel(ctx)
	result := make(chan connection.SendResult, 1)
	go func() { result <- sender.SendFinal(call, connection.FinalCommand{StreamID: "s", Content: "ambiguous"}) }()
	receive(t, written)
	stop()
	if got := receive(t, result); got.Certainty != connection.Unknown {
		t.Fatalf("canceled after write=%+v", got)
	}
	releaseAck()
	retry, err := client.ReserveFinal(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Release()
	got := retry.SendFinal(ctx, connection.FinalCommand{StreamID: "new-stream", Content: "retry"})
	if got.Certainty != connection.NotSent || got.Code != connection.SendCode(wecom.CodePoisoned) || finals.Load() != 1 {
		t.Fatalf("late ack/retry=%+v count=%d", got, finals.Load())
	}
	cancel()
	receive(t, done)
}

func TestCanceledSendConsumesReservation(t *testing.T) {
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(raw, &f)
			_ = acknowledge(ctx, c, f, 0)
		}
	})
	events := make(chan wecom.Event, 1)
	client := wrap(t, protocolClient(t, server.URL, nil), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	e := receive(t, events)
	sender, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	canceled, stop := context.WithCancel(ctx)
	stop()
	if got := sender.SendFinal(canceled, connection.FinalCommand{StreamID: "s", Content: "first"}); got.Certainty != connection.NotSent {
		t.Fatal(got)
	}
	if got := sender.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "again"}); got.Certainty != connection.NotSent || got.Code != connection.SendUsed {
		t.Fatalf("canceled handle reused: %+v", got)
	}
	cancel()
	receive(t, done)
}

type observedLeases struct {
	availableLeaseStore
	renews, releases atomic.Int64
}

func (s *observedLeases) Renew(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
	s.renews.Add(1)
	return s.availableLeaseStore.Renew(ctx, g, ttl)
}
func (s *observedLeases) Release(context.Context, domain.OwnerGrant) error {
	s.releases.Add(1)
	return nil
}

type originEvent struct {
	event wecom.Event
	grant domain.OwnerGrant
}

func senderSupervisor(t *testing.T, url string, store connection.LeaseStore, revision *atomic.Int64, events chan<- originEvent) *connection.Supervisor {
	t.Helper()
	source := accountSourceFunc(func(context.Context) ([]domain.Account, error) {
		return []domain.Account{{ID: "account-1", BotID: "bot-1", CredentialRef: "FIXTURE_SECRET", Revision: revision.Load(), Enabled: true}}, nil
	})
	resolver := credentialFunc(func(context.Context, domain.Account) (connection.CredentialMaterial, error) {
		return connection.CredentialMaterial{Secret: "fixture-secret"}, nil
	})
	factory := factoryFunc(func(_ context.Context, a domain.Account, g domain.OwnerGrant, m connection.CredentialMaterial) (connection.Client, error) {
		sdk, err := wecom.NewClient(wecom.Config{BotID: a.BotID, Secret: m.Secret, URL: "ws" + strings.TrimPrefix(url, "http"), AckTimeout: time.Second, WriteTimeout: 200 * time.Millisecond, HeartbeatInterval: 10 * time.Second, CloseTimeout: 200 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		return managed.NewClient(sdk, func(_ context.Context, e wecom.Event) error { events <- originEvent{e, g}; return nil }, policy(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	s, err := connection.NewSupervisor(store, source, resolver, factory, connection.Options{InstanceID: "instance-1", LeaseTTL: 300 * time.Millisecond, PollInterval: 5 * time.Millisecond, OperationTimeout: 20 * time.Millisecond, DrainTimeout: time.Second, MaxAccounts: 2})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func senderOrigin(e originEvent) connection.SenderOrigin {
	return connection.SenderOrigin{AccountID: e.grant.AccountID, InstanceID: e.grant.InstanceID, Epoch: e.grant.Epoch, Revision: e.grant.Revision, SocketGeneration: e.event.Generation, RequestID: e.event.RequestID}
}

func TestSupervisorDrainRenewsUntilSendEvidenceReleased(t *testing.T) {
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(raw, &f)
			_ = acknowledge(ctx, c, f, 0)
		}
	})
	leases := &observedLeases{}
	var revision atomic.Int64
	revision.Store(1)
	events := make(chan originEvent, 4)
	s := senderSupervisor(t, server.URL, leases, &revision, events)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	e := receive(t, events)
	sender, err := s.ReserveFinal(ctx, senderOrigin(e))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	if result := sender.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "final"}); result.Certainty != connection.Accepted {
		t.Fatal(result)
	}
	before := leases.renews.Load()
	cancel()
	eventually(t, func() bool { st, _ := s.Status("account-1"); return st.Phase == connection.PhaseDraining })
	eventually(t, func() bool { return leases.renews.Load() > before })
	if leases.releases.Load() != 0 {
		t.Fatal("lease released before evidence persisted")
	}
	select {
	case <-done:
		t.Fatal("Supervisor exited before Release")
	default:
	}
	if _, err = s.ReserveFinal(context.Background(), senderOrigin(e)); err == nil {
		t.Fatal("reserve after shutdown")
	}
	sender.Release()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if leases.releases.Load() != 1 {
		t.Fatal("normal shutdown did not release lease")
	}
}

func TestSupervisorRejectsOldOwnerWhenNewClientGenerationResets(t *testing.T) {
	var finals atomic.Int32
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("same"))
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(raw, &f)
			finals.Add(1)
			_ = acknowledge(ctx, c, f, 0)
		}
	})
	var revision atomic.Int64
	revision.Store(1)
	events := make(chan originEvent, 4)
	s := senderSupervisor(t, server.URL, &observedLeases{}, &revision, events)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	first := receive(t, events)
	old, err := s.ReserveFinal(ctx, senderOrigin(first))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	revision.Store(2)
	second := receive(t, events)
	if second.event.Generation != 1 || first.event.Generation != 1 || second.grant.Epoch <= first.grant.Epoch {
		t.Fatal("fixture did not cross owner with generation reset")
	}
	if _, err = s.ReserveFinal(ctx, senderOrigin(first)); !errors.Is(err, connection.ErrStaleOrigin) {
		t.Fatal(err)
	}
	if result := old.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "old"}); result.Certainty != connection.NotSent {
		t.Fatal(result)
	}
	fresh, err := s.ReserveFinal(ctx, senderOrigin(second))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if result := fresh.SendFinal(ctx, connection.FinalCommand{StreamID: "fresh", Content: "new"}); result.Certainty != connection.Accepted {
		t.Fatal(result)
	}
	if finals.Load() != 1 {
		t.Fatalf("finals=%d", finals.Load())
	}
	fresh.Release()
	old.Release()
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestReservationQuotaAndQuiesceNeverWrites(t *testing.T) {
	server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
		if acknowledge(ctx, c, s, 0) != nil {
			return
		}
		_ = writeJSON(ctx, c, textFrame("one"))
		_, _, _ = c.Read(ctx)
	})
	events := make(chan wecom.Event, 1)
	client := wrap(t, protocolClient(t, server.URL, nil), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	e := receive(t, events)
	target := connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation}
	slots := make([]connection.ReservedSender, 0, managed.MaxReservations)
	defer func() {
		for _, slot := range slots {
			slot.Release()
		}
	}()
	for range managed.MaxReservations {
		r, err := client.ReserveFinal(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		slots = append(slots, r)
	}
	if _, err := client.ReserveFinal(ctx, target); !errors.Is(err, connection.ErrSenderCapacity) {
		t.Fatal(err)
	}
	slots[0].Release()
	r, err := client.ReserveFinal(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	client.Quiesce()
	if _, err = client.ReserveFinal(ctx, target); !errors.Is(err, connection.ErrSenderUnavailable) {
		t.Fatal(err)
	}
	if got := r.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "closed"}); got.Certainty != connection.NotSent || got.Code != connection.SendQuiescing {
		t.Fatal(got)
	}
	cancel()
	receive(t, done)
}

func TestSenderPreservesProviderRejectionTimeoutAndCloseCertainty(t *testing.T) {
	for _, mode := range []string{"rejected", "timeout", "close"} {
		t.Run(mode, func(t *testing.T) {
			written := make(chan struct{})
			server, _ := wireServer(t, func(ctx context.Context, c *websocket.Conn, s map[string]any) {
				if acknowledge(ctx, c, s, 0) != nil {
					return
				}
				_ = writeJSON(ctx, c, textFrame("one"))
				_, raw, err := c.Read(ctx)
				if err != nil {
					return
				}
				var f map[string]any
				_ = json.Unmarshal(raw, &f)
				close(written)
				if mode == "rejected" {
					_ = acknowledge(ctx, c, f, 40014)
				}
				_, _, _ = c.Read(ctx)
			})
			events := make(chan wecom.Event, 1)
			client := wrap(t, protocolClient(t, server.URL, func(c *wecom.Config) { c.AckTimeout = 100 * time.Millisecond }), func(_ context.Context, e wecom.Event) error { events <- e; return nil }, policy())
			if _, err := client.ReserveFinal(context.Background(), connection.ReplyTarget{RequestID: "req", SocketGeneration: 1}); !errors.Is(err, connection.ErrSenderUnavailable) {
				t.Fatal("unauthenticated reserve", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- client.Run(ctx) }()
			e := receive(t, events)
			sender, err := client.ReserveFinal(ctx, connection.ReplyTarget{RequestID: e.RequestID, SocketGeneration: e.Generation})
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Release()
			result := make(chan connection.SendResult, 1)
			go func() { result <- sender.SendFinal(ctx, connection.FinalCommand{StreamID: "s", Content: "final"}) }()
			receive(t, written)
			if mode == "close" {
				cleanup, stop := context.WithTimeout(context.Background(), time.Second)
				err = client.Close(cleanup)
				stop()
				if err != nil {
					t.Fatal(err)
				}
			}
			got := receive(t, result)
			if mode == "rejected" {
				if got.Certainty != connection.Rejected || got.ProviderCode == nil || *got.ProviderCode != 40014 || got.Code != connection.SendCode(wecom.CodeRejected) {
					t.Fatal(got)
				}
			} else if got.Certainty != connection.Unknown || got.ProviderCode != nil {
				t.Fatal(got)
			}
			cancel()
			receive(t, done)
		})
	}
}
