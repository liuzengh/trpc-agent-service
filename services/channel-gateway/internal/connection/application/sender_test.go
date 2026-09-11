package application_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type finalClient struct {
	*client
	sends    atomic.Int64
	releases atomic.Int64
}
type fakeReservation struct {
	client *finalClient
	once   atomic.Bool
}

func (c *finalClient) ReserveFinal(context.Context, app.ReplyTarget) (app.ReservedSender, error) {
	return &fakeReservation{client: c}, nil
}
func (r *fakeReservation) SendFinal(context.Context, app.FinalCommand) app.SendResult {
	r.client.sends.Add(1)
	return app.SendResult{Certainty: app.Accepted}
}
func (r *fakeReservation) Release() {
	if r.once.CompareAndSwap(false, true) {
		r.client.releases.Add(1)
	}
}
func TestSenderLookupExactOwnerAndLostHandle(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	created := make(chan *finalClient, 10)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := &finalClient{client: newClient()}
		created <- c
		return c, nil
	})
	s, err := app.NewSupervisor(store, src, &resolver{}, fac, options())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	c := wait(t, created)
	wait(t, c.started)
	eventually(t, func() bool { st, _ := s.Status("account"); return st.Ready })
	origin := app.SenderOrigin{AccountID: "account", InstanceID: "instance", Epoch: 1, Revision: 1, SocketGeneration: 1, RequestID: "req"}
	for _, mutate := range []func(*app.SenderOrigin){func(o *app.SenderOrigin) { o.Epoch++ }, func(o *app.SenderOrigin) { o.InstanceID = "other" }, func(o *app.SenderOrigin) { o.Revision++ }} {
		bad := origin
		mutate(&bad)
		if _, err = s.ReserveFinal(ctx, bad); !errors.Is(err, app.ErrStaleOrigin) {
			t.Fatalf("mismatch=%v", err)
		}
	}
	sender, err := s.ReserveFinal(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	if c.sends.Load() != 0 {
		t.Fatal("reserve sent")
	}
	store.lost.Store(true)
	wait(t, c.stopped)
	result := sender.SendFinal(ctx, app.FinalCommand{StreamID: "s", Content: "final"})
	if result.Certainty != app.NotSent || c.sends.Load() != 0 {
		t.Fatalf("lost handle=%+v sends=%d", result, c.sends.Load())
	}
	sender.Release()
	sender.Release()
	if c.releases.Load() != 1 {
		t.Fatal("release not idempotent")
	}
	cancel()
	wait(t, done)
}
