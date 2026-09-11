package application_test

import (
	"context"
	"errors"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	"testing"
	"time"
)

func TestCredentialResolveOutlivesLeaseOperationBudget(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 4)}
	res := resolverFunc(func(ctx context.Context, _ domain.Account) (app.CredentialMaterial, error) {
		select {
		case <-time.After(3 * time.Second):
			return app.CredentialMaterial{Secret: "synthetic-secret"}, nil
		case <-ctx.Done():
			return app.CredentialMaterial{}, ctx.Err()
		}
	})
	opts := app.Options{InstanceID: "instance", PollInterval: 10 * time.Millisecond}
	_, cancel, done := start(t, store, src, res, fac, opts)
	defer cancel()
	select {
	case c := <-fac.created:
		wait(t, c.started)
	case <-time.After(4 * time.Second):
		t.Fatal("3-second Resolve was truncated by the 2-second lease budget")
	}
	cancel()
	wait(t, done)
}
func TestCredentialIOCancelledOnSourceFailureAndVersionChange(t *testing.T) {
	for _, mode := range []string{"source", "version", "owner"} {
		t.Run(mode, func(t *testing.T) {
			store := &leaseStore{}
			src := &source{accounts: []domain.Account{account(1)}}
			entered := make(chan struct{}, 1)
			cancelled := make(chan struct{}, 1)
			res := resolverFunc(func(ctx context.Context, _ domain.Account) (app.CredentialMaterial, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-ctx.Done()
				select {
				case cancelled <- struct{}{}:
				default:
				}
				return app.CredentialMaterial{}, ctx.Err()
			})
			_, cancel, done := start(t, store, src, res, &factory{}, options())
			defer cancel()
			wait(t, entered)
			switch mode {
			case "source":
				src.set(nil, errors.New("synthetic source unavailable"))
			case "version":
				src.set([]domain.Account{account(2)}, nil)
			case "owner":
				store.lost.Store(true)
			}
			wait(t, cancelled)
			cancel()
			wait(t, done)
		})
	}
}

func TestLateSuccessfulCredentialResponseDoesNotConstruct(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 10)}
	returned := make(chan struct{}, 1)
	res := resolverFunc(func(ctx context.Context, _ domain.Account) (app.CredentialMaterial, error) {
		<-ctx.Done()
		select {
		case returned <- struct{}{}:
		default:
		}
		return app.CredentialMaterial{Secret: "synthetic-secret"}, nil
	})
	opts := options()
	opts.CredentialResolveTimeout = 30 * time.Millisecond
	_, cancel, done := start(t, store, src, res, fac, opts)
	defer cancel()
	wait(t, returned)
	time.Sleep(40 * time.Millisecond)
	select {
	case <-fac.created:
		t.Fatal("expired resolver result constructed SDK client")
	default:
	}
	cancel()
	wait(t, done)
}
