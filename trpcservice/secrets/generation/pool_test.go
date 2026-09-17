package generation

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestPoolSharesGenerationAndRetiresOnlyFutureAcquires(t *testing.T) {
	provider := &countingProvider{value: []byte("credential"), version: 4}
	pool := New(provider)
	scope, ref := fixture()
	first, err := pool.Acquire(context.Background(), scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.Acquire(context.Background(), scope, ref)
	if err != nil || provider.calls != 1 {
		t.Fatalf("second=%v calls=%d", err, provider.calls)
	}
	if err := pool.Invalidate(scope, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Acquire(context.Background(), scope, ref); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("retired acquire=%v", err)
	}
	value, err := first.Secret()
	if err != nil || string(value.Bytes) != "credential" {
		t.Fatalf("in-flight value=%q err=%v", value.Bytes, err)
	}
	clear(value.Bytes)
	first.Release()
	second.Release()
	item := pool.entries[key{scope: scope, ref: ref}]
	if item == nil || len(item.value) != 0 {
		t.Fatalf("retired value not cleared: %#v", item)
	}
}

func TestPoolCollapsesConcurrentAcquire(t *testing.T) {
	provider := &countingProvider{value: []byte("credential"), version: 4, wait: make(chan struct{})}
	pool := New(provider)
	scope, ref := fixture()
	started := make(chan struct{})
	provider.started = started
	firstDone := make(chan error, 1)
	go func() {
		lease, err := pool.Acquire(context.Background(), scope, ref)
		if lease != nil {
			lease.Release()
		}
		firstDone <- err
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		lease, err := pool.Acquire(context.Background(), scope, ref)
		if lease != nil {
			lease.Release()
		}
		secondDone <- err
	}()
	close(provider.wait)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if provider.calls != 1 {
		t.Fatalf("resolve calls=%d", provider.calls)
	}
}

type countingProvider struct {
	mu      sync.Mutex
	calls   int
	value   []byte
	version int64
	started chan struct{}
	wait    chan struct{}
}

func (p *countingProvider) Resolve(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return secrets.SecretValue{}, err
	}
	p.mu.Lock()
	p.calls++
	started, wait := p.started, p.wait
	p.mu.Unlock()
	if started != nil {
		close(started)
	}
	if wait != nil {
		select {
		case <-ctx.Done():
			return secrets.SecretValue{}, ctx.Err()
		case <-wait:
		}
	}
	return secrets.SecretValue{Bytes: append([]byte(nil), p.value...), Version: p.version}, nil
}

func fixture() (secrets.Scope, secrets.SecretRef) {
	return secrets.Scope{TenantID: "tenant-a", Subject: "worker", Purpose: secrets.PurposeModelCall, ResourceID: "model", ResourceVersion: 3}, secrets.SecretRef{Ref: "vault://tenant/model", Version: 4}
}
