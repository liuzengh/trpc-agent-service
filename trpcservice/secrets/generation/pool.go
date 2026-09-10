// Package generation shares one exact SecretRef generation across concurrent
// client construction. Invalidation retires only that generation: leases that
// already acquired it continue, while every later acquire fails closed until a
// caller supplies a new SecretRef version.
package generation

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Pool struct {
	provider secrets.Provider
	mu       sync.Mutex
	entries  map[key]*entry
}

type key struct {
	scope secrets.Scope
	ref   secrets.SecretRef
}

type entry struct {
	value   []byte
	version int64
	refs    int
	loading bool
	retired bool
	ready   chan struct{}
}

type Lease struct {
	pool *Pool
	key  key
	one  sync.Once
}

func New(provider secrets.Provider) *Pool {
	return &Pool{provider: provider, entries: make(map[key]*entry)}
}

func (p *Pool) Acquire(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (*Lease, error) {
	if p == nil || p.provider == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return nil, err
	}
	key := key{scope: scope, ref: ref}
	for {
		p.mu.Lock()
		item, exists := p.entries[key]
		if exists && item.retired {
			p.mu.Unlock()
			return nil, runtime.ErrVersionMismatch
		}
		if exists && !item.loading {
			item.refs++
			p.mu.Unlock()
			return &Lease{pool: p, key: key}, nil
		}
		if exists {
			ready := item.ready
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ready:
				continue
			}
		}
		item = &entry{refs: 1, loading: true, ready: make(chan struct{})}
		p.entries[key] = item
		p.mu.Unlock()

		value, err := p.provider.Resolve(ctx, scope, ref)
		if err == nil && (value.Version != ref.Version || len(value.Bytes) == 0) {
			err = runtime.ErrVersionMismatch
		}
		p.mu.Lock()
		item.loading = false
		if err != nil {
			item.refs--
			if !item.retired {
				delete(p.entries, key)
			}
			close(item.ready)
			p.mu.Unlock()
			clear(value.Bytes)
			return nil, err
		}
		item.value = append([]byte(nil), value.Bytes...)
		item.version = value.Version
		close(item.ready)
		retired := item.retired
		p.mu.Unlock()
		clear(value.Bytes)
		if retired {
			return &Lease{pool: p, key: key}, nil
		}
		return &Lease{pool: p, key: key}, nil
	}
}

// Secret returns a private copy for exactly one acquired generation. The
// caller should clear the returned bytes after passing them to its client.
func (l *Lease) Secret() (secrets.SecretValue, error) {
	if l == nil || l.pool == nil {
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	l.pool.mu.Lock()
	defer l.pool.mu.Unlock()
	item, ok := l.pool.entries[l.key]
	if !ok || item.loading || item.refs < 1 || item.version != l.key.ref.Version || len(item.value) == 0 {
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	return secrets.SecretValue{Bytes: append([]byte(nil), item.value...), Version: item.version}, nil
}

// Release gives up the in-flight right to an exact credential generation.
// It is idempotent. Retired material is zeroed only after the last lease.
func (l *Lease) Release() {
	if l == nil || l.pool == nil {
		return
	}
	l.one.Do(func() { l.pool.release(l.key) })
}

func (p *Pool) release(key key) {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.entries[key]
	if !ok || item.refs < 1 {
		return
	}
	item.refs--
	if item.retired && item.refs == 0 && !item.loading {
		clear(item.value)
		item.value = nil
	}
}

// Invalidate retires one exact scope/ref version. Existing leases remain
// usable; new calls must resolve a successor version instead of reusing it.
func (p *Pool) Invalidate(scope secrets.Scope, ref secrets.SecretRef) error {
	if p == nil {
		return runtime.ErrCapabilityUnsupported
	}
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := key{scope: scope, ref: ref}
	item, exists := p.entries[key]
	if !exists {
		p.entries[key] = &entry{retired: true}
		return nil
	}
	item.retired = true
	if item.refs == 0 && !item.loading {
		clear(item.value)
		item.value = nil
	}
	return nil
}
