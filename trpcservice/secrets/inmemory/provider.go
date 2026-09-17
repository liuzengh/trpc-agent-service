// Package inmemory is a local SecretProvider that always returns defensive
// copies and exact versions.
package inmemory

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Provider struct {
	mu     sync.RWMutex
	values map[key][]byte
}

type key struct {
	Scope secrets.Scope
	Ref   secrets.SecretRef
}

func New() *Provider { return &Provider{values: make(map[key][]byte)} }
func (p *Provider) Put(scope secrets.Scope, ref secrets.SecretRef, value []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.values[key{Scope: scope, Ref: ref}] = append([]byte(nil), value...)
}
func (p *Provider) Resolve(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	if err := ctx.Err(); err != nil {
		return secrets.SecretValue{}, err
	}
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return secrets.SecretValue{}, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	value, ok := p.values[key{Scope: scope, Ref: ref}]
	if !ok {
		return secrets.SecretValue{}, runtime.ErrNotFound
	}
	return secrets.SecretValue{Bytes: append([]byte(nil), value...), Version: ref.Version}, nil
}
