package wecom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrRegistryClosed reports operations attempted after registry shutdown.
	ErrRegistryClosed = errors.New("wecom registry is closed")
	// ErrAccountExists reports a duplicate tenant/account registration.
	ErrAccountExists = errors.New("wecom account already exists")
	// ErrAccountMissing reports an unknown tenant/account registration.
	ErrAccountMissing = errors.New("wecom account not found")
)

// Account binds one tenant-local account key to a provider. Credentials stay
// inside the provider and are never copied into registry metadata.
type Account struct {
	TenantID  string
	AccountID string
	Provider  *Provider
}

// Registry stores providers by tenant/account scope and prevents accidental
// cross-tenant cache reuse.
type Registry struct {
	mu       sync.RWMutex
	closed   bool
	accounts map[string]*Provider
}

// NewRegistry creates an empty tenant/account provider registry.
func NewRegistry() *Registry { return &Registry{accounts: make(map[string]*Provider)} }

func accountKey(tenantID, accountID string) string { return tenantID + "\x00" + accountID }

// Register adds one tenant/account provider to the registry.
func (r *Registry) Register(account Account) error {
	if r == nil || strings.TrimSpace(account.TenantID) == "" || strings.TrimSpace(account.AccountID) == "" || account.Provider == nil {
		return fmt.Errorf("%w: account is invalid", ErrInvalid)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRegistryClosed
	}
	if r.accounts == nil {
		r.accounts = make(map[string]*Provider)
	}
	key := accountKey(account.TenantID, account.AccountID)
	if r.accounts[key] != nil {
		return ErrAccountExists
	}
	r.accounts[key] = account.Provider
	return nil
}

// Resolve returns the provider scoped to one tenant and account.
func (r *Registry) Resolve(tenantID, accountID string) (*Provider, error) {
	if r == nil {
		return nil, ErrAccountMissing
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrRegistryClosed
	}
	provider := r.accounts[accountKey(tenantID, accountID)]
	if provider == nil {
		return nil, ErrAccountMissing
	}
	return provider, nil
}

// Remove unregisters one tenant/account provider.
func (r *Registry) Remove(tenantID, accountID string) error {
	if r == nil {
		return ErrAccountMissing
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRegistryClosed
	}
	key := accountKey(tenantID, accountID)
	if r.accounts[key] == nil {
		return ErrAccountMissing
	}
	delete(r.accounts, key)
	return nil
}

// Close rejects new registry operations and releases account references.
func (r *Registry) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.accounts = nil
	r.mu.Unlock()
	return nil
}

// WorkerGroup limits concurrent account work and drains accepted work on
// Close. Dispatch callbacks receive the provider selected by tenant/account.
type WorkerGroup struct {
	registry *Registry
	sem      chan struct{}
	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

// NewWorkerGroup creates a bounded worker group for one provider registry.
func NewWorkerGroup(registry *Registry, limit int) (*WorkerGroup, error) {
	if registry == nil || limit < 1 {
		return nil, fmt.Errorf("%w: worker group configuration is invalid", ErrInvalid)
	}
	return &WorkerGroup{registry: registry, sem: make(chan struct{}, limit)}, nil
}

// Dispatch admits one bounded operation against a tenant/account provider.
func (g *WorkerGroup) Dispatch(ctx context.Context, tenantID, accountID string, fn func(context.Context, *Provider) error) error {
	if g == nil || ctx == nil || fn == nil {
		return fmt.Errorf("%w: dispatch is invalid", ErrInvalid)
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrRegistryClosed
	}
	g.wg.Add(1)
	g.mu.Unlock()
	defer g.wg.Done()
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	provider, err := g.registry.Resolve(tenantID, accountID)
	if err != nil {
		return err
	}
	return fn(ctx, provider)
}

// Close rejects admissions and waits for accepted work to finish.
func (g *WorkerGroup) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.wg.Wait()
	return nil
}
