package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var ErrProviderClosed = errors.New("backend provider is closed")

type Backend interface {
	Ready(context.Context) error
	Check(context.Context) error
	Session() session.Service
	Memory() frameworkmemory.Service
	Fingerprint() persistence.BackendFingerprint
	Close() error
}

type PersistentBackend interface {
	Backend
	Committer() persistence.Committer
}

type backendKey struct {
	tenantID  string
	profileID string
}

type BackendProvider struct {
	repository      tenant.Repository
	credentials     config.CredentialResolver
	sessionFencing  string
	messagingPrefix string
	messagingURL    string
	turnLimits      sessionfence.Limits

	mu        sync.Mutex
	backends  map[backendKey]Backend
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func NewBackendProvider(repository tenant.Repository, credentials config.CredentialResolver, fencing ...string) (*BackendProvider, error) {
	if repository == nil {
		return nil, errors.New("tenant repository is required")
	}
	if credentials == nil {
		return nil, errors.New("credential resolver is required")
	}
	mode := config.DefaultSessionFencing
	if len(fencing) > 0 && fencing[0] != "" {
		mode = fencing[0]
	}
	messagingPrefix := ""
	if len(fencing) > 1 {
		messagingPrefix = fencing[1]
	}
	messagingURL := ""
	if len(fencing) > 2 {
		messagingURL = fencing[2]
	}
	return &BackendProvider{
		repository: repository, credentials: credentials,
		sessionFencing:  mode,
		messagingPrefix: messagingPrefix,
		messagingURL:    messagingURL,
		turnLimits:      sessionfence.Limits{MaxTurnEvents: config.DefaultMaxTurnEvents, MaxTurnBytes: config.DefaultMaxTurnBytes},
		backends:        make(map[backendKey]Backend), closeDone: make(chan struct{}),
	}, nil
}

func (p *BackendProvider) SetTurnLimits(limits sessionfence.Limits) {
	p.mu.Lock()
	if limits.MaxTurnEvents > 0 {
		p.turnLimits.MaxTurnEvents = limits.MaxTurnEvents
	}
	if limits.MaxTurnBytes > 0 {
		p.turnLimits.MaxTurnBytes = limits.MaxTurnBytes
	}
	p.mu.Unlock()
}

// BackendFor creates no more than one backend for a tenant-scoped profile.
// Redis network initialization remains lazy inside Ready and can recover on a
// later call after a transient failure.
func (p *BackendProvider) BackendFor(ctx context.Context, profile tenant.StorageProfile) (Backend, error) {
	backend, err := p.backendWithoutReady(ctx, profile)
	if err != nil {
		return nil, err
	}
	if err := backend.Ready(ctx); err != nil {
		return nil, fmt.Errorf("storage profile unavailable: %w", err)
	}
	return backend, nil
}

// BackendIdentityFor returns the parsed immutable identity without network I/O.
func (p *BackendProvider) BackendIdentityFor(ctx context.Context, profile tenant.StorageProfile) (persistence.BackendFingerprint, error) {
	backend, err := p.backendWithoutReady(ctx, profile)
	if err != nil {
		return persistence.BackendFingerprint{}, err
	}
	return backend.Fingerprint(), nil
}

func (p *BackendProvider) backendWithoutReady(ctx context.Context, profile tenant.StorageProfile) (Backend, error) {
	authoritative, err := p.repository.GetStorageProfile(ctx, profile.TenantID, profile.ID)
	if err != nil {
		return nil, fmt.Errorf("storage profile lookup failed: %w", err)
	}
	profile = authoritative
	key := backendKey{tenantID: profile.TenantID, profileID: profile.ID}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrProviderClosed
	}
	backend := p.backends[key]
	if backend == nil {
		var err error
		backend, err = p.newBackend(profile)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.backends[key] = backend
	}
	p.mu.Unlock()
	return backend, nil
}

func (p *BackendProvider) newBackend(profile tenant.StorageProfile) (Backend, error) {
	switch profile.Kind {
	case tenant.StorageKindInMemory:
		if p.sessionFencing == "strong" {
			return nil, errors.New("strong session fencing rejects inmemory storage")
		}
		backend := NewInMemoryBackend()
		backend.fingerprint.StorageProfileID = profile.ID
		return backend, nil
	case tenant.StorageKindRedis:
		value, err := p.credentials.Resolve(profile.CredentialRef)
		if err != nil {
			return nil, errors.New("resolve storage credential")
		}
		redisURL, err := config.NormalizeRedisURL(value)
		if err != nil {
			return nil, errors.New("storage credential is not a valid Redis URL")
		}
		if p.sessionFencing == "strong" && strings.TrimSpace(p.messagingURL) != "" {
			messagingURL, normalizeErr := config.NormalizeRedisURL(p.messagingURL)
			if normalizeErr != nil || messagingURL != redisURL {
				return nil, errors.New("strong session fencing requires Messaging and Session Redis to use the same URL and DB")
			}
		}
		prefix := strings.TrimRight(strings.TrimSpace(profile.KeyPrefix), ":")
		if !profile.LegacyPrefix {
			prefix += ":tenant:" + profile.TenantID + ":profile:" + profile.ID
		}
		var backend *RedisBackend
		if p.sessionFencing == "strong" {
			backend, err = NewFencedRedisBackendWithConfig(redisURL, prefix, p.turnLimits, p.messagingPrefix, p.messagingURL)
		} else {
			backend, err = NewRedisBackend(redisURL, prefix)
		}
		if err != nil {
			return nil, err
		}
		fingerprintProfile := profile
		fingerprintProfile.KeyPrefix = prefix
		backend.fingerprint, err = persistence.FingerprintForProfile(fingerprintProfile, redisURL)
		return backend, err
	case tenant.StorageKindPostgres, tenant.StorageKindMySQL:
		if p.sessionFencing != "strong" {
			return nil, errors.New("SQL storage requires strong session fencing")
		}
		value, err := p.credentials.Resolve(profile.CredentialRef)
		if err != nil {
			return nil, errors.New("resolve SQL storage credential")
		}
		return NewSQLBackend(profile, value, p.turnLimits)
	default:
		return nil, errors.New("unsupported storage profile kind")
	}
}

func (p *BackendProvider) Ready(ctx context.Context) error {
	profiles, err := p.repository.ListActiveStorageProfiles(ctx)
	if err != nil {
		return fmt.Errorf("list active storage profiles: %w", err)
	}
	for _, profile := range profiles {
		if profile.Kind.IsSQL() {
			continue
		}
		if _, err := p.BackendFor(ctx, profile); err != nil {
			return err
		}
	}
	return nil
}

func (p *BackendProvider) Close() error {
	p.mu.Lock()
	if p.closed {
		done := p.closeDone
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.closeErr
	}
	p.closed = true
	keys := make([]backendKey, 0, len(p.backends))
	for key := range p.backends {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tenantID == keys[j].tenantID {
			return keys[i].profileID < keys[j].profileID
		}
		return keys[i].tenantID < keys[j].tenantID
	})
	backends := make([]Backend, 0, len(keys))
	for _, key := range keys {
		backends = append(backends, p.backends[key])
		delete(p.backends, key)
	}
	p.mu.Unlock()

	var closeErrors []error
	for _, backend := range backends {
		if err := backend.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	p.mu.Lock()
	p.closeErr = errors.Join(closeErrors...)
	close(p.closeDone)
	p.mu.Unlock()
	return p.closeErr
}

type InMemoryBackend struct {
	mu          sync.Mutex
	closed      bool
	sessions    session.Service
	memories    frameworkmemory.Service
	fingerprint persistence.BackendFingerprint
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		sessions:    sessioninmemory.NewSessionService(),
		memories:    memoryinmemory.NewMemoryService(),
		fingerprint: persistence.BackendFingerprint{SchemaVersion: persistence.FingerprintSchemaVersion, Kind: tenant.StorageKindInMemory, StorageProfileID: "inmemory", Namespace: "process"},
	}
}

func (b *InMemoryBackend) Fingerprint() persistence.BackendFingerprint { return b.fingerprint }

func (b *InMemoryBackend) Ready(ctx context.Context) error { return b.Check(ctx) }

func (b *InMemoryBackend) Check(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("inmemory backend is closed")
	}
	return nil
}

func (b *InMemoryBackend) Session() session.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions
}

func (b *InMemoryBackend) Memory() frameworkmemory.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.memories
}

func (b *InMemoryBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	sessions, memories := b.sessions, b.memories
	b.sessions, b.memories = nil, nil
	b.mu.Unlock()
	return errors.Join(closeSession(sessions), closeMemory(memories))
}

var _ Backend = (*RedisBackend)(nil)
var _ Backend = (*InMemoryBackend)(nil)
