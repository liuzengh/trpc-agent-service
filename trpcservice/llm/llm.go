// Package llm normalizes model backends into tenant/global infrastructure
// resources that agents reference by id, decoupling model choice from build
// time. It supports OpenAI (and any OpenAI-compatible endpoint), Anthropic, and
// Gemini protocols through a provider dispatch, so a custom model endpoint can
// target any of the three wire protocols without code changes.
package llm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/genai"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/anthropic"
	"trpc.group/trpc-go/trpc-agent-go/model/gemini"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// Scope values for an endpoint.
const (
	ScopeGlobal = "global"
	ScopeTenant = "tenant"
)

// ErrEndpointNotFound is returned when an endpoint does not exist.
var ErrEndpointNotFound = errors.New("llm: endpoint not found")

// Provider identifiers supported out of the box.
const (
	ProviderOpenAI       = "openai"
	ProviderOpenAICompat = "openai-compatible" // any third-party backend speaking the OpenAI wire protocol
	ProviderAnthropic    = "anthropic"
	ProviderGemini       = "gemini"
)

// Endpoint normalizes any supported LLM backend into an infrastructure
// resource. The Provider field selects the wire protocol; empty defaults to
// OpenAI-compatible so existing callers keep working.
type Endpoint struct {
	ID        string `json:"id"`
	Scope     string `json:"scope"`
	TenantID  string `json:"tenant_id,omitempty"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	BaseURL   string `json:"base_url"`
	ModelName string `json:"model_name"`
	// APIKey is a plaintext key (legacy). Prefer APIKeyRef: a credential-store
	// reference resolved at build time via Registry.SetKeySource.
	APIKey    string `json:"api_key,omitempty"`
	APIKeyRef string `json:"api_key_ref,omitempty"`
}

// KeySource resolves a credential value by reference (the secret store
// satisfies it). Kept as an interface so llm does not depend on the secret
// package.
type KeySource interface {
	Get(ctx context.Context, key string) (string, error)
}

// ModelFactory builds a model.Model for an endpoint.
type ModelFactory func(ctx context.Context, ep Endpoint) (model.Model, error)

// NormalizeProvider maps provider aliases (or an empty value) to a canonical
// provider id used for dispatch.
func NormalizeProvider(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "", ProviderOpenAI, ProviderOpenAICompat, "openai_compatible":
		return ProviderOpenAICompat
	case ProviderAnthropic, "claude":
		return ProviderAnthropic
	case ProviderGemini, "google", "vertex", "vertexai", "vertex-ai":
		return ProviderGemini
	default:
		return strings.ToLower(strings.TrimSpace(p))
	}
}

// factories maps a canonical provider id to its ModelFactory. It is mutable so
// operators can register additional providers at startup.
var factories = map[string]ModelFactory{
	ProviderOpenAICompat: openAIFactory,
	ProviderAnthropic:    anthropicFactory,
	ProviderGemini:       geminiFactory,
}

// RegisterFactory registers or overrides a factory for a provider id.
func RegisterFactory(provider string, f ModelFactory) {
	if f == nil {
		return
	}
	factories[NormalizeProvider(provider)] = f
}

// DefaultFactory dispatches on the endpoint provider to the matching factory.
func DefaultFactory(ctx context.Context, ep Endpoint) (model.Model, error) {
	p := NormalizeProvider(ep.Provider)
	f, ok := factories[p]
	if !ok {
		return nil, fmt.Errorf("llm: unsupported provider %q", ep.Provider)
	}
	return f(ctx, ep)
}

func openAIFactory(_ context.Context, ep Endpoint) (model.Model, error) {
	return openai.New(ep.ModelName,
		openai.WithBaseURL(ep.BaseURL),
		openai.WithAPIKey(ep.APIKey),
	), nil
}

func anthropicFactory(_ context.Context, ep Endpoint) (model.Model, error) {
	return anthropic.New(ep.ModelName,
		anthropic.WithBaseURL(ep.BaseURL),
		anthropic.WithAPIKey(ep.APIKey),
	), nil
}

func geminiFactory(ctx context.Context, ep Endpoint) (model.Model, error) {
	cfg := &genai.ClientConfig{
		APIKey:  ep.APIKey,
		Backend: genai.BackendGeminiAPI,
	}
	if ep.BaseURL != "" {
		cfg.HTTPOptions = genai.HTTPOptions{BaseURL: ep.BaseURL}
	}
	m, err := gemini.New(ctx, ep.ModelName, gemini.WithGeminiClientConfig(cfg))
	if err != nil {
		return nil, err
	}
	return m, nil
}

// store is the persistence contract behind Registry. The in-memory store
// keeps the service runnable without MySQL; the MySQL store (llm_mysql.go)
// is the production path.
type store interface {
	Create(ctx context.Context, ep Endpoint) error
	Update(ctx context.Context, ep Endpoint) error
	Delete(ctx context.Context, id string) error
	Get(ctx context.Context, id string) (Endpoint, error)
	List(ctx context.Context) ([]Endpoint, error)
	Upsert(ctx context.Context, ep Endpoint) error
}

// Registry routes endpoint ids to cached model instances backed by a
// swappable endpoint store.
type Registry struct {
	mu      sync.RWMutex
	cache   map[string]model.Model
	factory ModelFactory
	store   store
	keys    KeySource
}

// SetKeySource attaches a credential source used to resolve APIKeyRef at
// build time. nil leaves plaintext APIKey as the only path (dev/test).
func (r *Registry) SetKeySource(ks KeySource) {
	r.mu.Lock()
	r.keys = ks
	r.mu.Unlock()
}

// NewRegistry returns an in-memory registry using the given factory,
// defaulting to DefaultFactory (multi-protocol dispatch).
func NewRegistry(factory ModelFactory) *Registry {
	if factory == nil {
		factory = DefaultFactory
	}
	return &Registry{
		cache:   make(map[string]model.Model),
		factory: factory,
		store:   newMemStore(),
	}
}

// Upsert registers or updates an endpoint and invalidates its cached model.
func (r *Registry) Upsert(ctx context.Context, ep Endpoint) error {
	if err := r.store.Upsert(ctx, ep); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.cache, ep.ID)
	r.mu.Unlock()
	return nil
}

// Resolve returns a cached model.Model for the endpoint, building it lazily.
func (r *Registry) Resolve(ctx context.Context, endpointID string) (model.Model, error) {
	r.mu.RLock()
	if m, ok := r.cache[endpointID]; ok {
		r.mu.RUnlock()
		return m, nil
	}
	r.mu.RUnlock()

	ep, err := r.store.Get(ctx, endpointID)
	if err != nil {
		return nil, err
	}

	// Resolve a credential-store reference into the in-memory key used to
	// build the model. A missing reference is a hard error (misconfiguration
	// must not silently build a keyless model).
	if ep.APIKeyRef != "" {
		r.mu.RLock()
		ks := r.keys
		r.mu.RUnlock()
		if ks == nil {
			return nil, fmt.Errorf("llm: endpoint %q uses api_key_ref %q but no key source is configured", endpointID, ep.APIKeyRef)
		}
		key, err := ks.Get(ctx, ep.APIKeyRef)
		if err != nil {
			return nil, fmt.Errorf("llm: resolve api_key_ref %q: %w", ep.APIKeyRef, err)
		}
		ep.APIKey = key
	}

	m, err := r.factory(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf("llm: build model for %q: %w", endpointID, err)
	}

	r.mu.Lock()
	r.cache[endpointID] = m
	r.mu.Unlock()
	return m, nil
}

// Invalidate drops the cached model for an endpoint.
func (r *Registry) Invalidate(endpointID string) {
	r.mu.Lock()
	delete(r.cache, endpointID)
	r.mu.Unlock()
}

// List returns the endpoints visible to a tenant (global + its own); an empty
// tenantID returns all.
func (r *Registry) List(ctx context.Context, tenantID string) ([]Endpoint, error) {
	all, err := r.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Endpoint, 0, len(all))
	for _, ep := range all {
		if tenantID == "" || ep.Scope == ScopeGlobal || ep.TenantID == tenantID {
			out = append(out, ep)
		}
	}
	return out, nil
}

// Get returns an endpoint by id.
func (r *Registry) Get(ctx context.Context, id string) (Endpoint, error) {
	return r.store.Get(ctx, id)
}

// Create inserts an endpoint, failing if the id already exists.
func (r *Registry) Create(ctx context.Context, ep Endpoint) error {
	if ep.ID == "" {
		return errors.New("llm: endpoint id required")
	}
	if ep.Scope == "" {
		ep.Scope = ScopeTenant
	}
	return r.store.Create(ctx, ep)
}

// Update replaces an endpoint, failing if it does not exist.
func (r *Registry) Update(ctx context.Context, ep Endpoint) error {
	if err := r.store.Update(ctx, ep); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.cache, ep.ID)
	r.mu.Unlock()
	return nil
}

// Delete removes an endpoint and its cached model.
func (r *Registry) Delete(ctx context.Context, id string) error {
	if err := r.store.Delete(ctx, id); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.cache, id)
	r.mu.Unlock()
	return nil
}

// memStore keeps endpoints in a map; the zero-dependency dev/test backend.
type memStore struct {
	mu  sync.RWMutex
	eps map[string]Endpoint
}

func newMemStore() *memStore {
	return &memStore{eps: make(map[string]Endpoint)}
}

func (s *memStore) Create(_ context.Context, ep Endpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.eps[ep.ID]; ok {
		return ErrEndpointNotFound // caller maps to conflict
	}
	s.eps[ep.ID] = ep
	return nil
}

func (s *memStore) Update(_ context.Context, ep Endpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.eps[ep.ID]; !ok {
		return ErrEndpointNotFound
	}
	s.eps[ep.ID] = ep
	return nil
}

func (s *memStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.eps[id]; !ok {
		return ErrEndpointNotFound
	}
	delete(s.eps, id)
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (Endpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ep, ok := s.eps[id]
	if !ok {
		return Endpoint{}, ErrEndpointNotFound
	}
	return ep, nil
}

func (s *memStore) List(_ context.Context) ([]Endpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Endpoint, 0, len(s.eps))
	for _, ep := range s.eps {
		out = append(out, ep)
	}
	return out, nil
}

func (s *memStore) Upsert(_ context.Context, ep Endpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eps[ep.ID] = ep
	return nil
}

// NewMySQLRegistry returns a registry persisting endpoints in MySQL (table
// model_endpoints) while keeping model instances cached in memory.
func NewMySQLRegistry(db *sql.DB, factory ModelFactory) *Registry {
	if factory == nil {
		factory = DefaultFactory
	}
	return &Registry{
		cache:   make(map[string]model.Model),
		factory: factory,
		store:   &mysqlStore{db: db},
	}
}
