// Package llm normalizes model backends into tenant/global infrastructure
// resources that agents reference by id, decoupling model choice from build
// time. It supports OpenAI (and any OpenAI-compatible endpoint), Anthropic, and
// Gemini protocols through a provider dispatch, so a custom model endpoint can
// target any of the three wire protocols without code changes.
package llm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/genai"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/anthropic"
	"trpc.group/trpc-go/trpc-agent-go/model/gemini"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// DefaultHTTPTimeout bounds a single model HTTP request. Without it the
// framework's default client has no timeout, so a slow or hung backend (a
// relay like cn.meai.cloud under load) can block the worker for minutes.
// Overridable via SetHTTPTimeout for operators that need a longer budget.
const DefaultHTTPTimeout = 120 * time.Second

// httpTimeoutNanos is the effective model HTTP timeout in nanoseconds. It is
// atomic so SetHTTPTimeout (startup) and the provider factories (model-build
// path) can touch it without a data race.
var httpTimeoutNanos atomic.Int64

func init() {
	httpTimeoutNanos.Store(int64(DefaultHTTPTimeout))
}

// SetHTTPTimeout overrides the model HTTP timeout used by all provider
// factories. Call before building models (startup) for a consistent budget.
func SetHTTPTimeout(d time.Duration) {
	if d > 0 {
		httpTimeoutNanos.Store(int64(d))
	}
}

// modelHTTPTimeout returns the current model HTTP timeout as a duration.
func modelHTTPTimeout() time.Duration {
	return time.Duration(httpTimeoutNanos.Load())
}

// Scope values for an endpoint.
const (
	ScopeGlobal = "global"
	ScopeTenant = "tenant"
)

// ErrEndpointNotFound is returned when an endpoint does not exist.
var ErrEndpointNotFound = errors.New("llm: endpoint not found")

// ErrEndpointExists is returned when creating an endpoint whose id already
// exists. It is distinct from ErrEndpointNotFound so callers can map a
// duplicate-create to a conflict rather than a not-found.
var ErrEndpointExists = errors.New("llm: endpoint already exists")

// Provider identifiers supported out of the box.
const (
	ProviderOpenAI       = "openai"
	ProviderOpenAICompat = "openai-compatible" // any third-party backend speaking the OpenAI wire protocol
	ProviderAnthropic    = "anthropic"
	ProviderGemini       = "gemini"
)

// EndpointType values for an endpoint's purpose: chat models serve the agent
// loop, embedding models produce knowledge-base vectors.
const (
	EndpointTypeChat      = "chat"
	EndpointTypeEmbedding = "embedding"
)

// Endpoint normalizes any supported LLM backend into an infrastructure
// resource. The Provider field selects the wire protocol; empty defaults to
// OpenAI-compatible so existing callers keep working. Type defaults to chat
// when empty (backward compatibility).
type Endpoint struct {
	ID        string `json:"id"`
	Scope     string `json:"scope"`
	TenantID  string `json:"tenant_id,omitempty"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Type      string `json:"type,omitempty"` // chat | embedding
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

// KeySink stores a plaintext credential under a reference key. secret.Store
// satisfies it; nil means a plaintext API key passes through unchanged (the
// legacy in-memory/dev path), which the MySQL store then rejects.
type KeySink interface {
	Put(ctx context.Context, key, value string) error
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
// operators can register additional providers at startup; factoriesMu guards
// it because RegisterFactory (startup) and DefaultFactory (model-build path)
// may run concurrently.
var (
	factoriesMu sync.RWMutex
	factories   = map[string]ModelFactory{
		ProviderOpenAICompat: openAIFactory,
		ProviderAnthropic:    anthropicFactory,
		ProviderGemini:       geminiFactory,
	}
)

// RegisterFactory registers or overrides a factory for a provider id.
func RegisterFactory(provider string, f ModelFactory) {
	if f == nil {
		return
	}
	factoriesMu.Lock()
	factories[NormalizeProvider(provider)] = f
	factoriesMu.Unlock()
}

// DefaultFactory dispatches on the endpoint provider to the matching factory.
func DefaultFactory(ctx context.Context, ep Endpoint) (model.Model, error) {
	p := NormalizeProvider(ep.Provider)
	factoriesMu.RLock()
	f, ok := factories[p]
	factoriesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("llm: unsupported provider %q", ep.Provider)
	}
	return f(ctx, ep)
}

func openAIFactory(_ context.Context, ep Endpoint) (model.Model, error) {
	return openai.New(ep.ModelName,
		openai.WithBaseURL(ep.BaseURL),
		openai.WithAPIKey(ep.APIKey),
		openai.WithHTTPClientOptions(openai.WithHTTPClientTimeout(modelHTTPTimeout())),
	), nil
}

func anthropicFactory(_ context.Context, ep Endpoint) (model.Model, error) {
	return anthropic.New(ep.ModelName,
		anthropic.WithBaseURL(ep.BaseURL),
		anthropic.WithAPIKey(ep.APIKey),
		anthropic.WithHTTPClientOptions(anthropic.WithHTTPClientTimeout(modelHTTPTimeout())),
	), nil
}

func geminiFactory(ctx context.Context, ep Endpoint) (model.Model, error) {
	cfg := &genai.ClientConfig{
		APIKey:     ep.APIKey,
		Backend:    genai.BackendGeminiAPI,
		HTTPClient: &http.Client{Timeout: modelHTTPTimeout()},
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

// Store is the persistence contract behind Registry. The in-memory store
// keeps the service runnable without MySQL; MySQL and other backend stores
// are implemented in the infra/storage package against this interface.
type Store interface {
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
	store   Store
	keys    KeySource
	sink    KeySink
}

// SetKeySource attaches a credential source used to resolve APIKeyRef at
// build time. nil leaves plaintext APIKey as the only path (dev/test).
func (r *Registry) SetKeySource(ks KeySource) {
	r.mu.Lock()
	r.keys = ks
	r.mu.Unlock()
}

// SetKeySink attaches a credential store used to persist a plaintext API key
// as a reference before an endpoint is written. nil leaves the plaintext key
// unchanged (the legacy in-memory/dev path; the MySQL store rejects it).
func (r *Registry) SetKeySink(sink KeySink) {
	r.mu.Lock()
	r.sink = sink
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
	if err := r.normalizeCredentials(ctx, &ep); err != nil {
		return err
	}
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
	key, err := r.ResolveAPIKey(ctx, ep)
	if err != nil {
		return nil, err
	}
	ep.APIKey = key

	m, err := r.factory(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf("llm: build model for %q: %w", endpointID, err)
	}

	r.mu.Lock()
	r.cache[endpointID] = m
	r.mu.Unlock()
	return m, nil
}

// ResolveAPIKey resolves an endpoint's API key: a credential-store reference
// (APIKeyRef) wins, falling back to the legacy plaintext APIKey. Exported so
// the knowledge embedder can reuse the same credential resolution as chat.
func (r *Registry) ResolveAPIKey(ctx context.Context, ep Endpoint) (string, error) {
	if ep.APIKeyRef == "" {
		return ep.APIKey, nil
	}
	r.mu.RLock()
	ks := r.keys
	r.mu.RUnlock()
	if ks == nil {
		return "", fmt.Errorf("llm: endpoint %q uses api_key_ref %q but no key source is configured", ep.ID, ep.APIKeyRef)
	}
	key, err := ks.Get(ctx, ep.APIKeyRef)
	if err != nil {
		return "", fmt.Errorf("llm: resolve api_key_ref %q: %w", ep.APIKeyRef, err)
	}
	return key, nil
}

// credentialKey is the stable credential-store reference for an endpoint's
// API key (the convention documented in docs/adr/0002-secret-manager.md).
func credentialKey(id string) string {
	return "endpoint:" + id
}

// normalizeCredentials converts a plaintext API key into a credential-store
// reference before an endpoint is persisted, so plaintext never reaches the
// durable store. With a key sink configured, the key is stored under
// "endpoint:{id}" and replaced by that reference; without one the plaintext
// key is left unchanged (the legacy in-memory/dev path, which the MySQL store
// rejects).
func (r *Registry) normalizeCredentials(ctx context.Context, ep *Endpoint) error {
	if ep.APIKey == "" {
		return nil
	}
	r.mu.RLock()
	sink := r.sink
	r.mu.RUnlock()
	if sink == nil {
		return nil
	}
	ref := credentialKey(ep.ID)
	if err := sink.Put(ctx, ref, ep.APIKey); err != nil {
		return fmt.Errorf("llm: store api_key for %q: %w", ep.ID, err)
	}
	ep.APIKeyRef = ref
	ep.APIKey = ""
	return nil
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
	if err := r.normalizeCredentials(ctx, &ep); err != nil {
		return err
	}
	return r.store.Create(ctx, ep)
}

// Update replaces an endpoint, failing if it does not exist.
func (r *Registry) Update(ctx context.Context, ep Endpoint) error {
	if err := r.normalizeCredentials(ctx, &ep); err != nil {
		return err
	}
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
		return ErrEndpointExists
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

// NewRegistryWithStore returns a registry over the given store
// implementation, used by infra/storage to back a Registry with MySQL.
func NewRegistryWithStore(s Store, factory ModelFactory) *Registry {
	if factory == nil {
		factory = DefaultFactory
	}
	return &Registry{
		cache:   make(map[string]model.Model),
		factory: factory,
		store:   s,
	}
}
