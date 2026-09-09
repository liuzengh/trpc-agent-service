//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ErrTenantServiceUnavailable indicates that the service was not composed
// with a usable repository. Public methods return this stable error instead
// of panicking on a nil receiver or typed-nil dependency.
var ErrTenantServiceUnavailable = errors.New("tenant service unavailable")

// ErrScopedTenantReadUnavailable indicates that a caller requested a
// least-privilege tenant snapshot, but its service implementation does not
// expose the corresponding capability. Gateway and Delivery use this error
// to fail closed instead of silently loading a credential-bearing tenant.
var ErrScopedTenantReadUnavailable = errors.New("scoped tenant read unavailable")

// Service provides tenant management operations.
type Service interface {
	// CreateTenant creates a new tenant with generated ID.
	CreateTenant(ctx context.Context, name string, config TenantConfig) (*Tenant, error)

	// GetTenant retrieves a tenant by ID.
	GetTenant(ctx context.Context, tenantID string) (*Tenant, error)

	// GetTenantByWebhookToken retrieves a tenant by webhook token.
	GetTenantByWebhookToken(ctx context.Context, token string) (*Tenant, error)

	// UpdateTenant updates tenant configuration.
	UpdateTenant(ctx context.Context, tenant *Tenant) error

	// DeleteTenant marks a tenant as deleted.
	DeleteTenant(ctx context.Context, tenantID string) error

	// ListTenants lists all active tenants.
	ListTenants(ctx context.Context) ([]*Tenant, error)

	// Close closes the service.
	Close() error
}

// WebhookScopedReader resolves only the tenant metadata and channel
// credentials needed to authenticate one webhook. It must not return model,
// storage, or other channel credentials. Keeping this as an optional
// capability preserves the historical Service interface for control-plane
// callers while allowing ingress to enforce least privilege.
type WebhookScopedReader interface {
	GetTenantByWebhookTokenScoped(ctx context.Context, token string) (*Tenant, error)
}

// ChannelScopedReader resolves only one channel binding for outbound
// delivery. Implementations must not decrypt model, storage, or unrelated
// channel credentials in the returned snapshot.
type ChannelScopedReader interface {
	GetTenantChannelScoped(ctx context.Context, tenantID, channelType, accountID string) (*Tenant, error)
}

// TenantStatusReader returns only the lifecycle state needed by queue
// admission. Implementations must not decrypt or return the tenant's broader
// model, storage, governance, or channel configuration.
type TenantStatusReader interface {
	GetTenantStatus(ctx context.Context, tenantID string) (TenantStatus, error)
}

// TenantConfig holds the configuration for creating a tenant.
type TenantConfig struct {
	Agents     []AgentConfig    `json:"agents"`
	Models     []ModelConfig    `json:"models"`
	ToolPolicy ToolPolicy       `json:"toolPolicy"`
	Channels   []ChannelBinding `json:"channels"`
	Storage    StorageConfig    `json:"storage"`
	Governance GovernancePolicy `json:"governance"`
	Budget     BudgetConfig     `json:"budget"`
}

// TenantService implements Service.
type TenantService struct {
	repo            Repository
	cache           map[string]*cachedTenant
	cacheMu         sync.RWMutex
	cacheTTL        time.Duration
	keyMu           sync.RWMutex
	rotationMu      sync.Mutex
	keyID           string
	keyRing         map[string][]byte
	encryptKey      []byte
	storageValidate StorageConfigValidator
	secretResolver  SecretResolver
}

type cachedTenant struct {
	tenant    *Tenant
	expiresAt time.Time
}

// StorageConfigValidator validates operator-owned backend profile references
// for one tenant. It intentionally receives only profile IDs and public
// backend types; implementations must never return or log connection secrets.
type StorageConfigValidator func(context.Context, string, StorageConfig) error

// ServiceOption configures a TenantService without changing existing callers.
type ServiceOption func(*TenantService)

const defaultEncryptionKeyID = "v1"

// NewServiceWithKeyRing creates a tenant service with an explicit envelope
// encryption key ring. The active key is used for new writes; older keys are
// retained for decrypting credentials during a controlled rotation.
func NewServiceWithKeyRing(repo Repository, activeKeyID string, masterKeys map[string]string, options ...ServiceOption) (*TenantService, error) {
	if !validEncryptionKeyID(activeKeyID) {
		return nil, fmt.Errorf("invalid encryption key id")
	}
	if len(masterKeys) == 0 {
		return nil, fmt.Errorf("at least one encryption key is required")
	}
	ring := make(map[string][]byte, len(masterKeys))
	for id, material := range masterKeys {
		if !validEncryptionKeyID(id) || material == "" {
			return nil, fmt.Errorf("invalid encryption key configuration")
		}
		hash := sha256.Sum256([]byte(material))
		ring[id] = append([]byte(nil), hash[:]...)
	}
	active, ok := ring[activeKeyID]
	if !ok {
		return nil, fmt.Errorf("active encryption key is not present in key ring")
	}
	service := &TenantService{
		repo:       repo,
		cache:      make(map[string]*cachedTenant),
		cacheTTL:   0,
		keyID:      activeKeyID,
		keyRing:    ring,
		encryptKey: append([]byte(nil), active...),
	}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service, nil
}

// WithStorageConfigValidator makes profile existence/type/allowlist checks part
// of tenant admission and every repository read.
func WithStorageConfigValidator(validator StorageConfigValidator) ServiceOption {
	return func(service *TenantService) { service.storageValidate = validator }
}

// WithSecretResolver installs the operator-owned resolver used only by
// scoped channel reads. Keeping the resolver out of the default constructor
// prevents control-plane and list paths from materializing channel secrets;
// Gateway/Delivery compositions opt in explicitly when a binding uses refs.
func WithSecretResolver(resolver SecretResolver) ServiceOption {
	return func(service *TenantService) { service.secretResolver = resolver }
}

// AuthorizeModelSecretRef checks the operator binding used by an immutable
// model version without resolving or exposing the credential itself.
func (s *TenantService) AuthorizeModelSecretRef(ctx context.Context, tenantID, provider, model string, ref SecretRef) error {
	if s == nil || s.secretResolver == nil {
		return ErrSecretUnavailable
	}
	authorizer, ok := s.secretResolver.(TenantSecretBindingAuthorizer)
	if !ok {
		return ErrSecretUnavailable
	}
	return authorizer.AuthorizeForTenant(ctx, tenantID, provider, model, "model", ref)
}

// NewService creates a new tenant service.
func NewService(repo Repository, masterKey string, options ...ServiceOption) *TenantService {
	service, err := NewServiceWithKeyRing(repo, defaultEncryptionKeyID,
		map[string]string{defaultEncryptionKeyID: masterKey}, options...)
	if err != nil {
		// The legacy constructor historically accepted any non-empty value and
		// callers rely on that API. The fixed key ID/material shape above cannot
		// fail for this constructor, so keep a defensive fallback rather than
		// changing its no-error contract.
		panic(err)
	}
	return service
}

// CreateTenant creates a new tenant.
func (s *TenantService) CreateTenant(ctx context.Context, name string, config TenantConfig) (*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	if err := ValidateTenantName(name); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	tenant := &Tenant{
		ID:            uuid.New().String(),
		Name:          name,
		Status:        TenantStatusActive,
		ConfigVersion: 1,
		Agents:        config.Agents,
		Models:        config.Models,
		ToolPolicy:    config.ToolPolicy,
		Channels:      config.Channels,
		Storage:       config.Storage,
		Governance:    config.Governance,
		Budget:        config.Budget,
	}
	if err := s.validateConfig(ctx, tenant.ID, config); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	for i := range tenant.Channels {
		if tenant.Channels[i].WebhookKey == "" {
			tenant.Channels[i].WebhookKey = uuid.New().String()
		}
		tenant.Channels[i].EnsureAccountID()
	}

	// Encrypt sensitive fields
	stored, err := cloneTenant(tenant)
	if err != nil {
		return nil, fmt.Errorf("failed to clone tenant: %w", err)
	}
	if err := s.encryptSecrets(stored); err != nil {
		return nil, fmt.Errorf("failed to encrypt secrets: %w", err)
	}

	if err := s.repo.Create(ctx, stored); err != nil {
		return nil, err
	}

	s.cacheTenant(tenant)

	return cloneTenant(tenant)
}

// GetTenant retrieves a tenant by ID.
func (s *TenantService) GetTenant(ctx context.Context, tenantID string) (*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if s.cacheTTL > 0 {
		s.cacheMu.RLock()
		cached, ok := s.cache[tenantID]
		s.cacheMu.RUnlock()
		if ok && time.Now().Before(cached.expiresAt) {
			if err := s.validateConfig(ctx, cached.tenant.ID, configFromTenant(cached.tenant)); err != nil {
				return nil, fmt.Errorf("%w: cached backend profile: %v", ErrInvalidTenantConfig, err)
			}
			return cloneTenant(cached.tenant)
		}
	}

	// Fetch from repository
	tenant, err := s.repo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if tenant == nil {
		return nil, ErrTenantNotFound
	}

	tenant, err = s.prepareLoadedTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}

	s.cacheTenant(tenant)

	return cloneTenant(tenant)
}

// GetTenantStatus is the least-privilege lifecycle lookup for queue workers.
// The SQL repository implements a status-only query. A legacy repository that
// lacks that optional capability is read without invoking decryptSecrets, so
// compatibility does not turn a status check into a plaintext credential read.
func (s *TenantService) GetTenantStatus(ctx context.Context, tenantID string) (TenantStatus, error) {
	if err := s.requireRepository(); err != nil {
		return "", err
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if statusRepo, ok := s.repo.(StatusRepository); ok {
		return statusRepo.GetStatus(ctx, tenantID)
	}
	source, err := s.repo.GetByID(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if source == nil {
		return "", ErrTenantNotFound
	}
	return source.Status, nil
}

// GetTenantByWebhookToken retrieves a tenant by webhook token.
//
// Webhook lookups are an ingress capability and therefore intentionally
// return the least-privilege channel snapshot. Runtime components that need
// the complete tenant configuration must use GetTenant instead.
func (s *TenantService) GetTenantByWebhookToken(ctx context.Context, token string) (*Tenant, error) {
	return s.GetTenantByWebhookTokenScoped(ctx, token)
}

// GetTenantByWebhookTokenScoped resolves the tenant and the single channel
// selected by token. Only that channel's verification credentials are
// decrypted; model, storage, governance, budget and unrelated channel
// credentials remain absent from the returned snapshot.
func (s *TenantService) GetTenantByWebhookTokenScoped(ctx context.Context, token string) (*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	if token == "" || len(token) > 256 || !utf8.ValidString(token) || strings.ContainsAny(token, "\x00\r\n") {
		return nil, fmt.Errorf("%w: webhook lookup token is invalid", ErrInvalidTenantConfig)
	}
	tenant, err := s.repo.GetByWebhookToken(ctx, token)
	if err != nil {
		return nil, err
	}
	if tenant == nil {
		return nil, ErrTenantNotFound
	}
	tenant, err = s.prepareScopedChannelTenant(ctx, tenant, func(binding *ChannelBinding) bool {
		return binding != nil && binding.EffectiveWebhookKey() == token
	}, true)
	if err != nil {
		return nil, err
	}
	// A webhook is an active-ingress capability. Enforce this invariant at the
	// service boundary as well as in the repository query so custom repository
	// implementations and status races fail closed.
	if tenant.Status != TenantStatusActive {
		return nil, ErrTenantNotFound
	}

	return cloneTenant(tenant)
}

// GetTenantChannelScoped resolves the one channel binding used by an Outbox
// delivery. It deliberately bypasses the plaintext tenant cache and decrypts
// only the requested binding so Delivery never receives model, storage or
// unrelated channel credentials.
func (s *TenantService) GetTenantChannelScoped(
	ctx context.Context,
	tenantID, channelType, accountID string,
) (*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	if err := validateScopedChannelQuery(tenantID, channelType, accountID); err != nil {
		return nil, err
	}
	source, err := s.repo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return s.prepareScopedChannelTenant(ctx, source, func(binding *ChannelBinding) bool {
		return binding != nil && binding.Type == channelType && binding.EnsureAccountID() == accountID
	}, false)
}

func validateScopedChannelQuery(tenantID, channelType, accountID string) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if err := validateLogicalName("channel type", channelType, 64); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if err := validateLogicalName("channel account ID", accountID, 128); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	return nil
}

// prepareScopedChannelTenant establishes the repository ownership boundary,
// selects exactly one channel, and decrypts only that channel's credentials.
// The source may contain encrypted configuration for the whole tenant; that
// ciphertext is never transformed into plaintext on this path.
func (s *TenantService) prepareScopedChannelTenant(
	ctx context.Context,
	source *Tenant,
	match func(*ChannelBinding) bool,
	webhook bool,
) (*Tenant, error) {
	if source == nil {
		return nil, ErrTenantNotFound
	}
	value, err := cloneTenant(source)
	if err != nil {
		return nil, fmt.Errorf("failed to clone scoped tenant: %w", err)
	}
	if err := validateScopedTenantMetadata(value); err != nil {
		return nil, err
	}
	var selected *ChannelBinding
	for index := range value.Channels {
		if match(&value.Channels[index]) {
			candidate := value.Channels[index]
			selected = &candidate
			break
		}
	}
	if selected == nil {
		return nil, ErrTenantNotFound
	}
	if webhook && selected.EffectiveWebhookKey() == "" {
		return nil, ErrTenantNotFound
	}
	if err := s.decryptChannelSecrets(ctx, value.ID, selected); err != nil {
		return nil, fmt.Errorf("failed to decrypt scoped channel credentials for tenant %s: %w", value.ID, err)
	}
	// Keep only the fields required by Gateway/Delivery. In particular, do not
	// carry encrypted values for any other channel or tenant capability into a
	// process that only needs to verify or send one IM message.
	value.Agents = nil
	value.Models = nil
	value.Storage = StorageConfig{}
	value.Governance = GovernancePolicy{}
	value.Budget = BudgetConfig{}
	value.Channels = []ChannelBinding{*selected}
	return value, nil
}

func validateScopedTenantMetadata(value *Tenant) error {
	if value == nil {
		return ErrTenantNotFound
	}
	if err := ValidateTenantID(value.ID); err != nil {
		return fmt.Errorf("%w: persisted %v", ErrInvalidTenantConfig, err)
	}
	if err := ValidateTenantName(value.Name); err != nil {
		return fmt.Errorf("%w: persisted %v", ErrInvalidTenantConfig, err)
	}
	if value.Status != TenantStatusActive && value.Status != TenantStatusSuspended {
		return fmt.Errorf("%w: persisted tenant status is invalid", ErrInvalidTenantConfig)
	}
	if value.ConfigVersion <= 0 {
		return fmt.Errorf("%w: persisted config version must be positive", ErrInvalidTenantConfig)
	}
	return nil
}

// UpdateTenant updates tenant configuration.
func (s *TenantService) UpdateTenant(ctx context.Context, tenant *Tenant) error {
	if err := s.requireRepository(); err != nil {
		return err
	}
	if tenant == nil {
		return fmt.Errorf("%w: tenant ID and name are required", ErrInvalidTenantConfig)
	}
	if err := ValidateTenantID(tenant.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if err := ValidateTenantName(tenant.Name); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if tenant.Status != TenantStatusActive && tenant.Status != TenantStatusSuspended {
		return fmt.Errorf("%w: tenant status must be active or suspended", ErrInvalidTenantConfig)
	}
	if err := s.validateConfig(ctx, tenant.ID, configFromTenant(tenant)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	for i := range tenant.Channels {
		if tenant.Channels[i].WebhookKey == "" {
			tenant.Channels[i].WebhookKey = uuid.New().String()
		}
		tenant.Channels[i].EnsureAccountID()
	}
	// Encrypt sensitive fields
	stored, err := cloneTenant(tenant)
	if err != nil {
		return fmt.Errorf("failed to clone tenant: %w", err)
	}
	if err := s.encryptSecrets(stored); err != nil {
		return fmt.Errorf("failed to encrypt secrets: %w", err)
	}

	if err := s.repo.Update(ctx, stored); err != nil {
		return err
	}
	tenant.UpdatedAt = stored.UpdatedAt
	tenant.ConfigVersion = stored.ConfigVersion

	s.cacheTenant(tenant)

	return nil
}

// DeleteTenant marks a tenant as deleted.
func (s *TenantService) DeleteTenant(ctx context.Context, tenantID string) error {
	if err := s.requireRepository(); err != nil {
		return err
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenantConfig, err)
	}
	if err := s.repo.Delete(ctx, tenantID); err != nil {
		return err
	}

	// Remove from cache
	s.cacheMu.Lock()
	delete(s.cache, tenantID)
	s.cacheMu.Unlock()

	return nil
}

// ListTenants lists all active tenants.
func (s *TenantService) ListTenants(ctx context.Context) ([]*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	tenants, err := s.repo.List(ctx, TenantStatusActive)
	if err != nil {
		return nil, err
	}

	// Repository implementations are allowed to reuse internal pointers. Clone
	// before decrypting so plaintext credentials never mutate repository state or
	// become an alias owned by a caller.
	loaded := make([]*Tenant, 0, len(tenants))
	for _, value := range tenants {
		prepared, err := s.prepareLoadedTenant(ctx, value)
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, prepared)
	}

	return loaded, nil
}

// ListTenantsForIDs is the least-privilege listing path for scoped operators.
// It refuses to fall back to a broad repository read because doing so would
// load unrelated encrypted credentials before the HTTP layer filters them.
func (s *TenantService) ListTenantsForIDs(ctx context.Context, tenantIDs []string) ([]*Tenant, error) {
	if err := s.requireRepository(); err != nil {
		return nil, err
	}
	if len(tenantIDs) == 0 {
		return []*Tenant{}, nil
	}
	for _, tenantID := range tenantIDs {
		if err := ValidateTenantID(tenantID); err != nil {
			return nil, fmt.Errorf("invalid scoped tenant id: %w", err)
		}
	}
	scoped, ok := s.repo.(ScopedRepository)
	if !ok {
		return nil, ErrScopedTenantListingUnsupported
	}
	tenants, err := scoped.ListByIDs(ctx, TenantStatusActive, tenantIDs)
	if err != nil {
		return nil, err
	}
	loaded := make([]*Tenant, 0, len(tenants))
	for _, value := range tenants {
		prepared, err := s.prepareLoadedTenant(ctx, value)
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, prepared)
	}
	return loaded, nil
}

// prepareLoadedTenant establishes an ownership boundary around repository
// results before decrypting credentials. Repository is an adapter seam, and a
// custom implementation may return a pointer to its internal record; mutating
// that pointer would permanently replace ciphertext with plaintext.
func (s *TenantService) prepareLoadedTenant(ctx context.Context, source *Tenant) (*Tenant, error) {
	if source == nil {
		return nil, ErrTenantNotFound
	}
	value, err := cloneTenant(source)
	if err != nil {
		return nil, fmt.Errorf("failed to clone loaded tenant: %w", err)
	}
	if err := s.decryptSecrets(value); err != nil {
		return nil, fmt.Errorf("failed to decrypt secrets for tenant %s: %w", value.ID, err)
	}
	if err := s.validateLoadedTenant(ctx, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (s *TenantService) validateLoadedTenant(ctx context.Context, value *Tenant) error {
	if value == nil {
		return ErrTenantNotFound
	}
	if err := ValidateTenantID(value.ID); err != nil {
		return fmt.Errorf("%w: persisted %v", ErrInvalidTenantConfig, err)
	}
	if err := ValidateTenantName(value.Name); err != nil {
		return fmt.Errorf("%w: persisted %v", ErrInvalidTenantConfig, err)
	}
	if value.Status != TenantStatusActive && value.Status != TenantStatusSuspended {
		return fmt.Errorf("%w: persisted tenant status is invalid", ErrInvalidTenantConfig)
	}
	if value.ConfigVersion <= 0 {
		return fmt.Errorf("%w: persisted config version must be positive", ErrInvalidTenantConfig)
	}
	if err := s.validateConfig(ctx, value.ID, configFromTenant(value)); err != nil {
		return fmt.Errorf("%w: persisted config: %v", ErrInvalidTenantConfig, err)
	}
	return nil
}

func (s *TenantService) validateConfig(ctx context.Context, tenantID string, config TenantConfig) error {
	if err := ValidateConfig(config); err != nil {
		return err
	}
	if s != nil && s.storageValidate != nil {
		if err := s.storageValidate(ctx, tenantID, config.Storage); err != nil {
			return fmt.Errorf("storage backend profile: %w", err)
		}
	}
	return nil
}

// Close closes the service.
func (s *TenantService) Close() error {
	if s == nil {
		return ErrTenantServiceUnavailable
	}
	s.keyMu.Lock()
	for id, key := range s.keyRing {
		for index := range key {
			key[index] = 0
		}
		delete(s.keyRing, id)
	}
	for index := range s.encryptKey {
		s.encryptKey[index] = 0
	}
	s.encryptKey = nil
	s.keyID = ""
	s.keyMu.Unlock()

	s.cacheMu.Lock()
	clear(s.cache)
	s.cacheMu.Unlock()
	if err := s.requireRepository(); err != nil {
		return err
	}
	return s.repo.Close()
}

func (s *TenantService) requireRepository() error {
	if s == nil || isNilTenantDependency(s.repo) {
		return ErrTenantServiceUnavailable
	}
	return nil
}

func isNilTenantDependency(value interface{}) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// cacheTenant retains plaintext only when an explicit positive TTL is in
// effect. Repository-authoritative mode must not become an unbounded secret
// cache whose entries can never be read or evicted.
func (s *TenantService) cacheTenant(value *Tenant) {
	if s == nil || value == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if s.cacheTTL <= 0 {
		delete(s.cache, value.ID)
		return
	}
	s.cache[value.ID] = &cachedTenant{
		tenant:    mustCloneTenant(value),
		expiresAt: time.Now().Add(s.cacheTTL),
	}
}

// encryptSecrets encrypts sensitive fields in the tenant configuration. New
// tenant credentials are bound to both their tenant and their stable logical
// field, so a valid ciphertext cannot be transplanted to another record.
func (s *TenantService) encryptSecrets(tenant *Tenant) error {
	if s == nil {
		return ErrTenantServiceUnavailable
	}
	return transformTenantSecrets(tenant, func(fieldID, plaintext string) (string, error) {
		return s.encryptTenantCredential(tenant.ID, fieldID, plaintext)
	})
}

// decryptSecrets decrypts sensitive fields in the tenant configuration. It
// accepts historical unbound envelopes so existing rows remain readable.
func (s *TenantService) decryptSecrets(tenant *Tenant) error {
	if s == nil {
		return ErrTenantServiceUnavailable
	}
	return transformTenantSecrets(tenant, func(fieldID, ciphertext string) (string, error) {
		return s.decryptTenantCredential(tenant.ID, fieldID, ciphertext)
	})
}

// decryptChannelSecrets is the narrow counterpart to decryptSecrets. It is
// used by ingress and egress paths whose process should never materialize the
// rest of a tenant's credential set.
func (s *TenantService) decryptChannelSecrets(ctx context.Context, tenantID string, binding *ChannelBinding) error {
	if s == nil {
		return ErrTenantServiceUnavailable
	}
	if err := transformChannelSecrets(binding, func(fieldID, ciphertext string) (string, error) {
		return s.decryptTenantCredential(tenantID, fieldID, ciphertext)
	}); err != nil {
		return err
	}
	return s.resolveChannelSecretRefsForTenant(ctx, tenantID, binding)
}

// resolveChannelSecretRefs materializes only the selected channel's
// operator-owned references. It runs after envelope decryption so legacy
// inline credentials remain compatible, while reference failures are stable
// and never expose a resolver's provider-specific error text.
func (s *TenantService) resolveChannelSecretRefs(ctx context.Context, binding *ChannelBinding) error {
	return s.resolveChannelSecretRefsForTenant(ctx, "", binding)
}

func (s *TenantService) resolveChannelSecretRefsForTenant(ctx context.Context, tenantID string, binding *ChannelBinding) error {
	if binding == nil {
		return ErrTenantNotFound
	}
	for _, field := range []struct {
		name    *string
		ref     string
		refDest *string
		purpose string
	}{
		{name: &binding.Token, ref: binding.TokenRef, refDest: &binding.TokenRef, purpose: "token"},
		{name: &binding.Secret, ref: binding.SecretRef, refDest: &binding.SecretRef, purpose: "secret"},
		{name: &binding.EncodingAESKey, ref: binding.EncodingAESKeyRef, refDest: &binding.EncodingAESKeyRef, purpose: "encoding_aes_key"},
	} {
		if field.ref == "" {
			continue
		}
		if *field.name != "" {
			return ErrInvalidSecretRef
		}
		if s.secretResolver == nil {
			return ErrSecretUnavailable
		}
		if err := SecretRef(field.ref).Validate(); err != nil {
			return ErrInvalidSecretRef
		}
		ref := SecretRef(field.ref)
		var value []byte
		var err error
		if scoped, ok := s.secretResolver.(TenantSecretResolver); ok && tenantID != "" {
			// Channel credentials are bound to the tenant and stable channel
			// identity. The purpose separates token, secret and AES material.
			purpose := "channel_" + field.purpose
			value, err = scoped.ResolveForTenant(ctx, tenantID, binding.Type, binding.EnsureAccountID(), purpose, ref)
		} else {
			// Preserve source compatibility for audited custom resolvers used by
			// tests and legacy composition roots; production EnvSecretResolver is
			// tenant-aware and takes the branch above.
			value, err = s.secretResolver.Resolve(ctx, ref)
		}
		if err != nil {
			if errors.Is(err, ErrInvalidSecretRef) {
				return ErrInvalidSecretRef
			}
			return ErrSecretUnavailable
		}
		if len(value) == 0 {
			return ErrSecretUnavailable
		}
		*field.name = string(value)
		*field.refDest = ""
		for i := range value {
			value[i] = 0
		}
	}
	return nil
}

type tenantCredentialTransform func(fieldID, value string) (string, error)

func transformTenantSecrets(tenant *Tenant, transform tenantCredentialTransform) error {
	if tenant == nil {
		return ErrTenantNotFound
	}
	for i := range tenant.Models {
		if tenant.Models[i].APIKey != "" {
			updated, err := transform("model:"+tenant.Models[i].ModelName+":api_key", tenant.Models[i].APIKey)
			if err != nil {
				return err
			}
			tenant.Models[i].APIKey = updated
		}
	}
	for i := range tenant.Channels {
		if err := transformChannelSecrets(&tenant.Channels[i], transform); err != nil {
			return err
		}
	}
	if err := transformStorageSecrets(tenant.Storage.SessionConfig, "storage:session", transform); err != nil {
		return err
	}
	if err := transformStorageSecrets(tenant.Storage.MemoryConfig, "storage:memory", transform); err != nil {
		return err
	}
	return nil
}

func transformChannelSecrets(binding *ChannelBinding, transform tenantCredentialTransform) error {
	if binding == nil {
		return ErrTenantNotFound
	}
	// AccountID is part of the channel's persisted identity. Derive it for
	// historical rows that predate the explicit field before binding a v2
	// envelope, rather than falling back to an array index.
	accountID := binding.EnsureAccountID()
	prefix := "channel:" + binding.Type + ":" + accountID
	for _, field := range []struct {
		name   string
		target *string
	}{
		{name: "token", target: &binding.Token},
		{name: "secret", target: &binding.Secret},
		{name: "encoding_aes_key", target: &binding.EncodingAESKey},
	} {
		target := field.target
		if *target == "" {
			continue
		}
		updated, err := transform(prefix+":"+field.name, *target)
		if err != nil {
			return err
		}
		*target = updated
	}
	if err := transformNamedSecrets(
		binding.Config,
		[]string{"encoding_aes_key", "corp_secret", "token", "secret"},
		prefix+":config",
		transform,
	); err != nil {
		return err
	}
	return nil
}

func transformStorageSecrets(config map[string]string, prefix string, transform tenantCredentialTransform) error {
	return transformNamedSecrets(config, []string{"dsn", "url", "password", "access_key", "secret_key", "token"}, prefix, transform)
}

func transformNamedSecrets(config map[string]string, keys []string, prefix string, transform tenantCredentialTransform) error {
	for _, key := range keys {
		value := config[key]
		if value == "" {
			continue
		}
		updated, err := transform(prefix+":"+key, value)
		if err != nil {
			return fmt.Errorf("transform credential %s: %w", key, err)
		}
		config[key] = updated
	}
	return nil
}

func cloneTenant(source *Tenant) (*Tenant, error) {
	if source == nil {
		return nil, nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var cloned Tenant
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return &cloned, nil
}

// Clone returns an independent tenant snapshot suitable for a runtime
// component. Callers must not share a mutable control-plane pointer with a
// Worker, governance filter, or storage adapter.
func Clone(source *Tenant) (*Tenant, error) { return cloneTenant(source) }

func mustCloneTenant(source *Tenant) *Tenant {
	cloned, err := cloneTenant(source)
	if err != nil {
		panic(fmt.Sprintf("clone tenant invariant failed: %v", err))
	}
	return cloned
}

// RotateEncryptionKey installs a new active key and rewraps every non-deleted
// tenant credential. The old keys remain in the ring, so a partial migration
// or a rolling restart can continue decrypting legacy envelopes safely. The
// caller must provide a verified audit actor because each rewrap is a tenant
// configuration mutation.
func (s *TenantService) RotateEncryptionKey(ctx context.Context, newKeyID, newMasterKey string) error {
	if err := s.requireRepository(); err != nil {
		return err
	}
	s.rotationMu.Lock()
	defer s.rotationMu.Unlock()
	if !validEncryptionKeyID(newKeyID) || newMasterKey == "" {
		return fmt.Errorf("invalid encryption key rotation configuration")
	}
	if _, err := auditActorFromContext(ctx); err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(newMasterKey))
	newKey := append([]byte(nil), hash[:]...)

	s.keyMu.RLock()
	candidate := cloneKeyRing(s.keyRing)
	if existing, exists := candidate[newKeyID]; exists {
		if subtle.ConstantTimeCompare(existing, newKey) != 1 {
			s.keyMu.RUnlock()
			return fmt.Errorf("encryption key id %q is already bound to different key material", newKeyID)
		}
	}
	s.keyMu.RUnlock()
	candidate[newKeyID] = append([]byte(nil), newKey...)

	tenants, err := s.repo.List(ctx, "")
	if err != nil {
		return fmt.Errorf("load tenants for key rotation: %w", err)
	}
	prepared := make([]*Tenant, 0, len(tenants))
	for _, source := range tenants {
		if source == nil || source.Status == TenantStatusDeleted {
			continue
		}
		value, err := cloneTenant(source)
		if err != nil {
			return fmt.Errorf("clone tenant during key rotation: %w", err)
		}
		if err := transformTenantSecretsWithRing(value, candidate, newKeyID); err != nil {
			return fmt.Errorf("decrypt tenant %s during key rotation: %w", value.ID, err)
		}
		if err := transformTenantSecrets(value, func(fieldID, plaintext string) (string, error) {
			return encryptTenantCredentialWithKey(plaintext, newKeyID, newKey, value.ID, fieldID)
		}); err != nil {
			return fmt.Errorf("rewrap tenant %s during key rotation: %w", value.ID, err)
		}
		prepared = append(prepared, value)
	}

	// Publish the new key before persistence so concurrent writes use the new
	// envelope. Old keys stay available until every prepared row is updated.
	s.installKeyRing(candidate, newKeyID)
	for _, value := range prepared {
		if err := s.repo.Update(ctx, value); err != nil {
			return fmt.Errorf("persist rotated credentials for tenant %s (retry is safe): %w", value.ID, err)
		}
	}
	return nil
}

// encrypt is retained for callers that need a generic, legacy-compatible
// envelope. Tenant configuration must use encryptTenantCredential instead so
// each secret has authenticated tenant and field context.
func (s *TenantService) encrypt(plaintext string) (string, error) {
	if s == nil {
		return "", ErrTenantServiceUnavailable
	}
	if plaintext == "" {
		return "", nil
	}
	s.keyMu.RLock()
	keyID := s.keyID
	key := append([]byte(nil), s.keyRing[keyID]...)
	s.keyMu.RUnlock()
	if len(key) == 0 {
		return "", errors.New("encryption key is unavailable")
	}
	return encryptWithKey(plaintext, keyID, key)
}

// decrypt reads generic v1 and historical unversioned envelopes. It is kept
// for legacy callers; tenant configuration uses decryptTenantCredential to
// authenticate v2 tenant and field context.
func (s *TenantService) decrypt(ciphertext string) (string, error) {
	if s == nil {
		return "", ErrTenantServiceUnavailable
	}
	if ciphertext == "" {
		return "", nil
	}
	s.keyMu.RLock()
	ring := cloneKeyRing(s.keyRing)
	activeID := s.keyID
	s.keyMu.RUnlock()
	return decryptWithRing(ciphertext, ring, activeID)
}

func (s *TenantService) encryptTenantCredential(tenantID, fieldID, plaintext string) (string, error) {
	if s == nil {
		return "", ErrTenantServiceUnavailable
	}
	if plaintext == "" {
		return "", nil
	}
	s.keyMu.RLock()
	keyID := s.keyID
	key := append([]byte(nil), s.keyRing[keyID]...)
	s.keyMu.RUnlock()
	if len(key) == 0 {
		return "", errors.New("encryption key is unavailable")
	}
	return encryptTenantCredentialWithKey(plaintext, keyID, key, tenantID, fieldID)
}

func (s *TenantService) decryptTenantCredential(tenantID, fieldID, ciphertext string) (string, error) {
	if s == nil {
		return "", ErrTenantServiceUnavailable
	}
	if ciphertext == "" {
		return "", nil
	}
	s.keyMu.RLock()
	ring := cloneKeyRing(s.keyRing)
	activeID := s.keyID
	s.keyMu.RUnlock()
	return decryptWithRingAndAAD(ciphertext, ring, activeID, tenantCredentialAAD(tenantID, fieldID))
}

func encryptWithKey(plaintext, keyID string, key []byte) (string, error) {
	return encryptEnvelope(plaintext, keyID, key, "v1", nil)
}

func encryptTenantCredentialWithKey(plaintext, keyID string, key []byte, tenantID, fieldID string) (string, error) {
	return encryptEnvelope(plaintext, keyID, key, "v2", tenantCredentialAAD(tenantID, fieldID))
}

func encryptEnvelope(plaintext, keyID string, key []byte, version string, aad []byte) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if !validEncryptionKeyID(keyID) || len(key) == 0 || (version != "v1" && version != "v2") || (version == "v2" && len(aad) == 0) {
		return "", errors.New("encryption key is unavailable")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), aad)
	return "enc:" + version + ":" + keyID + ":" + base64.StdEncoding.EncodeToString(ciphertext), nil
}

func decryptWithRing(ciphertext string, ring map[string][]byte, activeID string) (string, error) {
	return decryptWithRingAndAAD(ciphertext, ring, activeID, nil)
}

func decryptWithRingAndAAD(ciphertext string, ring map[string][]byte, activeID string, aad []byte) (string, error) {
	keyID := ""
	encoded := ciphertext
	version := ""
	versioned := strings.HasPrefix(ciphertext, "enc:")
	if versioned {
		parts := strings.SplitN(ciphertext, ":", 4)
		if len(parts) != 4 || parts[0] != "enc" || (parts[1] != "v1" && parts[1] != "v2") || !validEncryptionKeyID(parts[2]) || parts[3] == "" {
			return "", errors.New("invalid encrypted credential envelope")
		}
		version, keyID, encoded = parts[1], parts[2], parts[3]
		if version == "v2" && len(aad) == 0 {
			return "", errors.New("encrypted credential requires tenant context")
		}
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", errors.New("invalid encrypted credential")
	}

	ids := make([]string, 0, len(ring))
	if versioned {
		ids = append(ids, keyID)
	} else {
		if activeID != "" {
			if _, ok := ring[activeID]; ok {
				ids = append(ids, activeID)
			}
		}
		for id := range ring {
			if id != activeID {
				ids = append(ids, id)
			}
		}
		start := 0
		if len(ids) > 0 && ids[0] == activeID {
			start = 1
		}
		if len(ids)-start > 1 {
			sort.Strings(ids[start:])
		}
	}
	for _, id := range ids {
		key, ok := ring[id]
		if !ok {
			continue
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			continue
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil || len(data) < gcm.NonceSize() {
			continue
		}
		associatedData := []byte(nil)
		if version == "v2" {
			associatedData = aad
		}
		plaintext, err := gcm.Open(nil, data[:gcm.NonceSize()], data[gcm.NonceSize():], associatedData)
		if err == nil {
			return string(plaintext), nil
		}
	}
	return "", errors.New("failed to decrypt credential")
}

func transformTenantSecretsWithRing(tenant *Tenant, ring map[string][]byte, activeID string) error {
	return transformTenantSecrets(tenant, func(fieldID, ciphertext string) (string, error) {
		return decryptWithRingAndAAD(ciphertext, ring, activeID, tenantCredentialAAD(tenant.ID, fieldID))
	})
}

func tenantCredentialAAD(tenantID, fieldID string) []byte {
	// Length-prefixing prevents separator ambiguity if future field identifiers
	// gain additional dynamic segments.
	return []byte(fmt.Sprintf("trpc-agent-go/tenant-credential/v2|%d:%s|%d:%s", len(tenantID), tenantID, len(fieldID), fieldID))
}

func cloneKeyRing(source map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(source))
	for id, key := range source {
		result[id] = append([]byte(nil), key...)
	}
	return result
}

func (s *TenantService) installKeyRing(ring map[string][]byte, activeID string) {
	s.keyMu.Lock()
	oldRing := s.keyRing
	s.keyRing = cloneKeyRing(ring)
	s.keyID = activeID
	s.encryptKey = append(s.encryptKey[:0], s.keyRing[activeID]...)
	s.keyMu.Unlock()
	for id, key := range oldRing {
		for index := range key {
			key[index] = 0
		}
		delete(oldRing, id)
	}
}

func validEncryptionKeyID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}
