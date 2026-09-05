package storage

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// FakeMetadataRepository is deterministic test storage for P1-04 metadata.
type FakeMetadataRepository struct {
	mu         sync.RWMutex
	bindings   map[string]tenant.ChannelBinding
	identities map[string]tenant.Identity
	audits     []BindingAuditEvent
}

func NewFakeMetadataRepository() *FakeMetadataRepository {
	return &FakeMetadataRepository{bindings: map[string]tenant.ChannelBinding{}, identities: map[string]tenant.Identity{}}
}

func metadataBindingKey(tenantID, channel, bindingID string) string {
	return tenantID + "\x00" + channel + "\x00" + bindingID
}

func metadataIdentityKey(tc tenant.TenantContext) string {
	return tc.TenantID + "\x00" + tc.Channel + "\x00" + tc.BindingID + "\x00" + tc.ExternalUser
}

func (f *FakeMetadataRepository) PutBinding(value tenant.ChannelBinding) error {
	if f == nil {
		return ErrInvalidArgument
	}
	value = value.Canonical()
	if err := value.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.bindings {
		if existing.Channel == value.Channel && existing.ExternalAppID == value.ExternalAppID && existing.TenantID != value.TenantID {
			return tenant.ErrBindingConflict
		}
	}
	key := metadataBindingKey(value.TenantID, value.Channel, value.ID)
	if _, exists := f.bindings[key]; exists {
		return ErrConflict
	}
	f.bindings[key] = value
	return nil
}

func (f *FakeMetadataRepository) ResolveBinding(ctx context.Context, channel, externalAppID string) (tenant.ChannelBinding, error) {
	if err := metadataContext(ctx); err != nil {
		return tenant.ChannelBinding{}, err
	}
	if channel != tenant.ChannelLark && channel != tenant.ChannelTelegram || externalAppID == "" {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	var found tenant.ChannelBinding
	for _, value := range f.bindings {
		if value.Channel == channel && value.ExternalAppID == externalAppID {
			if found.ID != "" {
				return tenant.ChannelBinding{}, tenant.ErrBindingConflict
			}
			found = value
		}
	}
	if found.ID == "" {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	return found, nil
}

func (f *FakeMetadataRepository) GetBinding(ctx context.Context, tc tenant.TenantContext, id string) (tenant.ChannelBinding, error) {
	if err := metadataTenantContext(ctx, tc); err != nil {
		return tenant.ChannelBinding{}, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok := f.bindings[metadataBindingKey(tc.TenantID, tc.Channel, id)]
	if !ok {
		return tenant.ChannelBinding{}, ErrNotFound
	}
	return value, nil
}

func (f *FakeMetadataRepository) CreateBinding(ctx context.Context, tc tenant.TenantContext, value tenant.ChannelBinding) error {
	if err := metadataTenantContext(ctx, tc); err != nil {
		return err
	}
	if value.TenantID != tc.TenantID || value.Channel != tc.Channel {
		return ErrTenantMismatch
	}
	return f.PutBinding(value)
}

func (f *FakeMetadataRepository) UpdateBinding(ctx context.Context, tc tenant.TenantContext, value tenant.ChannelBinding, expectedVersion int64) error {
	if err := metadataTenantContext(ctx, tc); err != nil {
		return err
	}
	if value.TenantID != tc.TenantID || value.Channel != tc.Channel || value.Version != expectedVersion+1 || expectedVersion < 1 {
		return ErrInvalidArgument
	}
	if err := value.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := metadataBindingKey(value.TenantID, value.Channel, value.ID)
	current, ok := f.bindings[key]
	if !ok {
		return ErrNotFound
	}
	if current.Version != expectedVersion {
		return ErrConflict
	}
	for _, existing := range f.bindings {
		if existing.Channel == value.Channel && existing.ExternalAppID == value.ExternalAppID && (existing.TenantID != value.TenantID || existing.ID != value.ID) {
			return tenant.ErrBindingConflict
		}
	}
	f.bindings[key] = value
	return nil
}

func (f *FakeMetadataRepository) ResolveIdentity(ctx context.Context, tc tenant.TenantContext) (tenant.Identity, error) {
	if err := metadataTenantContext(ctx, tc); err != nil {
		return tenant.Identity{}, err
	}
	candidate, _, err := tenant.DeriveIdentity(tc)
	if err != nil {
		return tenant.Identity{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := metadataIdentityKey(tc)
	if value, ok := f.identities[key]; ok {
		if value.Status == tenant.IdentityStatusDisabled {
			return tenant.Identity{}, tenant.ErrIdentityDisabled
		}
		if value.Scope != candidate.Scope || value.ExternalChat != candidate.ExternalChat || value.ExternalThreadID != candidate.ExternalThreadID {
			return tenant.Identity{}, tenant.ErrIdentityConflict
		}
		return value, nil
	}
	f.identities[key] = candidate
	return candidate, nil
}

func (f *FakeMetadataRepository) AppendBindingEvent(ctx context.Context, tc tenant.TenantContext, event BindingAuditEvent) error {
	if err := metadataTenantContext(ctx, tc); err != nil {
		return err
	}
	if event.TenantID != tc.TenantID || event.BindingID != tc.BindingID || event.Channel != tc.Channel || event.AuditID == "" || event.Operation == "" || event.Version < 1 || len(event.Operation) > 64 || len(event.ErrorType) > 80 || len(event.IdentityFingerprint) > 80 || len(event.SecretFingerprint) > 80 {
		return ErrInvalidArgument
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, event)
	return nil
}

func (f *FakeMetadataRepository) AuditEvents() []BindingAuditEvent {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return append([]BindingAuditEvent(nil), f.audits...)
}

func metadataContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	return ctx.Err()
}

func metadataTenantContext(ctx context.Context, tc tenant.TenantContext) error {
	if err := metadataContext(ctx); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return ErrTenantMismatch
	}
	if tc.Channel != tenant.ChannelLark && tc.Channel != tenant.ChannelTelegram {
		return tenant.ErrUnsupportedBindingChannel
	}
	return nil
}

var _ BindingMetadataRepository = (*FakeMetadataRepository)(nil)
var _ IdentityMetadataRepository = (*FakeMetadataRepository)(nil)
var _ BindingAuditRepository = (*FakeMetadataRepository)(nil)
