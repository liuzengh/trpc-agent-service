package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestScopedWebhookReadDecryptsOnlySelectedChannel(t *testing.T) {
	repo := &captureRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	config := validServiceTenantConfig()
	config.Channels = append(config.Channels, ChannelBinding{
		Type:     "telegram",
		AgentApp: "support",
		Token:    "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef",
		Secret:   strings.Repeat("T", 32),
		AccessPolicy: ChannelAccessPolicy{
			AllowDirectMessages: true,
			AllowedUsers:        []string{"telegram-user"},
		},
	})
	created, err := service.CreateTenant(context.Background(), "acme", config)
	if err != nil {
		t.Fatal(err)
	}
	first := created.Channels[0]

	// Corrupt credentials that are outside the requested channel. A scoped
	// read must not attempt to decrypt them, so the selected channel remains
	// available while a full runtime read correctly fails closed.
	repo.created.Models[0].APIKey = "enc:v2:v1:not-a-model-envelope"
	repo.created.Channels[1].Token = "enc:v2:v1:not-an-unrelated-channel-envelope"

	snapshot, err := service.GetTenantByWebhookTokenScoped(context.Background(), first.WebhookKey)
	if err != nil {
		t.Fatalf("scoped webhook read failed: %v", err)
	}
	if snapshot.ID != created.ID || snapshot.Status != TenantStatusActive || len(snapshot.Channels) != 1 {
		t.Fatalf("unexpected scoped tenant metadata: %#v", snapshot)
	}
	if snapshot.Channels[0].AccountID != first.AccountID || snapshot.Channels[0].Token != first.Token {
		t.Fatalf("selected channel was not decrypted: %#v", snapshot.Channels[0])
	}
	if snapshot.Models != nil || snapshot.Storage.SessionBackend != "" || snapshot.Storage.MemoryBackend != "" ||
		len(snapshot.Storage.SessionConfig) != 0 || len(snapshot.Storage.MemoryConfig) != 0 ||
		len(snapshot.Governance.DataMasking) != 0 || len(snapshot.Governance.ContentFilters) != 0 ||
		snapshot.Governance.AuditLevel != "" || snapshot.Budget.MaxTokensPerDay != 0 ||
		snapshot.Budget.MaxTokensPerRequest != 0 || snapshot.Budget.MaxCostPerDay != 0 ||
		snapshot.Budget.MaxConcurrentSessions != 0 || len(snapshot.Budget.AlertThresholds) != 0 {
		t.Fatalf("scoped webhook snapshot carried unrelated tenant capabilities: %#v", snapshot)
	}
	if snapshot.Channels[0].Secret != first.Secret {
		t.Fatalf("selected channel secret was not decrypted")
	}

	if _, err := service.GetTenant(context.Background(), created.ID); err == nil {
		t.Fatal("full tenant read accepted corrupted model credential")
	}
}

func TestScopedChannelReadDecryptsOnlyRequestedBinding(t *testing.T) {
	repo := &captureRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	config := validServiceTenantConfig()
	config.Channels = append(config.Channels, ChannelBinding{
		Type:     "telegram",
		AgentApp: "support",
		Token:    "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef",
		Secret:   strings.Repeat("T", 32),
		AccessPolicy: ChannelAccessPolicy{
			AllowDirectMessages: true,
			AllowedUsers:        []string{"telegram-user"},
		},
	})
	created, err := service.CreateTenant(context.Background(), "acme", config)
	if err != nil {
		t.Fatal(err)
	}
	requested := created.Channels[1]
	repo.created.Models[0].APIKey = "enc:v2:v1:not-a-model-envelope"
	repo.created.Channels[0].Token = "enc:v2:v1:not-an-unrelated-channel-envelope"

	snapshot, err := service.GetTenantChannelScoped(
		context.Background(), created.ID, requested.Type, requested.AccountID,
	)
	if err != nil {
		t.Fatalf("scoped channel read failed: %v", err)
	}
	if snapshot.ID != created.ID || len(snapshot.Channels) != 1 {
		t.Fatalf("unexpected scoped channel metadata: %#v", snapshot)
	}
	if got := snapshot.Channels[0]; got.Type != requested.Type || got.AccountID != requested.AccountID || got.Token != requested.Token || got.Secret != requested.Secret {
		t.Fatalf("requested channel was not returned in plaintext: %#v", got)
	}
	if snapshot.Models != nil || snapshot.Storage.SessionBackend != "" || snapshot.Storage.MemoryBackend != "" ||
		len(snapshot.Storage.SessionConfig) != 0 || len(snapshot.Storage.MemoryConfig) != 0 {
		t.Fatalf("scoped channel snapshot carried unrelated credentials: %#v", snapshot)
	}

	if _, err := service.GetTenantChannelScoped(context.Background(), created.ID, requested.Type, "missing-account"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("missing channel account error=%v, want ErrTenantNotFound", err)
	}
}

func TestScopedChannelReadValidatesQueryBeforeRepositoryAccess(t *testing.T) {
	repo := &countingScopedReadRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	for _, query := range [][3]string{
		{"", "telegram", "account"},
		{"tenant-a", "", "account"},
		{"tenant-a", "telegram", ""},
	} {
		if _, err := service.GetTenantChannelScoped(context.Background(), query[0], query[1], query[2]); !errors.Is(err, ErrInvalidTenantConfig) {
			t.Fatalf("query=%v error=%v, want ErrInvalidTenantConfig", query, err)
		}
	}
	if repo.getCalls != 0 {
		t.Fatalf("invalid scoped queries reached repository %d times", repo.getCalls)
	}
}

type countingScopedReadRepository struct {
	captureRepository
	getCalls int
}

func (r *countingScopedReadRepository) GetByID(ctx context.Context, id string) (*Tenant, error) {
	r.getCalls++
	return r.captureRepository.GetByID(ctx, id)
}
