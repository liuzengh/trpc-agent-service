package tenant

import (
	"context"
	"strings"
	"testing"
	"time"
)

type captureRepository struct {
	created *Tenant
}

// aliasingRepository intentionally returns its internal record. Repository
// implementations backed by an in-process cache can have this ownership
// shape; the service must clone before decrypting or returning it.
type aliasingRepository struct {
	value *Tenant
}

func (r *aliasingRepository) Create(_ context.Context, value *Tenant) error {
	r.value = value
	return nil
}
func (r *aliasingRepository) GetByID(context.Context, string) (*Tenant, error) { return r.value, nil }
func (r *aliasingRepository) GetByWebhookToken(context.Context, string) (*Tenant, error) {
	return r.value, nil
}
func (r *aliasingRepository) List(context.Context, TenantStatus) ([]*Tenant, error) {
	return []*Tenant{r.value}, nil
}
func (r *aliasingRepository) Update(_ context.Context, value *Tenant) error {
	r.value = value
	return nil
}
func (r *aliasingRepository) Delete(context.Context, string) error { return nil }
func (r *aliasingRepository) Close() error                         { return nil }

type aliasingScopedRepository struct{ *aliasingRepository }

func (r *aliasingScopedRepository) ListByIDs(context.Context, TenantStatus, []string) ([]*Tenant, error) {
	return []*Tenant{r.value}, nil
}

func (r *captureRepository) Create(_ context.Context, value *Tenant) error {
	cloned, err := cloneTenant(value)
	if err != nil {
		return err
	}
	r.created = cloned
	return nil
}
func (r *captureRepository) GetByID(context.Context, string) (*Tenant, error) {
	return cloneTenant(r.created)
}
func (r *captureRepository) GetByWebhookToken(context.Context, string) (*Tenant, error) {
	return cloneTenant(r.created)
}
func (r *captureRepository) List(context.Context, TenantStatus) ([]*Tenant, error) {
	value, err := cloneTenant(r.created)
	return []*Tenant{value}, err
}
func (r *captureRepository) Update(_ context.Context, value *Tenant) error {
	value.ConfigVersion++
	return nil
}
func (r *captureRepository) Delete(context.Context, string) error { return nil }
func (r *captureRepository) Close() error                         { return nil }

func TestCreateStoresEncryptedCredentialsWithoutDisabledCacheResidency(t *testing.T) {
	repo := &captureRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	created, err := service.CreateTenant(context.Background(), "acme", TenantConfig{
		Agents:     []AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt"}},
		Models:     []ModelConfig{{Provider: "openai", ModelName: "gpt", APIKey: "model-secret"}},
		ToolPolicy: ToolPolicy{Mode: "whitelist"},
		Channels: []ChannelBinding{{
			Type:           "wework",
			AgentApp:       "support",
			Token:          "VerificationToken123",
			Secret:         strings.Repeat("S", 43),
			EncodingAESKey: "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG",
			AppID:          "1000002",
			Config:         map[string]string{"corp_id": "ww0123456789abcdef"},
			AccessPolicy: ChannelAccessPolicy{
				AllowDirectMessages: true,
				AllowedUsers:        []string{"wework-user-1"},
			},
		}},
		Storage: StorageConfig{SessionBackend: "postgres", SessionProfile: "shared-postgres"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Channels[0].WebhookKey == "" || created.Channels[0].AccountID == "" {
		t.Fatalf("routing identifiers not initialized: %#v", created.Channels[0])
	}
	if created.Channels[0].Token != "VerificationToken123" || created.Channels[0].Secret != strings.Repeat("S", 43) {
		t.Fatalf("caller did not receive plaintext runtime credentials: %#v", created.Channels[0])
	}
	if repo.created.Channels[0].Token == created.Channels[0].Token ||
		repo.created.Channels[0].Secret == created.Channels[0].Secret ||
		repo.created.Channels[0].EncodingAESKey == created.Channels[0].EncodingAESKey {
		t.Fatalf("repository received plaintext credentials: %#v", repo.created.Channels[0])
	}
	if repo.created.Channels[0].WebhookKey != created.Channels[0].WebhookKey {
		t.Fatal("non-secret webhook lookup key changed during encryption")
	}
	if repo.created.Storage.SessionProfile != "shared-postgres" {
		t.Fatal("non-secret operator storage profile changed during encryption")
	}
	if len(repo.created.Storage.SessionConfig) != 0 {
		t.Fatal("tenant storage unexpectedly contains connection material")
	}
	if len(service.cache) != 0 {
		t.Fatalf("repository-authoritative mode retained %d plaintext cache entries", len(service.cache))
	}

	created.Channels[0].Token = "caller-mutation"
	loaded, err := service.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Channels[0].Token != "VerificationToken123" {
		t.Fatalf("caller mutation reached repository state: %q", loaded.Channels[0].Token)
	}
	if len(service.cache) != 0 {
		t.Fatalf("repository read retained %d plaintext cache entries", len(service.cache))
	}
	if _, err := service.GetTenantByWebhookToken(context.Background(), loaded.Channels[0].WebhookKey); err != nil {
		t.Fatal(err)
	}
	if len(service.cache) != 0 {
		t.Fatalf("webhook lookup retained %d plaintext cache entries", len(service.cache))
	}
}

func TestPositiveTenantCacheReturnsClonesAndClosePurgesSecrets(t *testing.T) {
	repo := &captureRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	service.cacheTTL = time.Minute
	created, err := service.CreateTenant(context.Background(), "acme", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(service.cache) != 1 {
		t.Fatalf("positive cache TTL retained %d entries, want 1", len(service.cache))
	}
	created.Models[0].APIKey = "caller-mutation"
	loaded, err := service.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Models[0].APIKey != "model-secret" {
		t.Fatalf("caller mutated cached secret: %q", loaded.Models[0].APIKey)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if len(service.cache) != 0 {
		t.Fatal("close did not purge plaintext tenant cache")
	}
	for _, value := range service.encryptKey {
		if value != 0 {
			t.Fatal("close did not clear derived encryption key")
		}
	}
}

func TestLoadedTenantDoesNotMutateAliasingRepository(t *testing.T) {
	repo := &aliasingRepository{}
	service := NewService(repo, "0123456789abcdef0123456789abcdef")
	created, err := service.CreateTenant(context.Background(), "acme", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}
	// CreateTenant stores the encrypted snapshot in the repository. Read paths
	// must not decrypt that same pointer in place.
	encryptedModel := repo.value.Models[0].APIKey
	encryptedChannel := repo.value.Channels[0].Token
	if encryptedModel == "model-secret" || encryptedChannel == "VerificationToken123" {
		t.Fatal("test repository did not retain encrypted credentials")
	}

	loaded, err := service.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Models[0].APIKey != "model-secret" || loaded.Channels[0].Token != "VerificationToken123" {
		t.Fatalf("loaded credentials were not decrypted: %#v", loaded)
	}
	if repo.value.Models[0].APIKey != encryptedModel || repo.value.Channels[0].Token != encryptedChannel {
		t.Fatal("read path decrypted credentials in repository-owned memory")
	}

	loaded.Models[0].APIKey = "caller-mutation"
	listed, err := service.ListTenants(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if listed[0].Models[0].APIKey != "model-secret" {
		t.Fatal("list path did not return an independent plaintext clone")
	}
	if repo.value.Models[0].APIKey != encryptedModel {
		t.Fatal("list path mutated repository-owned ciphertext")
	}

	scoped := &TenantService{repo: &aliasingScopedRepository{aliasingRepository: repo}, keyID: service.keyID,
		keyRing: cloneKeyRing(service.keyRing), encryptKey: append([]byte(nil), service.encryptKey...)}
	scopedList, err := scoped.ListTenantsForIDs(context.Background(), []string{created.ID})
	if err != nil {
		t.Fatal(err)
	}
	if scopedList[0].Channels[0].Token != "VerificationToken123" || repo.value.Channels[0].Token != encryptedChannel {
		t.Fatal("scoped list crossed repository ownership boundary")
	}
}

func validServiceTenantConfig() TenantConfig {
	return TenantConfig{
		Agents:     []AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt"}},
		Models:     []ModelConfig{{Provider: "openai", ModelName: "gpt", APIKey: "model-secret"}},
		ToolPolicy: ToolPolicy{Mode: "whitelist"},
		Channels: []ChannelBinding{{
			Type:           "wework",
			AgentApp:       "support",
			Token:          "VerificationToken123",
			Secret:         strings.Repeat("S", 43),
			EncodingAESKey: "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG",
			AppID:          "1000002",
			Config:         map[string]string{"corp_id": "ww0123456789abcdef"},
			AccessPolicy: ChannelAccessPolicy{
				AllowDirectMessages: true,
				AllowedUsers:        []string{"wework-user-1"},
			},
		}},
		Storage: StorageConfig{SessionBackend: "postgres", SessionProfile: "shared-postgres"},
	}
}
