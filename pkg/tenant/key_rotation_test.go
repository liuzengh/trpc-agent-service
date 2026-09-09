package tenant

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestEncryptionKeyRingReadsLegacyAndUsesVersionedEnvelope(t *testing.T) {
	legacyService := NewService(&mockRepository{}, "old-master")
	legacyCiphertext := legacyEncryptForTest(t, "old-master", "legacy-secret")

	service, err := NewServiceWithKeyRing(&mockRepository{}, "new", map[string]string{
		"old": "old-master",
		"new": "new-master",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := service.decrypt(legacyCiphertext); err != nil || got != "legacy-secret" {
		t.Fatalf("legacy decrypt = %q, %v", got, err)
	}
	versioned, err := service.encrypt("new-secret")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(versioned, "enc:v1:new:") {
		t.Fatalf("ciphertext %q is missing active key envelope", versioned)
	}
	if got, err := service.decrypt(versioned); err != nil || got != "new-secret" {
		t.Fatalf("versioned decrypt = %q, %v", got, err)
	}
	_ = legacyService
}

func TestRotateEncryptionKeyRewrapsPersistedCredentials(t *testing.T) {
	repo := &rotationRepository{}
	service := NewService(repo, "old-master")
	created, err := service.CreateTenant(context.Background(), "acme", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(repo.value.Models[0].APIKey, "enc:v2:v1:") {
		t.Fatalf("initial credential envelope = %q", repo.value.Models[0].APIKey)
	}

	ctx := ContextWithAuditActor(context.Background(), "rotation-operator")
	if err := service.RotateEncryptionKey(ctx, "v2", "new-master"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(repo.value.Models[0].APIKey, "enc:v2:v2:") {
		t.Fatalf("rotated credential envelope = %q", repo.value.Models[0].APIKey)
	}
	if repo.updates != 1 {
		t.Fatalf("repository updates = %d, want 1", repo.updates)
	}

	newService, err := NewServiceWithKeyRing(&rotationRepository{value: repo.value}, "v2", map[string]string{"v2": "new-master"})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := newService.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Models[0].APIKey; got != "model-secret" {
		t.Fatalf("rotated credential plaintext = %q", got)
	}
}

func TestRotateEncryptionKeyRequiresNewVersionAndAuditActor(t *testing.T) {
	service := NewService(&mockRepository{}, "old-master")
	if err := service.RotateEncryptionKey(context.Background(), "v2", "new-master"); !errors.Is(err, ErrAuditActorRequired) {
		t.Fatalf("missing actor error = %v", err)
	}
	if err := service.RotateEncryptionKey(ContextWithAuditActor(context.Background(), "operator"), "v1", "new-master"); err == nil {
		t.Fatal("reusing an existing key version must be rejected")
	}
}

func TestRotateEncryptionKeyCanResumeAfterPartialPersistenceFailure(t *testing.T) {
	repo := &rotationRepository{}
	service := NewService(repo, "old-master")
	if _, err := service.CreateTenant(context.Background(), "acme", validServiceTenantConfig()); err != nil {
		t.Fatal(err)
	}
	repo.failUpdates = true
	ctx := ContextWithAuditActor(context.Background(), "rotation-operator")
	if err := service.RotateEncryptionKey(ctx, "v2", "new-master"); err == nil {
		t.Fatal("rotation unexpectedly succeeded with persistence failure")
	}
	repo.failUpdates = false
	if err := service.RotateEncryptionKey(ctx, "v2", "new-master"); err != nil {
		t.Fatalf("rotation could not resume with the same key version: %v", err)
	}
	if !strings.HasPrefix(repo.value.Models[0].APIKey, "enc:v2:v2:") {
		t.Fatalf("resumed rotation envelope = %q", repo.value.Models[0].APIKey)
	}
}

func legacyEncryptForTest(t *testing.T, masterKey, plaintext string) string {
	t.Helper()
	hash := sha256.Sum256([]byte(masterKey))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(plaintext), nil))
}

type rotationRepository struct {
	value       *Tenant
	updates     int
	failUpdates bool
}

func (r *rotationRepository) Create(_ context.Context, value *Tenant) error {
	r.value = mustCloneTenant(value)
	return nil
}
func (r *rotationRepository) GetByID(_ context.Context, id string) (*Tenant, error) {
	if r.value == nil || r.value.ID != id {
		return nil, ErrTenantNotFound
	}
	return cloneTenant(r.value)
}
func (r *rotationRepository) GetByWebhookToken(context.Context, string) (*Tenant, error) {
	return cloneTenant(r.value)
}
func (r *rotationRepository) List(_ context.Context, _ TenantStatus) ([]*Tenant, error) {
	if r.value == nil {
		return []*Tenant{}, nil
	}
	value, err := cloneTenant(r.value)
	return []*Tenant{value}, err
}
func (r *rotationRepository) Update(_ context.Context, value *Tenant) error {
	if r.failUpdates {
		return errors.New("injected persistence failure")
	}
	if r.value == nil || r.value.ID != value.ID || r.value.ConfigVersion != value.ConfigVersion {
		return ErrTenantConflict
	}
	value.ConfigVersion++
	r.value = mustCloneTenant(value)
	r.updates++
	return nil
}
func (r *rotationRepository) Delete(context.Context, string) error { return nil }
func (r *rotationRepository) Close() error                         { return nil }
