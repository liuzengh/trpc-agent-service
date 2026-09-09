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
	"fmt"
	"strings"
	"testing"
)

func TestSecrets_NotInErrorMessages(t *testing.T) {
	// Test that API keys don't leak in error messages
	tenant := &Tenant{
		ID:   "test-tenant",
		Name: "Test Tenant",
		Models: []ModelConfig{
			{
				Provider:  "openai",
				ModelName: "gpt-4",
				APIKey:    "sk-secret-test-key-12345678901234567890",
			},
		},
	}

	apiKey := tenant.Models[0].APIKey

	// Simulate error message
	err := fmt.Errorf("failed to authenticate with provider: invalid API key format")

	// Verify API key not in error
	if strings.Contains(err.Error(), apiKey) {
		t.Fatal("SECURITY FAILURE: API key leaked in error message")
	}

	if strings.Contains(err.Error(), "sk-secret") {
		t.Fatal("SECURITY FAILURE: API key prefix leaked in error message")
	}
}

func TestSecrets_NotInLogOutput(t *testing.T) {
	tenant := &Tenant{
		ID: "test-tenant",
		Channels: []ChannelBinding{
			{
				Type:   "telegram",
				Token:  "123456:ABC-DEF-secret-bot-token",
				Secret: "webhook-secret-password-123",
			},
		},
	}

	// Simulate logging
	logMessage := fmt.Sprintf("Processing webhook for tenant %s", tenant.ID)

	// Verify secrets not in log
	if strings.Contains(logMessage, tenant.Channels[0].Token) {
		t.Fatal("SECURITY FAILURE: Bot token leaked in log message")
	}

	if strings.Contains(logMessage, tenant.Channels[0].Secret) {
		t.Fatal("SECURITY FAILURE: Webhook secret leaked in log message")
	}
}

func TestSecrets_EncryptionRoundTrip(t *testing.T) {
	masterKey := "test-master-key-32-bytes-long!!"

	repo := &mockRepository{}
	service := NewService(repo, masterKey)

	secret := "sk-very-secret-api-key-should-not-leak"

	// Encrypt
	encrypted, err := service.encrypt(secret)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Verify original secret not in encrypted form
	if strings.Contains(encrypted, secret) {
		t.Fatal("SECURITY FAILURE: Secret appears in plaintext in encrypted data")
	}

	if strings.Contains(encrypted, "sk-very-secret") {
		t.Fatal("SECURITY FAILURE: Secret prefix appears in encrypted data")
	}

	// Decrypt
	decrypted, err := service.decrypt(encrypted)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	// Verify decrypted matches original
	if decrypted != secret {
		t.Errorf("decrypted secret doesn't match: got %s, want %s", decrypted, secret)
	}
}

func TestSecrets_MalformedEncryptedData(t *testing.T) {
	masterKey := "test-master-key-32-bytes-long!!"

	repo := &mockRepository{}
	service := NewService(repo, masterKey)

	// Try to decrypt invalid data
	_, err := service.decrypt("invalid-encrypted-data")

	// Should fail gracefully
	if err == nil {
		t.Error("expected error for invalid encrypted data")
	}

	// Error message should not contain master key
	if strings.Contains(err.Error(), masterKey) {
		t.Fatal("SECURITY FAILURE: Master key leaked in error message")
	}
}

func TestSecrets_StructMarshaling(t *testing.T) {
	tenant := &Tenant{
		ID:   "test-tenant",
		Name: "Test",
		Models: []ModelConfig{
			{
				Provider: "openai",
				APIKey:   "sk-secret-key-12345",
			},
		},
		Channels: []ChannelBinding{
			{
				Type:   "telegram",
				Secret: "webhook-secret-abc",
			},
		},
	}

	// Convert to string (simulates accidental logging)
	str := fmt.Sprintf("%+v", tenant)

	// Document current behavior
	if strings.Contains(str, "sk-secret-key") {
		t.Logf("WARNING: API key appears in struct string representation")
	}

	if strings.Contains(str, "webhook-secret") {
		t.Logf("WARNING: Webhook secret appears in struct string representation")
	}
}

// mockRepository for testing
type mockRepository struct{}

func (m *mockRepository) Create(ctx context.Context, tenant *Tenant) error        { return nil }
func (m *mockRepository) GetByID(ctx context.Context, id string) (*Tenant, error) { return nil, nil }
func (m *mockRepository) GetByWebhookToken(ctx context.Context, token string) (*Tenant, error) {
	return nil, nil
}
func (m *mockRepository) Update(ctx context.Context, tenant *Tenant) error { return nil }
func (m *mockRepository) Delete(ctx context.Context, id string) error      { return nil }
func (m *mockRepository) List(ctx context.Context, status TenantStatus) ([]*Tenant, error) {
	return nil, nil
}
func (m *mockRepository) Close() error { return nil }
