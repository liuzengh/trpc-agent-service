//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"context"
	"strings"
	"testing"
)

func TestTenantCredentialV2RejectsTenantAndFieldTransplants(t *testing.T) {
	const masterKey = "0123456789abcdef0123456789abcdef"

	firstRepo := &rotationRepository{}
	firstService := NewService(firstRepo, masterKey)
	first, err := firstService.CreateTenant(context.Background(), "first", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(firstRepo.value.Models[0].APIKey, "enc:v2:") {
		t.Fatalf("tenant secret did not use v2 envelope: %q", firstRepo.value.Models[0].APIKey)
	}

	secondRepo := &rotationRepository{}
	secondService := NewService(secondRepo, masterKey)
	second, err := secondService.CreateTenant(context.Background(), "second", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}
	secondRepo.value.Models[0].APIKey = firstRepo.value.Models[0].APIKey
	if _, err := secondService.GetTenant(context.Background(), second.ID); err == nil {
		t.Fatal("cross-tenant credential transplant was accepted")
	}

	firstRepo.value.Channels[0].Token = firstRepo.value.Models[0].APIKey
	if _, err := firstService.GetTenant(context.Background(), first.ID); err == nil {
		t.Fatal("cross-field credential transplant was accepted")
	}
}

func TestTenantCredentialsReadV1AndLegacyEnvelopes(t *testing.T) {
	const masterKey = "0123456789abcdef0123456789abcdef"
	repo := &rotationRepository{}
	service := NewService(repo, masterKey)
	created, err := service.CreateTenant(context.Background(), "acme", validServiceTenantConfig())
	if err != nil {
		t.Fatal(err)
	}

	v1, err := service.encrypt("model-secret")
	if err != nil {
		t.Fatal(err)
	}
	repo.value.Models[0].APIKey = v1
	loaded, err := service.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("read v1 tenant credential: %v", err)
	}
	if got := loaded.Models[0].APIKey; got != "model-secret" {
		t.Fatalf("v1 credential plaintext = %q", got)
	}

	repo.value.Models[0].APIKey = legacyEncryptForTest(t, masterKey, "model-secret")
	loaded, err = service.GetTenant(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("read legacy tenant credential: %v", err)
	}
	if got := loaded.Models[0].APIKey; got != "model-secret" {
		t.Fatalf("legacy credential plaintext = %q", got)
	}
}
