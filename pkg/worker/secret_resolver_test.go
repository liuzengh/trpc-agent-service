//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type testSecretResolver struct {
	value []byte
	err   error
}

func (r testSecretResolver) Resolve(context.Context, tenant.SecretRef) ([]byte, error) {
	return append([]byte(nil), r.value...), r.err
}

func TestResolveModelCredentialUsesSecretRefWithoutLeakingResolverError(t *testing.T) {
	config := &tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_OPENAI"}
	resolved, err := resolveModelCredential(context.Background(), config, nil, testSecretResolver{value: []byte("operator-api-key")})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey != "operator-api-key" || resolved.APIKeyRef != config.APIKeyRef {
		t.Fatalf("resolved model = %#v", resolved)
	}
	if config.APIKey != "" {
		t.Fatal("resolver mutated source model configuration")
	}
}

func TestResolveModelCredentialReturnsStableError(t *testing.T) {
	config := &tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_OPENAI"}
	secretErr := errors.New("provider secret should not be returned")
	_, err := resolveModelCredential(context.Background(), config, nil, testSecretResolver{err: secretErr})
	if err == nil || !strings.Contains(err.Error(), "internal_error") {
		t.Fatalf("resolution error = %v", err)
	}
	if strings.Contains(err.Error(), secretErr.Error()) || strings.Contains(err.Error(), config.APIKeyRef) {
		t.Fatalf("resolution error leaked details: %v", err)
	}
}

func TestNewModelForTenantResolvesPinnedSecretWithoutMutatingSnapshot(t *testing.T) {
	config := tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_OPENAI"}
	mdl, err := NewModelForTenant(context.Background(), config, nil, testSecretResolver{value: []byte("operator-api-key")})
	if err != nil {
		t.Fatal(err)
	}
	if mdl.Info().Name != "gpt-4" {
		t.Fatalf("model info=%#v", mdl.Info())
	}
	if config.APIKey != "" {
		t.Fatal("model construction mutated immutable version snapshot")
	}
}

func TestResolveModelCredentialRequiresTenantScopedAuthorization(t *testing.T) {
	config := &tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_OPENAI"}
	tenantValue := &tenant.Tenant{ID: "tenant-a"}
	if _, err := resolveModelCredential(context.Background(), config, tenantValue, testSecretResolver{value: []byte("key")}); err == nil {
		t.Fatal("tenant-scoped model credential resolved through unscoped resolver")
	}
}
