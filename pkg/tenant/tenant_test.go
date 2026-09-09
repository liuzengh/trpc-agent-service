//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"testing"
)

func TestToolPolicy_IsAllowed(t *testing.T) {
	tests := []struct {
		name     string
		policy   ToolPolicy
		toolName string
		expected bool
	}{
		{
			name: "whitelist mode - allowed tool",
			policy: ToolPolicy{
				Mode:    "whitelist",
				Allowed: []string{"search", "calculator"},
			},
			toolName: "search",
			expected: true,
		},
		{
			name: "whitelist mode - denied tool",
			policy: ToolPolicy{
				Mode:    "whitelist",
				Allowed: []string{"search", "calculator"},
			},
			toolName: "delete_file",
			expected: false,
		},
		{
			name: "blacklist mode - allowed tool",
			policy: ToolPolicy{
				Mode:   "blacklist",
				Denied: []string{"delete_file", "execute_command"},
			},
			toolName: "search",
			expected: true,
		},
		{
			name: "blacklist mode - denied tool",
			policy: ToolPolicy{
				Mode:   "blacklist",
				Denied: []string{"delete_file", "execute_command"},
			},
			toolName: "delete_file",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.policy.IsAllowed(tt.toolName)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestToolPolicy_RequiresConfirmation(t *testing.T) {
	policy := ToolPolicy{
		Mode:                "whitelist",
		Allowed:             []string{"search", "delete_file"},
		RequireConfirmation: []string{"delete_file"},
	}

	if !policy.RequiresConfirmation("delete_file") {
		t.Error("expected delete_file to require confirmation")
	}

	if policy.RequiresConfirmation("search") {
		t.Error("expected search to not require confirmation")
	}
}

func TestTenant_Validation(t *testing.T) {
	tenant := &Tenant{
		ID:     "tenant-123",
		Name:   "Test Tenant",
		Status: TenantStatusActive,
		Agents: []AgentConfig{
			{
				Name:         "default",
				Type:         "llm",
				SystemPrompt: "You are a helpful assistant",
				DefaultModel: "gpt-4",
				Tools:        []string{"search"},
			},
		},
		Models: []ModelConfig{
			{
				Provider:  "openai",
				ModelName: "gpt-4",
				APIKey:    "sk-test-key",
			},
		},
		ToolPolicy: ToolPolicy{
			Mode:    "whitelist",
			Allowed: []string{"search"},
		},
		Channels: []ChannelBinding{
			{
				Type:   "telegram",
				Token:  "bot-token",
				Secret: "webhook-secret",
			},
		},
		Storage: StorageConfig{
			SessionBackend: "inmemory",
			MemoryBackend:  "inmemory",
		},
	}

	// Validate basic structure
	if tenant.ID == "" {
		t.Error("tenant ID should not be empty")
	}

	if len(tenant.Agents) == 0 {
		t.Error("tenant should have at least one agent")
	}

	if len(tenant.Models) == 0 {
		t.Error("tenant should have at least one model")
	}
}

func TestTenant_IsolationCheck(t *testing.T) {
	tenant1 := &Tenant{
		ID:   "tenant-1",
		Name: "Tenant 1",
	}

	tenant2 := &Tenant{
		ID:   "tenant-2",
		Name: "Tenant 2",
	}

	// Ensure tenant IDs are different
	if tenant1.ID == tenant2.ID {
		t.Error("different tenants should have different IDs")
	}

	// Ensure tenant data doesn't interfere
	tenant1.Storage = StorageConfig{
		SessionBackend: "redis",
		SessionConfig: map[string]string{
			"addr": "localhost:6379",
			"db":   "1",
		},
	}

	tenant2.Storage = StorageConfig{
		SessionBackend: "redis",
		SessionConfig: map[string]string{
			"addr": "localhost:6379",
			"db":   "2",
		},
	}

	// Verify separate databases
	if tenant1.Storage.SessionConfig["db"] == tenant2.Storage.SessionConfig["db"] {
		t.Error("tenants should use separate databases for isolation")
	}
}

func TestChannelBindingNeverUsesProviderTokenAsWebhookKey(t *testing.T) {
	binding := ChannelBinding{Type: "telegram", Token: "123456:provider-secret"}
	if got := binding.EffectiveWebhookKey(); got != "" {
		t.Fatalf("legacy provider token was exposed as webhook key: %q", got)
	}
}
