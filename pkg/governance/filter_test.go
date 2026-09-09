//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package governance

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestGovernanceFilter_BeforeToolInvocation(t *testing.T) {
	tests := []struct {
		name      string
		tenant    *tenant.Tenant
		toolName  string
		expectErr bool
	}{
		{
			name: "allowed tool in whitelist",
			tenant: &tenant.Tenant{
				ID: "tenant-1",
				ToolPolicy: tenant.ToolPolicy{
					Mode:    "whitelist",
					Allowed: []string{"search", "calculator"},
				},
			},
			toolName:  "search",
			expectErr: false,
		},
		{
			name: "denied tool in whitelist",
			tenant: &tenant.Tenant{
				ID: "tenant-1",
				ToolPolicy: tenant.ToolPolicy{
					Mode:    "whitelist",
					Allowed: []string{"search", "calculator"},
				},
			},
			toolName:  "delete_file",
			expectErr: true,
		},
		{
			name: "tool requires confirmation",
			tenant: &tenant.Tenant{
				ID: "tenant-1",
				ToolPolicy: tenant.ToolPolicy{
					Mode:                "whitelist",
					Allowed:             []string{"delete_file"},
					RequireConfirmation: []string{"delete_file"},
				},
			},
			toolName:  "delete_file",
			expectErr: true, // Should return ErrToolConfirmationRequired
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := NewGovernanceFilter(tt.tenant)
			err := filter.BeforeToolInvocation(context.Background(), tt.toolName, nil)

			if tt.expectErr && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestGovernanceFilter_DataMasking(t *testing.T) {
	tests := []struct {
		name     string
		rules    []tenant.MaskingRule
		input    string
		expected string
	}{
		{
			name: "mask credit card",
			rules: []tenant.MaskingRule{
				{Type: "credit_card"},
			},
			input:    "My card is 1234-5678-9012-3456",
			expected: "My card is ****-****-****-3456",
		},
		{
			name: "mask email",
			rules: []tenant.MaskingRule{
				{Type: "email"},
			},
			input:    "Contact me at user@example.com",
			expected: "Contact me at u***@example.com",
		},
		{
			name: "mask phone",
			rules: []tenant.MaskingRule{
				{Type: "phone"},
			},
			input:    "Call me at +1-555-1234",
			expected: "Call me at +1-***-1234",
		},
		{
			name: "mask SSN",
			rules: []tenant.MaskingRule{
				{Type: "ssn"},
			},
			input:    "SSN: 123-45-6789",
			expected: "SSN: ***-**-6789",
		},
		{
			name: "mask API key",
			rules: []tenant.MaskingRule{
				{Type: "api_key"},
			},
			input:    "Key: sk-abc123def456",
			expected: "Key: sk-***456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenantObj := &tenant.Tenant{
				ID: "tenant-1",
				Governance: tenant.GovernancePolicy{
					DataMasking: tt.rules,
				},
			}

			filter := NewGovernanceFilter(tenantObj)
			result, _ := filter.AfterToolInvocation(context.Background(), "test_tool", tt.input, nil)

			resultStr, ok := result.(string)
			if !ok {
				t.Fatal("expected string result")
			}

			if resultStr != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, resultStr)
			}
		})
	}
}

func TestGovernanceFilter_MasksNestedToolOutput(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}, {Type: "credit_card"}},
	}})
	input := map[string]interface{}{
		"profile": map[string]interface{}{"email": "alice@example.com"},
		"cards":   []interface{}{"1234-5678-9012-3456"},
	}
	result, err := filter.AfterToolInvocation(context.Background(), "lookup", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	masked := result.(map[string]interface{})
	profile := masked["profile"].(map[string]interface{})
	if profile["email"] == "alice@example.com" {
		t.Fatal("nested email was not masked")
	}
	if masked["cards"].([]interface{})[0] == "1234-5678-9012-3456" {
		t.Fatal("nested card number was not masked")
	}
}

func TestGovernanceFilter_RejectsCyclicToolOutput(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	cyclic := map[string]interface{}{"email": "alice@example.com"}
	cyclic["self"] = cyclic

	masked, err := filter.AfterToolInvocation(context.Background(), "lookup", cyclic, nil)
	if !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("cyclic output error = %v, want ErrUnsafeToolOutput", err)
	}
	if masked != nil {
		t.Fatalf("cyclic output must fail closed, got %#v", masked)
	}
}

func TestGovernanceFilter_RejectsNonJSONToolOutput(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})

	masked, err := filter.AfterToolInvocation(context.Background(), "lookup", make(chan string), nil)
	if !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("non-JSON output error = %v, want ErrUnsafeToolOutput", err)
	}
	if masked != nil {
		t.Fatalf("non-JSON output must fail closed, got %#v", masked)
	}
}

func TestGovernanceFilter_DoesNotMutateToolOutput(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	original := map[string]interface{}{"profile": map[string]interface{}{"email": "alice@example.com"}}

	masked, err := filter.AfterToolInvocation(context.Background(), "lookup", original, nil)
	if err != nil {
		t.Fatal(err)
	}
	if original["profile"].(map[string]interface{})["email"] != "alice@example.com" {
		t.Fatal("masking mutated the tool-owned output")
	}
	if masked.(map[string]interface{})["profile"].(map[string]interface{})["email"] == "alice@example.com" {
		t.Fatal("returned output was not masked")
	}
}

func TestContentFilter_FilterContent(t *testing.T) {
	filters := []tenant.ContentFilter{
		{
			Name:     "profanity",
			Type:     "keyword",
			Patterns: []string{"badword", "offensive"},
			Action:   "block",
		},
		{
			Name:     "sensitive",
			Type:     "regex",
			Patterns: []string{`\b\d{3}-\d{2}-\d{4}\b`}, // SSN pattern
			Action:   "warn",
		},
	}

	contentFilter := NewContentFilter(filters)

	tests := []struct {
		name           string
		content        string
		expectedAction string
		expectedMatch  bool
	}{
		{
			name:           "contains profanity",
			content:        "This is a badword in the text",
			expectedAction: "block",
			expectedMatch:  true,
		},
		{
			name:           "contains SSN",
			content:        "My SSN is 123-45-6789",
			expectedAction: "warn",
			expectedMatch:  true,
		},
		{
			name:           "clean content",
			content:        "This is clean text",
			expectedAction: "",
			expectedMatch:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, matched := contentFilter.FilterContent(tt.content)

			if matched != tt.expectedMatch {
				t.Errorf("expected matched=%v, got %v", tt.expectedMatch, matched)
			}

			if action != tt.expectedAction {
				t.Errorf("expected action=%q, got %q", tt.expectedAction, action)
			}
		})
	}
}

func TestContentFilterUsesMostRestrictiveMatchingAction(t *testing.T) {
	filter := NewContentFilter([]tenant.ContentFilter{
		{Name: "broad-observation", Type: "keyword", Patterns: []string{"secret"}, Action: "log"},
		{Name: "specific-denial", Type: "regex", Patterns: []string{`secret\s+key`}, Action: "block"},
		{Name: "warning", Type: "keyword", Patterns: []string{"key"}, Action: "warn"},
	})

	action, matched := filter.FilterContent("exposed secret key")
	if !matched || action != "block" {
		t.Fatalf("FilterContent = (%q, %v), want block", action, matched)
	}
}

func TestMaskingRule_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		rule  tenant.MaskingRule
		input string
	}{
		{
			name:  "empty input",
			rule:  tenant.MaskingRule{Type: "email"},
			input: "",
		},
		{
			name:  "no match",
			rule:  tenant.MaskingRule{Type: "email"},
			input: "No email here",
		},
		{
			name:  "multiple matches",
			rule:  tenant.MaskingRule{Type: "email"},
			input: "Contact user1@example.com or user2@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyMaskingRule(tt.input, tt.rule)
			// Should not panic or crash
			if result == "" && tt.input != "" {
				t.Error("result should not be empty for non-empty input")
			}
		})
	}
}
