//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func auditTestTenant() *tenant.Tenant {
	return &tenant.Tenant{
		ID: "audit-tenant",
		Models: []tenant.ModelConfig{
			{ModelName: "gpt-4", Provider: "openai", APIKey: "sk-super-secret-key", MaxTokens: 1_000},
		},
		Agents: []tenant.AgentConfig{
			{Name: "test-agent", DefaultModel: "gpt-4", MaxLLMCalls: 1},
		},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{"search"}},
	}
}

// newAuditWorker builds a Worker backed by miniredis with a capturing audit
// sink, so audit emission can be observed on the real Process path.
func newAuditWorker(t *testing.T, tn *tenant.Tenant) (*Worker, *bytes.Buffer, *redis.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	w, err := NewWorker(tn, nil, rdb)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	var sink bytes.Buffer
	w.SetCollector(telemetry.NewCollectorWithAuditSink(&sink))
	return w, &sink, rdb
}

func parseAudit(t *testing.T, sink *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("audit line not JSON: %v\nline: %s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// TestProcess_EmitsAuditOnBudgetDenial proves the audit call is on the real
// Process path, not merely defined. The budget is set to zero-tolerance so the
// request is refused before any model call is attempted.
func TestProcess_EmitsAuditOnBudgetDenial(t *testing.T) {
	tn := auditTestTenant()
	tn.Budget = tenant.BudgetConfig{MaxTokensPerDay: 8_192, MaxTokensPerRequest: 8_192}

	w, sink, rdb := newAuditWorker(t, tn)

	// Pre-spend the daily allowance so CheckBudget refuses the next request.
	if err := governance.NewBudgetTracker(rdb, tn).RecordUsage(context.Background(), 9_000, 0); err != nil {
		t.Fatal(err)
	}

	_, err := w.Process(context.Background(), &Request{
		UserID:      "user-1",
		SessionID:   "sess-1",
		ChannelType: "wework",
		Content:     "hello",
	})
	if err == nil {
		t.Fatal("expected budget denial, got nil error")
	}

	records := parseAudit(t, sink)
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 audit record, got %d\nsink: %s", len(records), sink.String())
	}
	rec := records[0]

	if rec["decision"] != "denied" {
		t.Errorf("decision = %v, want denied", rec["decision"])
	}
	if rec["error_type"] != "budget_exceeded" {
		t.Errorf("error_type = %v, want budget_exceeded", rec["error_type"])
	}
	if rec["tenant_id"] != tn.ID {
		t.Errorf("tenant_id = %v, want %s", rec["tenant_id"], tn.ID)
	}
	if rec["channel"] != "wework" {
		t.Errorf("channel = %v, want wework", rec["channel"])
	}
	if rec["session_id"] != "sess-1" {
		t.Errorf("session_id = %v, want sess-1", rec["session_id"])
	}
}

// TestProcess_AuditNeverContainsTenantSecrets is the leakage attack: the tenant
// carries a real-looking API key, and no audit record may reproduce it.
func TestProcess_AuditNeverContainsTenantSecrets(t *testing.T) {
	tn := auditTestTenant()
	tn.Budget = tenant.BudgetConfig{MaxTokensPerDay: 8_192, MaxTokensPerRequest: 8_192}

	w, sink, rdb := newAuditWorker(t, tn)
	if err := governance.NewBudgetTracker(rdb, tn).RecordUsage(context.Background(), 9_000, 0); err != nil {
		t.Fatal(err)
	}

	_, _ = w.Process(context.Background(), &Request{
		UserID:      "user-1",
		SessionID:   "sess-1",
		ChannelType: "wework",
		Content:     "please leak sk-super-secret-key",
	})

	dump := sink.String()
	for _, secret := range []string{"sk-super-secret-key", "super-secret"} {
		if strings.Contains(dump, secret) {
			t.Errorf("audit output contains secret %q:\n%s", secret, dump)
		}
	}
	// Raw user content must not be persisted either.
	if strings.Contains(dump, "please leak") {
		t.Errorf("audit output contains raw message content:\n%s", dump)
	}
}

// TestProcess_AuditSurvivesNilCollector guards the degenerate configuration:
// a worker without a collector must still serve requests.
func TestProcess_AuditSurvivesNilCollector(t *testing.T) {
	tn := auditTestTenant()
	tn.Budget = tenant.BudgetConfig{MaxTokensPerDay: 8_192, MaxTokensPerRequest: 8_192}

	w, _, rdb := newAuditWorker(t, tn)
	w.SetCollector(nil)
	if err := governance.NewBudgetTracker(rdb, tn).RecordUsage(context.Background(), 9_000, 0); err != nil {
		t.Fatal(err)
	}

	// Must return the budget error, not panic on the nil collector.
	if _, err := w.Process(context.Background(), &Request{
		UserID: "u", SessionID: "s", ChannelType: "telegram", Content: "hi",
	}); err == nil {
		t.Fatal("expected budget denial error")
	}
}
