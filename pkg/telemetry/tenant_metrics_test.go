//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestTenantMetricLabelerAggregatesNonAllowlistedTenants(t *testing.T) {
	labeler, err := NewTenantMetricLabeler([]string{"tenant-a", "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}
	for tenantID, want := range map[string]string{
		"tenant-a": MetricsTenantOtherLabel,
		"tenant-b": MetricsTenantOtherLabel,
		"tenant-c": MetricsTenantOtherLabel,
		"":         MetricsTenantUnknownLabel,
	} {
		if tenantID == "tenant-a" || tenantID == "tenant-b" {
			want = tenantID
		}
		if got := labeler.Label(tenantID); got != want {
			t.Errorf("Label(%q)=%q, want %q", tenantID, got, want)
		}
	}
}

func TestParseTenantMetricAllowlistRejectsInvalidOrUnboundedConfiguration(t *testing.T) {
	if _, err := ParseTenantMetricAllowlist("tenant-a, tenant-b"); err != nil {
		t.Fatalf("parse valid allowlist: %v", err)
	}
	for _, value := range []string{"tenant-a,,tenant-b", "tenant-a,tenant-a", "tenant-a\x00bad"} {
		if _, err := ParseTenantMetricAllowlist(value); err == nil {
			t.Fatalf("ParseTenantMetricAllowlist(%q) succeeded", value)
		}
	}
	tooMany := make([]string, 0, MaxMetricsTenantAllowlist+1)
	for index := 0; index <= MaxMetricsTenantAllowlist; index++ {
		tooMany = append(tooMany, fmt.Sprintf("tenant-%d", index))
	}
	if _, err := ParseTenantMetricAllowlist(strings.Join(tooMany, ",")); err == nil {
		t.Fatal("oversized allowlist succeeded")
	}
}

func TestTenantMetricLabelerIsConcurrentReadSafe(t *testing.T) {
	labeler, err := NewTenantMetricLabeler([]string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if got := labeler.Label(fmt.Sprintf("tenant-%d", index)); got != MetricsTenantOtherLabel {
					t.Errorf("non-allowlisted label=%q", got)
					return
				}
			}
		}(index)
	}
	group.Wait()
}

func TestMetricLabelerRequiresExactScopedPairs(t *testing.T) {
	labeler, err := NewMetricLabeler(
		[]string{"tenant-a"},
		[]string{"tenant-a/support"},
		[]string{"tenant-a/gpt-4"},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		got      string
		expected string
	}{
		{name: "exact agent", got: labeler.scopedLabel("tenant-a", "support", labeler.allowedAgents), expected: "support"},
		{name: "rotated agent", got: labeler.scopedLabel("tenant-a", "support-v2", labeler.allowedAgents), expected: MetricsTenantOtherLabel},
		{name: "other tenant agent", got: labeler.scopedLabel("tenant-b", "support", labeler.allowedAgents), expected: MetricsTenantOtherLabel},
		{name: "exact model", got: labeler.scopedLabel("tenant-a", "gpt-4", labeler.allowedModels), expected: "gpt-4"},
		{name: "missing model", got: labeler.scopedLabel("tenant-a", "", labeler.allowedModels), expected: MetricsTenantUnknownLabel},
	} {
		if test.got != test.expected {
			t.Errorf("%s label=%q, want %q", test.name, test.got, test.expected)
		}
	}
}

func TestMetricLabelerRejectsInvalidOrUnboundedScopedConfiguration(t *testing.T) {
	for _, scopes := range []string{
		"tenant-a",
		"tenant-a/",
		"tenant-a/support/extra",
		"tenant-b/support",
		"tenant-a/support,tenant-a/support",
	} {
		if _, err := ParseMetricLabelAllowlists("tenant-a", scopes, ""); err == nil {
			t.Fatalf("agent scopes %q succeeded", scopes)
		}
	}
	tooMany := make([]string, 0, MaxMetricsScopedAllowlist+1)
	for index := 0; index <= MaxMetricsScopedAllowlist; index++ {
		tooMany = append(tooMany, fmt.Sprintf("tenant-a/agent-%d", index))
	}
	if _, err := ParseMetricLabelAllowlists("tenant-a", strings.Join(tooMany, ","), ""); err == nil {
		t.Fatal("oversized scoped allowlist succeeded")
	}
}

func TestCollectorMetricSeriesRemainBoundedAcrossNameRotation(t *testing.T) {
	const tenantID = "cardinality-regression-tenant"
	if err := ConfigureMetricLabels(tenantID, "", ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ConfigureMetricLabels("", "", ""); err != nil {
			t.Errorf("restore metric policy: %v", err)
		}
	}()

	collector := NewCollectorWithAuditSink(nil)
	for index := 0; index < MaxMetricsScopedAllowlist*2; index++ {
		collector.RecordRequestDuration(tenantID, fmt.Sprintf("agent-%d", index), time.Now())
		collector.RecordTokens(tenantID, fmt.Sprintf("model-%d", index), 1, 1)
	}

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, family := range families {
		if family.GetName() != "agent_request_duration_seconds" && family.GetName() != "agent_tokens_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, pair := range metric.Label {
				if pair.GetName() == "tenant_id" && pair.GetValue() == tenantID {
					counts[family.GetName()]++
					break
				}
			}
		}
	}
	if got := counts["agent_request_duration_seconds"]; got != 1 {
		t.Fatalf("request duration series=%d, want 1", got)
	}
	if got := counts["agent_tokens_total"]; got != 2 {
		t.Fatalf("token series=%d, want prompt and completion only", got)
	}
}
