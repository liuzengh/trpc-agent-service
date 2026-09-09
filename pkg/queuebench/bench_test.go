package queuebench

import (
	"context"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func TestConfigRejectsUnboundedOrDuplicateCases(t *testing.T) {
	cases := []Config{
		{MessagesPerTenant: 15, Consumers: []int{1}, MaxConnections: 1, MaxInflight: 1, Timeout: time.Second},
		{MessagesPerTenant: 16, Consumers: []int{1, 1}, MaxConnections: 1, MaxInflight: 1, Timeout: time.Second},
		{MessagesPerTenant: 16, Consumers: []int{1}, MaxConnections: 1, MaxInflight: 1, Timeout: 31 * time.Minute},
	}
	for _, config := range cases {
		if err := config.Validate(); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
}

func TestExecuteMemoryStoreVerifiesFiniteBacklog(t *testing.T) {
	config := DefaultConfig()
	config.MessagesPerTenant = 16
	config.Consumers = []int{2}
	config.WorkDuration = 0
	config.Timeout = 5 * time.Second
	store := reliable.NewMemoryStore()
	result, err := Execute(context.Background(), store, config,
		[]string{"queuebench-test-a", "queuebench-test-b"}, "weighted_hotspot", "fair", 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 32 || result.DuplicateEnqueues != 2 || result.DuplicateClaims != 0 {
		t.Fatalf("unexpected verified result: %#v", result)
	}
}
