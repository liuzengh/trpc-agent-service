//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func installPipelineSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	return recorder
}

func TestPipelineOperationSpansExcludeIdentityAndQueueIDs(t *testing.T) {
	recorder := installPipelineSpanRecorder(t)
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID:       "tenant-a",
		Status:   tenant.TenantStatusActive,
		Agents:   []tenant.AgentConfig{{Name: "assistant"}},
		Channels: []tenant.ChannelBinding{{AccountID: "corp-a", Type: string(channel.ChannelTypeWeWork)}},
	}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{Owner: "consumer-test"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &fakeAdapter{sent: make(chan *channel.OutboundMessage, 1)}
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, adapter)
	delivery, err := NewDelivery(store, tenantService, registry, DeliveryConfig{Owner: "delivery-test"})
	if err != nil {
		t.Fatal(err)
	}

	inbox := pipelineTestInbox()
	if inserted, err := store.EnqueueInbox(context.Background(), inbox); err != nil || !inserted {
		t.Fatalf("enqueue inbox: inserted=%v err=%v", inserted, err)
	}
	claimedInbox, err := store.ClaimInbox(context.Background(), "consumer-test", consumer.config.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claimedInbox)
	claimedOutbox, err := store.ClaimOutbox(context.Background(), "delivery-test", delivery.config.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	delivery.deliverOne(context.Background(), claimedOutbox)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	wantNames := []string{"inbox.process", "outbox.deliver"}
	for i, span := range spans {
		if span.Name() != wantNames[i] {
			t.Errorf("span %d name = %q, want %q", i, span.Name(), wantNames[i])
		}
		attributes := span.Attributes()
		if len(attributes) != 1 || attributes[0].Key != "error.code" || attributes[0].Value.AsString() != "none" {
			t.Errorf("span %d attributes = %#v, want only error.code=none", i, attributes)
		}
		dump := fmt.Sprintf("%+v", span)
		for _, forbidden := range []string{"tenant-a", "message-1", "hello", "agent reply"} {
			if strings.Contains(dump, forbidden) {
				t.Errorf("span %d leaked %q: %s", i, forbidden, dump)
			}
		}
	}
}
