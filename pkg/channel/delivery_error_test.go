//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDeliveryErrorClassification(t *testing.T) {
	permanent, delay := DeliveryFailure(PermanentDeliveryError(errors.New("bad recipient")))
	if !permanent || delay != 0 {
		t.Fatalf("permanent=%v delay=%s", permanent, delay)
	}
	permanent, delay = DeliveryFailure(RateLimitedDeliveryError(errors.New("429"), 7*time.Second))
	if permanent || delay != 7*time.Second {
		t.Fatalf("permanent=%v delay=%s", permanent, delay)
	}
}

func TestDeliveryErrorZeroValueIsSafe(t *testing.T) {
	zero := &DeliveryError{}
	if got := zero.Error(); got == "" {
		t.Fatal("zero-value DeliveryError must have a stable message")
	}
	if got := zero.Unwrap(); got != nil {
		t.Fatalf("zero-value DeliveryError unwrap=%v, want nil", got)
	}

	var typedNil *DeliveryError
	var err error = typedNil
	if got := err.Error(); got == "" {
		t.Fatal("typed-nil DeliveryError must have a stable message")
	}
	if permanent, delay := DeliveryFailure(err); !permanent || delay != 0 {
		t.Fatalf("typed-nil DeliveryError classification permanent=%v delay=%s, want permanent", permanent, delay)
	}
}

func TestWeWorkStreamingUnsupportedIsPermanent(t *testing.T) {
	err := NewWeWorkAdapter().SendStreamChunk(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("unsupported streaming must return an error")
	}
	if !errors.Is(err, ErrStreamingUnsupported) {
		t.Fatalf("error=%v, want ErrStreamingUnsupported", err)
	}
	if permanent, delay := DeliveryFailure(err); !permanent || delay != 0 {
		t.Fatalf("unsupported streaming classification permanent=%v delay=%s, want permanent", permanent, delay)
	}
}

func TestOutboundDeliveryProgress(t *testing.T) {
	msg := &OutboundMessage{Metadata: map[string]string{"delivery_cursor": "2"}}
	cursor, err := OutboundDeliveryCursor(msg)
	if err != nil || cursor != 2 {
		t.Fatalf("cursor=%d err=%v", cursor, err)
	}
	SetOutboundDeliveryProgress(msg, 3, false)
	next, complete, err := OutboundDeliveryProgress(msg)
	if err != nil || next != 3 || complete {
		t.Fatalf("next=%d complete=%v err=%v", next, complete, err)
	}
}
