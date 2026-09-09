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
	"fmt"
	"net/http"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestTelegramDeliveryRejectsMalformedCredentialBeforeTransport(t *testing.T) {
	credential := "123456789:bad\ntoken"
	adapter := NewTelegramAdapter()
	called := false
	adapter.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("transport must not be called")
	})
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: credential}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
	})
	if !errors.Is(err, ErrChannelCredentialInvalid) {
		t.Fatalf("malformed credential error=%v", err)
	}
	if called || strings.Contains(err.Error(), credential) {
		t.Fatalf("malformed credential reached transport or leaked: called=%v err=%v", called, err)
	}
}

func TestTelegramTransportErrorCannotLeakCredentialURL(t *testing.T) {
	token := "123456789:" + strings.Repeat("A", 35)
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial failed for %s", req.URL.String())
	})
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: token}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
	})
	if !errors.Is(err, ErrChannelTransport) {
		t.Fatalf("transport error class=%v", err)
	}
	if strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "api.telegram.org/bot") {
		t.Fatalf("credential URL leaked through transport error: %v", err)
	}
}

func TestWeWorkDeliveryRejectsMalformedCredentialBeforeTransport(t *testing.T) {
	credential := "corp-secret\r\n"
	adapter := NewWeWorkAdapter()
	called := false
	adapter.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("transport must not be called")
	})
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{
		AppID:  "1000002",
		Secret: credential,
		Config: map[string]string{"corp_id": "ww0123456789abcdef"},
	}, &OutboundMessage{ConversationID: "user-1", Content: "hello"})
	if !errors.Is(err, ErrChannelCredentialInvalid) {
		t.Fatalf("malformed credential error=%v", err)
	}
	if called || strings.Contains(err.Error(), credential) {
		t.Fatalf("malformed credential reached transport or leaked: called=%v err=%v", called, err)
	}
}

func TestWeWorkTransportErrorCannotLeakCredentialURL(t *testing.T) {
	secret := strings.Repeat("S", 43)
	adapter := NewWeWorkAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial failed for %s", req.URL.String())
	})
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{
		AppID:  "1000002",
		Secret: secret,
		Config: map[string]string{"corp_id": "ww0123456789abcdef"},
	}, &OutboundMessage{ConversationID: "user-1", Content: "hello"})
	if !errors.Is(err, ErrChannelTransport) {
		t.Fatalf("transport error class=%v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "corpsecret") {
		t.Fatalf("credential URL leaked through transport error: %v", err)
	}
}

func TestAdaptersRejectNilOrMalformedOutboundMessages(t *testing.T) {
	telegram := NewTelegramAdapter()
	if err := telegram.SendReply(context.Background(), &tenant.ChannelBinding{Token: "123456789:" + strings.Repeat("A", 35)}, nil); !errors.Is(err, ErrInvalidOutboundMessage) {
		t.Fatalf("Telegram nil message error=%v, want ErrInvalidOutboundMessage", err)
	}
	wework := NewWeWorkAdapter()
	if err := wework.SendReply(context.Background(), &tenant.ChannelBinding{
		AppID: "1000002", Secret: strings.Repeat("S", 43),
		Config: map[string]string{"corp_id": "ww0123456789abcdef"},
	}, nil); !errors.Is(err, ErrInvalidOutboundMessage) {
		t.Fatalf("WeWork nil message error=%v, want ErrInvalidOutboundMessage", err)
	}
}

func TestZeroValueAdaptersFailClosedWithoutPanic(t *testing.T) {
	telegram := &TelegramAdapter{}
	err := telegram.SendReply(context.Background(), &tenant.ChannelBinding{Token: "123456789:" + strings.Repeat("A", 35)}, &OutboundMessage{ConversationID: "chat", Content: "hello"})
	if !errors.Is(err, ErrChannelTransport) {
		t.Fatalf("zero-value Telegram error=%v, want ErrChannelTransport", err)
	}
	wework := &WeWorkAdapter{}
	err = wework.SendReply(context.Background(), &tenant.ChannelBinding{
		AppID: "1000002", Secret: strings.Repeat("S", 43),
		Config: map[string]string{"corp_id": "ww0123456789abcdef"},
	}, &OutboundMessage{ConversationID: "user", Content: "hello"})
	if !errors.Is(err, ErrChannelTransport) {
		t.Fatalf("zero-value WeWork error=%v, want ErrChannelTransport", err)
	}
}
