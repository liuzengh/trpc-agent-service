//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestTelegramParseInboundSeparatesDedupAndReplyIDs(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{
		"update_id": 99123,
		"message": {
			"message_id": 42,
			"from": {"id": 7, "username": "alice"},
			"chat": {"id": -1009, "type": "supergroup"},
			"date": 1700000000,
			"text": "hello"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := NewTelegramAdapter().ParseInbound(req, &tenant.ChannelBinding{})
	if err != nil {
		t.Fatal(err)
	}
	if inbound.MessageID != "99123" {
		t.Fatalf("dedup ID = %q, want Telegram update_id", inbound.MessageID)
	}
	if inbound.ReplyToID != "42" {
		t.Fatalf("reply message ID = %q, want per-chat message_id", inbound.ReplyToID)
	}
	if !inbound.IsGroupChat || inbound.ConversationID != "-1009" {
		t.Fatalf("unexpected chat mapping: %#v", inbound)
	}
}

func TestTelegramParseInboundRejectsMultipleJSONValues(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{"update_id":1}{"update_id":2}`))
	if _, err := NewTelegramAdapter().ParseInbound(req, &tenant.ChannelBinding{}); err == nil {
		t.Fatal("multiple JSON values accepted")
	}
}

func TestTelegramParseInboundIgnoresUnsupportedUpdates(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"membership update", `{"update_id":1,"my_chat_member":{"chat":{"id":7}}}`},
		{"callback query", `{"update_id":2,"callback_query":{"id":"callback"}}`},
		{"non-text message", `{"update_id":3,"message":{"message_id":4,"from":{"id":7},"chat":{"id":9,"type":"private"},"date":1700000000,"photo":[{"file_id":"photo"}]}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(test.body))
			msg, err := NewTelegramAdapter().ParseInbound(req, &tenant.ChannelBinding{})
			if !errors.Is(err, ErrIgnoredInbound) {
				t.Fatalf("ParseInbound error = %v, want ErrIgnoredInbound", err)
			}
			if msg != nil {
				t.Fatalf("ignored update returned message: %#v", msg)
			}
		})
	}
}
