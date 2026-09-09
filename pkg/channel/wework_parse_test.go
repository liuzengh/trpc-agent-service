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

func TestWeWorkParseInboundAcceptsOnlyRoutableTextMessages(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantIgnored bool
		wantErr     string
	}{
		{
			name: "text",
			body: `<xml><FromUserName><![CDATA[user-1]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hello]]></Content><MsgId>42</MsgId><AgentID>7</AgentID></xml>`,
		},
		{
			name:        "image",
			body:        `<xml><FromUserName><![CDATA[user-1]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[image]]></MsgType><MsgId>42</MsgId></xml>`,
			wantIgnored: true,
		},
		{
			name:        "event",
			body:        `<xml><FromUserName><![CDATA[user-1]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[subscribe]]></Event><MsgId>42</MsgId></xml>`,
			wantIgnored: true,
		},
		{
			name:        "empty text",
			body:        `<xml><FromUserName><![CDATA[user-1]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[]]></Content><MsgId>42</MsgId></xml>`,
			wantIgnored: true,
		},
		{
			name:    "missing sender",
			body:    `<xml><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hello]]></Content><MsgId>42</MsgId></xml>`,
			wantErr: "required",
		},
		{
			name:    "missing message id",
			body:    `<xml><FromUserName><![CDATA[user-1]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hello]]></Content></xml>`,
			wantErr: "required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(test.body))
			msg, err := NewWeWorkAdapter().ParseInbound(req, &tenant.ChannelBinding{})
			if test.wantIgnored {
				if !errors.Is(err, ErrIgnoredInbound) {
					t.Fatalf("ParseInbound error=%v, want ErrIgnoredInbound", err)
				}
				if msg != nil {
					t.Fatalf("ignored event returned message: %#v", msg)
				}
				return
			}
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ParseInbound error=%v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil || msg.Content != "hello" || msg.ExternalUserID != "user-1" || msg.MessageID != "42" {
				t.Fatalf("unexpected parsed message: %#v", msg)
			}
		})
	}
}
