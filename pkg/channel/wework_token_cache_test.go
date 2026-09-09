// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestWeWorkSendReplyEncodesIntegerAgentID(t *testing.T) {
	adapter := NewWeWorkAdapter()
	var encodedAgentID json.RawMessage
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/cgi-bin/gettoken":
			return weWorkJSONResponse(req, `{"errcode":0,"access_token":"token-A","expires_in":3600}`), nil
		case "/cgi-bin/message/send":
			var payload struct {
				AgentID json.RawMessage `json:"agentid"`
			}
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				return nil, fmt.Errorf("decode WeWork request: %w", err)
			}
			encodedAgentID = append(encodedAgentID[:0], payload.AgentID...)
			return weWorkJSONResponse(req, `{"errcode":0}`), nil
		default:
			return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
		}
	})

	if err := adapter.SendReply(context.Background(), weWorkBinding(1), &OutboundMessage{
		ConversationID: "user-1",
		Content:        "hello",
	}); err != nil {
		t.Fatalf("SendReply error = %v", err)
	}
	if got, want := string(encodedAgentID), "1000002"; got != want {
		t.Fatalf("encoded agentid = %s, want JSON integer %s", got, want)
	}
}

func TestWeWorkSendReplyScopesAccessTokensByCredential(t *testing.T) {
	adapter := NewWeWorkAdapter()
	tokenRequests := 0
	var sentTokens []string
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/cgi-bin/gettoken":
			tokenRequests++
			corpID := req.URL.Query().Get("corpid")
			token := map[string]string{
				"ww0000000000000001": "token-A",
				"ww0000000000000002": "token-B",
			}[corpID]
			return weWorkJSONResponse(req, fmt.Sprintf(
				`{"errcode":0,"access_token":%q,"expires_in":3600}`,
				token,
			)), nil
		case "/cgi-bin/message/send":
			sentTokens = append(sentTokens, req.URL.Query().Get("access_token"))
			return weWorkJSONResponse(req, `{"errcode":0}`), nil
		default:
			return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
		}
	})

	bindings := []*tenant.ChannelBinding{
		{
			AccountID: "shared-agent-id",
			AppID:     "1000002",
			Secret:    strings.Repeat("A", 43),
			Config:    map[string]string{"corp_id": "ww0000000000000001"},
		},
		{
			AccountID: "shared-agent-id",
			AppID:     "1000002",
			Secret:    strings.Repeat("B", 43),
			Config:    map[string]string{"corp_id": "ww0000000000000002"},
		},
	}
	for i, binding := range bindings {
		if err := adapter.SendReply(context.Background(), binding, &OutboundMessage{
			ConversationID: fmt.Sprintf("user-%d", i),
			Content:        "hello",
		}); err != nil {
			t.Fatalf("SendReply(%d) error = %v", i, err)
		}
	}

	if tokenRequests != 2 {
		t.Fatalf("token endpoint requests = %d, want 2 tenant-scoped requests", tokenRequests)
	}
	if got, want := strings.Join(sentTokens, ","), "token-A,token-B"; got != want {
		t.Fatalf("message access tokens = %q, want %q", got, want)
	}
}

func TestWeWorkSendReplyBoundsAccessTokenCache(t *testing.T) {
	const cacheCapacity = 1024
	adapter := NewWeWorkAdapter()
	tokenRequests := 0
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/cgi-bin/gettoken" {
			tokenRequests++
			return weWorkJSONResponse(req, fmt.Sprintf(
				`{"errcode":0,"access_token":"token-%d","expires_in":3600}`,
				tokenRequests,
			)), nil
		}
		if req.URL.Path == "/cgi-bin/message/send" {
			return weWorkJSONResponse(req, `{"errcode":0}`), nil
		}
		return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
	})

	first := weWorkBinding(0)
	for i := 0; i <= cacheCapacity; i++ {
		if err := adapter.SendReply(context.Background(), weWorkBinding(i), &OutboundMessage{
			ConversationID: "user",
			Content:        "hello",
		}); err != nil {
			t.Fatalf("SendReply(%d) error = %v", i, err)
		}
	}
	if err := adapter.SendReply(context.Background(), first, &OutboundMessage{
		ConversationID: "user",
		Content:        "hello",
	}); err != nil {
		t.Fatalf("SendReply(first after capacity) error = %v", err)
	}

	if want := cacheCapacity + 2; tokenRequests != want {
		t.Fatalf("token endpoint requests = %d, want %d after oldest entry eviction", tokenRequests, want)
	}
}

func TestWeWorkSendReplyRefreshesTokenAfterCredentialRotation(t *testing.T) {
	adapter := NewWeWorkAdapter()
	tokenRequests := 0
	var sentTokens []string
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/cgi-bin/gettoken":
			tokenRequests++
			token := "token-old"
			if req.URL.Query().Get("corpsecret") == strings.Repeat("N", 43) {
				token = "token-new"
			}
			return weWorkJSONResponse(req, fmt.Sprintf(
				`{"errcode":0,"access_token":%q,"expires_in":3600}`,
				token,
			)), nil
		case "/cgi-bin/message/send":
			sentTokens = append(sentTokens, req.URL.Query().Get("access_token"))
			return weWorkJSONResponse(req, `{"errcode":0}`), nil
		default:
			return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
		}
	})

	binding := &tenant.ChannelBinding{
		AccountID: "account-1",
		AppID:     "1000002",
		Secret:    strings.Repeat("O", 43),
		Config:    map[string]string{"corp_id": "ww0000000000000001"},
	}
	if err := adapter.SendReply(context.Background(), binding, &OutboundMessage{
		ConversationID: "user",
		Content:        "before rotation",
	}); err != nil {
		t.Fatalf("SendReply(before rotation) error = %v", err)
	}
	binding.Secret = strings.Repeat("N", 43)
	if err := adapter.SendReply(context.Background(), binding, &OutboundMessage{
		ConversationID: "user",
		Content:        "after rotation",
	}); err != nil {
		t.Fatalf("SendReply(after rotation) error = %v", err)
	}

	if tokenRequests != 2 {
		t.Fatalf("token endpoint requests = %d, want 2 after credential rotation", tokenRequests)
	}
	if got, want := strings.Join(sentTokens, ","), "token-old,token-new"; got != want {
		t.Fatalf("message access tokens = %q, want %q", got, want)
	}
}

func TestWeWorkSendReplyInvalidatesProviderExpiredToken(t *testing.T) {
	adapter := NewWeWorkAdapter()
	tokenRequests := 0
	sendRequests := 0
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/cgi-bin/gettoken":
			tokenRequests++
			return weWorkJSONResponse(req, fmt.Sprintf(
				`{"errcode":0,"access_token":"token-%d","expires_in":3600}`,
				tokenRequests,
			)), nil
		case "/cgi-bin/message/send":
			sendRequests++
			if sendRequests == 1 {
				return weWorkJSONResponse(req, `{"errcode":42001,"errmsg":"access_token expired"}`), nil
			}
			return weWorkJSONResponse(req, `{"errcode":0}`), nil
		default:
			return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
		}
	})

	binding := weWorkBinding(1)
	msg := &OutboundMessage{ConversationID: "user", Content: "hello"}
	if err := adapter.SendReply(context.Background(), binding, msg); err == nil {
		t.Fatal("first SendReply unexpectedly succeeded")
	}
	if err := adapter.SendReply(context.Background(), binding, msg); err != nil {
		t.Fatalf("second SendReply error = %v", err)
	}
	if tokenRequests != 2 {
		t.Fatalf("token endpoint requests = %d, want 2 after provider token expiry", tokenRequests)
	}
}

func TestWeWorkSendReplyRejectsMalformedSuccessResponse(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"errcode":"0"}`,
		`{"errcode":0} {"errcode":0}`,
		`{"errcode":0} trailing`,
	} {
		t.Run(body, func(t *testing.T) {
			adapter := NewWeWorkAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/cgi-bin/gettoken" {
					return weWorkJSONResponse(req, `{"errcode":0,"access_token":"token-valid","expires_in":3600}`), nil
				}
				if req.URL.Path == "/cgi-bin/message/send" {
					return weWorkJSONResponse(req, body), nil
				}
				return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
			})

			msg := &OutboundMessage{ConversationID: "user", Content: "hello"}
			err := adapter.SendReply(context.Background(), weWorkBinding(20), msg)
			if err == nil {
				t.Fatalf("malformed provider response %q was accepted", body)
			}
			if permanent, delay := DeliveryFailure(err); permanent || delay != 0 {
				t.Fatalf("malformed response classification permanent=%v delay=%s, want retryable default", permanent, delay)
			}
			if _, ok := msg.Metadata[deliveryNextKey]; ok {
				t.Fatalf("malformed response advanced delivery cursor: %+v", msg.Metadata)
			}
		})
	}
}

func TestWeWorkTokenEndpointRejectsMalformedSuccessResponse(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"errcode":0,"access_token":"token-valid","expires_in":3600} {}`,
	} {
		t.Run(body, func(t *testing.T) {
			adapter := NewWeWorkAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return weWorkJSONResponse(req, body), nil
			})
			err := adapter.SendReply(context.Background(), weWorkBinding(21), &OutboundMessage{
				ConversationID: "user",
				Content:        "hello",
			})
			if err == nil {
				t.Fatalf("malformed token response %q was accepted", body)
			}
			if permanent, delay := DeliveryFailure(err); permanent || delay != 0 {
				t.Fatalf("malformed token response classification permanent=%v delay=%s, want retryable default", permanent, delay)
			}
		})
	}
}

func weWorkBinding(index int) *tenant.ChannelBinding {
	return &tenant.ChannelBinding{
		AccountID: fmt.Sprintf("account-%d", index),
		AppID:     "1000002",
		Secret:    strings.Repeat("S", 43),
		Config: map[string]string{
			"corp_id": fmt.Sprintf("ww%016x", index),
		},
	}
}

func weWorkJSONResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
