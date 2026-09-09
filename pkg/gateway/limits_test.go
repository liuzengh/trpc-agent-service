//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
)

type trackingBody struct {
	io.Reader
	closed bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

func TestReadLimitedBodyClosesConsumedBody(t *testing.T) {
	body := &trackingBody{Reader: strings.NewReader(`{"ok":true}`)}
	r := httptest.NewRequest(http.MethodPost, "/webhook", body)
	if _, err := readLimitedBody(r, MaxWebhookBodyBytes); err != nil {
		t.Fatalf("readLimitedBody returned error: %v", err)
	}
	if !body.closed {
		t.Fatal("consumed request body was not closed")
	}
}

func TestReadLimitedBody_WithinLimit(t *testing.T) {
	body := `{"text":"hello"}`
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))

	got, err := readLimitedBody(r, MaxWebhookBodyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != body {
		t.Fatalf("body mismatch: got %q want %q", got, body)
	}

	// Body must be rewound for downstream adapters.
	again, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("unexpected error re-reading body: %v", err)
	}
	if string(again) != body {
		t.Fatalf("body was not rewound: got %q want %q", again, body)
	}
}

func TestReadLimitedBody_RejectsOversizeContentLength(t *testing.T) {
	payload := strings.Repeat("a", 128)
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(payload))

	_, err := readLimitedBody(r, 64)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge, got %v", err)
	}
}

// A hostile client can lie about or omit Content-Length; the reader must still cap.
func TestReadLimitedBody_RejectsOversizeWithoutContentLength(t *testing.T) {
	payload := strings.Repeat("a", 4096)
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(payload))
	r.ContentLength = -1

	_, err := readLimitedBody(r, 64)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge for unknown content-length, got %v", err)
	}
}

func TestReadLimitedBody_AcceptsExactlyAtLimit(t *testing.T) {
	payload := strings.Repeat("a", 64)
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(payload))
	r.ContentLength = -1

	got, err := readLimitedBody(r, 64)
	if err != nil {
		t.Fatalf("payload exactly at limit must be accepted, got %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("got %d bytes, want 64", len(got))
	}
}

func TestReadLimitedBody_RejectsEmptyBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(""))

	_, err := readLimitedBody(r, MaxWebhookBodyBytes)
	if !errors.Is(err, ErrEmptyBody) {
		t.Fatalf("expected ErrEmptyBody, got %v", err)
	}
}

func TestValidatePayload_RejectsDeepNesting(t *testing.T) {
	depth := MaxJSONDepth + 10
	payload := []byte(strings.Repeat("[", depth) + strings.Repeat("]", depth))

	if err := validatePayload(payload, MaxJSONDepth); !errors.Is(err, ErrJSONTooDeep) {
		t.Fatalf("expected ErrJSONTooDeep, got %v", err)
	}
}

func TestValidatePayload_AcceptsShallowNesting(t *testing.T) {
	payload := []byte(`{"message":{"chat":{"id":1},"text":"hi"}}`)

	if err := validatePayload(payload, MaxJSONDepth); err != nil {
		t.Fatalf("shallow payload must be accepted, got %v", err)
	}
}

func TestValidatePayload_RejectsMultipleTopLevelJSONValues(t *testing.T) {
	for _, payload := range []string{
		`{} []`,
		`{"a":1}{"b":2}`,
		`[] true`,
	} {
		if err := validatePayload([]byte(payload), MaxJSONDepth); err == nil {
			t.Errorf("multiple top-level JSON values %q were accepted", payload)
		}
	}
}

func TestValidatePayload_RejectsMalformedJSONScalarValues(t *testing.T) {
	for _, payload := range []string{
		`123 garbage`,
		`"text" garbage`,
		`null false`,
	} {
		if err := validatePayload([]byte(payload), MaxJSONDepth); err == nil {
			t.Errorf("malformed scalar JSON %q was accepted", payload)
		}
	}
}

func TestValidatePayload_RejectsInvalidUTF8(t *testing.T) {
	payload := []byte{'{', '"', 'a', '"', ':', '"', 0xff, 0xfe, '"', '}'}

	if err := validatePayload(payload, MaxJSONDepth); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("expected ErrInvalidUTF8, got %v", err)
	}
}

func TestValidatePayload_RejectsMalformedJSON(t *testing.T) {
	for _, tc := range []string{
		`{"unterminated": `,
		`{"a":1,}`,
		`[1,2,`,
		`{"a" 1}`,
	} {
		if err := validatePayload([]byte(tc), MaxJSONDepth); err == nil {
			t.Errorf("malformed json %q must be rejected", tc)
		}
	}
}

// WeCom posts XML, which must pass content validation untouched.
func TestValidatePayload_AllowsNonJSONBody(t *testing.T) {
	payload := []byte(`<xml><Encrypt>abc</Encrypt></xml>`)

	if err := validatePayload(payload, MaxJSONDepth); err != nil {
		t.Fatalf("xml body must be accepted, got %v", err)
	}
}

func TestValidateInboundMessageRejectsUnsafeAttachments(t *testing.T) {
	base := &channel.InboundMessage{
		ExternalUserID: "user-1",
		ConversationID: "conversation-1",
		Content:        "hello",
		Attachments: []channel.Attachment{{
			Type:     "image",
			URL:      "https://cdn.example.test/image.png",
			MimeType: "image/png",
		}},
	}
	if err := validateInboundMessage(base); err != nil {
		t.Fatalf("valid attachment rejected: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*channel.Attachment)
	}{
		{name: "unknown type", edit: func(a *channel.Attachment) { a.Type = "script" }},
		{name: "missing URL", edit: func(a *channel.Attachment) { a.URL = "" }},
		{name: "invalid URL bytes", edit: func(a *channel.Attachment) { a.URL = string([]byte{'h', 't', 't', 'p', ':', '/', '/', 0xff}) }},
		{name: "control MIME", edit: func(a *channel.Attachment) { a.MimeType = "image/png\nX-Header: value" }},
		{name: "negative size", edit: func(a *channel.Attachment) { a.Size = -1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := *base
			attachment := base.Attachments[0]
			test.edit(&attachment)
			msg.Attachments = []channel.Attachment{attachment}
			if err := validateInboundMessage(&msg); err == nil {
				t.Fatalf("unsafe attachment was accepted: %+v", attachment)
			}
		})
	}
}

// FuzzValidatePayload asserts the validators never panic on arbitrary input and
// that an accepted payload is always valid UTF-8 within the depth bound.
func FuzzValidatePayload(f *testing.F) {
	f.Add([]byte(`{"text":"hello"}`))
	f.Add([]byte(`[[[[[[]]]]]]`))
	f.Add([]byte(`<xml><Encrypt>a</Encrypt></xml>`))
	f.Add([]byte{0xff, 0xfe})
	f.Add([]byte(``))
	f.Add([]byte(`{"a":`))

	f.Fuzz(func(t *testing.T, payload []byte) {
		err := validatePayload(payload, MaxJSONDepth)
		if err != nil {
			return
		}
		if !utf8.Valid(payload) {
			t.Fatalf("accepted invalid utf-8 payload: %q", payload)
		}
	})
}

// FuzzReadLimitedBody asserts the size cap holds for arbitrary bodies.
func FuzzReadLimitedBody(f *testing.F) {
	f.Add([]byte(`{"text":"hi"}`), int64(64))
	f.Add([]byte(``), int64(64))
	f.Add(bytes.Repeat([]byte("a"), 200), int64(64))

	f.Fuzz(func(t *testing.T, payload []byte, limit int64) {
		if limit <= 0 || limit > 1<<20 {
			t.Skip()
		}
		r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		r.ContentLength = -1

		got, err := readLimitedBody(r, limit)
		if err != nil {
			return
		}
		if int64(len(got)) > limit {
			t.Fatalf("accepted %d bytes over limit %d", len(got), limit)
		}
		if len(got) == 0 {
			t.Fatal("accepted an empty body")
		}
	})
}

func TestRateLimiter_DuplicateSourceDoesNotConsumeQuota(t *testing.T) {
	limiter := NewRateLimiter(newGatewayRedis(t))
	if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", "telegram\x00account\x00message-1"); err != nil || !allowed {
		t.Fatalf("first source allowed=%v err=%v", allowed, err)
	}
	for i := 0; i < 25; i++ {
		allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", "telegram\x00account\x00message-1")
		if err != nil || !allowed {
			t.Fatalf("duplicate %d allowed=%v err=%v", i, allowed, err)
		}
	}
	for i := 2; i <= rateLimitPerMinute; i++ {
		allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", fmt.Sprintf("telegram\x00account\x00message-%d", i))
		if err != nil || !allowed {
			t.Fatalf("unique source %d allowed=%v err=%v", i, allowed, err)
		}
	}
	allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", "telegram\x00account\x00message-over-limit")
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("new source bypassed the per-user limit after duplicate redeliveries")
	}
}

func TestRateLimiter_RejectedSourceCannotBypassQuota(t *testing.T) {
	limiter := NewRateLimiter(newGatewayRedis(t))
	for i := 0; i < rateLimitPerMinute; i++ {
		if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", fmt.Sprintf("source-%d", i)); err != nil || !allowed {
			t.Fatalf("source %d allowed=%v err=%v", i, allowed, err)
		}
	}
	const rejected = "source-rejected"
	if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", rejected); err != nil || allowed {
		t.Fatalf("over-limit source allowed=%v err=%v", allowed, err)
	}
	if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", rejected); err != nil || allowed {
		t.Fatalf("replayed rejected source allowed=%v err=%v", allowed, err)
	}
}

func TestRateLimiter_RejectsWithoutGrowingCounter(t *testing.T) {
	redisClient := newGatewayRedis(t)
	limiter := NewRateLimiter(redisClient)
	for i := 0; i < rateLimitPerMinute; i++ {
		if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", fmt.Sprintf("source-%d", i)); err != nil || !allowed {
			t.Fatalf("source %d allowed=%v err=%v", i, allowed, err)
		}
	}
	key := rateLimitHashKey("ratelimit", "tenant-a", "user-a")
	before, err := redisClient.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatalf("read counter before rejection: %v", err)
	}
	for i := 0; i < 100; i++ {
		if allowed, err := limiter.AllowSource(context.Background(), "tenant-a", "user-a", fmt.Sprintf("rejected-%d", i)); err != nil || allowed {
			t.Fatalf("rejected source %d allowed=%v err=%v", i, allowed, err)
		}
	}
	after, err := redisClient.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatalf("read counter after rejection: %v", err)
	}
	if after != before || after != rateLimitPerMinute {
		t.Fatalf("rejected requests changed counter: before=%d after=%d", before, after)
	}
}

func TestRateLimiter_SourceMarkerCannotOutliveCounterWindow(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	limiter := NewRateLimiter(redisClient)
	// The second source arrives near the end of the first fixed window. Its
	// marker must expire with the counter, rather than receiving a fresh full
	// minute and surviving into the next window.
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-first"); err != nil || !decision.allowed || !decision.newSource {
		t.Fatalf("first source decision=%+v err=%v", decision, err)
	}
	mr.FastForward(50 * time.Second)
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-late"); err != nil || !decision.allowed || !decision.newSource {
		t.Fatalf("late source decision=%+v err=%v", decision, err)
	}

	// The counter's original 60-second window has now elapsed. A new source
	// starts the next window, and the late source must be charged again because
	// its marker belonged to the expired window.
	mr.FastForward(12 * time.Second)
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-new-window"); err != nil || !decision.allowed || !decision.newSource {
		t.Fatalf("new-window source decision=%+v err=%v", decision, err)
	}
	decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-late")
	if err != nil {
		t.Fatalf("replayed late source returned error: %v", err)
	}
	if !decision.allowed || !decision.newSource {
		t.Fatalf("late source marker survived counter window: decision=%+v", decision)
	}

	key := rateLimitHashKey("ratelimit", "tenant-a", "user-a")
	count, err := redisClient.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatalf("read next-window counter: %v", err)
	}
	if count != 2 {
		t.Fatalf("next-window counter=%d, want 2 after charging replayed source", count)
	}
}

func TestRateLimiter_TenantLimitAppliesAcrossUsers(t *testing.T) {
	limiter := NewRateLimiterWithTenantLimit(newGatewayRedis(t), 2)
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-a"); err != nil || !decision.allowed || !decision.newSource {
		t.Fatalf("first tenant source decision=%+v err=%v", decision, err)
	}
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-b", "source-b"); err != nil || !decision.allowed || !decision.newSource {
		t.Fatalf("second tenant source decision=%+v err=%v", decision, err)
	}
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-c", "source-c"); err != nil || decision.allowed {
		t.Fatalf("third tenant source decision=%+v err=%v, want tenant cap rejection", decision, err)
	}
	if decision, err := limiter.allowSource(context.Background(), "tenant-a", "user-a", "source-a"); err != nil || !decision.allowed || decision.newSource {
		t.Fatalf("duplicate source decision=%+v err=%v, want free redelivery", decision, err)
	}
}
