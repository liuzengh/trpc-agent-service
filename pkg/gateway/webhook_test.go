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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const testWebhookToken = "wh-token-1"

func newGatewayRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client
}

// stubTenantService resolves a single fixed tenant by webhook token.
type stubTenantService struct {
	tenant.Service
	tenant *tenant.Tenant
}

type legacyTenantService struct{ tenant.Service }

func (s *stubTenantService) GetTenantByWebhookToken(_ context.Context, token string) (*tenant.Tenant, error) {
	if s.tenant != nil && token == testWebhookToken {
		return s.tenant, nil
	}
	return nil, fmt.Errorf("tenant not found for token")
}

func (s *stubTenantService) GetTenantByWebhookTokenScoped(ctx context.Context, token string) (*tenant.Tenant, error) {
	return s.GetTenantByWebhookToken(ctx, token)
}

func TestResolveWebhookTenantFailsClosedWithoutScopedReader(t *testing.T) {
	_, err := resolveWebhookTenant(context.Background(), &legacyTenantService{}, testWebhookToken)
	if !errors.Is(err, tenant.ErrScopedTenantReadUnavailable) {
		t.Fatalf("error=%v, want ErrScopedTenantReadUnavailable", err)
	}
}

// recordingAdapter captures the payload each stage observed so a test can assert
// that signature verification, parsing and dedup all saw the same bytes.
type recordingAdapter struct {
	mu           sync.Mutex
	verifiedBody string
	parsedBody   string
	content      string
	messageID    string
	verifyErr    error
	parseErr     error
	returnNil    bool
	replies      []string
}

type recordingAuditLogger struct {
	mu      sync.Mutex
	entries []telemetry.AuditLog
	err     error
}

type rejectingInboxStore struct {
	reliable.Store
	err   error
	calls int
}

func (s *rejectingInboxStore) EnqueueInbox(context.Context, *reliable.InboxMessage) (bool, error) {
	s.calls++
	return false, s.err
}

func (l *recordingAuditLogger) LogAudit(_ context.Context, entry *telemetry.AuditLog) error {
	if entry != nil {
		l.mu.Lock()
		l.entries = append(l.entries, *entry)
		l.mu.Unlock()
	}
	return l.err
}

func (a *recordingAdapter) VerifySignature(req *http.Request, _ *tenant.ChannelBinding) error {
	body, _ := io.ReadAll(req.Body)
	a.mu.Lock()
	a.verifiedBody = string(body)
	a.mu.Unlock()
	return a.verifyErr
}

func (a *recordingAdapter) ParseInbound(req *http.Request, _ *tenant.ChannelBinding) (*channel.InboundMessage, error) {
	// Mirrors the Telegram adapter: decode straight from the body without rewinding.
	var payload struct {
		Text string `json:"text"`
	}
	body, _ := io.ReadAll(req.Body)
	a.mu.Lock()
	a.parsedBody = string(body)
	a.mu.Unlock()

	if a.parseErr != nil {
		return nil, a.parseErr
	}
	if a.returnNil {
		return nil, nil
	}
	_ = json.Unmarshal(body, &payload)

	content := payload.Text
	if a.content != "" {
		content = a.content
	}
	msgID := a.messageID
	if msgID == "" {
		msgID = "msg-1"
	}
	return &channel.InboundMessage{
		ChannelType:    string(channel.ChannelTypeTelegram),
		ExternalUserID: "user-1",
		ConversationID: "conv-1",
		MessageID:      msgID,
		Content:        content,
		Timestamp:      time.Now(),
	}, nil
}

func (a *recordingAdapter) SendReply(_ context.Context, _ *tenant.ChannelBinding, msg *channel.OutboundMessage) error {
	a.mu.Lock()
	a.replies = append(a.replies, msg.Content)
	a.mu.Unlock()
	return nil
}

func (a *recordingAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *channel.StreamChunk) error {
	return nil
}
func (a *recordingAdapter) SupportsStreaming() bool             { return false }
func (a *recordingAdapter) HandleRateLimit(error) time.Duration { return 0 }

func newTestServer(t *testing.T, adapter *recordingAdapter) (*Server, *reliable.MemoryStore) {
	t.Helper()

	tn := &tenant.Tenant{
		ID:     "tenant-1",
		Name:   "test",
		Status: tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{{
			Type:       string(channel.ChannelTypeTelegram),
			Token:      testWebhookToken,
			WebhookKey: testWebhookToken,
			AgentApp:   "support",
			AccessPolicy: tenant.ChannelAccessPolicy{
				AllowDirectMessages: true,
				AllowedUsers:        []string{"user-1"},
			},
		}},
	}

	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeTelegram, adapter)

	redisClient := newGatewayRedis(t)
	store := reliable.NewMemoryStore()
	audit := &recordingAuditLogger{}
	return NewDurableServer(
		&stubTenantService{tenant: tn},
		registry,
		redisClient,
		health.New(health.WithRedis(redisClient)),
		store,
		audit,
	), store
}

func TestHandleWebhookAcknowledgesUnauthorizedIdentityWithoutEnqueue(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)
	resolved := s.tenantService.(*stubTenantService).tenant
	resolved.Channels[0].AccessPolicy.AllowedUsers = []string{"different-user"}

	response := postWebhook(t, s, `{"text":"hello"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("unauthorized identity status=%d, want provider acknowledgement 200", response.Code)
	}
	requireNoInbox(t, store)
	audit := s.audit.(*recordingAuditLogger)
	audit.mu.Lock()
	defer audit.mu.Unlock()
	if len(audit.entries) != 1 {
		t.Fatalf("identity denial audit entries=%d, want 1", len(audit.entries))
	}
	entry := audit.entries[0]
	if entry.TenantID != "tenant-1" || entry.ChannelType != "telegram" ||
		entry.UserID != "user-1" || entry.AgentName != "support" ||
		entry.Decision != "deny" || entry.ErrorType != "im_identity_denied" ||
		entry.SessionID == "" {
		t.Fatalf("identity denial audit entry=%+v", entry)
	}
}

func TestHandleWebhookLimitsUnauthorizedIdentityAndCoalescesRedeliveryAudit(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)
	resolved := s.tenantService.(*stubTenantService).tenant
	resolved.Channels[0].AccessPolicy.AllowedUsers = []string{"different-user"}
	audit := s.audit.(*recordingAuditLogger)

	// The same valid provider event remains acknowledgement-safe but produces
	// one durable denial audit and consumes one quota token.
	for attempt := 0; attempt < rateLimitPerMinute+5; attempt++ {
		if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusOK {
			t.Fatalf("duplicate unauthorized delivery %d status=%d, want 200", attempt, got)
		}
	}
	audit.mu.Lock()
	if got := len(audit.entries); got != 1 {
		audit.mu.Unlock()
		t.Fatalf("duplicate unauthorized delivery audits=%d, want 1", got)
	}
	audit.mu.Unlock()

	// New authenticated sources from the same unauthorized identity are capped
	// before authorization/auditing, so they cannot turn the callback path into
	// an unbounded synchronous audit writer.
	for source := 2; source <= rateLimitPerMinute; source++ {
		adapter.mu.Lock()
		adapter.messageID = fmt.Sprintf("unauthorized-%d", source)
		adapter.mu.Unlock()
		if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusOK {
			t.Fatalf("unauthorized source %d status=%d, want 200", source, got)
		}
	}
	adapter.mu.Lock()
	adapter.messageID = "unauthorized-over-limit"
	adapter.mu.Unlock()
	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusTooManyRequests {
		t.Fatalf("over-limit unauthorized source status=%d, want 429", got)
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	if got := len(audit.entries); got != rateLimitPerMinute {
		t.Fatalf("audits after over-limit source=%d, want %d", got, rateLimitPerMinute)
	}
	requireNoInbox(t, store)
}

func claimInbox(t *testing.T, store reliable.Store) *reliable.InboxMessage {
	t.Helper()
	msg, err := store.ClaimInbox(context.Background(), "test-consumer", time.Minute)
	if err != nil {
		t.Fatalf("claim Inbox: %v", err)
	}
	return msg
}

func requireNoInbox(t *testing.T, store reliable.Store) {
	t.Helper()
	msg, err := store.ClaimInbox(context.Background(), "test-consumer", time.Minute)
	if errors.Is(err, reliable.ErrNoWork) {
		return
	}
	if err != nil {
		t.Fatalf("claim Inbox while checking absence: %v", err)
	}
	if msg != nil {
		t.Fatalf("unexpected Inbox message: %+v", msg)
	}
}

func postWebhook(t *testing.T, s *Server, body string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/webhook?token="+testWebhookToken, strings.NewReader(body))
	if mutate != nil {
		mutate(r)
	}
	w := httptest.NewRecorder()
	s.HandleWebhook(w, r)
	return w
}

// Regression: the dedup payload hash used to be computed after ParseInbound had
// already drained the body, so every message hashed the empty string and the
// payload-conflict check could never fire.
func TestHandleWebhook_DedupSeesRealPayload(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)

	body := `{"text":"hello"}`
	if got := postWebhook(t, s, body, nil).Code; got != http.StatusOK {
		t.Fatalf("got status %d, want 200", got)
	}

	adapter.mu.Lock()
	verified, parsed := adapter.verifiedBody, adapter.parsedBody
	adapter.mu.Unlock()

	if verified != body {
		t.Errorf("VerifySignature saw %q, want %q", verified, body)
	}
	if parsed != body {
		t.Errorf("ParseInbound saw %q, want %q", parsed, body)
	}

	stored := claimInbox(t, store)
	if stored == nil {
		t.Fatal("durable Inbox row was not created")
	}
	if want := payloadHashForTest([]byte(body)); stored.PayloadHash != want {
		t.Fatalf("stored hash %s, want hash of real payload %s", stored.PayloadHash, want)
	}
	if stored.PayloadHash == payloadHashForTest(nil) {
		t.Fatal("dedup stored the hash of an empty payload")
	}
	if stored.ReplyToID != stored.ExternalMessageID {
		t.Fatalf("fallback reply target=%q, want authoritative external ID %q", stored.ReplyToID, stored.ExternalMessageID)
	}
}

func TestHandleWebhookAcknowledgesIgnoredProviderUpdateWithoutEnqueue(t *testing.T) {
	adapter := &recordingAdapter{parseErr: channel.ErrIgnoredInbound}
	s, store := newTestServer(t, adapter)

	response := postWebhook(t, s, `{"my_chat_member":{"chat":{"id":7}}}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("ignored update status = %d, want 200", response.Code)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhookRejectsNilAdapterMessageWithoutEnqueue(t *testing.T) {
	adapter := &recordingAdapter{returnNil: true}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusBadRequest {
		t.Fatalf("nil adapter message status=%d, want 400", got)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhookRejectsUnsafeMessageIDBeforeRateLimit(t *testing.T) {
	adapter := &recordingAdapter{messageID: "msg\x00unsafe"}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusBadRequest {
		t.Fatalf("unsafe message ID status=%d, want 400", got)
	}
	requireNoInbox(t, store)
}

func payloadHashForTest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// Same message_id with a different payload must be treated as a conflict rather
// than silently deduplicated.
func TestHandleWebhook_PayloadConflictIsNotDeduplicated(t *testing.T) {
	adapter := &recordingAdapter{messageID: "msg-replay"}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusOK {
		t.Fatalf("initial payload status=%d", got)
	}
	if got := postWebhook(t, s, `{"text":"transfer all funds"}`, nil).Code; got != http.StatusConflict {
		t.Fatalf("conflicting payload status=%d, want 409", got)
	}
	if msg := claimInbox(t, store); msg == nil {
		t.Fatal("original Inbox row missing")
	}
}

func TestHandleWebhook_DoesNotLogExternalMessageID(t *testing.T) {
	adapter := &recordingAdapter{messageID: "provider-secret-message-id"}
	s, _ := newTestServer(t, adapter)
	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusOK {
		t.Fatalf("initial payload status=%d", got)
	}

	var logs bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	if got := postWebhook(t, s, `{"text":"changed"}`, nil).Code; got != http.StatusConflict {
		t.Fatalf("conflicting payload status=%d", got)
	}
	if strings.Contains(logs.String(), adapter.messageID) {
		t.Fatalf("raw external message ID appeared in gateway logs: %q", logs.String())
	}
}

func TestHandleWebhook_DoesNotLogCustomProviderError(t *testing.T) {
	providerError := errors.New("dial postgres://db-user:db-password@db.internal/agent?token=secret-token")
	adapter := &recordingAdapter{verifyErr: providerError}
	s, _ := newTestServer(t, adapter)

	var logs bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})

	if got := postWebhook(t, s, `{"text":"hello"}`, nil).Code; got != http.StatusUnauthorized {
		t.Fatalf("provider verification error status=%d, want 401", got)
	}
	if strings.Contains(logs.String(), providerError.Error()) || strings.Contains(logs.String(), "db-password") || strings.Contains(logs.String(), "secret-token") {
		t.Fatalf("provider error leaked into gateway logs: %q", logs.String())
	}
	if !strings.Contains(logs.String(), "error=internal_error") {
		t.Fatalf("gateway did not record a stable error category: %q", logs.String())
	}
}

func TestHandleWebhook_DuplicateRedeliveryDoesNotConsumeUserQuota(t *testing.T) {
	adapter := &recordingAdapter{}
	s, _ := newTestServer(t, adapter)
	for i := 0; i < rateLimitPerMinute+5; i++ {
		if got := postWebhook(t, s, `{"text":"same event"}`, nil).Code; got != http.StatusOK {
			t.Fatalf("duplicate delivery %d status=%d, want 200", i, got)
		}
	}
	adapter.mu.Lock()
	adapter.messageID = "new-event"
	adapter.mu.Unlock()
	if got := postWebhook(t, s, `{"text":"new event"}`, nil).Code; got != http.StatusOK {
		t.Fatalf("new source was rate limited after duplicate redelivery: status=%d", got)
	}
}

func TestHandleWebhook_RejectsOversizeBody(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)

	body := strings.Repeat("a", int(MaxWebhookBodyBytes)+1)
	w := postWebhook(t, s, body, nil)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got status %d, want 413", w.Code)
	}
	requireNoInbox(t, store)
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.verifiedBody != "" {
		t.Fatal("oversize body must be rejected before signature verification")
	}
}

func TestHandleWebhook_RejectsEmptyBody(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, "", nil).Code; got != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", got)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhook_RejectsDeeplyNestedJSON(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)

	depth := MaxJSONDepth + 25
	body := strings.Repeat(`{"a":`, depth) + `1` + strings.Repeat(`}`, depth)

	if got := postWebhook(t, s, body, nil).Code; got != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", got)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhook_RejectsInvalidUTF8(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, "{\"a\":\"\xff\xfe\"}", nil).Code; got != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", got)
	}
	requireNoInbox(t, store)
}

// An oversize body with no declared Content-Length must still be capped.
func TestHandleWebhook_RejectsOversizeWithUnknownLength(t *testing.T) {
	adapter := &recordingAdapter{}
	s, _ := newTestServer(t, adapter)

	body := strings.Repeat("a", int(MaxWebhookBodyBytes)+512)
	w := postWebhook(t, s, body, func(r *http.Request) { r.ContentLength = -1 })

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got status %d, want 413", w.Code)
	}
}

// Message text beyond the cap is rejected instead of changing user intent.
func TestHandleWebhook_RejectsOversizeMessageContentWithoutEnqueue(t *testing.T) {
	adapter := &recordingAdapter{content: strings.Repeat("x", MaxMessageContentBytes+5000)}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, `{"text":"ignored"}`, nil).Code; got != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize content status=%d, want 413", got)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhook_RejectsMissingToken(t *testing.T) {
	adapter := &recordingAdapter{}
	s, _ := newTestServer(t, adapter)

	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{"text":"hi"}`))
	w := httptest.NewRecorder()
	s.HandleWebhook(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", w.Code)
	}
}

func TestHandleWebhook_RejectsUnknownToken(t *testing.T) {
	adapter := &recordingAdapter{}
	s, _ := newTestServer(t, adapter)

	r := httptest.NewRequest(http.MethodPost, "/webhook?token=bogus", strings.NewReader(`{"text":"hi"}`))
	w := httptest.NewRecorder()
	s.HandleWebhook(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", w.Code)
	}
}

func TestHandleWebhook_RejectsInactiveTenantEvenWhenResolverReturnsIt(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)
	s.tenantService.(*stubTenantService).tenant.Status = tenant.TenantStatusSuspended

	r := postWebhook(t, s, `{"text":"hi"}`, nil)
	if r.Code != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", r.Code)
	}
	requireNoInbox(t, store)
}

func TestHandleWebhookAcknowledgesTenantSuspendedDuringDurableEnqueue(t *testing.T) {
	adapter := &recordingAdapter{}
	s, store := newTestServer(t, adapter)
	rejecting := &rejectingInboxStore{Store: store, err: reliable.ErrTenantInactive}
	s.inbox = rejecting

	response := postWebhook(t, s, `{"text":"hi"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d, want provider acknowledgement 200", response.Code)
	}
	if rejecting.calls != 1 {
		t.Fatalf("durable enqueue calls=%d, want 1", rejecting.calls)
	}
	requireNoInbox(t, store)
}

// A failed signature check must stop the request before it reaches the worker.
func TestHandleWebhook_RejectsBadSignature(t *testing.T) {
	adapter := &recordingAdapter{verifyErr: fmt.Errorf("invalid signature")}
	s, store := newTestServer(t, adapter)

	if got := postWebhook(t, s, `{"text":"hi"}`, nil).Code; got != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", got)
	}
	requireNoInbox(t, store)
}
