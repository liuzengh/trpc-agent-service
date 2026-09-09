package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

type fakeTenantService struct{ value *tenant.Tenant }

type legacyTenantService struct{ tenant.Service }

func pipelineTestInbox() *reliable.InboxMessage {
	return &reliable.InboxMessage{
		TenantID:          "tenant-a",
		ChannelType:       "wework",
		ChannelAccountID:  "corp-a",
		AgentApp:          "assistant",
		ExternalMessageID: "message-1",
		ConversationID:    "conversation-1",
		ReplyToID:         "provider-message-1",
		UserID:            "user-1",
		SessionID:         "session-1",
		IsGroupChat:       false,
		SessionOwnerID:    "user-1",
		RoutingVersion:    reliable.CurrentInboxRoutingVersion,
		PayloadHash:       strings.Repeat("a", 64),
		Payload:           []byte(`{"content":"hello","isGroupChat":false,"sessionOwnerId":"user-1"}`),
	}
}

func (s *fakeTenantService) CreateTenant(context.Context, string, tenant.TenantConfig) (*tenant.Tenant, error) {
	return nil, errors.New("not implemented in fake")
}
func (s *fakeTenantService) GetTenant(_ context.Context, id string) (*tenant.Tenant, error) {
	if s.value != nil && s.value.ID == id {
		return s.value, nil
	}
	return nil, tenant.ErrTenantNotFound
}
func (s *fakeTenantService) GetTenantChannelScoped(_ context.Context, id, channelType, accountID string) (*tenant.Tenant, error) {
	if s.value == nil || s.value.ID != id {
		return nil, tenant.ErrTenantNotFound
	}
	for _, binding := range s.value.Channels {
		candidate := binding
		if candidate.Type == channelType && candidate.EnsureAccountID() == accountID {
			copy := *s.value
			copy.Channels = []tenant.ChannelBinding{candidate}
			copy.Agents = nil
			copy.Models = nil
			copy.Storage = tenant.StorageConfig{}
			copy.Governance = tenant.GovernancePolicy{}
			copy.Budget = tenant.BudgetConfig{}
			return &copy, nil
		}
	}
	return nil, tenant.ErrTenantNotFound
}
func (s *fakeTenantService) GetTenantByWebhookToken(context.Context, string) (*tenant.Tenant, error) {
	return nil, tenant.ErrTenantNotFound
}
func (s *fakeTenantService) UpdateTenant(context.Context, *tenant.Tenant) error {
	return errors.New("not implemented in fake")
}
func (s *fakeTenantService) DeleteTenant(context.Context, string) error {
	return errors.New("not implemented in fake")
}
func (s *fakeTenantService) ListTenants(context.Context) ([]*tenant.Tenant, error) {
	return []*tenant.Tenant{s.value}, nil
}
func (s *fakeTenantService) Close() error { return nil }

func TestResolveDeliveryTenantFailsClosedWithoutScopedReader(t *testing.T) {
	_, err := resolveDeliveryTenant(context.Background(), &legacyTenantService{}, &reliable.OutboxMessage{
		TenantID: "tenant-a", ChannelType: "telegram", ChannelAccountID: "account-a",
	})
	if !errors.Is(err, tenant.ErrScopedTenantReadUnavailable) {
		t.Fatalf("error=%v, want ErrScopedTenantReadUnavailable", err)
	}
}

type statusOnlyTenantService struct {
	fakeTenantService
	statusCalls int
}

func (s *statusOnlyTenantService) GetTenantStatus(context.Context, string) (tenant.TenantStatus, error) {
	s.statusCalls++
	return tenant.TenantStatusActive, nil
}

func TestConsumerStatusCheckUsesLeastPrivilegeReader(t *testing.T) {
	service := &statusOnlyTenantService{}
	status, err := resolveConsumerTenantStatus(context.Background(), service, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if status != tenant.TenantStatusActive || service.statusCalls != 1 {
		t.Fatalf("status=%q calls=%d, want active/1", status, service.statusCalls)
	}
}

type fakeWorker struct {
	requests chan *worker.Request
}

type inactiveAwareWorker struct {
	requests chan *worker.Request
}

type emptyResponseWorker struct{}

type emptyContentWorker struct{}

type typedNilStatusWorker struct{}

type unknownOutcomeWorker struct{}

type approvalWorker struct {
	deadline time.Time
}

type summaryWorker struct{}

func (summaryWorker) ProcessMessage(_ context.Context, req *worker.Request) (*worker.Response, error) {
	return &worker.Response{
		ContentType: "text", SessionID: req.SessionID, Content: "summary reply",
		Summary: &summarycoord.EnqueueRequest{
			Key: summarycoord.Key{
				TenantID: req.TenantID, AgentAppID: "app-1",
				SessionOwnerID: req.SessionOwnerID, SessionID: req.SessionID,
			},
			AgentVersionID: "version-1", TargetEventSequence: 4,
		},
	}, nil
}

// directApprovalWorker models LocalClient, which returns the Worker error
// directly instead of serializing it through the HTTP 428 response boundary.
type directApprovalWorker struct {
	deadline time.Time
}

// unsafeApprovalResumeWorker models LocalClient returning the Worker sentinel
// directly instead of serializing it through the HTTP 423 boundary.
type unsafeApprovalResumeWorker struct{}

type blockRecordingStore struct {
	*reliable.MemoryStore
	blockCalls int
	retryCalls int
	blockCause error
}

type conflictCompletionStore struct {
	*blockRecordingStore
}

type summaryCompletionStore struct {
	*blockRecordingStore
	called  bool
	request summarycoord.EnqueueRequest
}

func (s *summaryCompletionStore) CompleteInboxWithSummary(
	ctx context.Context,
	id int64,
	lease reliable.Lease,
	reply reliable.OutboxReply,
	request summarycoord.EnqueueRequest,
) (*reliable.OutboxMessage, error) {
	s.called = true
	s.request = request
	return s.MemoryStore.CompleteInbox(ctx, id, lease, reply)
}

func (s *conflictCompletionStore) CompleteInbox(context.Context, int64, reliable.Lease, reliable.OutboxReply) (*reliable.OutboxMessage, error) {
	return nil, reliable.ErrOutboxConflict
}

type deadLetterRecordingStore struct {
	*reliable.MemoryStore
	deadLetterCalls int
	deadLetterCause error
}

func (s *blockRecordingStore) BlockInbox(ctx context.Context, id int64, lease reliable.Lease, cause error) error {
	s.blockCalls++
	s.blockCause = cause
	return s.MemoryStore.BlockInbox(ctx, id, lease, cause)
}

func (s *blockRecordingStore) BlockOutbox(ctx context.Context, id int64, lease reliable.Lease, cause error) error {
	s.blockCalls++
	s.blockCause = cause
	return s.MemoryStore.BlockOutbox(ctx, id, lease, cause)
}

func (s *blockRecordingStore) RetryInbox(
	ctx context.Context,
	id int64,
	lease reliable.Lease,
	cause error,
	nextAttempt time.Time,
) error {
	s.retryCalls++
	return s.MemoryStore.RetryInbox(ctx, id, lease, cause, nextAttempt)
}

func (s *blockRecordingStore) RetryInboxAfter(
	ctx context.Context,
	id int64,
	lease reliable.Lease,
	cause error,
	delay time.Duration,
) error {
	s.retryCalls++
	return s.MemoryStore.RetryInboxAfter(ctx, id, lease, cause, delay)
}

func (s *deadLetterRecordingStore) DeadLetterInbox(
	ctx context.Context,
	id int64,
	lease reliable.Lease,
	cause error,
) error {
	s.deadLetterCalls++
	s.deadLetterCause = cause
	return s.MemoryStore.DeadLetterInbox(ctx, id, lease, cause)
}

// approvalClockStore models a durable store whose authoritative clock lags the
// Consumer node. It lets this package prove that Consumer forwards a
// structurally valid challenge rather than applying its own wall-clock policy.
type approvalClockStore struct {
	*reliable.MemoryStore
	called   bool
	deadline time.Time
}

type expiryReaperRecordingStore struct {
	*reliable.MemoryStore
	calls chan int
}

func (s *expiryReaperRecordingStore) ReapExpired(ctx context.Context, batchSize int) (reliable.ExpiredWorkReapResult, error) {
	select {
	case s.calls <- batchSize:
	default:
	}
	return s.MemoryStore.ReapExpired(ctx, batchSize)
}

func (s *approvalClockStore) WaitInboxApproval(
	_ context.Context,
	_ int64,
	_ reliable.Lease,
	_ error,
	_ time.Duration,
	deadline time.Time,
) error {
	s.called = true
	s.deadline = deadline
	return nil
}

func (w *fakeWorker) ProcessMessage(_ context.Context, req *worker.Request) (*worker.Response, error) {
	w.requests <- req
	return &worker.Response{ContentType: "text", SessionID: req.SessionID, Content: "agent reply"}, nil
}

func (w *inactiveAwareWorker) ProcessMessage(_ context.Context, req *worker.Request) (*worker.Response, error) {
	w.requests <- req
	return &worker.Response{ContentType: "text", Content: "reply"}, nil
}

func (emptyResponseWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return nil, nil
}

func (emptyContentWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return &worker.Response{ContentType: "text"}, nil
}

func (typedNilStatusWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	var statusErr *worker.HTTPStatusError
	return nil, statusErr
}

func (unknownOutcomeWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return nil, &worker.WorkerProtocolError{}
}

func (w approvalWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return nil, &worker.HTTPStatusError{
		StatusCode:        http.StatusPreconditionRequired,
		ApprovalExpiresAt: w.deadline,
	}
}

func (w directApprovalWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return nil, &governance.ApprovalRequiredError{
		Challenge: governance.ApprovalChallenge{ExpiresAt: w.deadline},
	}
}

func (unsafeApprovalResumeWorker) ProcessMessage(context.Context, *worker.Request) (*worker.Response, error) {
	return nil, worker.ErrApprovalResumeUnsafe
}

type fakeAdapter struct {
	sent chan *channel.OutboundMessage
}

type invalidProgressAdapter struct{}

func (a *fakeAdapter) VerifySignature(*http.Request, *tenant.ChannelBinding) error { return nil }
func (a *fakeAdapter) ParseInbound(*http.Request, *tenant.ChannelBinding) (*channel.InboundMessage, error) {
	return nil, errors.New("not used")
}
func (a *fakeAdapter) SendReply(_ context.Context, _ *tenant.ChannelBinding, msg *channel.OutboundMessage) error {
	channel.SetOutboundDeliveryProgress(msg, 1, true)
	a.sent <- msg
	return nil
}
func (a *fakeAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *channel.StreamChunk) error {
	return errors.New("not supported")
}
func (a *fakeAdapter) SupportsStreaming() bool             { return false }
func (a *fakeAdapter) HandleRateLimit(error) time.Duration { return 0 }

func (invalidProgressAdapter) VerifySignature(*http.Request, *tenant.ChannelBinding) error {
	return nil
}
func (invalidProgressAdapter) ParseInbound(*http.Request, *tenant.ChannelBinding) (*channel.InboundMessage, error) {
	return nil, errors.New("not used")
}
func (invalidProgressAdapter) SendReply(_ context.Context, _ *tenant.ChannelBinding, msg *channel.OutboundMessage) error {
	channel.SetOutboundDeliveryProgress(msg, 2, true)
	return nil
}
func (invalidProgressAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *channel.StreamChunk) error {
	return errors.New("not supported")
}
func (invalidProgressAdapter) SupportsStreaming() bool             { return false }
func (invalidProgressAdapter) HandleRateLimit(error) time.Duration { return 0 }

func TestInboxToDeliveryPipeline(t *testing.T) {
	store := reliable.NewMemoryStore()
	binding := tenant.ChannelBinding{
		AccountID: "corp-a",
		Type:      string(channel.ChannelTypeWeWork),
		Token:     "token",
	}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID:       "tenant-a",
		Status:   tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{binding},
		Agents:   []tenant.AgentConfig{{Name: "support"}},
	}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	adapter := &fakeAdapter{sent: make(chan *channel.OutboundMessage, 1)}
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, adapter)

	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner:          "consumer-test",
		Concurrency:    1,
		PollInterval:   time.Millisecond,
		LeaseDuration:  6 * time.Second,
		ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := NewDelivery(store, tenantService, registry, DeliveryConfig{
		Owner:           "delivery-test",
		Concurrency:     1,
		PollInterval:    time.Millisecond,
		LeaseDuration:   6 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	inbound := channel.InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      string(channel.ChannelTypeWeWork),
		ChannelAccountID: "corp-a",
		ExternalUserID:   "user-a",
		ConversationID:   "conversation-a",
		MessageID:        "message-a",
		ReplyToID:        "provider-message-a",
		Content:          "hello",
	}
	payload, err := json.Marshal(inbound)
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.EnqueueInbox(context.Background(), &reliable.InboxMessage{
		TenantID:          inbound.TenantID,
		ChannelType:       inbound.ChannelType,
		ChannelAccountID:  inbound.ChannelAccountID,
		AgentApp:          "support",
		ExternalMessageID: inbound.MessageID,
		ConversationID:    inbound.ConversationID,
		ReplyToID:         inbound.ReplyToID,
		UserID:            inbound.ExternalUserID,
		SessionID:         "tenant-a:wework:corp-a:user-a",
		PayloadHash:       strings.Repeat("a", 64),
		Payload:           payload,
	}); err != nil || !inserted {
		t.Fatalf("enqueue: inserted=%v err=%v", inserted, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	deliveryDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(ctx) }()
	go func() { deliveryDone <- delivery.Run(ctx) }()

	select {
	case sent := <-adapter.sent:
		if sent.ConversationID != inbound.ConversationID || sent.ReplyToID != inbound.ReplyToID || sent.Content != "agent reply" {
			t.Fatalf("unexpected delivery: %#v", sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not deliver reply")
	}
	cancel()
	if err := <-consumerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-deliveryDone; err != nil {
		t.Fatal(err)
	}

	select {
	case req := <-workerClient.requests:
		if req.Content != "hello" || req.AgentApp != "support" {
			t.Fatalf("unexpected worker request: %#v", req)
		}
	default:
		t.Fatal("worker was not called")
	}
}

func TestConsumerRunsOneExpiryReaperLoopPerProcess(t *testing.T) {
	store := &expiryReaperRecordingStore{
		MemoryStore: reliable.NewMemoryStore(),
		calls:       make(chan int, 1),
	}
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive}}
	consumer, err := NewConsumer(store, tenantService, &fakeWorker{requests: make(chan *worker.Request, 1)}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
		ExpiryReapInterval: time.Hour, ExpiryReapBatchSize: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	select {
	case batchSize := <-store.calls:
		if batchSize != 7 {
			t.Fatalf("reaper batch=%d, want 7", batchSize)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not start expiry reaper")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop expiry reaper on cancellation")
	}
}

func TestDeliveryRunsOneExpiryReaperLoopPerProcess(t *testing.T) {
	store := &expiryReaperRecordingStore{
		MemoryStore: reliable.NewMemoryStore(),
		calls:       make(chan int, 1),
	}
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive}}
	delivery, err := NewDelivery(store, tenantService, channel.NewAdapterRegistry(), DeliveryConfig{
		Owner: "delivery-test", LeaseDuration: 6 * time.Second, DeliveryTimeout: time.Second,
		ExpiryReapInterval: time.Hour, ExpiryReapBatchSize: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- delivery.Run(ctx) }()
	select {
	case batchSize := <-store.calls:
		if batchSize != 9 {
			t.Fatalf("reaper batch=%d, want 9", batchSize)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not start expiry reaper")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("delivery run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not stop expiry reaper on cancellation")
	}
}

func TestExpiryReaperConfigurationRejectsInvalidValues(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenants := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	if _, err := NewConsumer(store, tenants, workerClient, ConsumerConfig{
		Owner: "consumer", ExpiryReapInterval: -time.Second,
	}); err == nil {
		t.Fatal("consumer accepted negative expiry reaper interval")
	}
	if _, err := NewDelivery(store, tenants, channel.NewAdapterRegistry(), DeliveryConfig{
		Owner: "delivery", ExpiryReapBatchSize: reliable.MaxExpiredWorkReapBatchSize + 1,
	}); err == nil {
		t.Fatal("delivery accepted unbounded expiry reaper batch")
	}
}

func TestConsumerPropagatesGroupSessionOwnerAndActor(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbound := channel.InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      string(channel.ChannelTypeTelegram),
		ChannelAccountID: "bot-a",
		ExternalUserID:   "alice",
		ConversationID:   "group-a",
		MessageID:        "message-a",
		Content:          "hello group",
		IsGroupChat:      true,
	}
	payload, err := json.Marshal(inbound)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "legacy-group-session"
	inserted, err := store.EnqueueInbox(context.Background(), &reliable.InboxMessage{
		TenantID: inbound.TenantID, ChannelType: inbound.ChannelType,
		ChannelAccountID: inbound.ChannelAccountID, AgentApp: "assistant",
		ExternalMessageID: inbound.MessageID, ConversationID: inbound.ConversationID,
		UserID: inbound.ExternalUserID, SessionID: sessionID,
		PayloadHash: strings.Repeat("b", 64), Payload: payload,
	})
	if err != nil || !inserted {
		t.Fatalf("enqueue group message: inserted=%v err=%v", inserted, err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	request := <-workerClient.requests
	expectedOwner, err := channel.SessionOwnerIDFor(&inbound, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if request.SessionOwnerID != expectedOwner {
		t.Fatalf("group session owner=%q, want %q", request.SessionOwnerID, expectedOwner)
	}
	if request.UserID != "alice" || !request.IsGroupChat || request.SessionID != sessionID {
		t.Fatalf("group actor/session propagation lost: %#v", request)
	}
}

func TestConsumerUsesAtomicSummaryCompletionCapability(t *testing.T) {
	store := &summaryCompletionStore{blockRecordingStore: &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}}
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive}}
	consumer, err := NewConsumer(store, tenantService, summaryWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueInbox(context.Background(), pipelineTestInbox()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if !store.called || store.request.AgentVersionID != "version-1" || store.request.TargetEventSequence != 4 {
		t.Fatalf("summary completion called=%v request=%#v", store.called, store.request)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery", time.Minute); err != nil {
		t.Fatalf("outbox was not committed: %v", err)
	}
}

func TestConsumerBlocksSummaryResponseWhenAtomicCompletionIsUnavailable(t *testing.T) {
	store := &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive}}
	consumer, err := NewConsumer(store, tenantService, summaryWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueInbox(context.Background(), pipelineTestInbox()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if store.blockCalls != 1 || store.retryCalls != 0 {
		t.Fatalf("summary capability failure block=%d retry=%d", store.blockCalls, store.retryCalls)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("reply escaped without summary transaction: %v", err)
	}
}

func TestConsumerDoesNotFallbackToCurrentTenantAgentForUnpinnedInbox(t *testing.T) {
	store := &deadLetterRecordingStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID:     "tenant-a",
		Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "current-default"}},
	}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueInbox(context.Background(), pipelineTestInbox()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a damaged historical projection. Current ingestion rejects this,
	// but the Consumer must not reinterpret it with mutable tenant configuration.
	claim.AgentApp = ""
	consumer.processOne(context.Background(), claim)
	if store.deadLetterCalls != 1 || store.deadLetterCause == nil {
		t.Fatalf("unpinned inbox was not dead-lettered: calls=%d cause=%v", store.deadLetterCalls, store.deadLetterCause)
	}
	if !strings.Contains(store.deadLetterCause.Error(), "pinned agent app") {
		t.Fatalf("dead-letter cause=%v", store.deadLetterCause)
	}
	select {
	case request := <-workerClient.requests:
		t.Fatalf("consumer executed an unpinned inbox with agent %q", request.AgentApp)
	default:
	}
	if _, err := store.ClaimInbox(context.Background(), "consumer-next", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("dead-lettered inbox remained claimable: %v", err)
	}
}

func TestPipelineRejectsLeaseShorterThanOperationWindow(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a"}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}

	if _, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner:          "consumer-test",
		LeaseDuration:  10 * time.Second,
		ProcessTimeout: 10 * time.Second,
	}); err == nil {
		t.Fatal("consumer accepted a lease with no persistence margin")
	}

	registry := channel.NewAdapterRegistry()
	if _, err := NewDelivery(store, tenantService, registry, DeliveryConfig{
		Owner:           "delivery-test",
		LeaseDuration:   10 * time.Second,
		DeliveryTimeout: 10 * time.Second,
	}); err == nil {
		t.Fatal("delivery accepted a lease with no persistence margin")
	}
}

func TestPipelineRejectsInvertedRetryWindow(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a"}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	if _, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-test", RetryBase: 2 * time.Second, RetryMaximum: time.Second,
	}); err == nil {
		t.Fatal("consumer accepted retry maximum below retry base")
	}
	if _, err := NewDelivery(store, tenantService, channel.NewAdapterRegistry(), DeliveryConfig{
		Owner: "delivery-test", RetryBase: 2 * time.Second, RetryMaximum: time.Second,
	}); err == nil {
		t.Fatal("delivery accepted retry maximum below retry base")
	}
}

func TestConsumerFairQueueFailsClosedWithoutDurableCapability(t *testing.T) {
	legacyStore := struct{ reliable.Store }{Store: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a"}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	if _, err := NewConsumer(legacyStore, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-fair", FairQueue: true,
	}); !errors.Is(err, reliable.ErrFairQueueUnavailable) {
		t.Fatalf("fair consumer error=%v, want ErrFairQueueUnavailable", err)
	}
	if _, err := NewConsumer(reliable.NewMemoryStore(), tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-fair", FairQueue: true,
	}); err != nil {
		t.Fatalf("memory fair consumer rejected: %v", err)
	}
}

func TestPipelineConstructorsRejectTypedNilDependencies(t *testing.T) {
	var nilStore *reliable.MemoryStore
	var nilTenants *fakeTenantService
	var nilWorker *fakeWorker
	var nilRegistry *channel.AdapterRegistry
	validStore := reliable.NewMemoryStore()
	validTenants := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a"}}
	validWorker := &fakeWorker{requests: make(chan *worker.Request, 1)}
	validRegistry := channel.NewAdapterRegistry()

	if _, err := NewConsumer(nilStore, validTenants, validWorker, ConsumerConfig{Owner: "consumer"}); err == nil {
		t.Fatal("consumer accepted typed-nil store")
	}
	if _, err := NewConsumer(validStore, nilTenants, validWorker, ConsumerConfig{Owner: "consumer"}); err == nil {
		t.Fatal("consumer accepted typed-nil tenant service")
	}
	if _, err := NewConsumer(validStore, validTenants, nilWorker, ConsumerConfig{Owner: "consumer"}); err == nil {
		t.Fatal("consumer accepted typed-nil worker client")
	}
	if _, err := NewDelivery(nilStore, validTenants, validRegistry, DeliveryConfig{Owner: "delivery"}); err == nil {
		t.Fatal("delivery accepted typed-nil store")
	}
	if _, err := NewDelivery(validStore, nilTenants, validRegistry, DeliveryConfig{Owner: "delivery"}); err == nil {
		t.Fatal("delivery accepted typed-nil tenant service")
	}
	if _, err := NewDelivery(validStore, validTenants, nilRegistry, DeliveryConfig{Owner: "delivery"}); err == nil {
		t.Fatal("delivery accepted typed-nil adapter registry")
	}
}

func TestMinimumSafeLeaseSaturatesDurationOverflow(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	if got := minimumSafeLease(maxDuration); got != maxDuration {
		t.Fatalf("minimumSafeLease(max)=%s, want saturated max duration", got)
	}
}

func TestConsumerHandlesNilContextWithDurableTrace(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	workerClient := &fakeWorker{requests: make(chan *worker.Request, 1)}
	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	inbox.TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(nil, claim)
	if _, err := store.ClaimOutbox(context.Background(), "delivery", time.Minute); err != nil {
		t.Fatalf("nil context prevented durable completion: %v", err)
	}
}

func TestDeliveryRejectsNonSequentialAdapterProgress(t *testing.T) {
	store := &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{{AccountID: "corp-a", Type: string(channel.ChannelTypeWeWork)}},
	}}
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, invalidProgressAdapter{})
	delivery, err := NewDelivery(store, tenantService, registry, DeliveryConfig{
		Owner: "delivery-test", LeaseDuration: 6 * time.Second, DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(context.Background(), claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryClaim, err := store.ClaimOutbox(context.Background(), "delivery-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	delivery.deliverOne(context.Background(), deliveryClaim)
	if store.blockCalls != 1 {
		t.Fatalf("invalid adapter progress block calls=%d, want 1", store.blockCalls)
	}
	if store.blockCause == nil || !strings.Contains(store.blockCause.Error(), "cursor advanced") {
		t.Fatalf("invalid adapter progress cause=%v, want reconciliation reason", store.blockCause)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("invalid adapter progress remained claimable outbox %d: %v", outbox.ID, err)
	}
}

func TestConsumerSuspendedTenantBlocksInboxWithoutRetry(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusSuspended}}
	workerClient := &inactiveAwareWorker{requests: make(chan *worker.Request, 1)}
	consumer, err := NewConsumer(store, tenantService, workerClient, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("suspended tenant message remained claimable: %v", err)
	}
}

func TestConsumerBlocksInboxOnOutboxCompletionConflict(t *testing.T) {
	store := &conflictCompletionStore{blockRecordingStore: &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, &fakeWorker{requests: make(chan *worker.Request, 1)}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if store.blockCalls != 1 {
		t.Fatalf("BlockInbox calls=%d, want 1", store.blockCalls)
	}
	if !errors.Is(store.blockCause, reliable.ErrOutboxConflict) {
		t.Fatalf("BlockInbox cause=%v, want ErrOutboxConflict", store.blockCause)
	}
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("conflicted Inbox remained claimable: %v", err)
	}
}

func TestConsumerBlocksWhenWorkerOutcomeProofIsUnknown(t *testing.T) {
	store := &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, unknownOutcomeWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if store.blockCalls != 1 || !errors.Is(store.blockCause, worker.ErrWorkerExecutionOutcomeUnknown) {
		t.Fatalf("unknown Worker outcome was not blocked: calls=%d cause=%v", store.blockCalls, store.blockCause)
	}
	if store.retryCalls != 0 {
		t.Fatalf("unknown Worker outcome consumed a retry: retry calls=%d", store.retryCalls)
	}
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("unknown Worker outcome remained automatically claimable: %v", err)
	}
}

func TestDeliverySuspendedTenantBlocksOutboxWithoutRetry(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusSuspended}}
	registry := channel.NewAdapterRegistry()
	delivery, err := NewDelivery(store, tenantService, registry, DeliveryConfig{
		Owner: "delivery-test", LeaseDuration: 6 * time.Second, DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(context.Background(), claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryClaim, err := store.ClaimOutbox(context.Background(), "delivery-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	delivery.deliverOne(context.Background(), deliveryClaim)
	if _, err := store.ClaimOutbox(context.Background(), "delivery-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("suspended tenant Outbox remained claimable: %v", err)
	}
	if err := store.ReplayOutbox(context.Background(), outbox.ID, "operator", "tenant reactivated"); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerDeadLettersEmptyWorkerResponse(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{{AccountID: "corp-a", Type: string(channel.ChannelTypeWeWork)}},
		Agents:   []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, emptyResponseWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("empty worker response was not dead-lettered: %v", err)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("empty worker response created an outbox reply: %v", err)
	}
}

func TestConsumerDeadLettersUnpersistableWorkerResponse(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{{AccountID: "corp-a", Type: string(channel.ChannelTypeWeWork)}},
		Agents:   []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, emptyContentWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("unpersistable worker response was not dead-lettered: %v", err)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("unpersistable worker response created an outbox reply: %v", err)
	}
}

func TestConsumerDoesNotPanicOnTypedNilHTTPStatusError(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, typedNilStatusWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("consumer panicked on typed-nil HTTP status error: %v", recovered)
		}
	}()
	consumer.processOne(context.Background(), claim)
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("typed-nil worker error should enter retry wait: %v", err)
	}
}

func TestConsumerApprovalWaitDoesNotConsumeInboxAttempts(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, approvalWorker{
		deadline: time.Now().Add(time.Minute),
	}, ConsumerConfig{
		Owner: "consumer-test", PollInterval: time.Millisecond,
		RetryBase: time.Millisecond, RetryMaximum: time.Second,
		LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), first)
	if first.AttemptCount != 1 {
		t.Fatalf("initial attempt=%d, want 1", first.AttemptCount)
	}
	time.Sleep(3 * time.Millisecond)
	second, err := store.ClaimInbox(context.Background(), "consumer-test-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.AttemptCount != 1 {
		t.Fatalf("approval wait consumed attempt: got %d, want 1", second.AttemptCount)
	}
	consumer.processOne(context.Background(), second)
	if _, err := store.ClaimInbox(context.Background(), "consumer-test-3", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("approval wait should schedule a future poll: %v", err)
	}
}

func TestConsumerDirectApprovalErrorDoesNotConsumeInboxAttempts(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, directApprovalWorker{
		deadline: time.Now().Add(time.Minute),
	}, ConsumerConfig{
		Owner: "consumer-test", PollInterval: time.Millisecond,
		RetryBase: time.Millisecond, RetryMaximum: time.Second,
		LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Millisecond)
		}
		claimed, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
		if err != nil {
			t.Fatalf("claim approval poll %d: %v", attempt+1, err)
		}
		if claimed.AttemptCount != 1 {
			t.Fatalf("direct approval poll %d consumed attempt: got %d, want 1", attempt+1, claimed.AttemptCount)
		}
		consumer.processOne(context.Background(), claimed)
	}
}

func TestConsumerDirectUnsafeApprovalResumeBlocksInbox(t *testing.T) {
	store := &blockRecordingStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	consumer, err := NewConsumer(store, tenantService, unsafeApprovalResumeWorker{}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if store.blockCalls != 1 || !errors.Is(store.blockCause, worker.ErrApprovalResumeUnsafe) {
		t.Fatalf("unsafe local approval resume was not blocked: calls=%d cause=%v", store.blockCalls, store.blockCause)
	}
	if store.retryCalls != 0 {
		t.Fatalf("unsafe local approval resume consumed a retry: retry calls=%d", store.retryCalls)
	}
	if _, err := store.ClaimInbox(context.Background(), "consumer-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("unsafe approval resume remained automatically claimable: %v", err)
	}
	if err := store.ReplayInbox(context.Background(), claim.ID, "operator", "approval transcript reconciled"); err != nil {
		t.Fatalf("unsafe approval resume was not available for audited replay: %v", err)
	}
}

func TestConsumerDefersApprovalExpiryToDurableStoreClock(t *testing.T) {
	store := &approvalClockStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{Name: "assistant"}},
	}}
	// A PostgreSQL challenge issued five minutes before a Consumer whose clock
	// is six minutes fast is valid in the durable store but already appears
	// expired to the Consumer's local clock.
	deadline := time.Now().UTC().Add(-time.Minute)
	consumer, err := NewConsumer(store, tenantService, approvalWorker{deadline: deadline}, ConsumerConfig{
		Owner: "consumer-test", LeaseDuration: 6 * time.Second, ProcessTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	inbox.AgentApp = "assistant"
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	consumer.processOne(context.Background(), claim)
	if !store.called || !store.deadline.Equal(deadline) {
		t.Fatalf("approval deadline was not delegated to durable store: called=%t deadline=%v", store.called, store.deadline)
	}
}

// Ensure the canonical payload remains JSON rather than an opaque provider
// body that Consumer cannot decode.
func TestCanonicalPayloadRoundTrip(t *testing.T) {
	input := &channel.InboundMessage{Content: "hello", Metadata: map[string]string{"k": "v"}}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var output channel.InboundMessage
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&output); err != nil {
		t.Fatal(err)
	}
	if output.Content != input.Content || output.Metadata["k"] != "v" {
		t.Fatalf("round trip mismatch: %#v", output)
	}
}
