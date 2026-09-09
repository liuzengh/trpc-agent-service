//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmem "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// approvalRetryModel returns a tool call until it observes the corresponding
// tool result in the model-visible transcript. This makes the test distinguish
// a genuine pending-tool resume from a second model-planned tool call.
type approvalRetryModel struct {
	mu       sync.Mutex
	requests [][]model.Message
}

func (m *approvalRetryModel) GenerateContent(_ context.Context, request *model.Request) (<-chan *model.Response, error) {
	messages := append([]model.Message(nil), request.Messages...)
	m.mu.Lock()
	m.requests = append(m.requests, messages)
	m.mu.Unlock()

	response := &model.Response{
		ID:     "approval-retry-response",
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Index: 0,
			Message: model.Message{
				Role: model.RoleAssistant,
			},
		}},
	}
	if containsToolResult(messages) {
		response.Choices[0].Message.Content = "approved deletion complete"
	} else {
		response.Choices[0].Message.ToolCalls = []model.ToolCall{{
			ID:   "delete-call-1",
			Type: "function",
			Function: model.FunctionDefinitionParam{
				Name:      "delete_file",
				Arguments: []byte(`{"path":"/tmp/report"}`),
			},
		}}
	}
	responses := make(chan *model.Response, 1)
	responses <- response
	close(responses)
	return responses, nil
}

func (*approvalRetryModel) Info() model.Info { return model.Info{Name: "approval-retry-model"} }

func (m *approvalRetryModel) Requests() [][]model.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]model.Message, len(m.requests))
	for i := range m.requests {
		result[i] = append([]model.Message(nil), m.requests[i]...)
	}
	return result
}

type approvalRetryTool struct{ calls atomic.Int32 }

func (*approvalRetryTool) Declaration() *tool.Declaration {
	return &tool.Declaration{
		Name:        "delete_file",
		Description: "Deletes an approved report",
		InputSchema: &tool.Schema{
			Type: "object",
			Properties: map[string]*tool.Schema{
				"path": {Type: "string"},
			},
		},
	}
}

func (t *approvalRetryTool) Call(context.Context, []byte) (any, error) {
	t.calls.Add(1)
	return map[string]any{"deleted": true}, nil
}

// approvalResumePanicRunner makes any accidental replay visible to the test.
// A multi-tool pending event must fail closed before Worker reaches Runner.
type approvalResumePanicRunner struct{ calls atomic.Int32 }

func (r *approvalResumePanicRunner) Run(
	context.Context,
	string,
	string,
	model.Message,
	...agent.RunOption,
) (<-chan *event.Event, error) {
	r.calls.Add(1)
	return nil, errors.New("runner must not execute an ambiguous approval retry")
}

func (*approvalResumePanicRunner) Close() error { return nil }

// approvalContractStorageAdapter is deliberately never reached by the
// constructor test below. It provides the strict production storage type so
// the test can prove approval-store validation happens before backend setup.
type approvalContractStorageAdapter struct{ storage.StorageAdapter }

func (*approvalContractStorageAdapter) AcquireServices(
	context.Context,
	*tenant.Tenant,
) (session.Service, memory.Service, func(), error) {
	return sessioninmem.NewSessionService(), memoryinmem.NewMemoryService(), func() {}, nil
}

func (*approvalContractStorageAdapter) AtomicWriteFenceEnabled() bool { return true }

// approvalStoreWithoutRecovery satisfies only the historical write interface.
// Strict production mode must reject it rather than discovering too late that
// a granted queue retry cannot inspect or resume its pending tool call.
type approvalStoreWithoutRecovery struct{}

func (approvalStoreWithoutRecovery) CreateChallenge(
	context.Context,
	governance.ApprovalRequest,
	time.Duration,
) (governance.ApprovalChallenge, error) {
	return governance.ApprovalChallenge{}, governance.ErrApprovalStoreUnavailable
}

func (approvalStoreWithoutRecovery) Grant(context.Context, string, string) (governance.ApprovalGrant, error) {
	return governance.ApprovalGrant{}, governance.ErrApprovalStoreUnavailable
}

func (approvalStoreWithoutRecovery) Consume(context.Context, governance.ApprovalRequest, string) error {
	return governance.ErrApprovalStoreUnavailable
}

func containsToolResult(messages []model.Message) bool {
	for _, message := range messages {
		if message.Role == model.RoleTool && message.ToolID == "delete-call-1" {
			return true
		}
	}
	return false
}

func countUserMessage(events []event.Event, content string) int {
	count := 0
	for _, evt := range events {
		if evt.Response == nil {
			continue
		}
		for _, choice := range evt.Response.Choices {
			if choice.Message.Role == model.RoleUser && choice.Message.Content == content {
				count++
			}
		}
	}
	return count
}

func TestProcessApprovalRetryResumesPendingToolWithoutDuplicateUserMessage(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	tenantSnapshot := &tenant.Tenant{
		ID: "approval-retry-tenant",
		ToolPolicy: tenant.ToolPolicy{
			Mode:                "whitelist",
			Allowed:             []string{"delete_file"},
			RequireConfirmation: []string{"delete_file"},
		},
	}
	sessionService := sessioninmem.NewSessionService()
	resumableSessionService := newApprovalPauseSessionService(sessionService)
	approvalStore := governance.NewMemoryApprovalStore()
	modelStub := &approvalRetryModel{}
	toolStub := &approvalRetryTool{}
	agentRuntime := llmagent.New(
		"approval-retry-agent",
		llmagent.WithModel(modelStub),
		llmagent.WithTools([]tool.Tool{toolStub}),
		llmagent.WithMaxLLMCalls(4),
		llmagent.WithMaxToolIterations(3),
	)
	r := runner.NewRunner(
		"approval-retry-app",
		agentRuntime,
		runner.WithSessionService(resumableSessionService),
		runner.WithPlugins(governance.NewPluginWithApprovalStore(
			governance.NewGovernanceFilter(tenantSnapshot),
			"approval-retry-governance",
			approvalStore,
		)),
	)
	t.Cleanup(func() { _ = r.Close() })
	w := &Worker{
		tenant:         tenantSnapshot,
		runner:         r,
		sessionService: resumableSessionService,
		approvalStore:  approvalStore,
		sessionLocks:   storage.NewSessionLockManager(rdb),
		collector:      telemetry.NewCollector(),
		appName:        "approval-retry-app",
		agentName:      "approval-retry-agent",
	}
	request := &Request{
		TenantID:       tenantSnapshot.ID,
		ChannelType:    "telegram",
		UserID:         "alice",
		SessionID:      "session-1",
		IdempotencyKey: "inbox:42",
		Content:        "delete the report",
	}

	_, err = w.Process(ctx, request)
	var required *governance.ApprovalRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("first process error=%v, want approval requirement", err)
	}
	challenges, err := approvalStore.ListChallenges(ctx, tenantSnapshot.ID, 10)
	if err != nil || len(challenges) != 1 {
		t.Fatalf("approval challenges=%#v err=%v", challenges, err)
	}
	firstChallengeID := challenges[0].ChallengeID
	// The consumer may poll a waiting Inbox message before an operator grants
	// it. That retry must retain the exact same resume point without adding a
	// user message, model turn, or replacement challenge.
	_, err = w.Process(ctx, request)
	if !errors.As(err, &required) {
		t.Fatalf("waiting approval retry error=%v, want approval requirement", err)
	}
	challenges, err = approvalStore.ListChallenges(ctx, tenantSnapshot.ID, 10)
	if err != nil || len(challenges) != 1 || challenges[0].ChallengeID != firstChallengeID {
		t.Fatalf("waiting retry challenge=%#v err=%v, want original challenge", challenges, err)
	}
	pendingSession, err := sessionService.GetSession(ctx, session.Key{
		AppName: "approval-retry-app", UserID: "alice", SessionID: "session-1",
	})
	if err != nil {
		t.Fatalf("load pending session: %v", err)
	}
	if count := countUserMessage(pendingSession.Events, request.Content); count != 1 {
		t.Fatalf("pending retry persisted user messages=%d, want 1", count)
	}
	if len(pendingSession.Events) != 2 || pendingSession.Events[1].Response == nil ||
		len(pendingSession.Events[1].Response.Choices) != 1 ||
		len(pendingSession.Events[1].Response.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("pending session lost its sole resumable tool call: %#v", pendingSession.Events)
	}
	if calls := modelStub.Requests(); len(calls) != 1 {
		t.Fatalf("waiting approval invoked model %d times, want 1", len(calls))
	}
	if _, err := approvalStore.Grant(ctx, challenges[0].ChallengeID, "operator-1"); err != nil {
		t.Fatalf("grant approval: %v", err)
	}

	response, err := w.Process(ctx, request)
	if err != nil {
		t.Fatalf("approved retry: %v", err)
	}
	if response.Content != "approved deletion complete" {
		t.Fatalf("response content=%q", response.Content)
	}
	if calls := toolStub.calls.Load(); calls != 1 {
		t.Fatalf("tool calls=%d, want exactly one", calls)
	}

	sess, err := sessionService.GetSession(ctx, session.Key{
		AppName: "approval-retry-app", UserID: "alice", SessionID: "session-1",
	})
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if count := countUserMessage(sess.Events, request.Content); count != 1 {
		t.Fatalf("persisted user messages=%d, want 1", count)
	}
	requests := modelStub.Requests()
	if len(requests) != 2 {
		t.Fatalf("model calls=%d, want first tool plan plus resumed final response", len(requests))
	}
	if !containsToolResult(requests[len(requests)-1]) {
		t.Fatalf("final model request did not contain the resumed tool result: %#v", requests[len(requests)-1])
	}
	if count := countModelUserMessage(requests[len(requests)-1], request.Content); count != 1 {
		t.Fatalf("final model request user messages=%d, want 1", count)
	}
}

func TestProcessApprovalRetryWaitsForGrantBeforeRunner(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	request := &Request{
		TenantID: "approval-pending-tenant", UserID: "alice", SessionOwnerID: "alice",
		SessionID: "session-pending", IdempotencyKey: "inbox:pending-1", Content: "delete the report",
	}
	args := []byte(`{"path":"/tmp/report"}`)
	argsHash, err := governance.CanonicalArgsHash(args)
	if err != nil {
		t.Fatal(err)
	}
	store := governance.NewMemoryApprovalStore()
	challenge, err := store.CreateChallenge(ctx, governance.ApprovalRequest{
		TenantID: request.TenantID, UserID: request.UserID, SessionOwnerID: request.SessionOwnerID,
		SessionID: request.SessionID, ToolName: "delete_file", ArgsHash: argsHash,
		InvocationID: request.IdempotencyKey,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sessionService := sessioninmem.NewSessionService()
	key := session.Key{AppName: "approval-pending-app", UserID: request.SessionOwnerID, SessionID: request.SessionID}
	sess, err := sessionService.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	userEvent := event.NewResponseEvent(request.IdempotencyKey, "user", &model.Response{
		ID: "pending-user", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: request.Content}}},
	})
	userEvent.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, userEvent); err != nil {
		t.Fatal(err)
	}
	pending := event.NewResponseEvent(request.IdempotencyKey, "agent", &model.Response{
		ID: "pending-tool", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
			ID: "delete-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "delete_file", Arguments: args},
		}}}}},
	})
	pending.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, pending); err != nil {
		t.Fatal(err)
	}
	runner := &approvalResumePanicRunner{}
	w := &Worker{
		tenant: &tenant.Tenant{ID: request.TenantID, ToolPolicy: tenant.ToolPolicy{
			Mode: "whitelist", Allowed: []string{"delete_file"}, RequireConfirmation: []string{"delete_file"},
		}},
		runner: runner, sessionService: sessionService, approvalStore: store,
		sessionLocks: storage.NewSessionLockManager(rdb), appName: key.AppName, agentName: "agent", confirmationEnabled: true,
	}
	// The direct Process seam should report the pending challenge before it can
	// acquire a Runner lease or invoke a model/tool. The pre-created transcript
	// makes this an approval retry rather than a fresh user turn.
	_, err = w.Process(ctx, request)
	var required *governance.ApprovalRequiredError
	if !errors.As(err, &required) || required.Challenge.ChallengeID != challenge.ChallengeID {
		t.Fatalf("pending retry error=%v, want challenge %q", err, challenge.ChallengeID)
	}
	if runner.calls.Load() != 0 {
		t.Fatalf("pending approval invoked Runner %d times", runner.calls.Load())
	}
}

func TestNewProductionWorkerRejectsApprovalStoreWithoutRecoveryCapabilities(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	tenantSnapshot := &tenant.Tenant{
		ID:     "approval-contract-tenant",
		Status: tenant.TenantStatusActive,
		Models: []tenant.ModelConfig{{
			ModelName: "gpt-4", Provider: "openai", APIKey: "test-key",
		}},
		Agents: []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4", Tools: []string{"delete_file"}}},
		ToolPolicy: tenant.ToolPolicy{
			Mode: "whitelist", Allowed: []string{"delete_file"}, RequireConfirmation: []string{"delete_file"},
		},
		Storage: tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	_, err = NewProductionWorkerWithOptionsContext(
		context.Background(),
		tenantSnapshot,
		&approvalContractStorageAdapter{},
		rdb,
		Options{
			AppName: "support", AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1",
			ApprovalStore:      approvalStoreWithoutRecovery{},
			SummaryCheckpoints: summarycoord.NewMemorySink(nil),
		},
	)
	if !errors.Is(err, ErrApprovalRecoveryStoreRequired) {
		t.Fatalf("production approval store error=%v, want ErrApprovalRecoveryStoreRequired", err)
	}
}

func TestShouldResumeApprovalRequiresDurableIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	request := &Request{
		TenantID:       "approval-identity-tenant",
		UserID:         "alice",
		SessionOwnerID: "alice",
		SessionID:      "session-1",
		Content:        "this is a new message, not a retry",
	}
	args := []byte(`{"path":"/tmp/report"}`)
	argsHash, err := governance.CanonicalArgsHash(args)
	if err != nil {
		t.Fatalf("canonical approval arguments: %v", err)
	}
	approvalStore := governance.NewMemoryApprovalStore()
	// This is the legacy fallback scope: two different user turns in one
	// Session would both formerly appear as InvocationID=session-1.
	_, err = approvalStore.CreateChallenge(ctx, governance.ApprovalRequest{
		TenantID:       request.TenantID,
		UserID:         request.UserID,
		SessionOwnerID: request.SessionOwnerID,
		SessionID:      request.SessionID,
		ToolName:       "delete_file",
		ArgsHash:       argsHash,
		InvocationID:   request.SessionID,
	}, time.Minute)
	if err != nil {
		t.Fatalf("create legacy-scope challenge: %v", err)
	}

	sessionService := sessioninmem.NewSessionService()
	key := session.Key{AppName: "approval-identity-app", UserID: request.SessionOwnerID, SessionID: request.SessionID}
	sess, err := sessionService.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	userEvent := event.NewResponseEvent(request.SessionID, "user", &model.Response{
		ID:     "legacy-user",
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleUser, Content: "delete the report"},
		}},
	})
	userEvent.RequestID = request.SessionID
	if err := sessionService.AppendEvent(ctx, sess, userEvent); err != nil {
		t.Fatalf("append user event: %v", err)
	}
	pending := event.NewResponseEvent(request.SessionID, "approval-identity-agent", &model.Response{
		ID:     "legacy-pending",
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
				ID: "delete-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "delete_file", Arguments: args},
			}}},
		}},
	})
	pending.RequestID = request.SessionID
	if err := sessionService.AppendEvent(ctx, sess, pending); err != nil {
		t.Fatalf("append pending tool call: %v", err)
	}

	w := &Worker{approvalStore: approvalStore, sessionService: sessionService, appName: key.AppName}
	resume, err := w.shouldResumeApproval(ctx, request)
	if err != nil {
		t.Fatalf("check legacy approval retry: %v", err)
	}
	if resume {
		t.Fatal("request without an idempotency key resumed an older approval")
	}
}

func TestProcessRejectsConfirmationWithoutDurableIdempotencyKey(t *testing.T) {
	w := &Worker{
		tenant: &tenant.Tenant{
			ID: "approval-identity-process-tenant",
			ToolPolicy: tenant.ToolPolicy{
				Mode: "whitelist", Allowed: []string{"delete_file"}, RequireConfirmation: []string{"delete_file"},
			},
		},
		confirmationEnabled: true,
	}
	_, err := w.Process(context.Background(), &Request{
		TenantID:  "approval-identity-process-tenant",
		UserID:    "alice",
		SessionID: "session-1",
		Content:   "delete the report",
	})
	if !errors.Is(err, ErrApprovalIdentityRequired) {
		t.Fatalf("confirmation request without idempotency key error=%v, want ErrApprovalIdentityRequired", err)
	}
}

func TestProcessApprovalRetryRejectsMultiToolPendingStateWithoutReplay(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	request := &Request{
		TenantID:       "approval-multi-tool-tenant",
		ChannelType:    "telegram",
		UserID:         "alice",
		SessionOwnerID: "alice",
		SessionID:      "session-1",
		IdempotencyKey: "inbox:multi-tool-42",
		Content:        "retry the approval",
	}
	args := []byte(`{"path":"/tmp/report"}`)
	argsHash, err := governance.CanonicalArgsHash(args)
	if err != nil {
		t.Fatalf("canonical approval arguments: %v", err)
	}
	approvalStore := governance.NewMemoryApprovalStore()
	challenge, err := approvalStore.CreateChallenge(ctx, governance.ApprovalRequest{
		TenantID:       request.TenantID,
		UserID:         request.UserID,
		SessionOwnerID: request.SessionOwnerID,
		SessionID:      request.SessionID,
		ToolName:       "delete_file",
		ArgsHash:       argsHash,
		InvocationID:   invocationIdentity(request),
	}, time.Minute)
	if err != nil {
		t.Fatalf("create approval challenge: %v", err)
	}

	sessionService := sessioninmem.NewSessionService()
	sessionKey := session.Key{
		AppName: "approval-multi-tool-app", UserID: request.SessionOwnerID, SessionID: request.SessionID,
	}
	sess, err := sessionService.CreateSession(ctx, sessionKey, nil)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	userEvent := event.NewResponseEvent(invocationIdentity(request), "user", &model.Response{
		ID:     "multi-tool-user",
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleUser, Content: request.Content},
		}},
	})
	userEvent.RequestID = invocationIdentity(request)
	if err := sessionService.AppendEvent(ctx, sess, userEvent); err != nil {
		t.Fatalf("append user event: %v", err)
	}
	// The transcript contains two tool calls. Even though one exactly matches
	// the challenge, resuming only it could reorder or skip the other call.
	// The Worker must require reconciliation rather than guess.
	pending := event.NewResponseEvent(invocationIdentity(request), "approval-multi-tool-agent", &model.Response{
		ID:     "multi-tool-pending",
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{
				{ID: "delete-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "delete_file", Arguments: args}},
				{ID: "audit-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "audit_file", Arguments: []byte(`{"path":"/tmp/report"}`)}},
			}},
		}},
	})
	pending.RequestID = invocationIdentity(request)
	if err := sessionService.AppendEvent(ctx, sess, pending); err != nil {
		t.Fatalf("append multi-tool pending event: %v", err)
	}

	panicRunner := &approvalResumePanicRunner{}
	w := &Worker{
		tenant:         &tenant.Tenant{ID: request.TenantID, ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"}},
		runner:         panicRunner,
		sessionService: sessionService,
		approvalStore:  approvalStore,
		sessionLocks:   storage.NewSessionLockManager(rdb),
		appName:        sessionKey.AppName,
		agentName:      "approval-multi-tool-agent",
	}
	if _, err := w.Process(ctx, request); !errors.Is(err, ErrApprovalResumeUnsafe) {
		t.Fatalf("multi-tool approval retry error=%v, want ErrApprovalResumeUnsafe", err)
	}
	if calls := panicRunner.calls.Load(); calls != 0 {
		t.Fatalf("ambiguous approval retry invoked Runner %d times, want 0", calls)
	}
	active, err := approvalStore.FindActiveApproval(ctx, governance.ApprovalInvocationScope{
		TenantID:       request.TenantID,
		UserID:         request.UserID,
		SessionOwnerID: request.SessionOwnerID,
		SessionID:      request.SessionID,
		InvocationID:   invocationIdentity(request),
	})
	if err != nil || active.ChallengeID != challenge.ChallengeID {
		t.Fatalf("ambiguous retry changed pending approval active=%#v err=%v", active, err)
	}
	stored, err := sessionService.GetSession(ctx, sessionKey)
	if err != nil {
		t.Fatalf("reload pending session: %v", err)
	}
	if len(stored.Events) != 2 || len(stored.Events[1].Response.Choices[0].Message.ToolCalls) != 2 {
		t.Fatalf("ambiguous retry changed the pending multi-tool transcript: %#v", stored.Events)
	}
}

func countModelUserMessage(messages []model.Message, content string) int {
	count := 0
	for _, message := range messages {
		if message.Role == model.RoleUser && message.Content == content {
			count++
		}
	}
	return count
}

func TestShouldResumeApprovalRejectsChallengeConsumedAfterAdmission(t *testing.T) {
	ctx := context.Background()
	request := &Request{
		TenantID: "approval-race-tenant", UserID: "alice", SessionOwnerID: "alice",
		SessionID: "session-race", IdempotencyKey: "inbox:race-1", Content: "retry",
		ApprovalResumeChallengeID: "challenge-race",
	}
	args := []byte(`{"path":"/tmp/report"}`)
	argsHash, err := governance.CanonicalArgsHash(args)
	if err != nil {
		t.Fatal(err)
	}
	store := governance.NewMemoryApprovalStore()
	challenge, err := store.CreateChallenge(ctx, governance.ApprovalRequest{
		TenantID: request.TenantID, UserID: request.UserID, SessionOwnerID: request.SessionOwnerID,
		SessionID: request.SessionID, ToolName: "delete_file", ArgsHash: argsHash,
		InvocationID: request.IdempotencyKey,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request.ApprovalResumeChallengeID = challenge.ChallengeID
	if _, err := store.Grant(ctx, challenge.ChallengeID, "operator"); err != nil {
		t.Fatal(err)
	}
	sessionService := sessioninmem.NewSessionService()
	key := session.Key{AppName: "approval-race-app", UserID: request.SessionOwnerID, SessionID: request.SessionID}
	sess, err := sessionService.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	userEvent := event.NewResponseEvent(request.IdempotencyKey, "user", &model.Response{
		ID: "user", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: request.Content}}},
	})
	userEvent.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, userEvent); err != nil {
		t.Fatal(err)
	}
	pending := event.NewResponseEvent(request.IdempotencyKey, "agent", &model.Response{
		ID: "pending", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
			ID: "delete-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "delete_file", Arguments: args},
		}}}}},
	})
	pending.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, pending); err != nil {
		t.Fatal(err)
	}
	// Simulate another worker winning the one-time grant between HTTP admission
	// and this worker's session lease.
	if err := store.ConsumeGranted(ctx, challenge.Request); err != nil {
		t.Fatalf("consume granted: %v", err)
	}
	w := &Worker{approvalStore: store, sessionService: sessionService, appName: key.AppName}
	resume, err := w.shouldResumeApproval(ctx, request)
	if resume || !errors.Is(err, ErrApprovalResumeUnsafe) {
		t.Fatalf("resume=%v err=%v, want fail-closed consumed-challenge error", resume, err)
	}
}

func TestShouldResumeApprovalRejectsOrphanedPendingTranscript(t *testing.T) {
	ctx := context.Background()
	request := &Request{
		TenantID: "approval-orphan-tenant", UserID: "alice", SessionOwnerID: "alice",
		SessionID: "session-orphan", IdempotencyKey: "inbox:orphan-1", Content: "retry",
	}
	args := []byte(`{"path":"/tmp/report"}`)
	argsHash, err := governance.CanonicalArgsHash(args)
	if err != nil {
		t.Fatal(err)
	}
	store := governance.NewMemoryApprovalStore()
	challenge, err := store.CreateChallenge(ctx, governance.ApprovalRequest{
		TenantID: request.TenantID, UserID: request.UserID, SessionOwnerID: request.SessionOwnerID,
		SessionID: request.SessionID, ToolName: "delete_file", ArgsHash: argsHash,
		InvocationID: request.IdempotencyKey,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(ctx, challenge.ChallengeID, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeGranted(ctx, challenge.Request); err != nil {
		t.Fatal(err)
	}
	sessionService := sessioninmem.NewSessionService()
	key := session.Key{AppName: "approval-orphan-app", UserID: request.SessionOwnerID, SessionID: request.SessionID}
	sess, err := sessionService.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	userEvent := event.NewResponseEvent(request.IdempotencyKey, "user", &model.Response{
		ID: "user", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: request.Content}}},
	})
	userEvent.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, userEvent); err != nil {
		t.Fatal(err)
	}
	pending := event.NewResponseEvent(request.IdempotencyKey, "agent", &model.Response{
		ID: "pending", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
			ID: "delete-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "delete_file", Arguments: args},
		}}}}},
	})
	pending.RequestID = request.IdempotencyKey
	if err := sessionService.AppendEvent(ctx, sess, pending); err != nil {
		t.Fatal(err)
	}
	w := &Worker{approvalStore: store, sessionService: sessionService, appName: key.AppName}
	resume, err := w.shouldResumeApproval(ctx, request)
	if resume || !errors.Is(err, ErrApprovalResumeUnsafe) {
		t.Fatalf("resume=%v err=%v, want fail-closed orphaned transcript error", resume, err)
	}
}
