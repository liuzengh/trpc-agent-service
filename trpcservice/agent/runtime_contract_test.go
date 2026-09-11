package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

type failingRunnerProvider struct{ err error }

func (p failingRunnerProvider) Acquire(context.Context, config.TenantConfig) (runner.Runner, func(), error) {
	return nil, nil, p.err
}

type failingRoleResolver struct{ err error }

func (r failingRoleResolver) RoleFor(context.Context, string, string) (identity.Role, error) {
	return "", r.err
}

type rejectingModelInputValidator struct{ err error }

func (v rejectingModelInputValidator) ValidateInputs(config.ModelConfig, []config.ModelInputKind) error {
	return v.err
}

type rejectingUsageGovernor struct{ err error }

func (g rejectingUsageGovernor) Reserve(context.Context, governance.UsageReservationRequest) (governance.UsageReservation, error) {
	return governance.UsageReservation{}, g.err
}

func (rejectingUsageGovernor) Renew(context.Context, governance.UsageReservation, time.Duration) error {
	return nil
}

func (rejectingUsageGovernor) SettleKnown(context.Context, governance.UsageReservation, governance.SettledUsage) error {
	return nil
}

func (rejectingUsageGovernor) SettleUnknown(context.Context, governance.UsageReservation) error {
	return nil
}

func newContractRuntime(t *testing.T, repository tenant.Repository, options ...RuntimeOption) (*Runtime, *storage.MemoryStateStore) {
	t.Helper()
	state := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		state,
		time.Minute,
		time.Hour,
		options...,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	return runtime, state
}

func TestNewRuntimeRequiresCoreDependencies(t *testing.T) {
	t.Parallel()
	repository := tenant.NewMemoryRepository()
	runners := assembly.NewFactory(testutil.NewFakeModel("runtime reply"))
	idempotency := storage.NewMemoryIdempotencyStore()
	state := storage.NewMemoryStateStore()
	tests := []struct {
		name        string
		repository  tenant.Repository
		runners     runnerProvider
		idempotency storage.IdempotencyStore
		state       runtimeStateStore
		processing  time.Duration
		completed   time.Duration
		options     []RuntimeOption
	}{
		{name: "repository", runners: runners, idempotency: idempotency, state: state, processing: time.Minute, completed: time.Hour},
		{name: "runners", repository: repository, idempotency: idempotency, state: state, processing: time.Minute, completed: time.Hour},
		{name: "idempotency", repository: repository, runners: runners, state: state, processing: time.Minute, completed: time.Hour},
		{name: "state", repository: repository, runners: runners, idempotency: idempotency, processing: time.Minute, completed: time.Hour},
		{name: "processing ttl", repository: repository, runners: runners, idempotency: idempotency, state: state, completed: time.Hour},
		{name: "completed ttl", repository: repository, runners: runners, idempotency: idempotency, state: state, processing: time.Minute},
		{name: "nil option", repository: repository, runners: runners, idempotency: idempotency, state: state, processing: time.Minute, completed: time.Hour, options: []RuntimeOption{nil}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRuntime(test.repository, test.runners, test.idempotency, test.state, test.processing, test.completed, test.options...); err == nil {
				t.Fatal("NewRuntime() error = nil")
			}
		})
	}
}

func TestRuntimeHandleRejectsInvalidIngressBeforeSideEffects(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{}
	tests := []struct {
		name      string
		bindingID string
		inbound   channels.InboundMessage
		want      string
	}{
		{name: "invalid message", bindingID: "binding", inbound: channels.InboundMessage{}, want: "validate inbound message"},
		{name: "web owner", bindingID: "web-console", inbound: channels.InboundMessage{MessageID: "message-1", Channel: channels.Web, ConversationID: "conv-1", SenderID: "user-1"}, want: "web owner ID"},
		{name: "binding", bindingID: " ", inbound: channels.InboundMessage{MessageID: "message-2", Channel: channels.Telegram, ConversationID: "conv-2", SenderID: "user-2"}, want: "external binding ID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtime.Handle(context.Background(), test.bindingID, test.inbound)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Handle() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRuntimeHandleFailsWhenActorRoleCannotBeResolved(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	roleErr := errors.New("membership store unavailable")
	runtime, state := newContractRuntime(t, repository, WithTenantRoleResolver(failingRoleResolver{err: roleErr}))

	_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-role", Channel: channels.Telegram, ConversationID: "chat-role", SenderID: "external-user",
		ActorPlatformUserID: "platform-user", Text: "hello",
	})
	if err == nil || !strings.Contains(err.Error(), roleErr.Error()) {
		t.Fatalf("Handle() error = %v, want role resolution failure", err)
	}
	if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox = %d, want 0", got)
	}
}

func TestRuntimeHandleRejectsNativeFileInputWithoutCapabilityValidator(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	runtime, state := newContractRuntime(t, repository)

	_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-image", Channel: channels.Telegram, ConversationID: "chat-image", SenderID: "external-user", Text: "analyse image",
		Files: []channels.InboundFile{{Name: "issue.png", MimeType: "image/png", ArtifactName: "input/issue.png", Version: 1, SizeBytes: 10}},
	})
	if err == nil || !strings.Contains(err.Error(), "model input validator is required") {
		t.Fatalf("Handle() error = %v, want model input validator error", err)
	}
	if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox = %d, want 0", got)
	}
}

func TestRuntimeHandleReportsRunnerAcquisitionFailure(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	state := storage.NewMemoryStateStore()
	runnerErr := errors.New("runner unavailable")
	runtime, err := NewRuntime(
		repository,
		failingRunnerProvider{err: runnerErr},
		storage.NewMemoryIdempotencyStore(),
		state,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-runner", Channel: channels.Telegram, ConversationID: "chat-runner", SenderID: "external-user", Text: "hello",
	})
	if err == nil || !strings.Contains(err.Error(), runnerErr.Error()) {
		t.Fatalf("Handle() error = %v, want runner failure", err)
	}
	if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox = %d, want 0", got)
	}
}

func TestRuntimeHandleRequiresUsageGovernorWhenPolicyEnablesUsageLimits(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	_, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels:   []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "telegram-bot-a"}},
		Governance: config.GovernancePolicy{MaxConcurrentRuns: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, state := newContractRuntime(t, repository)
	_, err = runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-governed", Channel: channels.Telegram, ConversationID: "chat-governed", SenderID: "external-user", Text: "hello",
	})
	if err == nil || !strings.Contains(err.Error(), "model usage governor is required") {
		t.Fatalf("Handle() error = %v, want usage governor error", err)
	}
	if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox = %d, want 0", got)
	}
}

func TestRuntimeHandleReportsInvocationFactoryFailure(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	factoryErr := errors.New("governance context unavailable")
	runtime, state := newContractRuntime(t, repository, WithInvocationFactory(func(context.Context, tenant.Snapshot, string, channels.InboundMessage, string) (governance.Invocation, error) {
		return governance.Invocation{}, factoryErr
	}))
	_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-invocation", Channel: channels.Telegram, ConversationID: "chat-invocation", SenderID: "external-user", Text: "hello",
	})
	if err == nil || !strings.Contains(err.Error(), factoryErr.Error()) {
		t.Fatalf("Handle() error = %v, want invocation failure", err)
	}
	if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox = %d, want 0", got)
	}
}

func TestRuntimeHandleRejectsRateLimitAndModelCapabilityFailures(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")

	t.Run("rate limit", func(t *testing.T) {
		limitErr := errors.New("request rate exceeded")
		runtime, state := newContractRuntime(t, repository, WithTenantRateLimiter(rejectingRateLimiter{err: limitErr}))
		_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
			MessageID: "message-rate", Channel: channels.Telegram, ConversationID: "chat-rate", SenderID: "external-user", Text: "hello",
		})
		if err == nil || !strings.Contains(err.Error(), limitErr.Error()) {
			t.Fatalf("Handle() error = %v, want rate limit failure", err)
		}
		if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
			t.Fatalf("pending outbox = %d, want 0", got)
		}
	})

	t.Run("model capability", func(t *testing.T) {
		capabilityErr := errors.New("image input is disabled")
		runtime, state := newContractRuntime(t, repository, WithModelInputValidator(rejectingModelInputValidator{err: capabilityErr}))
		_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
			MessageID: "message-model-input", Channel: channels.Telegram, ConversationID: "chat-model-input", SenderID: "external-user", Text: "inspect attachment",
			Files: []channels.InboundFile{{Name: "photo.png", MimeType: "image/png", ArtifactName: "input/photo.png", Version: 1, SizeBytes: 10}},
		})
		if err == nil || !strings.Contains(err.Error(), capabilityErr.Error()) {
			t.Fatalf("Handle() error = %v, want capability failure", err)
		}
		if got := pendingOutboxCount(t, state, "tenant-a"); got != 0 {
			t.Fatalf("pending outbox = %d, want 0", got)
		}
	})
}

func TestRuntimeHandleReturnsDurableGovernanceRejectionReplies(t *testing.T) {
	t.Run("request rate", func(t *testing.T) {
		repository := tenant.NewMemoryRepository()
		publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
		runtime, state := newContractRuntime(t, repository, WithTenantRateLimiter(rejectingRateLimiter{err: governance.ErrTenantRateLimited}))
		result, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
			MessageID: "message-rate-rejected", Channel: channels.Telegram, ConversationID: "chat-rate", SenderID: "external-user", Text: "hello",
		})
		if err != nil || result.SessionKey == "" {
			t.Fatalf("Handle() = %#v, %v, want completed governance reply", result, err)
		}
		assertGovernanceReply(t, state, "tenant-a", "message-rate-rejected", "请求过于频繁")
	})

	for _, test := range []struct {
		name   string
		err    error
		policy config.GovernancePolicy
		want   string
	}{
		{name: "concurrent run", err: governance.ErrConcurrentRunLimit, policy: config.GovernancePolicy{MaxConcurrentRuns: 1}, want: "正在处理较多请求"},
		{name: "token budget", err: governance.ErrTokenBudget, policy: config.GovernancePolicy{TokenBudgetPerHour: 100, TokenReservation: 10}, want: "模型用量已达上限"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := tenant.NewMemoryRepository()
			_, err := repository.Publish(context.Background(), config.TenantConfig{
				TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
				Channels:   []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "telegram-bot-a"}},
				Governance: test.policy,
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime, state := newContractRuntime(t, repository, WithUsageGovernor(rejectingUsageGovernor{err: test.err}))
			result, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
				MessageID: "message-governance-" + strings.ReplaceAll(test.name, " ", "-"),
				Channel:   channels.Telegram, ConversationID: "chat-governance", SenderID: "external-user", Text: "hello",
			})
			if err != nil || result.SessionKey == "" {
				t.Fatalf("Handle() = %#v, %v, want completed governance reply", result, err)
			}
			assertGovernanceReply(t, state, "tenant-a", resultMessageID(test.name), test.want)
		})
	}
}

func resultMessageID(name string) string {
	return "message-governance-" + strings.ReplaceAll(name, " ", "-")
}

func assertGovernanceReply(t *testing.T, state *storage.MemoryStateStore, tenantID, messageID, wantText string) {
	t.Helper()
	events, err := state.ListPendingOutbox(context.Background(), tenantID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].RequestID != messageID || !strings.Contains(string(events[0].Payload), wantText) {
		t.Fatalf("governance outbox = %+v, want request %q containing %q", events, messageID, wantText)
	}
	audits, err := state.ListAudit(context.Background(), tenantID, messageID)
	if err != nil {
		t.Fatal(err)
	}
	if len(audits) != 1 || audits[0].Action != "governance_rejection" {
		t.Fatalf("governance audit = %+v, want one governance_rejection", audits)
	}
}

func TestRuntimeHandleRejectsInvalidInvocationAndExhaustedUnitBudget(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")

	t.Run("invalid invocation", func(t *testing.T) {
		runtime, _ := newContractRuntime(t, repository, WithInvocationFactory(func(context.Context, tenant.Snapshot, string, channels.InboundMessage, string) (governance.Invocation, error) {
			return governance.Invocation{}, nil
		}))
		_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
			MessageID: "message-invalid-invocation", Channel: channels.Telegram, ConversationID: "chat-invocation", SenderID: "external-user", Text: "hello",
		})
		if err == nil || !strings.Contains(err.Error(), "validate governance invocation") {
			t.Fatalf("Handle() error = %v, want invocation validation failure", err)
		}
	})

	t.Run("unit budget", func(t *testing.T) {
		runtime, _ := newContractRuntime(t, repository, WithInvocationFactory(func(context.Context, tenant.Snapshot, string, channels.InboundMessage, string) (governance.Invocation, error) {
			return governance.Invocation{
				Execution: governance.ExecutionContext{TenantID: "tenant-a", Role: "member", TraceID: "trace-budget", PolicyVersion: "v1"},
				Budget:    governance.NewCallBudget(1),
				Units:     governance.NewUnitBudget(0),
			}, nil
		}))
		_, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
			MessageID: "message-budget", Channel: channels.Telegram, ConversationID: "chat-budget", SenderID: "external-user", Text: "hello",
		})
		if !errors.Is(err, governance.ErrBudgetUnitsExceeded) {
			t.Fatalf("Handle() error = %v, want ErrBudgetUnitsExceeded", err)
		}
	})
}
