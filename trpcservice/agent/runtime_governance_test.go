package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

func TestRuntimePassesRequestInvocationToRunner(t *testing.T) {
	t.Parallel()

	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	capturingRunner := &invocationCapturingRunner{}
	runtime, err := NewRuntime(
		repository,
		staticRunnerProvider{runner: capturingRunner},
		storage.NewMemoryIdempotencyStore(),
		storage.NewMemoryStateStore(),
		time.Minute,
		time.Hour,
		WithInvocationFactory(func(_ context.Context, snapshot tenant.Snapshot, _ string, _ channels.InboundMessage, _ string) (governance.Invocation, error) {
			return governance.Invocation{
				Execution: governance.ExecutionContext{TenantID: snapshot.Config.TenantID, Role: "support", TraceID: "trace-1", PolicyVersion: "v1"},
				Budget:    governance.NewCallBudget(2),
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	_, err = runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Telegram, ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := capturingRunner.invocation.Execution.TenantID, "tenant-a"; got != want {
		t.Fatalf("runner invocation tenant ID = %q, want %q", got, want)
	}
	if capturingRunner.invocation.Budget == nil {
		t.Fatal("runner invocation must carry one shared tool-call budget")
	}
}

type staticRunnerProvider struct{ runner runner.Runner }

func (p staticRunnerProvider) Acquire(context.Context, config.TenantConfig) (runner.Runner, func(), error) {
	return p.runner, func() {}, nil
}

type invocationCapturingRunner struct{ invocation governance.Invocation }

func (r *invocationCapturingRunner) Run(ctx context.Context, _ string, _ string, _ model.Message, _ ...agent.RunOption) (<-chan *event.Event, error) {
	invocation, ok := governance.InvocationFromContext(ctx)
	if !ok {
		return nil, errors.New("missing governance invocation")
	}
	r.invocation = invocation
	events := make(chan *event.Event, 1)
	events <- &event.Event{Response: &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("runtime reply")}}}}
	close(events)
	return events, nil
}

func (*invocationCapturingRunner) Close() error { return nil }

type userCapturingRunner struct {
	userID string
}

func (r *userCapturingRunner) Run(_ context.Context, userID string, _ string, _ model.Message, _ ...agent.RunOption) (<-chan *event.Event, error) {
	r.userID = userID
	events := make(chan *event.Event, 1)
	events <- &event.Event{Response: &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("runtime reply")}}}}
	close(events)
	return events, nil
}

func (*userCapturingRunner) Close() error { return nil }

func TestRuntimeGroupUsesGroupSubjectInsteadOfActorPlatformUser(t *testing.T) {
	t.Parallel()
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	capturing := &userCapturingRunner{}
	runtime, err := NewRuntime(
		repository,
		staticRunnerProvider{runner: capturing},
		storage.NewMemoryIdempotencyStore(),
		storage.NewMemoryStateStore(),
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	groupSubject := "group:telegram:telegram-bot-a:group-1"
	_, err = runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "group-message-1", Channel: channels.Telegram, ConversationID: "group-1", SenderID: "external-user-1",
		ConversationScope: channels.ConversationGroup, SubjectID: groupSubject, ActorPlatformUserID: "platform-user-1",
		TriggerType: channels.TriggerMention, Text: "hello group",
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if capturing.userID != groupSubject {
		t.Fatalf("runner user ID = %q, want group subject %q; actor identity must not become framework user", capturing.userID, groupSubject)
	}
}
