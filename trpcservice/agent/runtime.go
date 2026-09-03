package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	tutorialAppName            = "tutorial-app"
	tutorialAgentName          = "tutorial-agent"
	leaseReleaseTimeout        = 2 * time.Second
	idempotencyFinalizeTimeout = 2 * time.Second
)

// ChatResult is the transport-neutral result of one tutorial chat turn.
type ChatResult struct {
	Reply      string
	RequestID  string
	EventCount int
	MessageID  string
	Replayed   bool
}

// Runtime owns the Agent Runner and its platform state services.
type Runtime struct {
	runner         runner.Runner
	sessionService session.Service
	coordinator    coordination.Coordinator
	idempotency    idempotency.Store
	closeOnce      sync.Once
	closeErr       error
}

// NewRuntime creates an LLMAgent with an in-memory Session service and a local
// per-Session coordinator.
func NewRuntime(selectedModel model.Model, stream bool) (*Runtime, error) {
	sessionService := inmemory.NewSessionService()
	coordinator := coordination.NewLocalCoordinator()
	idempotencyStore := idempotency.NewLocalStore()
	runtime, err := NewRuntimeWithServices(
		selectedModel,
		sessionService,
		coordinator,
		idempotencyStore,
		stream,
	)
	if err != nil {
		_ = idempotencyStore.Close()
		_ = coordinator.Close()
		_ = sessionService.Close()
		return nil, err
	}
	return runtime, nil
}

// NewRuntimeWithSession creates an LLMAgent with a caller-provided Session
// service and a local coordinator. Runtime takes ownership of sessionService
// after a successful call.
func NewRuntimeWithSession(
	selectedModel model.Model,
	sessionService session.Service,
	stream bool,
) (*Runtime, error) {
	coordinator := coordination.NewLocalCoordinator()
	idempotencyStore := idempotency.NewLocalStore()
	runtime, err := NewRuntimeWithServices(
		selectedModel,
		sessionService,
		coordinator,
		idempotencyStore,
		stream,
	)
	if err != nil {
		_ = idempotencyStore.Close()
		_ = coordinator.Close()
		return nil, err
	}
	return runtime, nil
}

// NewRuntimeWithServices creates an LLMAgent with caller-provided platform
// services. Runtime owns all services after a successful call.
func NewRuntimeWithServices(
	selectedModel model.Model,
	sessionService session.Service,
	coordinator coordination.Coordinator,
	idempotencyStore idempotency.Store,
	stream bool,
) (*Runtime, error) {
	if selectedModel == nil {
		return nil, errors.New("model is required")
	}
	if sessionService == nil {
		return nil, errors.New("session service is required")
	}
	if coordinator == nil {
		return nil, errors.New("session coordinator is required")
	}
	if idempotencyStore == nil {
		return nil, errors.New("idempotency store is required")
	}
	agentInstance := llmagent.New(
		tutorialAgentName,
		llmagent.WithModel(selectedModel),
		llmagent.WithDescription("A minimal agent for learning tRPC-Agent-Go"),
		llmagent.WithInstruction(
			"Reply clearly and use the conversation history supplied by the session. "+
				"If the user told you their name earlier, use that history when asked.",
		),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: stream}),
	)

	return &Runtime{
		runner: runner.NewRunner(
			tutorialAppName,
			agentInstance,
			runner.WithSessionService(sessionService),
		),
		sessionService: sessionService,
		coordinator:    coordinator,
		idempotency:    idempotencyStore,
	}, nil
}

// NewDemoRuntime creates a runnable LLMAgent without external dependencies.
func NewDemoRuntime() *Runtime {
	runtime, err := NewRuntime(NewTutorialModel(), false)
	if err != nil {
		panic(err)
	}
	return runtime
}

// Chat runs a non-retriable caller turn with a generated message ID. HTTP and
// IM adapters should use ChatWithMessageID so retries reuse the external ID.
func (r *Runtime) Chat(
	ctx context.Context,
	userID string,
	sessionID string,
	text string,
) (ChatResult, error) {
	return r.ChatWithMessageID(ctx, uuid.NewString(), userID, sessionID, text)
}

// ChatWithMessageID runs one idempotent user turn and drains the complete
// Runner event stream.
func (r *Runtime) ChatWithMessageID(
	ctx context.Context,
	messageID string,
	userID string,
	sessionID string,
	text string,
) (ChatResult, error) {
	if r == nil || r.runner == nil {
		return ChatResult{}, errors.New("agent runtime is not initialized")
	}
	if r.coordinator == nil {
		return ChatResult{}, errors.New("session coordinator is not initialized")
	}
	if r.idempotency == nil {
		return ChatResult{}, errors.New("idempotency store is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	messageID = strings.TrimSpace(messageID)
	userID = strings.TrimSpace(userID)
	sessionID = strings.TrimSpace(sessionID)
	text = strings.TrimSpace(text)
	if messageID == "" {
		return ChatResult{}, errors.New("message_id is required")
	}
	if userID == "" {
		return ChatResult{}, errors.New("user_id is required")
	}
	if sessionID == "" {
		return ChatResult{}, errors.New("session_id is required")
	}
	if text == "" {
		return ChatResult{}, errors.New("message is required")
	}

	key := idempotency.Key{
		AppName:   tutorialAppName,
		UserID:    userID,
		SessionID: sessionID,
		MessageID: messageID,
	}
	fingerprint := messageFingerprint(text)
	for {
		begin, err := r.idempotency.Begin(ctx, key, fingerprint)
		if err != nil {
			return ChatResult{}, fmt.Errorf("begin idempotent chat: %w", err)
		}
		switch begin.Status {
		case idempotency.BeginCompleted:
			return chatResultFromIdempotency(messageID, begin.Result, true), nil
		case idempotency.BeginProcessing:
			cached, waitErr := r.idempotency.Wait(ctx, key, fingerprint)
			if errors.Is(waitErr, idempotency.ErrRetry) {
				continue
			}
			if waitErr != nil {
				return ChatResult{}, fmt.Errorf("wait for idempotent chat: %w", waitErr)
			}
			return chatResultFromIdempotency(messageID, cached, true), nil
		case idempotency.BeginStarted:
			if begin.Attempt == nil {
				return ChatResult{}, errors.New("idempotency store returned a nil attempt")
			}
			return r.executeIdempotentChat(
				ctx,
				begin.Attempt,
				messageID,
				userID,
				sessionID,
				text,
			)
		default:
			return ChatResult{}, fmt.Errorf(
				"idempotency store returned unknown status %d",
				begin.Status,
			)
		}
	}
}

func (r *Runtime) executeIdempotentChat(
	requestCtx context.Context,
	attempt idempotency.Attempt,
	messageID string,
	userID string,
	sessionID string,
	text string,
) (ChatResult, error) {
	result, runErr := r.runChatTurn(
		attempt.Context(),
		userID,
		sessionID,
		text,
	)
	finalizeCtx, cancel := context.WithTimeout(
		context.WithoutCancel(requestCtx),
		idempotencyFinalizeTimeout,
	)
	defer cancel()
	if runErr != nil {
		failErr := attempt.Fail(finalizeCtx)
		if failErr != nil {
			return ChatResult{}, errors.Join(
				runErr,
				fmt.Errorf("fail idempotent chat: %w", failErr),
			)
		}
		return ChatResult{}, runErr
	}
	cached := idempotency.Result{
		Reply:      result.Reply,
		RequestID:  result.RequestID,
		EventCount: result.EventCount,
	}
	if err := attempt.Complete(finalizeCtx, cached); err != nil {
		return ChatResult{}, fmt.Errorf("complete idempotent chat: %w", err)
	}
	result.MessageID = messageID
	return result, nil
}

func (r *Runtime) runChatTurn(
	ctx context.Context,
	userID string,
	sessionID string,
	text string,
) (result ChatResult, err error) {
	lease, err := r.coordinator.Acquire(ctx, coordination.Key{
		AppName:   tutorialAppName,
		UserID:    userID,
		SessionID: sessionID,
	})
	if err != nil {
		return ChatResult{}, fmt.Errorf("coordinate tutorial session: %w", err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			leaseReleaseTimeout,
		)
		defer cancel()
		if releaseErr := lease.Release(releaseCtx); releaseErr != nil {
			result = ChatResult{}
			err = errors.Join(
				err,
				fmt.Errorf("release tutorial session lease: %w", releaseErr),
			)
		}
	}()
	leaseCtx := coordination.ContextWithFencingToken(
		lease.Context(),
		lease.FencingToken(),
	)

	events, err := r.runner.Run(
		leaseCtx,
		userID,
		sessionID,
		model.NewUserMessage(text),
	)
	if err != nil {
		return ChatResult{}, fmt.Errorf("run tutorial agent: %w", err)
	}

	result, runErr := collectChatResult(leaseCtx, events)
	if runErr != nil {
		return ChatResult{}, runErr
	}
	return result, nil
}

func chatResultFromIdempotency(
	messageID string,
	result idempotency.Result,
	replayed bool,
) ChatResult {
	return ChatResult{
		Reply:      result.Reply,
		RequestID:  result.RequestID,
		EventCount: result.EventCount,
		MessageID:  messageID,
		Replayed:   replayed,
	}
}

func messageFingerprint(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

// Ready checks whether all state services required for a chat are available.
func (r *Runtime) Ready(ctx context.Context) error {
	if r == nil || r.sessionService == nil {
		return errors.New("session service is not initialized")
	}
	if _, err := r.sessionService.ListAppStates(ctx, tutorialAppName); err != nil {
		return fmt.Errorf("session service is not ready: %w", err)
	}
	if r.coordinator == nil {
		return errors.New("session coordinator is not initialized")
	}
	if err := r.coordinator.Ready(ctx); err != nil {
		return fmt.Errorf("session coordinator is not ready: %w", err)
	}
	if r.idempotency == nil {
		return errors.New("idempotency store is not initialized")
	}
	if err := r.idempotency.Ready(ctx); err != nil {
		return fmt.Errorf("idempotency store is not ready: %w", err)
	}
	return nil
}

func collectChatResult(
	ctx context.Context,
	events <-chan *event.Event,
) (ChatResult, error) {
	var result ChatResult
	var partial strings.Builder
	var runErr error

	for evt := range events {
		if evt == nil {
			continue
		}
		result.EventCount++
		if evt.RequestID != "" {
			result.RequestID = evt.RequestID
		}
		if evt.Response == nil {
			continue
		}
		if evt.Response.Error != nil {
			runErr = fmt.Errorf(
				"agent run failed (%s): %s",
				evt.Response.Error.Type,
				evt.Response.Error.Message,
			)
			continue
		}
		for _, choice := range evt.Response.Choices {
			if choice.Delta.Content != "" {
				partial.WriteString(choice.Delta.Content)
			}
			if choice.Message.Role == model.RoleAssistant && choice.Message.Content != "" {
				result.Reply = choice.Message.Content
			}
		}
	}

	if runErr != nil {
		return ChatResult{}, runErr
	}
	if cause := context.Cause(ctx); cause != nil {
		return ChatResult{}, cause
	}
	if strings.TrimSpace(result.Reply) == "" {
		result.Reply = partial.String()
	}
	if strings.TrimSpace(result.Reply) == "" {
		return ChatResult{}, errors.New("agent returned an empty reply")
	}
	return result, nil
}

// Close releases the Runner and all platform services owned by Runtime.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var runnerErr error
		if r.runner != nil {
			runnerErr = r.runner.Close()
		}
		var coordinatorErr error
		if r.coordinator != nil {
			coordinatorErr = r.coordinator.Close()
		}
		var idempotencyErr error
		if r.idempotency != nil {
			idempotencyErr = r.idempotency.Close()
		}
		var sessionErr error
		if r.sessionService != nil {
			sessionErr = r.sessionService.Close()
		}
		r.closeErr = errors.Join(
			runnerErr,
			coordinatorErr,
			idempotencyErr,
			sessionErr,
		)
	})
	return r.closeErr
}
