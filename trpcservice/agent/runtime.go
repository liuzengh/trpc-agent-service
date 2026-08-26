package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	tutorialAppName   = "tutorial-app"
	tutorialAgentName = "tutorial-agent"
)

// ChatResult is the transport-neutral result of one tutorial chat turn.
type ChatResult struct {
	Reply      string
	RequestID  string
	EventCount int
}

// Runtime owns the Agent Runner and its configured Session service.
type Runtime struct {
	runner         runner.Runner
	sessionService session.Service
	closeOnce      sync.Once
	closeErr       error

	// The tutorial uses one process and favors clarity over throughput. The
	// production design replaces this lock with per-session distributed
	// coordination and shared storage.
	chatMu sync.Mutex
}

// NewRuntime creates an LLMAgent with the selected model and a default
// in-memory Session service.
func NewRuntime(selectedModel model.Model, stream bool) (*Runtime, error) {
	sessionService := inmemory.NewSessionService()
	runtime, err := NewRuntimeWithSession(
		selectedModel,
		sessionService,
		stream,
	)
	if err != nil {
		_ = sessionService.Close()
		return nil, err
	}
	return runtime, nil
}

// NewRuntimeWithSession creates an LLMAgent with a caller-provided Session
// service. Runtime takes ownership of sessionService after a successful call.
func NewRuntimeWithSession(
	selectedModel model.Model,
	sessionService session.Service,
	stream bool,
) (*Runtime, error) {
	if selectedModel == nil {
		return nil, errors.New("model is required")
	}
	if sessionService == nil {
		return nil, errors.New("session service is required")
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

// Chat runs one user turn and drains the complete Runner event stream.
func (r *Runtime) Chat(
	ctx context.Context,
	userID string,
	sessionID string,
	text string,
) (ChatResult, error) {
	if r == nil || r.runner == nil {
		return ChatResult{}, errors.New("agent runtime is not initialized")
	}
	userID = strings.TrimSpace(userID)
	sessionID = strings.TrimSpace(sessionID)
	text = strings.TrimSpace(text)
	if userID == "" {
		return ChatResult{}, errors.New("user_id is required")
	}
	if sessionID == "" {
		return ChatResult{}, errors.New("session_id is required")
	}
	if text == "" {
		return ChatResult{}, errors.New("message is required")
	}

	r.chatMu.Lock()
	defer r.chatMu.Unlock()

	events, err := r.runner.Run(
		ctx,
		userID,
		sessionID,
		model.NewUserMessage(text),
	)
	if err != nil {
		return ChatResult{}, fmt.Errorf("run tutorial agent: %w", err)
	}

	result, runErr := collectChatResult(ctx, events)
	if runErr != nil {
		return ChatResult{}, runErr
	}
	return result, nil
}

// Ready checks whether the configured Session service is reachable.
func (r *Runtime) Ready(ctx context.Context) error {
	if r == nil || r.sessionService == nil {
		return errors.New("session service is not initialized")
	}
	if _, err := r.sessionService.ListAppStates(ctx, tutorialAppName); err != nil {
		return fmt.Errorf("session service is not ready: %w", err)
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
	if err := ctx.Err(); err != nil {
		return ChatResult{}, err
	}
	if strings.TrimSpace(result.Reply) == "" {
		result.Reply = partial.String()
	}
	if strings.TrimSpace(result.Reply) == "" {
		return ChatResult{}, errors.New("agent returned an empty reply")
	}
	return result, nil
}

// Close releases the Runner and the Session service owned by Runtime.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var runnerErr error
		if r.runner != nil {
			runnerErr = r.runner.Close()
		}
		var sessionErr error
		if r.sessionService != nil {
			sessionErr = r.sessionService.Close()
		}
		r.closeErr = errors.Join(runnerErr, sessionErr)
	})
	return r.closeErr
}
