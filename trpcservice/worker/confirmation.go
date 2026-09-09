package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

var ErrNoPendingConfirmation = errors.New("no pending tool confirmation")

const pendingConfirmationPrefix = "pending:"

// PendingToolCall is the immutable dangerous call awaiting a user decision.
type PendingToolCall struct {
	TenantID   string          `json:"tenant_id"`
	AppID      string          `json:"app_id"`
	SessionID  string          `json:"session_id"`
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments"`
	CreatedAt  time.Time       `json:"created_at"`
}

// PendingStore persists one pending call per session.
type PendingStore interface {
	Save(context.Context, PendingToolCall) error
	Get(context.Context, string) (PendingToolCall, error)
	Delete(context.Context, string) error
}

// RedisPendingStore stores confirmation state with a hard TTL.
type RedisPendingStore struct {
	client redis.Cmdable
	ttl    time.Duration
}

// NewRedisPendingStore constructs a confirmation store.
func NewRedisPendingStore(client redis.Cmdable, ttl time.Duration) (*RedisPendingStore, error) {
	if client == nil {
		return nil, errors.New("pending confirmation Redis client is required")
	}
	if ttl <= 0 {
		return nil, errors.New("pending confirmation TTL must be positive")
	}
	return &RedisPendingStore{client: client, ttl: ttl}, nil
}

// Save replaces the pending call for one session.
func (s *RedisPendingStore) Save(ctx context.Context, call PendingToolCall) error {
	if call.SessionID == "" || call.TenantID == "" || call.AppID == "" || call.ToolName == "" {
		return errors.New("pending call tenant, app, session, and tool are required")
	}
	data, err := json.Marshal(call)
	if err != nil {
		return fmt.Errorf("encode pending tool call: %w", err)
	}
	if err := s.client.Set(
		ctx, pendingConfirmationPrefix+call.SessionID, data, s.ttl,
	).Err(); err != nil {
		return fmt.Errorf("save pending tool call: %w", err)
	}
	return nil
}

// Get loads an unexpired pending call.
func (s *RedisPendingStore) Get(ctx context.Context, sessionID string) (PendingToolCall, error) {
	data, err := s.client.Get(ctx, pendingConfirmationPrefix+sessionID).Bytes()
	if errors.Is(err, redis.Nil) {
		return PendingToolCall{}, ErrNoPendingConfirmation
	}
	if err != nil {
		return PendingToolCall{}, fmt.Errorf("load pending tool call: %w", err)
	}
	var call PendingToolCall
	if err := json.Unmarshal(data, &call); err != nil {
		return PendingToolCall{}, fmt.Errorf("decode pending tool call: %w", err)
	}
	return call, nil
}

// Delete clears a pending decision.
func (s *RedisPendingStore) Delete(ctx context.Context, sessionID string) error {
	if err := s.client.Del(ctx, pendingConfirmationPrefix+sessionID).Err(); err != nil {
		return fmt.Errorf("delete pending tool call: %w", err)
	}
	return nil
}

// DangerousToolRegistry exposes platform danger metadata and executable tools.
type DangerousToolRegistry interface {
	IsDangerous(string) bool
	Lookup(string) (agenttool.Tool, bool)
}

// ConfirmationGate integrates framework callbacks with direct confirmed calls.
type ConfirmationGate interface {
	Callbacks(tenant.Snapshot) *agenttool.Callbacks
	Resolve(
		context.Context,
		tenant.Snapshot,
		string,
		[]storage.UserEvent,
		[]agenttool.Tool,
	) (<-chan Event, bool, error)
}

// RedisConfirmationGate implements the standard confirm/cancel flow.
type RedisConfirmationGate struct {
	store    PendingStore
	registry DangerousToolRegistry
	now      func() time.Time
}

// NewRedisConfirmationGate constructs a dangerous-tool confirmation gate.
func NewRedisConfirmationGate(
	store PendingStore,
	registry DangerousToolRegistry,
) (*RedisConfirmationGate, error) {
	if store == nil || registry == nil {
		return nil, errors.New("confirmation pending store and tool registry are required")
	}
	return &RedisConfirmationGate{store: store, registry: registry, now: time.Now}, nil
}

// Callbacks returns a request-scoped pre-tool interceptor.
func (g *RedisConfirmationGate) Callbacks(snapshot tenant.Snapshot) *agenttool.Callbacks {
	callbacks := agenttool.NewCallbacks()
	callbacks.RegisterBeforeTool(agenttool.BeforeToolCallbackStructured(
		func(ctx context.Context, args *agenttool.BeforeToolArgs) (*agenttool.BeforeToolResult, error) {
			if args == nil || !g.registry.IsDangerous(args.ToolName) {
				return &agenttool.BeforeToolResult{}, nil
			}
			invocation, ok := agent.InvocationFromContext(ctx)
			if !ok || invocation.Session == nil || invocation.Session.ID == "" {
				return nil, errors.New("dangerous tool call has no invocation session")
			}
			call := PendingToolCall{
				TenantID:   snapshot.Tenant.ID,
				AppID:      snapshot.App.ID,
				SessionID:  invocation.Session.ID,
				ToolCallID: args.ToolCallID,
				ToolName:   args.ToolName,
				Arguments:  append(json.RawMessage(nil), args.Arguments...),
				CreatedAt:  g.now().UTC(),
			}
			if err := g.store.Save(ctx, call); err != nil {
				return nil, err
			}
			return &agenttool.BeforeToolResult{CustomResult: map[string]any{
				"status":    "confirmation_required",
				"tool_name": args.ToolName,
				"message":   "危险操作需要用户确认，请回复“确认”或“取消”。",
			}}, nil
		},
	))
	return callbacks
}

// Resolve consumes confirmation replies before another model invocation.
func (g *RedisConfirmationGate) Resolve(
	ctx context.Context,
	snapshot tenant.Snapshot,
	sessionID string,
	messages []storage.UserEvent,
	visibleTools []agenttool.Tool,
) (<-chan Event, bool, error) {
	call, err := g.store.Get(ctx, sessionID)
	if errors.Is(err, ErrNoPendingConfirmation) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if call.TenantID != snapshot.Tenant.ID || call.AppID != snapshot.App.ID {
		return nil, true, errors.New("pending tool call ownership mismatch")
	}
	if len(messages) == 0 {
		return confirmationEvents("请回复“确认”或“取消”。", "", "tool_confirm_pending"), true, nil
	}
	decision := strings.ToLower(strings.TrimSpace(messages[len(messages)-1].Text))
	switch decision {
	case "取消", "cancel":
		if err := g.store.Delete(ctx, sessionID); err != nil {
			return nil, true, err
		}
		return confirmationEvents("已取消危险工具调用。", "", "tool_denied"), true, nil
	case "确认", "confirm":
		if err := g.store.Delete(ctx, sessionID); err != nil {
			return nil, true, err
		}
		toolInstance := findTool(visibleTools, call.ToolName)
		if toolInstance == nil {
			return confirmationEvents("", "待确认工具已不在租户白名单中。", "tool_denied"), true, nil
		}
		callable, ok := toolInstance.(agenttool.CallableTool)
		if !ok {
			return confirmationEvents("", "待确认工具不可直接调用。", "tool_denied"), true, nil
		}
		result, callErr := callable.Call(ctx, call.Arguments)
		if callErr != nil {
			return confirmationEvents("", callErr.Error(), "tool_confirmed"), true, nil
		}
		encoded, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return confirmationEvents("", marshalErr.Error(), "tool_confirmed"), true, nil
		}
		return directToolEvents(call.ToolName, string(encoded)), true, nil
	default:
		return confirmationEvents(
			"危险操作仍在等待确认，请回复“确认”或“取消”。",
			"",
			"tool_confirm_pending",
		), true, nil
	}
}

func findTool(tools []agenttool.Tool, name string) agenttool.Tool {
	for _, candidate := range tools {
		if candidate != nil && candidate.Declaration() != nil &&
			candidate.Declaration().Name == name {
			return candidate
		}
	}
	return nil
}

func confirmationEvents(text, errorText, decision string) <-chan Event {
	output := make(chan Event, 2)
	if errorText != "" {
		output <- Event{Type: "error", Error: errorText, Decision: decision}
	} else {
		output <- Event{Type: "text_delta", Text: text, Decision: decision}
	}
	output <- Event{Type: "done"}
	close(output)
	return output
}

func directToolEvents(toolName, result string) <-chan Event {
	output := make(chan Event, 3)
	output <- Event{Type: "tool_call", ToolName: toolName, Decision: "tool_confirmed"}
	output <- Event{Type: "tool_result", ToolName: toolName, Text: result}
	output <- Event{Type: "done"}
	close(output)
	return output
}
