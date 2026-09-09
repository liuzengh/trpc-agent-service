// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	// ErrActorIdentityRequired means a worker-owned memory service was called
	// without the authenticated actor context that binds the operation.
	ErrActorIdentityRequired = errors.New("memory actor identity is required")
	// ErrMemoryScopeViolation means a memory key attempted to escape the
	// tenant-scoped app or the current Runner session owner.
	ErrMemoryScopeViolation = errors.New("memory scope violation")
)

// ActorIdentity is the authenticated caller identity carried through one
// Runner execution. SessionOwnerID is the framework key used by Session;
// UserID remains the provider ID used for authorization and audit. MemoryUserID
// is a distinct, provider-scoped storage identity and must not be caller input.
type ActorIdentity struct {
	UserID         string
	MemoryUserID   string
	SessionOwnerID string
	IsGroupChat    bool
}

type actorIdentityContextKey struct{}

// ContextWithActorIdentity attaches a validated execution identity to ctx.
// It is intended for the Worker boundary and for adapters that implement a
// trusted in-process Runner seam; request payloads must never be copied here.
func ContextWithActorIdentity(ctx context.Context, identity ActorIdentity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, actorIdentityContextKey{}, identity)
}

// ActorIdentityFromContext returns the actor identity attached to ctx.
func ActorIdentityFromContext(ctx context.Context) (ActorIdentity, bool) {
	if ctx == nil {
		return ActorIdentity{}, false
	}
	identity, ok := ctx.Value(actorIdentityContextKey{}).(ActorIdentity)
	if !ok || !validActorIdentity(identity) {
		return ActorIdentity{}, false
	}
	return identity, true
}

func validActorIdentity(identity ActorIdentity) bool {
	for _, value := range []string{identity.UserID, identity.SessionOwnerID} {
		if value == "" || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	if identity.MemoryUserID != "" && (!utf8.ValidString(identity.MemoryUserID) || strings.ContainsAny(identity.MemoryUserID, "\x00\r\n")) {
		return false
	}
	return true
}

func (identity ActorIdentity) memoryUserID() string {
	if identity.MemoryUserID != "" {
		return identity.MemoryUserID
	}
	// Only trusted in-process adapters using ContextWithActorIdentity directly
	// retain the original key contract. Worker.Process always fills this field;
	// it never searches a historical raw-ID key as a fallback.
	return identity.UserID
}

// contextWithMemoryActor derives storage identity after the request and its
// Session owner have been authenticated/validated. Raw provider identity stays
// unchanged in Request, approval, audit, and Session keys.
func contextWithMemoryActor(ctx context.Context, tenantID string, req *Request, strict bool) (context.Context, error) {
	channelType, accountID := req.ChannelType, req.ChannelAccountID
	if channelType == "" || accountID == "" {
		if strict {
			return nil, fmt.Errorf("%w: channel and account are required", ErrActorIdentityRequired)
		}
		// Non-production direct Go callers have no provider binding. Give that
		// explicit local seam its own namespace, never a raw provider alias.
		channelType, accountID = "internal", "inprocess"
	}
	actorID, err := channel.MemoryActorID(tenantID, channelType, accountID, req.UserID)
	if err != nil {
		return nil, ErrActorIdentityRequired
	}
	if token, tokenErr := fence.TokenFromContext(ctx); tokenErr == nil {
		if token.TenantID != tenantID || token.UserID != req.UserID ||
			(token.MemoryUserID != "" && token.MemoryUserID != actorID) {
			return nil, fence.ErrScopeMismatch
		}
		token.MemoryUserID = actorID
		ctx = fence.WithToken(ctx, token)
	}
	return ContextWithActorIdentity(ctx, ActorIdentity{
		UserID: req.UserID, MemoryUserID: actorID,
		SessionOwnerID: req.SessionOwnerID, IsGroupChat: req.IsGroupChat,
	}), nil
}

// actorScopedMemoryService adapts the upstream memory.Service contract to
// the split actor/session identity model. The underlying service remains the
// lifecycle owner; Close deliberately does not close it.
type actorScopedMemoryService struct {
	base           memory.Service
	appName        string
	requireContext bool
	tools          []tool.Tool
}

func newActorScopedMemoryService(base memory.Service, appName string) (*actorScopedMemoryService, error) {
	if base == nil {
		return nil, fmt.Errorf("memory service is required")
	}
	if appName == "" || !utf8.ValidString(appName) || strings.ContainsAny(appName, "\x00\r\n") {
		return nil, fmt.Errorf("memory app name is invalid")
	}
	tools, err := buildActorScopedMemoryTools(base)
	if err != nil {
		return nil, err
	}
	return &actorScopedMemoryService{
		base:           base,
		appName:        appName,
		requireContext: true,
		tools:          tools,
	}, nil
}

func (s *actorScopedMemoryService) scopedUserKey(ctx context.Context, key memory.UserKey) (memory.UserKey, error) {
	if s == nil || s.base == nil {
		return memory.UserKey{}, ErrMemoryScopeViolation
	}
	if key.AppName != s.appName {
		return memory.UserKey{}, fmt.Errorf("%w: app name does not match worker scope", ErrMemoryScopeViolation)
	}
	identity, ok := ActorIdentityFromContext(ctx)
	if !ok {
		if s.requireContext {
			return memory.UserKey{}, ErrActorIdentityRequired
		}
		return key, nil
	}
	// The upstream memory tools derive owner from invocation.Session. Requiring
	// that owner here prevents a custom tool from selecting another user's
	// memory by supplying a different key.
	if key.UserID != identity.SessionOwnerID {
		return memory.UserKey{}, fmt.Errorf("%w: user key is not the current session owner", ErrMemoryScopeViolation)
	}
	key.UserID = identity.memoryUserID()
	return key, nil
}

func (s *actorScopedMemoryService) scopedMemoryKey(ctx context.Context, key memory.Key) (memory.Key, error) {
	if key.AppName != s.appName {
		return memory.Key{}, fmt.Errorf("%w: app name does not match worker scope", ErrMemoryScopeViolation)
	}
	identity, ok := ActorIdentityFromContext(ctx)
	if !ok {
		if s.requireContext {
			return memory.Key{}, ErrActorIdentityRequired
		}
		return key, nil
	}
	if key.UserID != identity.SessionOwnerID {
		return memory.Key{}, fmt.Errorf("%w: memory key is not the current session owner", ErrMemoryScopeViolation)
	}
	key.UserID = identity.memoryUserID()
	return key, nil
}

func (s *actorScopedMemoryService) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) error {
	scoped, err := s.scopedUserKey(ctx, key)
	if err != nil {
		return err
	}
	return s.base.AddMemory(ctx, scoped, value, topics, opts...)
}

func (s *actorScopedMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) error {
	scoped, err := s.scopedMemoryKey(ctx, key)
	if err != nil {
		return err
	}
	return s.base.UpdateMemory(ctx, scoped, value, topics, opts...)
}

func (s *actorScopedMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	scoped, err := s.scopedMemoryKey(ctx, key)
	if err != nil {
		return err
	}
	return s.base.DeleteMemory(ctx, scoped)
}

func (s *actorScopedMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	scoped, err := s.scopedUserKey(ctx, key)
	if err != nil {
		return err
	}
	return s.base.ClearMemories(ctx, scoped)
}

func (s *actorScopedMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	scoped, err := s.scopedUserKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.base.ReadMemories(ctx, scoped, limit)
}

func (s *actorScopedMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	scoped, err := s.scopedUserKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.base.SearchMemories(ctx, scoped, query, opts...)
}

func (s *actorScopedMemoryService) Tools() []tool.Tool {
	if s == nil {
		return nil
	}
	return append([]tool.Tool(nil), s.tools...)
}

// buildActorScopedMemoryTools replaces the upstream standard memory tools
// with fresh instances that resolve the wrapped service from Invocation. A
// backend's native tools are intentionally omitted: many adapters build their
// closures around the underlying service and derive user identity directly
// from the shared group Session, which would bypass scopedUserKey. Operators
// can still expose such a tool through ToolResolver after providing an
// adapter-specific actor-scope contract.
func buildActorScopedMemoryTools(base memory.Service) (tools []tool.Tool, err error) {
	if base == nil {
		return nil, ErrMemoryScopeViolation
	}
	var raw []tool.Tool
	func() {
		defer func() {
			if recover() != nil {
				err = fmt.Errorf("memory service tools unavailable: %w", ErrMemoryScopeViolation)
			}
		}()
		raw = base.Tools()
	}()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(raw))
	for _, candidate := range raw {
		declaration, ok := safeToolDeclaration(candidate)
		if !ok {
			return nil, fmt.Errorf("memory service returned a tool without a declaration name")
		}
		name := declaration.Name
		factory, ok := actorScopedMemoryToolFactory(name)
		if !ok {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("memory service returned duplicate tool %q", name)
		}
		seen[name] = struct{}{}
		value := factory()
		if _, valid := safeToolDeclaration(value); !valid {
			return nil, fmt.Errorf("scoped memory tool %q has no valid declaration", name)
		}
		tools = append(tools, value)
	}
	return tools, nil
}

func actorScopedMemoryToolFactory(name string) (func() tool.Tool, bool) {
	switch name {
	case memory.AddToolName:
		return func() tool.Tool { return memorytool.NewAddTool() }, true
	case memory.UpdateToolName:
		return func() tool.Tool { return memorytool.NewUpdateTool() }, true
	case memory.DeleteToolName:
		return func() tool.Tool { return memorytool.NewDeleteTool() }, true
	case memory.ClearToolName:
		return func() tool.Tool { return memorytool.NewClearTool() }, true
	case memory.SearchToolName:
		return func() tool.Tool { return memorytool.NewSearchTool() }, true
	case memory.LoadToolName:
		return func() tool.Tool { return memorytool.NewLoadTool() }, true
	default:
		return nil, false
	}
}

func (s *actorScopedMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if s == nil || s.base == nil || sess == nil {
		return nil
	}
	identity, ok := ActorIdentityFromContext(ctx)
	if !ok {
		if s.requireContext {
			return ErrActorIdentityRequired
		}
		return s.base.EnqueueAutoMemoryJob(ctx, sess)
	}
	if sess.AppName != s.appName || sess.UserID != identity.SessionOwnerID {
		return ErrMemoryScopeViolation
	}
	// A group transcript is shared state, not a personal profile. The platform
	// deliberately disables automatic extraction here; explicit group memory
	// requires a separate, audited namespace and is not inferred from a speaker.
	if identity.IsGroupChat {
		return nil
	}
	copy := sess.Clone()
	copy.UserID = identity.memoryUserID()
	copy.Hash = session.HashString(copy.AppName + ":" + copy.UserID + ":" + copy.ID)
	return s.base.EnqueueAutoMemoryJob(ctx, copy)
}

// Close is a no-op because the StorageAdapter owns the underlying service.
func (s *actorScopedMemoryService) Close() error { return nil }
