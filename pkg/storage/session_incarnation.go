package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"sync"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// SessionIncarnationStateKey moves with the Session's canonical State during
// backend migration, while a recreated or expired Session receives a new UUID.
const SessionIncarnationStateKey = "platform:session_incarnation_id"

var ErrSessionIncarnation = errors.New("session incarnation is unavailable or protected")

// SessionInvocationLeaseKey is shared by Worker, Summary, and initialization.
func SessionInvocationLeaseKey(tenantID string, key session.Key) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + key.AppName + "\x00" + key.UserID + "\x00" + key.SessionID))
	return "session-invocation:" + hex.EncodeToString(digest[:])
}

type sessionLeaseBindingKey struct{}

type sessionLeaseBinding struct {
	key   session.Key
	lease *Lease
	mu    sync.Mutex
	id    string
}

// ContextWithSessionLease lets strict fenced Session operations initialize a
// legacy Session only while the complete invocation owns its renewable lease.
func ContextWithSessionLease(ctx context.Context, key session.Key, lease *Lease) context.Context {
	return context.WithValue(ctx, sessionLeaseBindingKey{}, &sessionLeaseBinding{key: key, lease: lease})
}

// SessionIncarnationFromContext retains the incarnation observed by Runner even
// if the final receipt's bounded Session read is unavailable.
func SessionIncarnationFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	binding, _ := ctx.Value(sessionLeaseBindingKey{}).(*sessionLeaseBinding)
	if binding == nil {
		return ""
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	return binding.id
}

func SessionIncarnationID(value *session.Session) (string, error) {
	if value == nil {
		return "", nil
	}
	raw, found := value.GetState(SessionIncarnationStateKey)
	if !found {
		return "", nil
	}
	id := string(raw)
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		return "", ErrSessionIncarnation
	}
	return id, nil
}

func protectedSessionState(state session.StateMap) error {
	if _, found := state[SessionIncarnationStateKey]; found {
		return ErrSessionIncarnation
	}
	return nil
}

func (s *FencedSessionService) invocationBinding(ctx context.Context, key session.Key) (*sessionLeaseBinding, error) {
	if !s.scope.strict {
		return nil, nil
	}
	binding, _ := ctx.Value(sessionLeaseBindingKey{}).(*sessionLeaseBinding)
	if binding == nil {
		return nil, nil
	}
	if binding.key != key || binding.lease == nil || binding.lease.lock == nil || binding.lease.manager == nil ||
		binding.lease.lock.Key != SessionInvocationLeaseKey(s.scope.tenantID, key) {
		return nil, ErrSessionIncarnation
	}
	select {
	case <-binding.lease.Done():
		return nil, ErrStaleWriter
	default:
	}
	if binding.lease.Err() != nil || !binding.lease.manager.ValidateLock(ctx, binding.lease.lock) {
		return nil, ErrStaleWriter
	}
	return binding, nil
}

// New Sessions persist their identity in the initial write. A database may
// normalize CreatedAt precision between CreateSession and its first reread.
func (s *FencedSessionService) createSessionWithIncarnation(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	binding, err := s.invocationBinding(ctx, key)
	if err != nil {
		return nil, err
	}
	var id string
	if binding != nil {
		binding.mu.Lock()
		defer binding.mu.Unlock()
		if _, err := s.invocationBinding(ctx, key); err != nil {
			return nil, err
		}
		if binding.id != "" {
			return nil, ErrSessionIncarnation
		}
		state = maps.Clone(state)
		if state == nil {
			state = make(session.StateMap)
		}
		id = uuid.NewString()
		state[SessionIncarnationStateKey] = []byte(id)
	}
	value, err := s.inner.CreateSession(ctx, key, state, opts...)
	if err != nil {
		return nil, err
	}
	if err := s.validateReturnedSession(ctx, value); err != nil {
		return nil, err
	}
	if binding == nil {
		return s.bindSessionIncarnation(ctx, key, value)
	}
	returnedID, err := SessionIncarnationID(value)
	if err != nil || returnedID != id {
		return nil, ErrSessionIncarnation
	}
	if _, err := s.invocationBinding(ctx, key); err != nil {
		return nil, err
	}
	binding.id = id
	return value.Clone(), nil
}

// Called only inside the strict Session service's execution fence.
func (s *FencedSessionService) bindSessionIncarnation(ctx context.Context, key session.Key, value *session.Session) (*session.Session, error) {
	if value == nil || !s.scope.strict {
		return value, nil
	}
	id, err := SessionIncarnationID(value)
	if err != nil {
		return nil, err
	}
	binding, err := s.invocationBinding(ctx, key)
	if err != nil {
		return nil, err
	}
	if binding == nil {
		return value, nil
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	if id == "" {
		// Another operation in this invocation may have bound the initially
		// unmarked value while this call waited. Never overwrite that binding.
		current, readErr := s.inner.GetSession(ctx, key, session.WithEventNum(1))
		if readErr != nil {
			return nil, readErr
		}
		if current == nil || !current.CreatedAt.Equal(value.CreatedAt) {
			return nil, ErrSessionIncarnation
		}
		id, err = SessionIncarnationID(current)
		if err != nil {
			return nil, err
		}
	}
	if binding.id != "" && binding.id != id {
		return nil, ErrSessionIncarnation
	}
	if id == "" {
		if _, err := s.invocationBinding(ctx, key); err != nil {
			return nil, err
		}
		id = uuid.NewString()
		if err := s.inner.UpdateSessionState(ctx, key, session.StateMap{SessionIncarnationStateKey: []byte(id)}); err != nil {
			return nil, err
		}
	}
	value = value.Clone()
	value.SetState(SessionIncarnationStateKey, []byte(id))
	binding.id = id
	return value, nil
}
