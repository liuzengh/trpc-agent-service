package trpcagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const SnapshotVersion = "trpc-agent-go-session/v1"

var ErrOverlay = errors.New("attempt session overlay failed")
var ErrSnapshot = errors.New("invalid accepted session snapshot")
var ErrCapacity = errors.New("session snapshot exceeds configured capacity")

type snapshot struct {
	Version string           `json:"version"`
	Session *session.Session `json:"session"`
}

// overlay owns exactly one attempt-local session. No pointer is shared between
// attempts and no SDK write has authority to accept formal history.
type overlay struct {
	summary                *overlaySummary
	tracer                 trace.Tracer
	appends, snapshotBytes int64
	mu                     sync.Mutex
	key                    session.Key
	stored                 *session.Session
	capacity               int
	sticky                 error
	// Test seam exercises SDK's swallowed-persistence-error behavior.
	beforeAppend func(*event.Event) error
}

func sessionKey(tenantID, sessionID string) session.Key {
	sum := sha256.Sum256([]byte(tenantID))
	return session.Key{AppName: "worker_v1_" + hex.EncodeToString(sum[:]), UserID: "session", SessionID: sessionID}
}
func newOverlay(tenantID, sessionID string, accepted []byte, capacity int) (*overlay, error) {
	return newOverlayState(tenantID, sessionID, accepted, capacity, false)
}
func newOverlayState(tenantID, sessionID string, accepted []byte, capacity int, allowSummary bool) (*overlay, error) {
	if tenantID == "" || sessionID == "" || capacity <= 0 {
		return nil, ErrSnapshot
	}
	key := sessionKey(tenantID, sessionID)
	s := &overlay{key: key, capacity: capacity}
	if len(accepted) == 0 {
		s.stored = session.NewSession(key.AppName, key.UserID, key.SessionID)
	} else {
		if len(accepted) > capacity {
			return nil, ErrCapacity
		}
		var value snapshot
		if err := json.Unmarshal(accepted, &value); err != nil || value.Version != SnapshotVersion || value.Session == nil {
			return nil, ErrSnapshot
		}
		s.stored = value.Session
		if keyFromSession(s.stored) != key || (!allowSummary && len(s.stored.Summaries) > 0) {
			return nil, ErrSnapshot
		}
	}
	return s, nil
}
func keyFromSession(s *session.Session) session.Key {
	if s == nil {
		return session.Key{}
	}
	return session.Key{AppName: s.AppName, UserID: s.UserID, SessionID: s.ID}
}
func (s *overlay) fail(err error) error {
	if err != nil && s.sticky == nil {
		s.sticky = fmt.Errorf("%w: %w", ErrOverlay, err)
	}
	return err
}
func (s *overlay) check(ctx context.Context, key session.Key) error {
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if key != s.key {
		return s.fail(ErrSnapshot)
	}
	if s.sticky != nil {
		return s.sticky
	}
	return nil
}
func (s *overlay) encoded() ([]byte, error) {
	body, err := json.Marshal(snapshot{Version: SnapshotVersion, Session: s.stored.Clone()})
	if err != nil {
		return nil, s.fail(ErrSnapshot)
	}
	s.snapshotBytes = int64(len(body))
	if len(body) > s.capacity {
		return nil, s.fail(ErrCapacity)
	}
	return body, nil
}
func (s *overlay) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sticky != nil {
		return nil, s.sticky
	}
	if s.summary != nil {
		if s.summary.closed {
			return nil, s.fail(ErrSummaryClosed)
		}
		// Snapshot has no context: never wait on a network-bound summary job
		// or export a candidate that omits its pending result. Poisoning the
		// Attempt also prevents a late model response from being imported.
		if len(s.summary.gate) != 0 {
			return nil, s.fail(ErrSummaryInProgress)
		}
	}
	return s.encoded()
}
func (s *overlay) stats() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appends, s.snapshotBytes
}

func (s *overlay) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.sticky }
func (s *overlay) CreateSession(ctx context.Context, key session.Key, state session.StateMap, _ ...session.Option) (created *session.Session, resultErr error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "session.overlay.create")
	defer func() { telemetrytrace.End(span, resultErr) }()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, key); err != nil {
		return nil, err
	}
	// The attempt is initialized before Runner.Run; replacing it could lose history.
	return nil, s.fail(errors.New("attempt session already exists"))
}
func (s *overlay) GetSession(ctx context.Context, key session.Key, _ ...session.Option) (loaded *session.Session, resultErr error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "session.overlay.get")
	defer func() { telemetrytrace.End(span, resultErr) }()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, key); err != nil {
		return nil, err
	}
	// JSON roundtrip is intentionally deep; SDK Session.Clone retains event
	// response pointers, which must not alias accepted input or another attempt.
	body, err := s.encoded()
	if err != nil {
		return nil, err
	}
	var value snapshot
	if err = json.Unmarshal(body, &value); err != nil {
		return nil, s.fail(err)
	}
	return value.Session, nil
}
func (s *overlay) ListSessions(ctx context.Context, key session.UserKey, _ ...session.Option) ([]*session.Session, error) {
	value, err := s.GetSession(ctx, session.Key{AppName: key.AppName, UserID: key.UserID, SessionID: s.key.SessionID})
	if err != nil {
		return nil, err
	}
	return []*session.Session{value}, nil
}
func (s *overlay) DeleteSession(ctx context.Context, key session.Key, _ ...session.Option) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, key); err != nil {
		return err
	}
	return s.fail(errors.New("attempt session deletion disabled"))
}
func (s *overlay) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, key); err != nil {
		return err
	}
	for k := range state {
		if strings.HasPrefix(k, session.StateAppPrefix) || strings.HasPrefix(k, session.StateUserPrefix) {
			return s.fail(errors.New("cross-session state disabled"))
		}
	}
	for k, v := range state {
		s.stored.SetState(k, append([]byte(nil), v...))
	}
	_, err := s.encoded()
	return err
}
func (s *overlay) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, _ ...session.Option) (resultErr error) {
	if evt == nil || evt.Response == nil || !evt.IsPartial {
		var span trace.Span
		ctx, span = telemetrytrace.Start(s.tracer, ctx, "session.overlay.append")
		defer func() { telemetrytrace.End(span, resultErr) }()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, keyFromSession(sess)); err != nil {
		return err
	}
	if evt == nil {
		return s.fail(errors.New("nil session event"))
	}
	if s.beforeAppend != nil {
		if err := s.beforeAppend(evt); err != nil {
			return s.fail(err)
		}
	}
	for k := range evt.StateDelta {
		if strings.HasPrefix(k, session.StateAppPrefix) || strings.HasPrefix(k, session.StateUserPrefix) {
			return s.fail(errors.New("cross-session event state disabled"))
		}
	}
	// Ignore SDK event-window options: V1 never silently truncates accepted history.
	if sess != s.stored {
		sess.UpdateUserSession(evt)
	}
	s.stored.UpdateUserSession(evt)
	s.appends++
	_, err := s.encoded()
	return err
}
func (s *overlay) disabled() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fail(errors.New("cross-session state writes disabled"))
}
func (s *overlay) UpdateAppState(context.Context, string, session.StateMap) error {
	return s.disabled()
}
func (s *overlay) DeleteAppState(context.Context, string, string) error { return s.disabled() }
func (s *overlay) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return s.disabled()
}
func (s *overlay) DeleteUserState(context.Context, session.UserKey, string) error {
	return s.disabled()
}
func (s *overlay) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key
	key.AppName = appName
	if err := s.check(ctx, key); err != nil {
		return nil, err
	}
	return session.StateMap{}, nil
}
func (s *overlay) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx, session.Key{AppName: key.AppName, UserID: key.UserID, SessionID: s.key.SessionID}); err != nil {
		return nil, err
	}
	return session.StateMap{}, nil
}

var _ session.Service = (*overlay)(nil)
