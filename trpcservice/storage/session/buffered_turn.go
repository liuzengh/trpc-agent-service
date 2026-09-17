package session

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

// BufferedTurn is the service-owned transaction bridge used until upstream
// exposes a native atomic commit hook.
type BufferedTurn struct {
	mu        sync.Mutex
	store     AtomicSessionStore
	backing   agentsession.Service
	key       SessionKey
	appName   string
	userID    string
	closed    bool
	committed bool
	service   *bufferedSessionService
}

func NewBufferedTurn(store AtomicSessionStore, backing agentsession.Service, key SessionKey, userID string) (*BufferedTurn, error) {
	return NewBufferedTurnScoped(store, backing, key, key.TenantID+"/"+key.AgentAppID, userID)
}

func NewBufferedTurnScoped(store AtomicSessionStore, backing agentsession.Service, key SessionKey, appName, userID string) (*BufferedTurn, error) {
	return NewDurableBufferedTurnScoped(store, backing, key, appName, userID)
}

// NewDurableBufferedTurnScoped scopes one SDK session.Service to a fenced turn.
// The backing SDK service is the only Session/Event/State persistence authority;
// AtomicSessionStore remains a coordination journal for input order, fence and
// durable outbox effects.
func NewDurableBufferedTurnScoped(store AtomicSessionStore, backing agentsession.Service, key SessionKey, appName, userID string) (*BufferedTurn, error) {
	if store == nil || backing == nil || key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" || appName == "" || userID == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if appName != key.TenantID+"/"+key.AgentAppID {
		return nil, runtime.ErrTenantScope
	}
	turn := &BufferedTurn{store: store, backing: backing, key: key, appName: appName, userID: userID}
	turn.service = &bufferedSessionService{turn: turn}
	return turn, nil
}

func (t *BufferedTurn) SessionService() agentsession.Service { return t.service }

func (t *BufferedTurn) Commit(ctx context.Context, request CommitTurnRequest) (CommitTurnResult, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return CommitTurnResult{}, runtime.ErrCommitConflict
	}
	request.SessionKey = t.key
	// Session/Event/State have already been persisted through the official SDK
	// service. Do not mirror them into the platform coordination tables: that
	// would recreate a second Session source of truth. The remaining commit is
	// deliberately best-effort coordination for fence/order/outbox semantics.
	request.Events = nil
	request.StateDelta = nil
	request.SummaryCandidate = nil
	t.mu.Unlock()
	result, err := t.store.CommitTurn(ctx, request)
	if err != nil {
		return CommitTurnResult{}, err
	}
	t.mu.Lock()
	t.closed, t.committed = true, true
	t.mu.Unlock()
	return result, nil
}

func (t *BufferedTurn) Rollback(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return runtime.ErrCommitConflict
	}
	t.closed = true
	// The SDK may already have synchronously persisted events or state before
	// a later platform coordination failure. Its public API has no atomic
	// cross-store rollback hook, so rollback deliberately only discards this
	// turn's in-process coordination metadata.
	return nil
}

type bufferedSessionService struct{ turn *BufferedTurn }

func (s *bufferedSessionService) validateKey(key agentsession.Key) error {
	if key.AppName != s.turn.appName || key.UserID != s.turn.userID || key.SessionID != s.turn.key.SessionID {
		return runtime.ErrTenantScope
	}
	return nil
}

func (s *bufferedSessionService) CreateSession(ctx context.Context, key agentsession.Key, state agentsession.StateMap, options ...agentsession.Option) (*agentsession.Session, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s.turn.backing.CreateSession(ctx, key, cloneAgentState(state), options...)
}

func (s *bufferedSessionService) GetSession(ctx context.Context, key agentsession.Key, options ...agentsession.Option) (*agentsession.Session, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	copy, err := s.turn.backing.GetSession(ctx, key, options...)
	if err != nil {
		return nil, err
	}
	if copy == nil {
		copy, err = s.turn.backing.CreateSession(ctx, key, agentsession.StateMap{})
		if err != nil {
			return nil, err
		}
	}
	return copy, nil
}

func (s *bufferedSessionService) ListSessions(ctx context.Context, key agentsession.UserKey, options ...agentsession.Option) ([]*agentsession.Session, error) {
	if key.AppName != s.turn.appName || key.UserID != s.turn.userID {
		return nil, runtime.ErrTenantScope
	}
	value, err := s.GetSession(ctx, agentsession.Key{AppName: key.AppName, UserID: key.UserID, SessionID: s.turn.key.SessionID}, options...)
	if err != nil {
		return nil, err
	}
	return []*agentsession.Session{value}, nil
}

func (s *bufferedSessionService) DeleteSession(context.Context, agentsession.Key, ...agentsession.Option) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) UpdateAppState(context.Context, string, agentsession.StateMap) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) DeleteAppState(context.Context, string, string) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) ListAppStates(ctx context.Context, app string) (agentsession.StateMap, error) {
	if app != s.turn.appName {
		return nil, runtime.ErrTenantScope
	}
	return nil, runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) UpdateUserState(context.Context, agentsession.UserKey, agentsession.StateMap) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) ListUserStates(ctx context.Context, key agentsession.UserKey) (agentsession.StateMap, error) {
	if key.AppName != s.turn.appName || key.UserID != s.turn.userID {
		return nil, runtime.ErrTenantScope
	}
	return nil, runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) DeleteUserState(context.Context, agentsession.UserKey, string) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *bufferedSessionService) UpdateSessionState(ctx context.Context, key agentsession.Key, state agentsession.StateMap) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateKey(key); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.turn.backing.UpdateSessionState(ctx, key, cloneAgentState(state))
}

func (s *bufferedSessionService) AppendEvent(ctx context.Context, session *agentsession.Session, value *agentevent.Event, options ...agentsession.Option) error {
	if session == nil || value == nil {
		return runtime.ErrCommitConflict
	}
	if err := s.validateKey(agentsession.Key{AppName: session.AppName, UserID: session.UserID, SessionID: session.ID}); err != nil {
		return err
	}
	if value.ID == "" {
		return runtime.ErrCommitConflict
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	// Append through the official implementation. In particular, do not append
	// to a platform event table or mutate session.Events/session.State here:
	// the SDK is their only persistence authority.
	if err := s.turn.backing.AppendEvent(ctx, session, value, options...); err != nil {
		return err
	}
	return nil
}

func (s *bufferedSessionService) ensureOpen() error {
	s.turn.mu.Lock()
	defer s.turn.mu.Unlock()
	if s.turn.closed {
		return runtime.ErrCommitConflict
	}
	return nil
}

func (s *bufferedSessionService) CreateSessionSummary(context.Context, *agentsession.Session, string, bool) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) EnqueueSummaryJob(context.Context, *agentsession.Session, string, bool) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) GetSessionSummaryText(ctx context.Context, session *agentsession.Session, options ...agentsession.SummaryOption) (string, bool) {
	return "", false
}
func (s *bufferedSessionService) Close() error { return nil }

func cloneAgentState(in agentsession.StateMap) agentsession.StateMap {
	out := make(agentsession.StateMap, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

var _ TurnTx = (*BufferedTurn)(nil)
var _ agentsession.Service = (*bufferedSessionService)(nil)
