package storage

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// sqlStagingSession reads committed history through the official SQL service
// and stages all current-turn Session writes for the platform Committer.
type sqlStagingSession struct {
	base               session.Service
	limits             sessionfence.Limits
	postgresSummaryOff bool
}

func newSQLStagingSession(base session.Service, limits sessionfence.Limits, postgresSummaryOff bool) *sqlStagingSession {
	return &sqlStagingSession{base: base, limits: limits, postgresSummaryOff: postgresSummaryOff}
}

func (s *sqlStagingSession) StartTurn(key session.Key, fence sessionfence.Fence) *sessionfence.Turn {
	return sessionfence.NewTurn(key, fence)
}

func (s *sqlStagingSession) Prepare(turn *sessionfence.Turn) (sessionfence.TurnCommit, error) {
	return sessionfence.PrepareTurn(turn)
}

func (s *sqlStagingSession) Discard(turn *sessionfence.Turn) { sessionfence.DiscardTurn(turn) }

func (s *sqlStagingSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	existing, err := s.GetSession(ctx, key, opts...)
	if err != nil || existing != nil {
		return existing, err
	}
	created := session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state))
	if err := sessionfence.StageCreatedSession(ctx, key, created); err != nil {
		return nil, err
	}
	return created, nil
}

func (s *sqlStagingSession) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	current, err := s.base.GetSession(ctx, key, opts...)
	if err == nil && s.postgresSummaryOff {
		clearSummaries(current)
	}
	return current, err
}

func (s *sqlStagingSession) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	items, err := s.base.ListSessions(ctx, key, opts...)
	if err == nil && s.postgresSummaryOff {
		for _, item := range items {
			clearSummaries(item)
		}
	}
	return items, err
}

func clearSummaries(current *session.Session) {
	if current == nil {
		return
	}
	current.SummariesMu.Lock()
	current.Summaries = make(map[string]*session.Summary)
	current.SummariesMu.Unlock()
}

func (s *sqlStagingSession) DeleteSession(context.Context, session.Key, ...session.Option) error {
	return sessionfence.ErrFencedSessionWriteUnsupported
}
func (s *sqlStagingSession) UpdateAppState(context.Context, string, session.StateMap) error {
	return sessionfence.ErrFencedSessionWriteUnsupported
}
func (s *sqlStagingSession) DeleteAppState(context.Context, string, string) error {
	return sessionfence.ErrFencedSessionWriteUnsupported
}
func (s *sqlStagingSession) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	return s.base.ListAppStates(ctx, appName)
}
func (s *sqlStagingSession) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return sessionfence.ErrFencedSessionWriteUnsupported
}
func (s *sqlStagingSession) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return s.base.ListUserStates(ctx, key)
}
func (s *sqlStagingSession) DeleteUserState(context.Context, session.UserKey, string) error {
	return sessionfence.ErrFencedSessionWriteUnsupported
}
func (s *sqlStagingSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return sessionfence.StageSessionState(ctx, key, state)
}
func (s *sqlStagingSession) AppendEvent(ctx context.Context, sess *session.Session, current *event.Event, _ ...session.Option) error {
	return sessionfence.StageEvent(ctx, sess, current, s.limits)
}
func (s *sqlStagingSession) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, forceRefresh bool) error {
	if s.postgresSummaryOff {
		return persistence.ErrPostgresSummaryDisabled
	}
	return s.base.CreateSessionSummary(ctx, sess, filterKey, forceRefresh)
}
func (s *sqlStagingSession) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, forceRefresh bool) error {
	if s.postgresSummaryOff {
		return persistence.ErrPostgresSummaryDisabled
	}
	return s.base.EnqueueSummaryJob(ctx, sess, filterKey, forceRefresh)
}
func (s *sqlStagingSession) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	if s.postgresSummaryOff {
		return "", false
	}
	return s.base.GetSessionSummaryText(ctx, sess, opts...)
}
func (s *sqlStagingSession) Close() error { return s.base.Close() }

var _ sessionfence.StagingSession = (*sqlStagingSession)(nil)
