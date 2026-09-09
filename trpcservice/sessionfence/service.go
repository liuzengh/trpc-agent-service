// Package sessionfence provides the fenced Session namespace used by Phase 4.
package sessionfence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	ErrSessionFenceLost              = errors.New("session fence lost")
	ErrFencedSessionWriteUnsupported = errors.New("fenced session write is unsupported")
	ErrTurnTooLarge                  = errors.New("session turn too large")
)

const DefaultMaxListSessions = 1000

type Limits struct {
	MaxTurnEvents int
	MaxTurnBytes  int
}

type TurnStatus string

const (
	TurnActive    TurnStatus = "active"
	TurnPrepared  TurnStatus = "prepared"
	TurnDiscarded TurnStatus = "discarded"
)

type Turn struct {
	mu           sync.RWMutex
	TaskID       string
	SessionCoord string
	UserCoord    string
	SessionSeq   int64
	LeaseEpoch   int64
	LockToken    string
	SessionKey   session.Key
	BaseSession  *session.Session
	Events       []event.Event
	FinalState   session.StateMap
	Status       TurnStatus
	Bytes        int
	OverLimit    bool
}

type TurnCommit struct {
	SessionCoord  string
	SessionSeq    int64
	Events        []event.Event
	FinalState    session.StateMap
	AppName       string
	UserID        string
	SessionID     string
	UserCoord     string
	TraceParent   string `json:"trace_parent,omitempty"`
	DigestVersion int    `json:"digest_version,omitempty"`
}

type Fence struct {
	TaskID       string
	SessionCoord string
	UserCoord    string
	SessionSeq   int64
	LeaseEpoch   int64
	LockToken    string
}

// StagingSession is implemented by Redis and SQL Session wrappers that read
// committed history from their backend while staging the current turn in memory.
type StagingSession interface {
	session.Service
	StartTurn(session.Key, Fence) *Turn
	Prepare(*Turn) (TurnCommit, error)
	Discard(*Turn)
}

type contextKey struct{}

func WithTurn(ctx context.Context, turn *Turn) context.Context {
	return context.WithValue(ctx, contextKey{}, turn)
}
func turnFrom(ctx context.Context) *Turn {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(contextKey{}).(*Turn)
	return t
}

// CurrentTurn returns the staged turn carried by ctx, if any.
func CurrentTurn(ctx context.Context) *Turn { return turnFrom(ctx) }

// NewTurn creates a backend-neutral staging buffer.
func NewTurn(key session.Key, fence Fence) *Turn {
	return &Turn{TaskID: fence.TaskID, SessionCoord: fence.SessionCoord, UserCoord: fence.UserCoord, SessionSeq: fence.SessionSeq, LeaseEpoch: fence.LeaseEpoch, LockToken: fence.LockToken, SessionKey: key, Status: TurnActive, FinalState: make(session.StateMap), Bytes: 2}
}

// StageCreatedSession attaches a newly created Session to the active turn.
func StageCreatedSession(ctx context.Context, key session.Key, sess *session.Session) error {
	turn := turnFrom(ctx)
	if turn == nil || sess == nil {
		return ErrFencedSessionWriteUnsupported
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.Status != TurnActive || turn.SessionKey != key {
		return ErrSessionFenceLost
	}
	turn.BaseSession = sess.Clone()
	turn.FinalState = sess.SnapshotState()
	return nil
}

// StageSessionState records the final Session state without backend I/O.
func StageSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	turn := turnFrom(ctx)
	if turn == nil {
		return ErrFencedSessionWriteUnsupported
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.Status != TurnActive || turn.SessionKey != key {
		return ErrSessionFenceLost
	}
	turn.FinalState = cloneState(state)
	return nil
}

// StageEvent appends one Event and updates the staged final state.
func StageEvent(ctx context.Context, sess *session.Session, current *event.Event, limits Limits) error {
	if sess == nil || current == nil {
		return session.ErrNilSession
	}
	turn := turnFrom(ctx)
	if turn == nil {
		return ErrFencedSessionWriteUnsupported
	}
	copyEvent := *current
	encoded, err := json.Marshal(copyEvent)
	if err != nil {
		return err
	}
	eventBytes := len(encoded)
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.Status != TurnActive {
		return ErrSessionFenceLost
	}
	if turn.SessionKey.AppName != sess.AppName || turn.SessionKey.UserID != sess.UserID || turn.SessionKey.SessionID != sess.ID {
		return ErrFencedSessionWriteUnsupported
	}
	if turn.OverLimit {
		return nil
	}
	separatorBytes := 0
	if len(turn.Events) > 0 {
		separatorBytes = 1
	}
	if len(turn.Events)+1 > limits.MaxTurnEvents || turn.Bytes+separatorBytes+eventBytes > limits.MaxTurnBytes {
		turn.OverLimit = true
		return nil
	}
	turn.Events = append(turn.Events, copyEvent)
	turn.Bytes += separatorBytes + eventBytes
	sess.EventMu.Lock()
	sess.Events = append(sess.Events, copyEvent)
	sess.EventMu.Unlock()
	sess.ApplyEventStateDelta(current)
	turn.FinalState = sess.SnapshotState()
	return nil
}

// PrepareTurn freezes a staged turn for Redis or SQL persistence.
func PrepareTurn(turn *Turn) (TurnCommit, error) {
	if turn == nil {
		return TurnCommit{}, ErrSessionFenceLost
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.Status != TurnActive {
		return TurnCommit{}, ErrSessionFenceLost
	}
	if turn.OverLimit {
		return TurnCommit{}, ErrTurnTooLarge
	}
	commit := TurnCommit{SessionCoord: turn.SessionCoord, SessionSeq: turn.SessionSeq, Events: append([]event.Event(nil), turn.Events...), FinalState: cloneState(turn.FinalState), AppName: turn.SessionKey.AppName, UserID: turn.SessionKey.UserID, SessionID: turn.SessionKey.SessionID, UserCoord: keyspace.UserCoord(turn.SessionKey.AppName, turn.SessionKey.UserID)}
	turn.Status = TurnPrepared
	return commit, nil
}

// DiscardTurn makes a staged turn unusable and releases its payload.
func DiscardTurn(turn *Turn) {
	if turn == nil {
		return
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	turn.Status = TurnDiscarded
	turn.Events = nil
	turn.FinalState = nil
}

type Service struct {
	client     *redis.Client
	rootPrefix string
	limits     Limits
	mu         sync.RWMutex
	closed     bool
}

func New(redisURL, prefix string, coordinationPrefix ...string) (*Service, error) {
	if redisURL == "" || prefix == "" {
		return nil, errors.New("redis URL and prefix are required")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	root := prefix
	if len(coordinationPrefix) > 0 && coordinationPrefix[0] != "" {
		root = coordinationPrefix[0]
	}
	return &Service{client: redis.NewClient(opts), rootPrefix: root, limits: Limits{MaxTurnEvents: 512, MaxTurnBytes: 2 << 20}}, nil
}

func (s *Service) SetLimits(limits Limits) {
	s.mu.Lock()
	if limits.MaxTurnEvents > 0 {
		s.limits.MaxTurnEvents = limits.MaxTurnEvents
	}
	if limits.MaxTurnBytes > 0 {
		s.limits.MaxTurnBytes = limits.MaxTurnBytes
	}
	s.mu.Unlock()
}

func (s *Service) Ready(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errors.New("session service is closed")
	}
	return s.client.Ping(ctx).Err()
}

func (s *Service) StartTurn(key session.Key, fence Fence) *Turn {
	return NewTurn(key, fence)
}

func (t *Turn) IsOverLimit() bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.OverLimit
}

func (s *Service) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	if existing, err := s.GetSession(ctx, key, options...); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state))
	for _, opt := range options {
		_ = opt
	}
	if turnFrom(ctx) != nil {
		if err := StageCreatedSession(ctx, key, sess); err != nil {
			return nil, err
		}
		return sess, nil
	}
	return nil, ErrFencedSessionWriteUnsupported
}

func (s *Service) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	userHash := keyspace.UserCoord(key.AppName, key.UserID)
	coord, err := s.client.HGet(ctx, s.userIndexKey(userHash), key.SessionID).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	metaRaw, err := s.client.HGetAll(ctx, s.metaCoordKey(coord)).Result()
	if err != nil {
		return nil, err
	}
	if len(metaRaw) == 0 {
		return nil, ErrSessionFenceLost
	}
	if metaRaw["app_name"] != key.AppName || metaRaw["user_id"] != key.UserID || metaRaw["session_id"] != key.SessionID || metaRaw["session_coord"] != coord {
		return nil, ErrSessionFenceLost
	}
	var state session.StateMap
	if raw := metaRaw["state_json"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return nil, err
		}
	}
	rawEvents, err := s.client.LRange(ctx, s.eventsCoordKey(coord), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	events := make([]event.Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var e event.Event
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	created, err := time.Parse(time.RFC3339Nano, metaRaw["created_at"])
	if err != nil {
		return nil, ErrSessionFenceLost
	}
	updated, err := time.Parse(time.RFC3339Nano, metaRaw["updated_at"])
	if err != nil {
		return nil, ErrSessionFenceLost
	}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state), session.WithSessionEvents(events), session.WithSessionCreatedAt(created), session.WithSessionUpdatedAt(updated))
	sess.ApplyEventFiltering(options...)
	return sess, nil
}

func (s *Service) ListSessions(ctx context.Context, userKey session.UserKey, options ...session.Option) ([]*session.Session, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	var opts session.Options
	for _, option := range options {
		option(&opts)
	}
	offset, limit := 0, DefaultMaxListSessions
	unpaged := opts.ListSessionPage == nil
	if opts.ListSessionPage != nil {
		if opts.ListSessionPage.Offset < 0 || opts.ListSessionPage.Limit < 0 {
			return nil, errors.New("invalid session list page")
		}
		offset = opts.ListSessionPage.Offset
		if opts.ListSessionPage.Limit > 0 {
			limit = opts.ListSessionPage.Limit
		}
	}
	if limit > DefaultMaxListSessions {
		return nil, fmt.Errorf("session list limit exceeds %d", DefaultMaxListSessions)
	}
	indexKey := s.userIndexKey(keyspace.UserCoord(userKey.AppName, userKey.UserID))
	result := make([]*session.Session, 0, minInt(limit, 16))
	var cursor uint64
	seen := 0
	for {
		fields, next, err := s.client.HScan(ctx, indexKey, cursor, "", 64).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i+1 < len(fields); i += 2 {
			if seen < offset {
				seen++
				continue
			}
			if len(result) >= limit && !unpaged {
				break
			}
			if unpaged && len(result) >= DefaultMaxListSessions {
				return nil, fmt.Errorf("session list exceeds %d entries; pagination is required", DefaultMaxListSessions)
			}
			sessionID, coord := fields[i], fields[i+1]
			item, err := s.getSessionByCoord(ctx, userKey.AppName, userKey.UserID, sessionID, coord, options...)
			if err != nil {
				return nil, err
			}
			if item != nil {
				result = append(result, item)
			}
			seen++
		}
		if (!unpaged && len(result) >= limit) || next == 0 {
			break
		}
		cursor = next
	}
	return result, nil
}

func (s *Service) getSessionByCoord(ctx context.Context, appName, userID, sessionID, coord string, options ...session.Option) (*session.Session, error) {
	metaRaw, err := s.client.HGetAll(ctx, s.metaCoordKey(coord)).Result()
	if err != nil {
		return nil, err
	}
	if len(metaRaw) == 0 {
		return nil, ErrSessionFenceLost
	}
	if metaRaw["app_name"] != appName || metaRaw["user_id"] != userID || metaRaw["session_id"] != sessionID || metaRaw["session_coord"] != coord {
		return nil, ErrSessionFenceLost
	}
	var state session.StateMap
	if raw := metaRaw["state_json"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return nil, err
		}
	}
	rawEvents, err := s.client.LRange(ctx, s.eventsCoordKey(coord), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	events := make([]event.Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var e event.Event
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	var opts session.Options
	for _, option := range options {
		option(&opts)
	}
	if opts.ListSessionOnlyMeta {
		events = nil
	}
	if opts.EventPage != nil {
		if opts.EventPage.Offset < 0 || opts.EventPage.Limit < 0 {
			return nil, errors.New("invalid session event page")
		}
		end := len(events) - opts.EventPage.Offset
		if end < 0 {
			end = 0
		}
		start := end - opts.EventPage.Limit
		if start < 0 {
			start = 0
		}
		if opts.EventPage.Limit == 0 {
			events = nil
		} else {
			paged := append([]event.Event(nil), events[start:end]...)
			events = paged
		}
	}
	created, err := time.Parse(time.RFC3339Nano, metaRaw["created_at"])
	if err != nil {
		return nil, ErrSessionFenceLost
	}
	updated, err := time.Parse(time.RFC3339Nano, metaRaw["updated_at"])
	if err != nil {
		return nil, ErrSessionFenceLost
	}
	sess := session.NewSession(appName, userID, sessionID, session.WithSessionState(state), session.WithSessionEvents(events), session.WithSessionCreatedAt(created), session.WithSessionUpdatedAt(updated))
	sess.ApplyEventFiltering(options...)
	return sess, nil
}

func (s *Service) DeleteSession(context.Context, session.Key, ...session.Option) error {
	return ErrFencedSessionWriteUnsupported
}
func (s *Service) UpdateAppState(context.Context, string, session.StateMap) error {
	return ErrFencedSessionWriteUnsupported
}
func (s *Service) DeleteAppState(context.Context, string, string) error {
	return ErrFencedSessionWriteUnsupported
}
func (s *Service) ListAppStates(context.Context, string) (session.StateMap, error) {
	return session.StateMap{}, nil
}
func (s *Service) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return ErrFencedSessionWriteUnsupported
}
func (s *Service) ListUserStates(context.Context, session.UserKey) (session.StateMap, error) {
	return session.StateMap{}, nil
}
func (s *Service) DeleteUserState(context.Context, session.UserKey, string) error {
	return ErrFencedSessionWriteUnsupported
}

func (s *Service) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return StageSessionState(ctx, key, state)
}

func (s *Service) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, _ ...session.Option) error {
	s.mu.RLock()
	limits := s.limits
	s.mu.RUnlock()
	return StageEvent(ctx, sess, e, limits)
}

func (s *Service) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	return nil
}
func (s *Service) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	return nil
}
func (s *Service) GetSessionSummaryText(context.Context, *session.Session, ...session.SummaryOption) (string, bool) {
	return "", false
}

// Prepare returns a staged commit without writing Redis. The messaging store
// uses this form to atomically commit Session and Inbox state in one Lua call.
func (s *Service) Prepare(turn *Turn) (TurnCommit, error) {
	return PrepareTurn(turn)
}
func (s *Service) Discard(turn *Turn) {
	DiscardTurn(turn)
}

func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	return s.client.Close()
}

func (s *Service) userIndexKey(userCoord string) string {
	return keyspace.FencedUserIndex(s.rootPrefix, userCoord)
}
func (s *Service) metaCoordKey(coord string) string {
	return keyspace.FencedMeta(s.rootPrefix, coord)
}
func (s *Service) eventsCoordKey(coord string) string {
	return keyspace.FencedEvents(s.rootPrefix, coord)
}

func UserCoord(key session.UserKey) string { return keyspace.UserCoord(key.AppName, key.UserID) }
func UserCoordFor(parts ...string) string  { return keyspace.UserCoord(parts...) }
func SessionCoordFor(tenantID, channelBindingID, runnerUserID, sessionID string) string {
	return keyspace.SessionCoord(tenantID, channelBindingID, runnerUserID, sessionID)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func cloneState(in session.StateMap) session.StateMap {
	out := make(session.StateMap, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

var _ session.Service = (*Service)(nil)
var _ StagingSession = (*Service)(nil)
