package sessionturn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	summarypkg "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

var (
	// ErrSessionServiceClosed indicates that Close has already been called.
	ErrSessionServiceClosed = errors.New("sessionturn: session service is closed")
	// ErrTurnScopeClosed indicates that a staged turn is replay-only, committed,
	// aborted, or sealed after a failed commit.
	ErrTurnScopeClosed = errors.New("sessionturn: staged turn is closed")
	// ErrTurnScopeMismatch indicates that a turn context was used with another
	// service or another session key.
	ErrTurnScopeMismatch = errors.New("sessionturn: staged turn scope mismatch")
	// ErrScopedStateUnsupported rejects app:/user: writes through a strict
	// session turn. Those scopes have their own persistence lifecycle.
	ErrScopedStateUnsupported = errors.New("sessionturn: app/user state is not writable in a strict turn")
	// ErrSummaryUnsupported indicates that no SQL summary generator was
	// configured for this service instance.
	ErrSummaryUnsupported = errors.New("sessionturn: session summaries are disabled")
	// ErrSessionExists indicates CreateSession targeted an existing session.
	ErrSessionExists = errors.New("sessionturn: session already exists")
)

// TransactionalService is a framework session.Service with an explicit strict
// turn boundary. The context returned by BeginTurn must be passed to Runner so
// all session writes are staged in the matching Turn.
type TransactionalService interface {
	session.Service
	BeginTurn(context.Context, session.Key, string) (context.Context, Turn, error)
}

// Turn is one local execution of a stable logical session turn.
type Turn interface {
	// Replay returns the canonical payload when this logical turn was already
	// committed (or after Commit resolves to a canonical committed payload).
	Replay() ([]byte, bool)
	// Commit atomically persists staged events, session state, and replay data.
	// The bool is true when PostgreSQL already held the canonical commit.
	Commit(context.Context, []byte) ([]byte, bool, error)
	// Abort closes only this in-process staging scope. It deliberately does not
	// call Postgres.Abort, so a transient worker retry can take over the stable
	// turn ID and fence this execution.
	Abort()
	// Err returns the first staging or commit error.
	Err() error
}

// AtomicTurn is the optional PostgreSQL extension used when another durable
// subsystem must commit beside the Session turn. The participant runs after
// the canonical turn row is known and before the single transaction commits.
// Callers must fail closed when they require this guarantee and Turn does not
// implement AtomicTurn.
type AtomicTurn interface {
	Turn
	CommitWithParticipant(context.Context, []byte, CommitParticipant) ([]byte, bool, error)
}

var (
	_ TransactionalService                   = (*SessionService)(nil)
	_ interface{ DatabaseIdentity() string } = (*SessionService)(nil)
	_ Turn                                   = (*stagedTurn)(nil)
	_ AtomicTurn                             = (*stagedTurn)(nil)
)

// SessionService adapts strict PostgreSQL turns to trpc-agent-go's session
// Service. It intentionally does not implement optional Track, Window, Search,
// or StateInitialization interfaces.
type SessionService struct {
	store         *Postgres
	tenantID      string
	ownedPool     *pgxpool.Pool
	closed        atomic.Bool
	closeOnce     sync.Once
	summaryMu     sync.RWMutex
	summarizer    summarypkg.SessionSummarizer
	summaryCancel context.CancelFunc
	summaryWG     sync.WaitGroup
	summaryOwner  string
}

// NewSessionService creates a non-owning adapter. Close does not close the
// Postgres pool supplied by the caller.
func NewSessionService(store *Postgres) (*SessionService, error) {
	return newSessionService(store, "")
}

// NewSessionServiceForTenant binds summary rows and jobs to the immutable
// tenant identity that owns this runtime. Session event tables remain keyed by
// the opaque app namespace, while the explicit tenant column prevents an
// accidentally reused namespace from crossing the lifecycle control plane.
func NewSessionServiceForTenant(store *Postgres, tenantID string) (*SessionService, error) {
	return newSessionService(store, tenantID)
}

func newSessionService(store *Postgres, tenantID string) (*SessionService, error) {
	if store == nil || store.pool == nil {
		return nil, fmt.Errorf("%w: postgres turn store is nil", ErrInvalidRequest)
	}
	return &SessionService{store: store, tenantID: tenantID}, nil
}

// ConfigureSummarizer enables durable summary generation. It must be called
// before the service is handed to a runner; the worker itself claims jobs from
// PostgreSQL so another node can resume them after a restart.
func (s *SessionService) ConfigureSummarizer(summarizer summarypkg.SessionSummarizer) error {
	if summarizer == nil {
		return ErrSummaryUnsupported
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	if s.summarizer != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.summarizer = summarizer
	s.summaryCancel = cancel
	s.summaryOwner = "summary-" + uuid.NewString()
	s.summaryWG.Add(1)
	go s.summaryWorker(ctx)
	return nil
}

// OpenSessionService opens and owns a PostgreSQL pool, verifies connectivity,
// and read-only verifies the independently migrated strict-turn schema.
// Returned errors never echo the supplied DSN (and therefore cannot expose
// credentials embedded in it).
func OpenSessionService(ctx context.Context, dsn string) (*SessionService, error) {
	return openSessionService(ctx, dsn, "")
}

// OpenSessionServiceForTenant is the production constructor used by a
// tenant-bound Runtime. The pool and strict-turn behavior are identical to
// OpenSessionService; only lifecycle summary rows receive an explicit tenant
// scope.
func OpenSessionServiceForTenant(ctx context.Context, dsn, tenantID string) (*SessionService, error) {
	return openSessionService(ctx, dsn, tenantID)
}

func openSessionService(ctx context.Context, dsn, tenantID string) (*SessionService, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: postgres DSN is required", ErrInvalidRequest)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, safeOpenError(ctx, "configure PostgreSQL")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return nil, safeOpenError(ctx, "connect to PostgreSQL")
	}
	store, err := NewPostgres(pool)
	if err != nil {
		return nil, safeOpenError(ctx, "initialize PostgreSQL store")
	}
	if err := store.VerifySchema(ctx); err != nil {
		return nil, safeOpenError(ctx, "verify PostgreSQL schema")
	}
	closeOnError = false
	return &SessionService{store: store, tenantID: tenantID, ownedPool: pool}, nil
}

// DatabaseIdentity exposes the strict turn store's credential-free database
// binding. It remains stable for the lifetime of the configured pool and is
// used to reject cross-database atomic participants before execution starts.
func (s *SessionService) DatabaseIdentity() string {
	if s == nil || s.store == nil {
		return ""
	}
	return s.store.DatabaseIdentity()
}

func safeOpenError(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sessionturn: %s: %w", stage, err)
	}
	return fmt.Errorf("sessionturn: %s failed", stage)
}

// BeginTurn begins or takes over one stable logical turn. A committed turn is
// represented by a replay-only Turn; callers must skip Runner in that case.
// App/user overlays are loaded immediately after the strict session snapshot;
// they are durable but deliberately outside the turn's atomic commit boundary.
func (s *SessionService) BeginTurn(
	ctx context.Context,
	key session.Key,
	idempotencyKey string,
) (context.Context, Turn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.ensureOpen(); err != nil {
		return ctx, nil, err
	}
	if err := validateKey(key); err != nil {
		return ctx, nil, err
	}
	if prior, ok := ctx.Value(turnContextKey{}).(*stagedTurn); ok && prior != nil {
		return ctx, nil, fmt.Errorf("%w: a turn is already attached to context", ErrTurnScopeMismatch)
	}
	turnID, err := DeriveTurnID(key, idempotencyKey)
	if err != nil {
		return ctx, nil, err
	}
	begun, err := s.store.Begin(ctx, BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		return ctx, nil, err
	}
	t := &stagedTurn{
		service: s,
		key:     key,
		handle:  begun.Handle,
		phase:   turnPhaseActive,
	}
	if begun.Replayed {
		t.phase = turnPhaseReplayed
		t.replay = append([]byte(nil), begun.Replay...)
		t.replayAvailable = true
		return context.WithValue(ctx, turnContextKey{}, t), t, nil
	}
	if begun.Snapshot == nil {
		return ctx, nil, fmt.Errorf("%w: fresh Begin returned no snapshot", ErrCorruptData)
	}
	appState, err := s.store.ListAppStates(ctx, key.AppName)
	if err != nil {
		return ctx, nil, fmt.Errorf("sessionturn: load application state for turn: %w", err)
	}
	userState, err := s.store.ListUserStates(ctx, session.UserKey{
		AppName: key.AppName,
		UserID:  key.UserID,
	})
	if err != nil {
		return ctx, nil, fmt.Errorf("sessionturn: load user state for turn: %w", err)
	}
	t.session = begun.Snapshot.TRPCSession()
	if err := s.applySummaries(ctx, key, begun.Snapshot, t.session); err != nil {
		return ctx, nil, err
	}
	t.overlay = mergeStateOverlay(t.session, appState, userState)
	return context.WithValue(ctx, turnContextKey{}, t), t, nil
}

// CreateSession creates a persisted session outside a turn. Inside a matching
// turn it initializes the staged session and performs no database write.
func (s *SessionService) CreateSession(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
	_ ...session.Option,
) (*session.Session, error) {
	if err := key.CheckUserKey(); err != nil {
		return nil, err
	}
	if key.SessionID == "" {
		key.SessionID = uuid.NewString()
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return nil, err
	} else if ok {
		return t.initializeSession(key, state)
	}
	if err := validateSessionState(state); err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	_, err := s.store.Create(normalizeContext(ctx), key, state)
	if err != nil {
		return nil, err
	}
	return s.GetSession(ctx, key)
}

// GetSession returns the turn-local staged session for a matching turn
// context. Outside a turn it loads the persisted snapshot and state overlays.
func (s *SessionService) GetSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	opt := applySessionOptions(opts...)
	if err := session.ValidateGetSessionOptions(opt, true); err != nil {
		return nil, err
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return nil, err
	} else if ok {
		return t.sessionView(key, opt)
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	snapshot, err := s.store.Load(normalizeContext(ctx), key)
	if err != nil || snapshot == nil {
		return nil, err
	}
	sess := snapshot.TRPCSession()
	if err := s.applySummaries(ctx, key, snapshot, sess); err != nil {
		return nil, err
	}
	if err := s.applyOverlay(normalizeContext(ctx), sess); err != nil {
		return nil, err
	}
	return filteredSessionView(sess, opt)
}

// ListSessions loads persisted sessions outside a strict turn.
func (s *SessionService) ListSessions(
	ctx context.Context,
	userKey session.UserKey,
	opts ...session.Option,
) ([]*session.Session, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	opt := applySessionOptions(opts...)
	if err := session.ValidateListSessionsOptions(opt); err != nil {
		return nil, err
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return nil, err
	} else if ok {
		err := fmt.Errorf("%w: ListSessions is unavailable inside a strict turn", ErrTurnScopeMismatch)
		t.fail(err)
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	ctx = normalizeContext(ctx)
	snapshots, err := s.store.List(ctx, userKey)
	if err != nil {
		return nil, err
	}
	appState, err := s.store.ListAppStates(ctx, userKey.AppName)
	if err != nil {
		return nil, err
	}
	userState, err := s.store.ListUserStates(ctx, userKey)
	if err != nil {
		return nil, err
	}
	result := make([]*session.Session, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		sess := snapshot.TRPCSession()
		if err := s.applySummaries(ctx, snapshot.Key, snapshot, sess); err != nil {
			return nil, err
		}
		mergeStateOverlay(sess, appState, userState)
		if opt.ListSessionOnlyMeta {
			sess = metadataOnlySession(sess)
		} else {
			sess, err = filteredSessionView(sess, opt)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, sess)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].UpdatedAt.After(result[j].UpdatedAt)
		}
		return result[i].ID > result[j].ID
	})
	return applySessionListPage(result, opt), nil
}

// DeleteSession deletes a persisted session outside a strict turn.
func (s *SessionService) DeleteSession(
	ctx context.Context,
	key session.Key,
	_ ...session.Option,
) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return err
	} else if ok {
		err := fmt.Errorf("%w: DeleteSession is unavailable inside a strict turn", ErrTurnScopeMismatch)
		t.fail(err)
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.store.Delete(normalizeContext(ctx), key)
}

// UpdateAppState updates application state outside a strict turn.
func (s *SessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	if appName == "" {
		return session.ErrAppNameRequired
	}
	if err := s.rejectScopedWrite(ctx, "UpdateAppState"); err != nil {
		return err
	}
	if _, err := normalizeAppState(state); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.store.UpdateAppState(normalizeContext(ctx), appName, state)
}

// DeleteAppState deletes application state outside a strict turn.
func (s *SessionService) DeleteAppState(ctx context.Context, appName, key string) error {
	if appName == "" {
		return session.ErrAppNameRequired
	}
	if err := s.rejectScopedWrite(ctx, "DeleteAppState"); err != nil {
		return err
	}
	if _, err := normalizeAppStateKey(key); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.store.DeleteAppState(normalizeContext(ctx), appName, key)
}

// ListAppStates returns application state without app: prefixes.
func (s *SessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s.store.ListAppStates(normalizeContext(ctx), appName)
}

// UpdateUserState updates user state outside a strict turn.
func (s *SessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	if err := key.CheckUserKey(); err != nil {
		return err
	}
	if err := s.rejectScopedWrite(ctx, "UpdateUserState"); err != nil {
		return err
	}
	if _, err := normalizeUserState(state); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.store.UpdateUserState(normalizeContext(ctx), key, state)
}

// ListUserStates returns user state without user: prefixes.
func (s *SessionService) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s.store.ListUserStates(normalizeContext(ctx), key)
}

// DeleteUserState deletes user state outside a strict turn.
func (s *SessionService) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	if err := key.CheckUserKey(); err != nil {
		return err
	}
	if err := s.rejectScopedWrite(ctx, "DeleteUserState"); err != nil {
		return err
	}
	if _, err := normalizeUserStateKey(stateKey); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.store.DeleteUserState(normalizeContext(ctx), key, stateKey)
}

// UpdateSessionState stages state in a strict turn or commits it through a
// synthetic one-operation turn outside that boundary.
func (s *SessionService) UpdateSessionState(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return err
	} else if ok {
		return t.updateSessionState(key, state)
	}
	if err := validateSessionState(state); err != nil {
		return err
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	ctx = normalizeContext(ctx)
	existing, err := s.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("sessionturn: update session state: session not found")
	}
	return s.runSynthetic(ctx, key, func(sess *session.Session) ([]event.Event, error) {
		for stateKey, value := range state {
			sess.SetState(stateKey, value)
		}
		return nil, nil
	})
}

// AppendEvent stages an event in a strict turn or commits it through a
// synthetic one-operation turn outside that boundary.
func (s *SessionService) AppendEvent(
	ctx context.Context,
	sess *session.Session,
	evt *event.Event,
	opts ...session.Option,
) error {
	if sess == nil {
		return fmt.Errorf("%w: session is nil", ErrInvalidRequest)
	}
	key := keyFromSession(sess)
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if t, ok, err := s.turnFromContext(ctx); err != nil {
		return err
	} else if ok {
		return t.appendEvent(key, sess, evt, opts...)
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	cloned, err := cloneEventJSON(evt)
	if err != nil {
		return err
	}
	if err := validateEventState(cloned); err != nil {
		return err
	}
	ctx = normalizeContext(ctx)
	existing, err := s.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("sessionturn: append event: session not found")
	}
	err = s.runSynthetic(ctx, key, func(stored *session.Session) ([]event.Event, error) {
		stored.UpdateUserSession(cloned, opts...)
		if eventIsPersistable(cloned) {
			return []event.Event{*cloned}, nil
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	// Preserve the framework contract that the caller's in-memory session is
	// updated, while doing so only after the durable commit succeeds.
	sess.UpdateUserSession(cloned, opts...)
	return nil
}

func (s *SessionService) CreateSessionSummary(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	summaryGenerator := s.getSummarizer()
	if summaryGenerator == nil {
		return ErrSummaryUnsupported
	}
	if sess == nil {
		return fmt.Errorf("%w: session is nil", ErrInvalidRequest)
	}
	if !force && !summaryGenerator.ShouldSummarize(sess) {
		return nil
	}
	write, snapshot, err := s.summaryWrite(ctx, sess, filterKey)
	if err != nil {
		return err
	}
	working := snapshot.TRPCSession()
	if err := s.applySummaries(ctx, snapshot.Key, snapshot, working); err != nil {
		return err
	}
	working.Events = filteredSummaryEvents(working.Events, filterKey)
	text, err := summaryGenerator.Summarize(normalizeContext(ctx), working)
	if err != nil {
		return err
	}
	write.SummaryText = text
	if err := s.store.PutSummary(normalizeContext(ctx), write); err != nil {
		return err
	}
	if stored, ok := s.store.SummaryForTenant(ctx, s.tenantID, snapshot.Key, snapshot, filterKey); ok {
		sess.SummariesMu.Lock()
		if sess.Summaries == nil {
			sess.Summaries = make(map[string]*session.Summary)
		}
		sess.Summaries[filterKey] = stored
		sess.SummariesMu.Unlock()
	}
	return nil
}

func (s *SessionService) EnqueueSummaryJob(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	if sess == nil {
		return fmt.Errorf("%w: session is nil", ErrInvalidRequest)
	}
	if !force {
		if generator := s.getSummarizer(); generator != nil && !generator.ShouldSummarize(sess) {
			return nil
		}
	}
	write, _, err := s.summaryWrite(ctx, sess, filterKey)
	if err != nil {
		return err
	}
	return s.store.EnqueueSummaryJob(normalizeContext(ctx), summaryJob{
		JobID: "summary-" + uuid.NewString(), TenantID: s.tenantID, Key: write.Key, FilterKey: write.FilterKey,
		RequestedFromSequence:    write.CoveredFromSequence,
		RequestedThroughSequence: write.CoveredThroughSequence,
		SessionVersion:           write.SessionVersion, BoundaryVersion: write.BoundaryVersion,
		LastEventID: write.LastEventID, SourceSHA256: write.SourceSHA256,
		GeneratorVersion: write.GeneratorVersion, PromptVersion: write.PromptVersion,
	})
}

func (s *SessionService) GetSessionSummaryText(
	ctx context.Context,
	sess *session.Session,
	opts ...session.SummaryOption,
) (string, bool) {
	if sess == nil || s.ensureOpen() != nil {
		return "", false
	}
	options := &session.SummaryOptions{FilterKey: session.SummaryFilterKeyAllContents}
	for _, option := range opts {
		if option != nil {
			option(options)
		}
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	snapshot, err := s.store.Load(normalizeContext(ctx), key)
	if err != nil || snapshot == nil {
		return "", false
	}
	value, ok := s.store.SummaryForTenant(normalizeContext(ctx), s.tenantID, key, snapshot, options.FilterKey)
	if !ok {
		return "", false
	}
	return value.Summary, true
}

// Close closes the adapter and, for OpenSessionService, its owned pool.
func (s *SessionService) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.summaryMu.Lock()
		if s.summaryCancel != nil {
			s.summaryCancel()
		}
		s.summaryMu.Unlock()
		s.summaryWG.Wait()
		if s.ownedPool != nil {
			s.ownedPool.Close()
		}
	})
	return nil
}

func (s *SessionService) getSummarizer() summarypkg.SessionSummarizer {
	s.summaryMu.RLock()
	defer s.summaryMu.RUnlock()
	return s.summarizer
}

func (s *SessionService) applySummaries(ctx context.Context, key session.Key, snapshot *Snapshot, sess *session.Session) error {
	if snapshot == nil || sess == nil {
		return nil
	}
	summaries, err := s.store.AllSummariesForTenant(normalizeContext(ctx), s.tenantID, key, snapshot)
	if err != nil {
		return err
	}
	sess.SummariesMu.Lock()
	sess.Summaries = summaries
	sess.SummariesMu.Unlock()
	return nil
}

func (s *SessionService) summaryWrite(ctx context.Context, sess *session.Session, filterKey string) (SummaryWrite, *Snapshot, error) {
	key := sessKey(sess)
	if err := key.CheckSessionKey(); err != nil {
		return SummaryWrite{}, nil, err
	}
	snapshot, err := s.store.Load(normalizeContext(ctx), key)
	if err != nil {
		return SummaryWrite{}, nil, err
	}
	if snapshot == nil || snapshot.LastEventSequence == 0 || len(snapshot.Events) == 0 {
		return SummaryWrite{}, nil, fmt.Errorf("%w: no events to summarize", ErrSummaryBoundary)
	}
	through := snapshot.LastEventSequence
	from := int64(1)
	lastID := snapshot.Events[through-1].ID
	metadata := map[string]any{}
	if generator := s.getSummarizer(); generator != nil {
		metadata = generator.Metadata()
	}
	generatorVersion := summaryMetadataString(metadata, "generator_version", defaultSummaryGenerator)
	modelVersion := summaryMetadataString(metadata, "model", "")
	promptVersion := summaryMetadataString(metadata, "prompt", defaultSummaryPrompt)
	return SummaryWrite{
		TenantID: s.tenantID, Key: key, FilterKey: filterKey, CoveredFromSequence: from,
		CoveredThroughSequence: through, LastEventID: lastID,
		SessionVersion: snapshot.Version, SummaryVersion: 1,
		BoundaryVersion:  session.SummaryBoundaryVersion,
		GeneratorVersion: generatorVersion, ModelVersion: modelVersion,
		PromptVersion: promptVersion, SourceSHA256: summarySourceHashSnapshot(snapshot.Events, from, through, filterKey),
		Events: snapshot.Events,
	}, snapshot, nil
}

func sessKey(sess *session.Session) session.Key {
	if sess == nil {
		return session.Key{}
	}
	return session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
}

func summaryMetadataString(metadata map[string]any, key, fallback string) string {
	if value, ok := metadata[key].(string); ok && value != "" {
		return value
	}
	return fallback
}

func filteredSummaryEvents(events []event.Event, filterKey string) []event.Event {
	if filterKey == "" {
		return append([]event.Event(nil), events...)
	}
	result := make([]event.Event, 0, len(events))
	for _, item := range events {
		if item.Filter(filterKey) {
			result = append(result, item)
		}
	}
	return result
}

func (s *SessionService) summaryWorker(ctx context.Context) {
	defer s.summaryWG.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		owner := s.summaryOwner
		job, err := s.store.claimSummaryJob(ctx, owner, defaultSummaryLease)
		if err == nil && job != nil {
			if processErr := s.processSummaryJob(ctx, owner, job); processErr != nil {
				_ = s.store.failSummaryJob(context.Background(), job.JobID, owner, processErr)
			} else {
				_ = s.store.completeSummaryJob(context.Background(), job.JobID, owner)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *SessionService) processSummaryJob(ctx context.Context, owner string, job *summaryJob) error {
	_ = owner
	generator := s.getSummarizer()
	if generator == nil {
		return ErrSummaryUnsupported
	}
	snapshot, err := s.store.Load(normalizeContext(ctx), job.Key)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("%w: session does not exist", ErrSummaryBoundary)
	}
	if job.RequestedThroughSequence > snapshot.LastEventSequence || job.RequestedThroughSequence > int64(len(snapshot.Events)) {
		return ErrSummaryBoundary
	}
	events := snapshot.Events[job.RequestedFromSequence-1 : job.RequestedThroughSequence]
	if summarySourceHashSnapshot(snapshot.Events, job.RequestedFromSequence, job.RequestedThroughSequence, job.FilterKey) != job.SourceSHA256 ||
		snapshot.Events[job.RequestedThroughSequence-1].ID != job.LastEventID {
		return ErrSummaryBoundary
	}
	working := snapshot.TRPCSession()
	working.Events = filteredSummaryEvents(events, job.FilterKey)
	text, err := generator.Summarize(normalizeContext(ctx), working)
	if err != nil {
		return err
	}
	return s.store.PutSummary(normalizeContext(ctx), SummaryWrite{
		TenantID: job.TenantID, Key: job.Key, FilterKey: job.FilterKey,
		CoveredFromSequence:    job.RequestedFromSequence,
		CoveredThroughSequence: job.RequestedThroughSequence,
		LastEventID:            job.LastEventID, SessionVersion: job.SessionVersion,
		SummaryVersion: 1, BoundaryVersion: job.BoundaryVersion,
		GeneratorVersion: job.GeneratorVersion,
		PromptVersion:    job.PromptVersion, SourceSHA256: job.SourceSHA256,
		SummaryText: text, Events: snapshot.Events,
	})
}

func (s *SessionService) ensureOpen() error {
	if s == nil || s.store == nil || s.closed.Load() {
		return ErrSessionServiceClosed
	}
	return nil
}

func (s *SessionService) applyOverlay(ctx context.Context, sess *session.Session) error {
	appState, err := s.store.ListAppStates(ctx, sess.AppName)
	if err != nil {
		return err
	}
	userState, err := s.store.ListUserStates(ctx, session.UserKey{
		AppName: sess.AppName,
		UserID:  sess.UserID,
	})
	if err != nil {
		return err
	}
	mergeStateOverlay(sess, appState, userState)
	return nil
}

func (s *SessionService) runSynthetic(
	ctx context.Context,
	key session.Key,
	mutate func(*session.Session) ([]event.Event, error),
) error {
	turnID := "synthetic_" + uuid.NewString()
	begun, err := s.store.Begin(ctx, BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		return err
	}
	if begun.Replayed || begun.Snapshot == nil {
		return fmt.Errorf("%w: synthetic turn did not return a fresh snapshot", ErrCorruptData)
	}
	sess := begun.Snapshot.TRPCSession()
	events, err := mutate(sess)
	if err != nil {
		return err
	}
	_, err = s.store.Commit(ctx, CommitRequest{
		Handle: begun.Handle,
		Events: events,
		State:  sessionStateWithoutOverlay(sess.SnapshotState()),
	})
	return err
}

func (s *SessionService) rejectScopedWrite(ctx context.Context, operation string) error {
	t, ok, err := s.turnFromContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	err = fmt.Errorf("%w: %s", ErrScopedStateUnsupported, operation)
	t.fail(err)
	return err
}

func (s *SessionService) turnFromContext(ctx context.Context) (*stagedTurn, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	t, ok := ctx.Value(turnContextKey{}).(*stagedTurn)
	if !ok || t == nil {
		return nil, false, nil
	}
	if t.service != s {
		return nil, true, fmt.Errorf("%w: context belongs to another session service", ErrTurnScopeMismatch)
	}
	return t, true, nil
}

type turnContextKey struct{}

type turnPhase uint8

const (
	turnPhaseActive turnPhase = iota
	turnPhaseCommitting
	turnPhaseCommitted
	turnPhaseAborted
	turnPhaseReplayed
)

type stagedTurn struct {
	mu sync.Mutex

	service *SessionService
	key     session.Key
	handle  Handle
	phase   turnPhase
	session *session.Session
	overlay session.StateMap
	events  []event.Event
	err     error

	replay          []byte
	replayAvailable bool
}

func (t *stagedTurn) Replay() ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.replayAvailable {
		return nil, false
	}
	return append([]byte(nil), t.replay...), true
}

func (t *stagedTurn) Commit(ctx context.Context, replay []byte) ([]byte, bool, error) {
	return t.CommitWithParticipant(ctx, replay, nil)
}

func (t *stagedTurn) CommitWithParticipant(
	ctx context.Context,
	replay []byte,
	participant CommitParticipant,
) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.phase {
	case turnPhaseReplayed, turnPhaseCommitted:
		if participant == nil {
			return append([]byte(nil), t.replay...), true, nil
		}
		// Re-enter the store so a rolling-upgrade recovery can complete a
		// legacy committed Turn and its Inbox/Outbox bundle atomically. The
		// persisted canonical replay, not replay, is passed to participant.
	case turnPhaseActive:
		// Continue below while holding the scope lock. This seals the staged
		// snapshot against context.WithoutCancel cleanup writes.
	case turnPhaseCommitting, turnPhaseAborted:
		return nil, false, t.failLocked(ErrTurnScopeClosed)
	default:
		return nil, false, t.failLocked(ErrTurnScopeClosed)
	}
	if t.err != nil {
		return nil, false, t.err
	}
	if err := t.service.ensureOpen(); err != nil {
		return nil, false, t.failLocked(err)
	}
	state := session.StateMap{}
	events := []event.Event(nil)
	if t.phase == turnPhaseActive {
		var err error
		state, err = t.stateForCommitLocked()
		if err != nil {
			return nil, false, t.failLocked(err)
		}
		events = append(events, t.events...)
	}
	t.phase = turnPhaseCommitting
	result, err := t.service.store.CommitWithParticipant(normalizeContext(ctx), CommitRequest{
		Handle: t.handle,
		Events: events,
		State:  state,
		Replay: append([]byte(nil), replay...),
	}, participant)
	if err != nil {
		return nil, false, t.failLocked(err)
	}
	t.phase = turnPhaseCommitted
	t.replay = append([]byte(nil), result.Replay...)
	t.replayAvailable = true
	return append([]byte(nil), result.Replay...), result.Replayed, nil
}

func (t *stagedTurn) Abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase == turnPhaseActive || t.phase == turnPhaseCommitting {
		t.phase = turnPhaseAborted
		_ = t.failLocked(ErrTurnScopeClosed)
	}
}

func (t *stagedTurn) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *stagedTurn) fail(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failLocked(err)
}

func (t *stagedTurn) failLocked(err error) error {
	if err == nil {
		return t.err
	}
	if t.err == nil {
		t.err = err
	}
	return err
}

func (t *stagedTurn) requireActiveKeyLocked(key session.Key) error {
	if t.phase != turnPhaseActive {
		// A replay-only turn can still be re-entered by CommitWithParticipant
		// to finish a rolling-upgrade bundle. A read through its context must
		// report that the scope is closed without poisoning that recovery path.
		if t.phase == turnPhaseReplayed {
			return ErrTurnScopeClosed
		}
		return t.failLocked(ErrTurnScopeClosed)
	}
	if t.err != nil {
		return t.err
	}
	if key != t.key {
		return t.failLocked(fmt.Errorf("%w: got %+v, want %+v", ErrTurnScopeMismatch, key, t.key))
	}
	return nil
}

func (t *stagedTurn) initializeSession(
	key session.Key,
	state session.StateMap,
) (*session.Session, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.requireActiveKeyLocked(key); err != nil {
		return nil, err
	}
	if err := validateSessionState(state); err != nil {
		return nil, t.failLocked(err)
	}
	for stateKey, value := range state {
		t.session.SetState(stateKey, value)
	}
	return t.session, nil
}

func (t *stagedTurn) sessionView(key session.Key, opt *session.Options) (*session.Session, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.requireActiveKeyLocked(key); err != nil {
		return nil, err
	}
	if !sessionViewNeedsCopy(opt) {
		return t.session, nil
	}
	return filteredSessionView(t.session, opt)
}

func (t *stagedTurn) updateSessionState(key session.Key, state session.StateMap) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.requireActiveKeyLocked(key); err != nil {
		return err
	}
	if err := validateSessionState(state); err != nil {
		return t.failLocked(err)
	}
	for stateKey, value := range state {
		t.session.SetState(stateKey, value)
	}
	return nil
}

func (t *stagedTurn) appendEvent(
	key session.Key,
	provided *session.Session,
	evt *event.Event,
	opts ...session.Option,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.requireActiveKeyLocked(key); err != nil {
		return err
	}
	cloned, err := cloneEventJSON(evt)
	if err != nil {
		return t.failLocked(err)
	}
	if err := validateEventState(cloned); err != nil {
		return t.failLocked(err)
	}
	t.session.UpdateUserSession(cloned, opts...)
	if provided != t.session {
		providedClone, cloneErr := cloneEventJSON(cloned)
		if cloneErr != nil {
			return t.failLocked(cloneErr)
		}
		provided.UpdateUserSession(providedClone, opts...)
	}
	if eventIsPersistable(cloned) {
		t.events = append(t.events, *cloned)
	}
	return nil
}

func (t *stagedTurn) stateForCommitLocked() (session.StateMap, error) {
	if t.session == nil || keyFromSession(t.session) != t.key {
		return nil, fmt.Errorf("%w: staged session identity changed", ErrTurnScopeMismatch)
	}
	state := t.session.SnapshotState()
	projected := make(session.StateMap)
	for key, value := range state {
		if strings.HasPrefix(key, session.StateAppPrefix) ||
			strings.HasPrefix(key, session.StateUserPrefix) {
			projected[key] = value
		}
	}
	if !equalState(projected, t.overlay) {
		return nil, ErrScopedStateUnsupported
	}
	return sessionStateWithoutOverlay(state), nil
}

func validateSessionState(state session.StateMap) error {
	for key := range state {
		if strings.HasPrefix(key, session.StateAppPrefix) ||
			strings.HasPrefix(key, session.StateUserPrefix) {
			return fmt.Errorf("%w: state key %q", ErrScopedStateUnsupported, key)
		}
	}
	return nil
}

func validateEventState(evt *event.Event) error {
	if evt == nil {
		return fmt.Errorf("%w: event is nil", ErrInvalidRequest)
	}
	for key := range evt.StateDelta {
		if strings.HasPrefix(key, session.StateAppPrefix) ||
			strings.HasPrefix(key, session.StateUserPrefix) {
			return fmt.Errorf("%w: event state key %q", ErrScopedStateUnsupported, key)
		}
	}
	return nil
}

func cloneEventJSON(evt *event.Event) (*event.Event, error) {
	if evt == nil {
		return nil, fmt.Errorf("%w: event is nil", ErrInvalidRequest)
	}
	encoded, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: deep-copy event: %w", err)
	}
	var cloned event.Event
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return nil, fmt.Errorf("sessionturn: deep-copy event: %w", err)
	}
	return &cloned, nil
}

func cloneSessionDeep(sess *session.Session) (*session.Session, error) {
	if sess == nil {
		return nil, nil
	}
	cloned := sess.Clone()
	events := cloned.GetEvents()
	deepEvents := make([]event.Event, 0, len(events))
	for i := range events {
		deep, err := cloneEventJSON(&events[i])
		if err != nil {
			return nil, err
		}
		deepEvents = append(deepEvents, *deep)
	}
	cloned.EventMu.Lock()
	cloned.Events = deepEvents
	cloned.EventMu.Unlock()
	return cloned, nil
}

func filteredSessionView(sess *session.Session, opt *session.Options) (*session.Session, error) {
	cloned, err := cloneSessionDeep(sess)
	if err != nil {
		return nil, err
	}
	if cloned == nil {
		return nil, nil
	}
	if opt != nil && opt.EventPage != nil {
		cloned.EventMu.Lock()
		cloned.Events = eventPage(cloned.Events, opt.EventPage)
		cloned.EventMu.Unlock()
		return cloned, nil
	}
	if opt != nil {
		cloned.ApplyEventFiltering(
			session.WithEventNum(opt.EventNum),
			session.WithEventTime(opt.EventTime),
		)
	}
	return cloned, nil
}

func eventPage(events []event.Event, page *session.EventPage) []event.Event {
	if page == nil || page.Offset >= len(events) {
		return []event.Event{}
	}
	end := len(events) - page.Offset
	start := end - page.Limit
	if start < 0 {
		start = 0
	}
	result := make([]event.Event, end-start)
	copy(result, events[start:end])
	return result
}

func sessionViewNeedsCopy(opt *session.Options) bool {
	return opt != nil && (opt.EventPage != nil || opt.EventNum != 0 || !opt.EventTime.IsZero())
}

func applySessionOptions(opts ...session.Option) *session.Options {
	opt := &session.Options{}
	for _, apply := range opts {
		if apply != nil {
			apply(opt)
		}
	}
	return opt
}

func applySessionListPage(sessions []*session.Session, opt *session.Options) []*session.Session {
	if opt == nil || opt.ListSessionPage == nil {
		return sessions
	}
	page := opt.ListSessionPage
	if page.Offset >= len(sessions) {
		return []*session.Session{}
	}
	end := page.Offset + page.Limit
	if end > len(sessions) {
		end = len(sessions)
	}
	return sessions[page.Offset:end]
}

func metadataOnlySession(sess *session.Session) *session.Session {
	return session.NewSession(
		sess.AppName,
		sess.UserID,
		sess.ID,
		session.WithSessionState(sess.SnapshotState()),
		session.WithSessionCreatedAt(sess.CreatedAt),
		session.WithSessionUpdatedAt(sess.UpdatedAt),
	)
}

func mergeStateOverlay(
	sess *session.Session,
	appState session.StateMap,
	userState session.StateMap,
) session.StateMap {
	overlay := make(session.StateMap, len(appState)+len(userState))
	for key, value := range appState {
		projected := session.StateAppPrefix + key
		sess.SetState(projected, value)
		overlay[projected] = append([]byte(nil), value...)
		if value == nil {
			overlay[projected] = nil
		}
	}
	for key, value := range userState {
		projected := session.StateUserPrefix + key
		sess.SetState(projected, value)
		overlay[projected] = append([]byte(nil), value...)
		if value == nil {
			overlay[projected] = nil
		}
	}
	return overlay
}

func sessionStateWithoutOverlay(state session.StateMap) session.StateMap {
	result := make(session.StateMap)
	for key, value := range state {
		if strings.HasPrefix(key, session.StateAppPrefix) ||
			strings.HasPrefix(key, session.StateUserPrefix) {
			continue
		}
		result[key] = append([]byte(nil), value...)
		if value == nil {
			result[key] = nil
		}
	}
	return result
}

func equalState(left, right session.StateMap) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || (leftValue == nil) != (rightValue == nil) ||
			!bytes.Equal(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func eventIsPersistable(evt *event.Event) bool {
	return evt != nil && evt.Response != nil && !evt.IsPartial && evt.IsValidContent()
}

func keyFromSession(sess *session.Session) session.Key {
	if sess == nil {
		return session.Key{}
	}
	return session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
