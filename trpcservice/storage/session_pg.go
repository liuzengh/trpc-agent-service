package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// PGSessionService implements the framework session.Service over the
// platform's session / session_event / summary tables — the "event append +
// state snapshot" two-layer store:
//
//   - session_event is append-only; the UNIQUE (session_id, event_seq)
//     constraint keeps events ordered and gapless, and violating it fails the
//     whole append transaction so the journal and the state snapshot can never
//     drift apart. Redeliveries are handled one level up, so this constraint
//     only orders events within a single session.
//   - session.state is a materialized snapshot for fast reads; the event
//     stream is the source of truth a crashed session can be replayed from.
//   - summary compresses old events: GetSession replays only the events
//     after covered_event_id and exposes the summary under
//     Session.Summaries, so long sessions replay only the uncovered tail.
//   - App/user-scoped state (app:/user: prefixes) is not supported by this
//     backend: the platform's agent definitions don't use those scopes.
//
// Key mapping: framework Key{AppName, UserID, SessionID} →
// session{app_id, user_id, session_key}; AppName is the agent_app UUID the
// per-app assembler stamps onto the runner.
type PGSessionService struct {
	pool    *pgxpool.Pool
	tenants *appTenantResolver

	// summarizer, when set, enables the summary pipeline: the runner calls
	// EnqueueSummaryJob after every event append, and the background worker
	// summarizes sessions whose uncovered events pass the threshold.
	summarizer summary.SessionSummarizer
	jobs       chan summaryJob
	stop       chan struct{}
	wg         sync.WaitGroup
}

// summaryJob is one queued summarization request.
type summaryJob struct {
	key       session.Key
	filterKey string
	force     bool
}

// summaryQueueSize bounds pending jobs; a full queue drops the job (the next
// appended event re-enqueues), so a summarizer outage cannot block the worker.
const summaryQueueSize = 256

// PGSessionOption customizes the PG session service.
type PGSessionOption func(*PGSessionService)

// WithSummarizer enables asynchronous summary generation (framework
// summarizer, event-count threshold) persisted into the summary table.
func WithSummarizer(s summary.SessionSummarizer) PGSessionOption {
	return func(svc *PGSessionService) {
		svc.summarizer = s
	}
}

// NewPGSessionService creates the service on an established pool.
func NewPGSessionService(pool *pgxpool.Pool, opts ...PGSessionOption) *PGSessionService {
	s := &PGSessionService{pool: pool, tenants: newAppTenantResolver(pool)}
	for _, opt := range opts {
		opt(s)
	}
	if s.summarizer != nil {
		s.jobs = make(chan summaryJob, summaryQueueSize)
		s.stop = make(chan struct{})
		s.wg.Add(1)
		go s.summaryLoop()
	}
	return s
}

// ErrStateScopeUnsupported is returned by the app/user-scoped state methods:
// this backend stores session-scoped state only.
var ErrStateScopeUnsupported = errors.New("pg session: app/user state scopes are not supported")

// CreateSession implements session.Service. An existing session for the same
// (app_id, session_key) is returned as-is (state is not overwritten).
func (s *PGSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, _ ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	tenantID, err := s.tenantForApp(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	stateJSON, err := encodeState(state)
	if err != nil {
		return nil, err
	}

	sess := &session.Session{
		ID: key.SessionID, AppName: key.AppName, UserID: key.UserID,
		State: session.StateMap{},
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (app_id, session_key) DO NOTHING
		 RETURNING created_at, updated_at`,
		tenantID, key.AppName, key.SessionID, key.UserID, channelOf(key.SessionID), stateJSON,
	).Scan(&sess.CreatedAt, &sess.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Existing session: return it (events are loaded by GetSession).
		existing, err := s.GetSession(ctx, key)
		if err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf("insert session: %w", err)
	}
	for k, v := range state {
		sess.State[k] = v
	}
	return sess, nil
}

// GetSession implements session.Service: loads the snapshot and replays
// events from the journal. When a summary exists, replay is incremental —
// only events after covered_event_id are loaded, and the summary is exposed
// under Session.Summaries (with its boundary) so the framework prepends it
// instead of the compressed history. A missing session returns (nil, nil).
func (s *PGSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	var (
		stateRaw         []byte
		sessID           string
		createdAt, updAt time.Time
	)
	err := s.pool.QueryRow(ctx,
		`SELECT id, state, created_at, updated_at FROM session
		 WHERE app_id = $1 AND session_key = $2`,
		key.AppName, key.SessionID,
	).Scan(&sessID, &stateRaw, &createdAt, &updAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query session: %w", err)
	}

	state, err := decodeState(stateRaw)
	if err != nil {
		return nil, err
	}
	sess := &session.Session{
		ID: key.SessionID, AppName: key.AppName, UserID: key.UserID,
		State: state, CreatedAt: createdAt, UpdatedAt: updAt,
	}
	var afterSeq int64
	sum, has, err := s.loadSummary(ctx, sessID)
	if err != nil {
		return nil, err
	}
	if has {
		afterSeq = sum.seq
		sess.Summaries = map[string]*session.Summary{
			sum.filterKey: {
				Summary:   sum.text,
				UpdatedAt: sum.coveredAt,
				Boundary:  session.NewSummaryBoundaryWithEventID(sum.filterKey, sum.coveredAt, sum.eventID),
			},
		}
	}
	events, err := s.loadEvents(ctx, sessID, afterSeq, opts...)
	if err != nil {
		return nil, err
	}
	sess.Events = events
	return sess, nil
}

// fullJournalTables is a session's complete journal: the hot table plus the
// rows the monthly sweep moved to the archive, in the one event_seq order the
// append path keeps monotonic across both.
const fullJournalTables = `
	SELECT event_seq, event FROM session_event WHERE session_id = $1
	UNION ALL
	SELECT event_seq, event FROM session_event_archive WHERE session_id = $1`

// FullJournal reads every event of the session in event_seq order, ignoring
// the summary cursor and the archive boundary — unlike GetSession, which
// returns only the uncovered tail.
func (s *PGSessionService) FullJournal(ctx context.Context, key session.Key) ([]event.Event, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	sessID, err := s.sessionID(ctx, key)
	if err != nil {
		return nil, err
	}
	if sessID == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event FROM (`+fullJournalTables+`) journal ORDER BY event_seq`, sessID)
	if err != nil {
		return nil, fmt.Errorf("query full journal: %w", err)
	}
	defer rows.Close()

	var out []event.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		var evt event.Event
		if err := json.Unmarshal(raw, &evt); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		out = append(out, evt)
	}
	return out, rows.Err()
}

// FullJournalIDs is FullJournal reduced to the ordered event IDs: IDs alone
// keep a 100k-event session a few hundred kilobytes instead of its full JSON.
// event->>'id' is the framework's event identity, and dual write preserves it
// because the fanout hands both backends the same event — so an ID that
// appears on both sides is the same event.
func (s *PGSessionService) FullJournalIDs(ctx context.Context, key session.Key) ([]string, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	sessID, err := s.sessionID(ctx, key)
	if err != nil {
		return nil, err
	}
	if sessID == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT COALESCE(event->>'id', '') FROM (`+fullJournalTables+`) journal ORDER BY event_seq`, sessID)
	if err != nil {
		return nil, fmt.Errorf("query full journal ids: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan event id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ListSessions implements session.Service.
func (s *PGSessionService) ListSessions(ctx context.Context, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, session_key, created_at, updated_at FROM session
		 WHERE app_id = $1 AND user_id = $2 ORDER BY updated_at DESC`,
		userKey.AppName, userKey.UserID)
	if err != nil {
		return nil, fmt.Errorf("query sessions: %w", err)
	}
	defer rows.Close()

	onlyMeta := applySessionOpts(opts).ListSessionOnlyMeta
	var out []*session.Session
	for rows.Next() {
		var sessID, sessionKey string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&sessID, &sessionKey, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		sess := &session.Session{
			ID: sessionKey, AppName: userKey.AppName, UserID: userKey.UserID,
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		}
		if !onlyMeta {
			events, err := s.loadEvents(ctx, sessID, 0)
			if err != nil {
				return nil, err
			}
			sess.Events = events
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

// DeleteSession implements session.Service: events and summary go first
// (FK children), then the session row.
func (s *PGSessionService) DeleteSession(ctx context.Context, key session.Key, _ ...session.Option) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sessID string
	err = tx.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id = $1 AND session_key = $2 FOR UPDATE`,
		key.AppName, key.SessionID).Scan(&sessID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // already gone
	}
	if err != nil {
		return fmt.Errorf("find session: %w", err)
	}
	// Children first: neither FK declares ON DELETE CASCADE, so deleting the
	// session row while events still reference it would violate the FK. The
	// archive has no FK at all, so it would otherwise keep the conversation
	// forever — a deletion that leaves the content behind is not a deletion.
	for _, q := range []string{
		`DELETE FROM session_event WHERE session_id = $1`,
		`DELETE FROM session_event_archive WHERE session_id = $1`,
		`DELETE FROM summary WHERE session_id = $1`,
	} {
		if _, err := tx.Exec(ctx, q, sessID); err != nil {
			return fmt.Errorf("delete children: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM session WHERE id = $1`, sessID); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return tx.Commit(ctx)
}

// AppendEvent implements session.Service: the canonical in-memory update runs
// first (framework semantics), then event + snapshot persist in one
// transaction. The session row is locked FOR UPDATE so concurrent appends
// serialize, and the next event_seq is taken across the hot table and the
// archive so it stays monotonic for the whole life of the session — archival
// must not rewind it. If the lock is ever lost the unique constraint aborts
// the transaction instead of letting the snapshot advance past a dropped
// event.
func (s *PGSessionService) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	if sess == nil || e == nil {
		return errors.New("pg session: nil session or event")
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}

	before := sess.GetEventCount()
	sess.UpdateUserSession(e, opts...) // append valid events + apply state delta
	journal := sess.GetEventCount() > before

	eventJSON, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	stateJSON, err := encodeState(sess.State)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sessID, err := s.ensureSession(ctx, tx, key, stateJSON)
	if err != nil {
		return err
	}
	if journal {
		var seq int64
		// The archive counts too. The monthly sweep moves old events out of the
		// hot table, so a session whose events were all archived — a user
		// returning after the retention window — would restart at 1 and reuse
		// seqs that still exist in session_event_archive. Reused seqs are
		// invisible to the replay cursor (event_seq > afterSeq), and the next
		// archive pass would violate uk_session_event_archive_seq, a conflict
		// the copy's ON CONFLICT (id) cannot absorb — wedging every later
		// sweep. The FOR UPDATE on the session row keeps the result gapless.
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(event_seq), 0) + 1 FROM (
				SELECT event_seq FROM session_event WHERE session_id = $1
				UNION ALL
				SELECT event_seq FROM session_event_archive WHERE session_id = $1
			) journal`,
			sessID).Scan(&seq); err != nil {
			return fmt.Errorf("next event_seq: %w", err)
		}
		// No ON CONFLICT: a collision means an invariant broke (a writer
		// outside the session lock), so failing rolls the journal and the
		// snapshot back together instead of leaving the state ahead of the
		// journal.
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_event (session_id, event_seq, event) VALUES ($1, $2, $3)`,
			sessID, seq, eventJSON); err != nil {
			return fmt.Errorf("append event (seq %d): %w", seq, err)
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session SET state = $2, updated_at = now() WHERE id = $1`,
		sessID, stateJSON); err != nil {
		return fmt.Errorf("update snapshot: %w", err)
	}
	return tx.Commit(ctx)
}

// UpdateSessionState implements session.Service: snapshot-only merge without
// appending an event. Keys with app:/user: prefixes are rejected (scoped
// methods are unsupported); a nil value deletes the key.
func (s *PGSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	for k := range state {
		if strings.HasPrefix(k, session.StateAppPrefix) || strings.HasPrefix(k, session.StateUserPrefix) {
			return fmt.Errorf("pg session: key %q needs the app/user-scoped methods, which are unsupported", k)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sessID, err := s.ensureSession(ctx, tx, key, nil)
	if err != nil {
		return err
	}
	var stateRaw []byte
	if err := tx.QueryRow(ctx, `SELECT state FROM session WHERE id = $1 FOR UPDATE`, sessID).Scan(&stateRaw); err != nil {
		return fmt.Errorf("lock snapshot: %w", err)
	}
	current, err := decodeState(stateRaw)
	if err != nil {
		return err
	}
	for k, v := range state {
		if v == nil {
			delete(current, k)
		} else {
			current[k] = v
		}
	}
	merged, err := encodeState(current)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE session SET state = $2, updated_at = now() WHERE id = $1`, sessID, merged); err != nil {
		return fmt.Errorf("update snapshot: %w", err)
	}
	return tx.Commit(ctx)
}

// CreateSessionSummary implements session.Service: summarize now, in the
// caller's context. The runner triggers it through EnqueueSummaryJob; this
// synchronous entry exists for the framework contract and for tests.
func (s *PGSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if s.summarizer == nil || sess == nil {
		return nil
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	return s.summarize(ctx, key, filterKey, force)
}

// EnqueueSummaryJob implements session.Service: summarization is asynchronous.
// The job
// carries only the session key; the worker reloads the session from PG, so a
// job survives the enqueueing worker's request context.
func (s *PGSessionService) EnqueueSummaryJob(_ context.Context, sess *session.Session, filterKey string, force bool) error {
	if s.summarizer == nil || sess == nil {
		return nil
	}
	job := summaryJob{
		key:       session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID},
		filterKey: filterKey,
		force:     force,
	}
	select {
	case s.jobs <- job:
	default:
		// Queue full: drop. The next appended event enqueues again, so the
		// summary merely lags; blocking the message worker would be worse.
		plog.Warnf("summary queue full, dropping job for session %s", sess.ID)
	}
	return nil
}

// summaryLoop drains the summary queue until Close.
func (s *PGSessionService) summaryLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case job := <-s.jobs:
			// Detached context with a generous deadline: summarization is an
			// LLM call of its own and must not die with the request ctx.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if err := s.summarize(ctx, job.key, job.filterKey, job.force); err != nil {
				plog.Warnf("summarize session %s: %v", job.key.SessionID, err)
			}
			cancel()
		}
	}
}

// summarize reloads the session from PG, asks the framework summarizer for a
// rolling summary, and persists it with the covered_event_id cursor. The
// previous summary is exposed on sess.Summaries so the summarizer rolls
// forward instead of starting over.
func (s *PGSessionService) summarize(ctx context.Context, key session.Key, filterKey string, force bool) error {
	sessID, err := s.sessionID(ctx, key)
	if err != nil {
		return err
	}
	if sessID == "" {
		return nil // session not persisted yet; the next append retries
	}

	sess := &session.Session{
		ID: key.SessionID, AppName: key.AppName, UserID: key.UserID,
		State:     session.StateMap{},
		Summaries: make(map[string]*session.Summary),
	}
	prev, has, err := s.loadSummary(ctx, sessID)
	if err != nil {
		return err
	}
	if has {
		sess.Summaries[prev.filterKey] = &session.Summary{
			Summary:   prev.text,
			UpdatedAt: prev.coveredAt,
			Boundary:  session.NewSummaryBoundaryWithEventID(prev.filterKey, prev.coveredAt, prev.eventID),
		}
	}
	rows, err := s.loadEventRows(ctx, sessID, 0)
	if err != nil {
		return err
	}
	for _, r := range rows {
		sess.Events = append(sess.Events, r.evt)
	}
	if len(rows) == 0 {
		return nil
	}

	if !force && !s.summarizer.ShouldSummarize(sess) {
		return nil
	}
	text, err := s.summarizer.Summarize(ctx, sess)
	if err != nil {
		return fmt.Errorf("summarize: %w", err)
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}

	// The summary covers everything the summarizer saw: all events loaded
	// above. Persist with a cursor guard so a stale concurrent job cannot
	// move the cursor backwards.
	covered := rows[len(rows)-1]
	_, err = s.pool.Exec(ctx,
		`INSERT INTO summary (session_id, summary_text, covered_event_id, filter_key, updated_at)
		 VALUES ($1, $2, $3, $4, now())
		 ON CONFLICT (session_id) DO UPDATE
		 SET summary_text = $2, covered_event_id = $3, filter_key = $4, updated_at = now()
		 WHERE COALESCE((SELECT event_seq FROM session_event WHERE id = summary.covered_event_id), -1) <= $5`,
		sessID, text, covered.id, filterKey, covered.seq)
	if err != nil {
		return fmt.Errorf("upsert summary: %w", err)
	}
	plog.Debugf("summary updated for session %s (covered seq %d)", key.SessionID, covered.seq)
	return nil
}

// GetSessionSummaryText implements session.Service: reads the summary table.
func (s *PGSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, _ ...session.SummaryOption) (string, bool) {
	if sess == nil {
		return "", false
	}
	var text string
	err := s.pool.QueryRow(ctx,
		`SELECT summary_text FROM summary s
		 JOIN session se ON se.id = s.session_id
		 WHERE se.app_id = $1 AND se.session_key = $2`,
		sess.AppName, sess.ID).Scan(&text)
	if err != nil {
		return "", false
	}
	return text, true
}

// UpdateAppState implements session.Service.
func (s *PGSessionService) UpdateAppState(context.Context, string, session.StateMap) error {
	return ErrStateScopeUnsupported
}

// DeleteAppState implements session.Service.
func (s *PGSessionService) DeleteAppState(context.Context, string, string) error {
	return ErrStateScopeUnsupported
}

// ListAppStates implements session.Service.
func (s *PGSessionService) ListAppStates(context.Context, string) (session.StateMap, error) {
	return nil, ErrStateScopeUnsupported
}

// UpdateUserState implements session.Service.
func (s *PGSessionService) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return ErrStateScopeUnsupported
}

// DeleteUserState implements session.Service.
func (s *PGSessionService) DeleteUserState(context.Context, session.UserKey, string) error {
	return ErrStateScopeUnsupported
}

// ListUserStates implements session.Service.
func (s *PGSessionService) ListUserStates(context.Context, session.UserKey) (session.StateMap, error) {
	return nil, ErrStateScopeUnsupported
}

// Close implements session.Service: stops the summary worker and waits for
// the in-flight job. The pool is owned by the caller.
func (s *PGSessionService) Close() error {
	if s.stop != nil {
		close(s.stop)
		s.wg.Wait()
	}
	return nil
}

// ensureSession returns the internal session UUID, inserting the row when
// missing, and locks it FOR UPDATE (must run inside a transaction).
//
// The insert uses ON CONFLICT DO NOTHING with one bounded re-select instead
// of a bare INSERT: two concurrent first messages of the same new session
// (lock TTL expiry, two replicas) can both miss the SELECT, and the conflict
// path lets the loser re-lock the winner's row and proceed.
func (s *PGSessionService) ensureSession(ctx context.Context, tx pgx.Tx, key session.Key, stateJSON []byte) (string, error) {
	for attempt := 0; attempt < 3; attempt++ {
		var sessID string
		err := tx.QueryRow(ctx,
			`SELECT id FROM session WHERE app_id = $1 AND session_key = $2 FOR UPDATE`,
			key.AppName, key.SessionID).Scan(&sessID)
		if err == nil {
			return sessID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("lock session: %w", err)
		}

		tenantID, err := s.tenantForApp(ctx, key.AppName)
		if err != nil {
			return "", err
		}
		if stateJSON == nil {
			stateJSON = []byte(`{}`)
		}
		err = tx.QueryRow(ctx,
			`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (app_id, session_key) DO NOTHING
			 RETURNING id`,
			tenantID, key.AppName, key.SessionID, key.UserID, channelOf(key.SessionID), stateJSON,
		).Scan(&sessID)
		if err == nil {
			return sessID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("insert session: %w", err)
		}
		// Lost the insert race: loop once more — the SELECT will now lock the
		// winner's row and succeed.
	}
	return "", fmt.Errorf("ensure session %s: concurrent insert did not settle", key.SessionID)
}

// loadEvents replays the journal in event_seq order, incrementally from
// afterSeq (0 = full replay), applying the EventTime / EventNum options in
// memory (per-session volumes are modest).
func (s *PGSessionService) loadEvents(ctx context.Context, sessID string, afterSeq int64, opts ...session.Option) ([]event.Event, error) {
	rows, err := s.loadEventRows(ctx, sessID, afterSeq)
	if err != nil {
		return nil, err
	}
	events := make([]event.Event, 0, len(rows))
	for _, r := range rows {
		events = append(events, r.evt)
	}

	o := applySessionOpts(opts)
	if !o.EventTime.IsZero() {
		kept := events[:0]
		for _, e := range events {
			if !e.Timestamp.Before(o.EventTime) {
				kept = append(kept, e)
			}
		}
		events = kept
	}
	if o.EventNum > 0 && len(events) > o.EventNum {
		events = events[len(events)-o.EventNum:]
	}
	return events, nil
}

// eventRow is one journaled event with its storage coordinates.
type eventRow struct {
	id        string // session_event row UUID (the covered_event_id target)
	seq       int64
	createdAt time.Time
	evt       event.Event
}

// loadEventRows loads events with their row IDs and seqs, in seq order,
// incrementally from afterSeq (0 = from the beginning).
func (s *PGSessionService) loadEventRows(ctx context.Context, sessID string, afterSeq int64) ([]eventRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, event_seq, created_at, event FROM session_event
		 WHERE session_id = $1 AND event_seq > $2 ORDER BY event_seq`, sessID, afterSeq)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var out []eventRow
	for rows.Next() {
		var r eventRow
		var raw []byte
		if err := rows.Scan(&r.id, &r.seq, &r.createdAt, &raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if err := json.Unmarshal(raw, &r.evt); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// summaryRow is the summary table row with the covered event's position
// resolved (the covered row lives in session_event, or in the archive after
// the monthly task has moved it).
type summaryRow struct {
	text      string
	filterKey string
	seq       int64     // covered event_seq: replay resumes after it
	eventID   string    // covered framework event ID (the prompt-side boundary)
	coveredAt time.Time // covered event creation time
}

// loadSummary reads the summary row and resolves its covered event; a covered
// event that no longer exists anywhere invalidates the row (treated as
// absent, so the session falls back to a full replay).
func (s *PGSessionService) loadSummary(ctx context.Context, sessID string) (summaryRow, bool, error) {
	var (
		sum       summaryRow
		coveredID string
		seq       *int64
		createdAt *time.Time
		eventRaw  []byte
	)
	err := s.pool.QueryRow(ctx,
		`SELECT s.summary_text, s.filter_key, s.covered_event_id,
		        COALESCE(e.event_seq, a.event_seq),
		        COALESCE(e.created_at, a.created_at),
		        COALESCE(e.event, a.event)
		 FROM summary s
		 LEFT JOIN session_event e ON e.id = s.covered_event_id
		 LEFT JOIN session_event_archive a ON a.id = s.covered_event_id
		 WHERE s.session_id = $1`, sessID,
	).Scan(&sum.text, &sum.filterKey, &coveredID, &seq, &createdAt, &eventRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return summaryRow{}, false, nil
	}
	if err != nil {
		return summaryRow{}, false, fmt.Errorf("query summary: %w", err)
	}
	if seq == nil || createdAt == nil {
		// Covered event vanished without archival (e.g. manual cleanup):
		// without its position the replay boundary is unknowable, so the
		// session falls back to a full replay with the summary unused.
		plog.Warnf("summary for session %s references missing event %s, ignoring", sessID, coveredID)
		return summaryRow{}, false, nil
	}
	sum.seq = *seq
	sum.coveredAt = *createdAt
	var evt event.Event
	if err := json.Unmarshal(eventRaw, &evt); err != nil {
		return summaryRow{}, false, fmt.Errorf("decode covered event: %w", err)
	}
	sum.eventID = evt.ID
	return sum, true, nil
}

// sessionID returns the internal session UUID, "" when the session does not
// exist yet.
func (s *PGSessionService) sessionID(ctx context.Context, key session.Key) (string, error) {
	var sessID string
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id = $1 AND session_key = $2`,
		key.AppName, key.SessionID).Scan(&sessID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query session id: %w", err)
	}
	return sessID, nil
}

// tenantForApp resolves a session row's tenant_id from its agent_app via the
// shared cached resolver.
func (s *PGSessionService) tenantForApp(ctx context.Context, appID string) (string, error) {
	return s.tenants.resolve(ctx, appID)
}

// channelOf extracts the channel segment from a session key
// (dm:{channel}:{user} / group:{channel}:{chat}); "unknown" when malformed.
func channelOf(sessionKey string) string {
	parts := strings.SplitN(sessionKey, ":", 3)
	if len(parts) >= 2 {
		return parts[1]
	}
	return "unknown"
}

// applySessionOpts evaluates the framework's functional options.
func applySessionOpts(opts []session.Option) *session.Options {
	o := &session.Options{}
	for _, fn := range opts {
		if fn != nil {
			fn(o)
		}
	}
	return o
}

// encodeState serializes the state snapshot; values are raw JSON by framework
// convention, so they embed cleanly into the jsonb column.
func encodeState(state session.StateMap) ([]byte, error) {
	raw := make(map[string]json.RawMessage, len(state))
	for k, v := range state {
		if len(v) == 0 {
			continue
		}
		raw[k] = json.RawMessage(v)
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}
	return data, nil
}

func decodeState(data []byte) (session.StateMap, error) {
	out := session.StateMap{}
	if len(data) == 0 {
		return out, nil
	}
	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}
	for k, v := range raw {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}
