// Package sessionstore isolates one execution attempt's session traffic from
// every shared backend until it is committed through the platform's own
// transaction.
//
// It exists because the framework's Runner writes to its session.Service
// during the run and, once cancelled, keeps writing through
// context.WithoutCancel (trpc-agent-go runner.go). Handing a Runner the
// shared Redis or in-memory service directly means a worker whose lease
// expired mid-flight can still land writes in shared state. A Workspace is
// what a Runner is given instead: nothing it writes escapes here until
// Snapshot is taken and the caller commits it, and Seal cannot be undone by
// an ignored or already-cancelled context.
package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// ErrSealed is returned by every mutating call once Seal has run. It is
// checked before anything else, including ctx.Err(): the framework's
// post-cancel persistence path calls with context.WithoutCancel, so honouring
// the seal cannot depend on the caller's context being alive.
var ErrSealed = errors.New("sessionstore: workspace sealed")

// ErrUnsupportedSharedState is returned by the cross-session state methods.
// The first milestone keeps app- and user-level state out of the execution
// workspace: a per-session fence cannot protect state shared across sessions
// (approved plan, "私有 Session 工作区"). Receiving it is sticky: Seal will
// not be enough on its own to tell the committer to abort, so the error must
// survive to the commit check too.
var ErrUnsupportedSharedState = errors.New("sessionstore: shared app/user state writes are not enabled for this milestone")

// ErrSessionNotFound mirrors the framework's own "session not found"
// condition without reaching into an unexported backend.
var ErrSessionNotFound = errors.New("sessionstore: session not found")

// SummaryIntent records one CreateSessionSummary/EnqueueSummaryJob call.
// Nothing is summarised here; the intent is committed as an outbox job so no
// background thread ever touches shared state outside the platform's own
// transaction.
type SummaryIntent struct {
	FilterKey   string
	Force       bool
	RequestedAt time.Time
}

// Prepared is the frozen, commit-ready view of one attempt. Events are the
// ones appended during this attempt only, in order, with their original IDs
// and Version preserved: session.Service implementations must not decide
// what an event's identity is, and event.Event.Clone mints a new UUID and
// bumps Version, so the workspace keeps its own copy instead of reusing
// Clone for durability.
type Prepared struct {
	Key     session.Key
	Events  []event.Event
	State   session.StateMap
	Summary []SummaryIntent

	// Created is true when the session did not exist before this attempt's
	// CreateSession call; Deleted is true when DeleteSession removed it.
	Created bool
	Deleted bool
}

// Options seeds a Workspace. Base is the committed snapshot this attempt
// starts from; nil means the session does not exist yet and CreateSession
// will establish it.
type Options struct {
	Base *session.Session
}

// Workspace is a single-attempt, session.Service implementation. It is safe
// for concurrent use: the framework's event loop and its post-cancel
// persistence path do not run in lockstep with the caller's Seal.
type Workspace struct {
	mu sync.Mutex

	key     session.Key
	base    *session.Session
	live    *session.Session // canonical local session, reflects every write so far
	created bool
	deleted bool

	events    []event.Event
	summaries []SummaryIntent

	sealed     bool
	sealReason string
	sticky     error
}

var _ session.Service = (*Workspace)(nil)

// NewWorkspace prepares an isolated session view over base. The key is taken
// from base when it is set; a nil base means CreateSession must be called
// before any read or append, which is the same contract the framework's own
// services impose on a not-yet-created session.
func NewWorkspace(opts Options) *Workspace {
	w := &Workspace{base: opts.Base}
	if opts.Base != nil {
		w.key = session.Key{AppName: opts.Base.AppName, UserID: opts.Base.UserID, SessionID: opts.Base.ID}
		w.live = opts.Base.Clone()
	}
	return w
}

// Seal makes the workspace permanently read-only. It is what a lease renewal
// failure or an abandoned attempt calls: any further write from the Runner,
// including a WithoutCancel-issued one, fails instead of reaching shared
// state. Sealing twice keeps the first reason.
func (w *Workspace) Seal(reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return
	}
	if reason == "" {
		reason = "sealed"
	}
	w.sealed = true
	w.sealReason = reason
}

// Sealed reports whether Seal has run.
func (w *Workspace) Sealed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sealed
}

// Err reports the sticky failure recorded by a rejected shared-state write,
// or the seal reason once sealed. Snapshot refuses to produce a commit-ready
// result while it is non-nil.
func (w *Workspace) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.errLocked()
}

func (w *Workspace) errLocked() error {
	if w.sticky != nil {
		return w.sticky
	}
	if w.sealed {
		return fmt.Errorf("%w: %s", ErrSealed, w.sealReason)
	}
	return nil
}

// markFailed records a sticky failure that blocks commit without aborting the
// ongoing run outright; the Runner keeps receiving the error from the call
// that triggered it and the workspace refuses to commit.
func (w *Workspace) markFailed(err error) {
	if w.sticky == nil {
		w.sticky = err
	}
}

// Snapshot freezes the workspace and returns the prepared write set. It is
// the only path by which anything recorded here becomes visible to a
// committer; sealed or failed workspaces return an error instead.
func (w *Workspace) Snapshot() (*Prepared, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return nil, err
	}
	if w.live == nil {
		return nil, ErrSessionNotFound
	}
	prepared := &Prepared{
		Key:     w.key,
		Events:  append([]event.Event(nil), w.events...),
		State:   w.live.SnapshotState(),
		Summary: append([]SummaryIntent(nil), w.summaries...),
		Created: w.created,
		Deleted: w.deleted,
	}
	// Seal on the way out: an attempt that has produced its prepared write set
	// must not keep writing while the commit is in flight. A commit retry
	// reads the frozen result, it does not re-run the Runner.
	w.sealed = true
	w.sealReason = "snapshot taken"
	return prepared, nil
}

// CreateSession mirrors the framework's own session services: an empty
// SessionID is assigned a fresh UUID.
func (w *Workspace) CreateSession(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
	opts ...session.Option,
) (*session.Session, error) {
	_ = opts
	if err := w.writeCtx(ctx, "CreateSession"); err != nil {
		return nil, err
	}
	if err := key.CheckUserKey(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return nil, err
	}
	if key.SessionID == "" {
		key.SessionID = uuid.NewString()
	}
	if w.live != nil && w.key == key {
		// The framework's Runner calls CreateSession-or-GetSession unconditionally
		// at the start of a run; recreating an already-tracked session would
		// silently discard this attempt's own earlier appends.
		return w.live.Clone(), nil
	}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	for k, v := range state {
		sess.SetState(k, v)
	}
	w.key = key
	w.live = sess
	w.created = true
	return w.live.Clone(), nil
}

// GetSession returns the live local view, which reflects every append and
// state update made so far within this attempt.
func (w *Workspace) GetSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) (*session.Session, error) {
	if err := w.readCtx(ctx, "GetSession"); err != nil {
		return nil, err
	}
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	opt := applyOptions(opts...)
	if err := session.ValidateGetSessionOptions(opt, false); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return nil, err
	}
	if w.live == nil || w.deleted || w.key != key {
		return nil, nil
	}
	sess := w.live.Clone()
	sess.ApplyEventFiltering(
		session.WithEventNum(opt.EventNum),
		session.WithEventTime(opt.EventTime),
	)
	return sess, nil
}

// ListSessions returns at most the single session this workspace tracks.
func (w *Workspace) ListSessions(
	ctx context.Context,
	userKey session.UserKey,
	opts ...session.Option,
) ([]*session.Session, error) {
	if err := w.readCtx(ctx, "ListSessions"); err != nil {
		return nil, err
	}
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	opt := applyOptions(opts...)
	if err := session.ValidateListSessionsOptions(opt); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return nil, err
	}
	if w.live == nil || w.deleted || w.live.AppName != userKey.AppName || w.live.UserID != userKey.UserID {
		return []*session.Session{}, nil
	}
	return []*session.Session{w.live.Clone()}, nil
}

// DeleteSession records the deletion; the workspace's own reads stop serving
// the session, but nothing is removed from shared state — that is the
// committer's decision.
func (w *Workspace) DeleteSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) error {
	_ = opts
	if err := w.writeCtx(ctx, "DeleteSession"); err != nil {
		return err
	}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return err
	}
	if w.live == nil || w.key != key {
		return ErrSessionNotFound
	}
	w.deleted = true
	return nil
}

// UpdateAppState is not enabled for this milestone; see ErrUnsupportedSharedState.
func (w *Workspace) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	_ = state
	if err := w.writeCtx(ctx, "UpdateAppState"); err != nil {
		return err
	}
	if appName == "" {
		return session.ErrAppNameRequired
	}
	w.rejectSharedState()
	return ErrUnsupportedSharedState
}

// DeleteAppState is not enabled for this milestone; see ErrUnsupportedSharedState.
func (w *Workspace) DeleteAppState(ctx context.Context, appName string, key string) error {
	_ = key
	if err := w.writeCtx(ctx, "DeleteAppState"); err != nil {
		return err
	}
	if appName == "" {
		return session.ErrAppNameRequired
	}
	w.rejectSharedState()
	return ErrUnsupportedSharedState
}

// ListAppStates always reports an empty app state: this milestone does not
// share app-level state across sessions, and a read must not error just
// because the workspace is not the store for it.
func (w *Workspace) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	if err := w.readCtx(ctx, "ListAppStates"); err != nil {
		return nil, err
	}
	if appName == "" {
		return nil, session.ErrAppNameRequired
	}
	return session.StateMap{}, nil
}

// UpdateUserState is not enabled for this milestone; see ErrUnsupportedSharedState.
func (w *Workspace) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	_ = state
	if err := w.writeCtx(ctx, "UpdateUserState"); err != nil {
		return err
	}
	if err := userKey.CheckUserKey(); err != nil {
		return err
	}
	w.rejectSharedState()
	return ErrUnsupportedSharedState
}

// ListUserStates always reports an empty user state; see ListAppStates.
func (w *Workspace) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	if err := w.readCtx(ctx, "ListUserStates"); err != nil {
		return nil, err
	}
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	return session.StateMap{}, nil
}

// DeleteUserState is not enabled for this milestone; see ErrUnsupportedSharedState.
func (w *Workspace) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	_ = key
	if err := w.writeCtx(ctx, "DeleteUserState"); err != nil {
		return err
	}
	if err := userKey.CheckUserKey(); err != nil {
		return err
	}
	w.rejectSharedState()
	return ErrUnsupportedSharedState
}

// UpdateSessionState applies a direct state change without appending an
// event, matching the framework's contract that app:/user: prefixes are
// reserved for the cross-session methods.
func (w *Workspace) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := w.writeCtx(ctx, "UpdateSessionState"); err != nil {
		return err
	}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	for k := range state {
		if strings.HasPrefix(k, session.StateAppPrefix) || strings.HasPrefix(k, session.StateUserPrefix) {
			return fmt.Errorf("sessionstore: %q is not a session-scoped key, use the app/user state methods instead", k)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return err
	}
	if w.live == nil || w.deleted || w.key != key {
		return ErrSessionNotFound
	}
	for k, v := range state {
		w.live.SetState(k, v)
	}
	w.live.UpdatedAt = time.Now()
	return nil
}

// AppendEvent mirrors the framework's own service: it updates the caller's
// live session (the Runner may keep using that pointer for later rounds of
// the same run without re-reading) and updates this workspace's canonical
// copy independently so a later GetSession hands out the same view. The
// journal records the append with its original identity for commit.
func (w *Workspace) AppendEvent(
	ctx context.Context,
	sess *session.Session,
	evt *event.Event,
	opts ...session.Option,
) error {
	if err := w.writeCtx(ctx, "AppendEvent"); err != nil {
		return err
	}
	if sess == nil {
		return session.ErrNilSession
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if evt == nil {
		return errors.New("sessionstore: event is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return err
	}
	if w.live == nil || w.deleted || w.key != key {
		return ErrSessionNotFound
	}
	// The caller's copy first: this is what the framework itself mutates in
	// place, and skipping it would desync a Runner that does not re-read.
	sess.UpdateUserSession(evt, opts...)
	w.live.UpdateUserSession(evt, opts...)
	if evt.Response != nil && !evt.IsPartial && evt.IsValidContent() {
		w.events = append(w.events, copyStoredEvent(evt))
	}
	return nil
}

// CreateSessionSummary records the intent; see SummaryIntent.
func (w *Workspace) CreateSessionSummary(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	return w.recordSummary(ctx, sess, filterKey, force, "CreateSessionSummary")
}

// EnqueueSummaryJob records the intent; see SummaryIntent.
func (w *Workspace) EnqueueSummaryJob(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	return w.recordSummary(ctx, sess, filterKey, force, "EnqueueSummaryJob")
}

func (w *Workspace) recordSummary(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
	op string,
) error {
	if err := w.writeCtx(ctx, op); err != nil {
		return err
	}
	if sess == nil {
		return session.ErrNilSession
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.errLocked(); err != nil {
		return err
	}
	if w.live == nil || w.deleted || w.key != key {
		return ErrSessionNotFound
	}
	w.summaries = append(w.summaries, SummaryIntent{
		FilterKey:   filterKey,
		Force:       force,
		RequestedAt: time.Now(),
	})
	return nil
}

// GetSessionSummaryText reads the summaries already on the caller's session
// view. Intents recorded by this attempt have not been executed — they run as
// an outbox job after commit — so they are deliberately invisible here, and no
// workspace state needs consulting to answer the question.
func (w *Workspace) GetSessionSummaryText(
	ctx context.Context,
	sess *session.Session,
	opts ...session.SummaryOption,
) (string, bool) {
	if sess == nil {
		return "", false
	}
	o := &session.SummaryOptions{}
	for _, opt := range opts {
		opt(o)
	}
	filterKey := o.FilterKey
	if filterKey == "" {
		filterKey = session.SummaryFilterKeyAllContents
	}
	sess.SummariesMu.RLock()
	defer sess.SummariesMu.RUnlock()
	if s, ok := sess.Summaries[filterKey]; ok && s != nil {
		return s.Summary, true
	}
	return "", false
}

// Close never blocks: a workspace holds no connection or file handle.
func (w *Workspace) Close() error { return nil }

func (w *Workspace) rejectSharedState() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.markFailed(fmt.Errorf("sessionstore: %w", ErrUnsupportedSharedState))
}

// writeCtx rejects a write before touching any workspace state: seal first,
// then a cancelled context, in that order. Seal must beat the framework's
// WithoutCancel persistence path, which arrives with a live context and would
// otherwise look indistinguishable from a normal append. errLocked re-checks
// the seal once the caller holds the mutex, so this fast path is not the only
// thing standing between a sealed workspace and a write.
func (w *Workspace) writeCtx(ctx context.Context, op string) error {
	if w.Sealed() {
		return fmt.Errorf("%w: %s", ErrSealed, op)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sessionstore: %s: %w", op, err)
	}
	return nil
}

// readCtx rejects reads once sealed or failed, so a committer cannot build a
// view out of a workspace that has already been handed off or rejected.
func (w *Workspace) readCtx(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sessionstore: %s: %w", op, err)
	}
	return nil
}

func applyOptions(opts ...session.Option) *session.Options {
	opt := &session.Options{}
	for _, o := range opts {
		o(opt)
	}
	return opt
}

// copyStoredEvent matches the framework's own in-memory storage shape — a
// value copy that drops the in-memory-only ExecutionTrace — while
// deliberately not going through event.Event.Clone, which mints a fresh UUID
// and bumps Version. The committed event keeps the identity the Runner gave
// it, so a replayed or retried commit is recognisable downstream.
func copyStoredEvent(e *event.Event) event.Event {
	stored := *e
	stored.ExecutionTrace = nil
	return stored
}
