package storage

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Tracer for the shared data layer. The framework instruments the agent, model
// and tool calls, but not the session/memory stores it delegates to; without
// this decorator the trace jumps straight from `agent.run` to the next tool
// call and the shared-state half of a turn (events appended, summary produced,
// memories read) is invisible in Jaeger. The platform owns the services it
// hands to the runner, so it wraps them here instead of patching the framework.
var storageTracer = otel.Tracer("trpc-agent-service/storage")

// startStoreSpan opens a span for one storage operation of a domain, tagged
// with the backend that served it (so latency can be attributed per backend in
// Jaeger).
func startStoreSpan(ctx context.Context, domain, op string, backend Backend, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	full := make([]attribute.KeyValue, 0, len(attrs)+2)
	full = append(full, attrs...)
	full = append(full,
		attribute.String("storage.domain", domain),
		attribute.String("storage.backend", string(backend)),
	)
	return storageTracer.Start(ctx, domain+"."+op, trace.WithAttributes(full...))
}

// tracingSessions decorates a session service with one span per storage
// operation. It implements session.Service by delegation: the wrapped service
// still does the work (and still owns tenant isolation via Key.AppName).
//
// It deliberately implements *only* session.Service. The optional capabilities
// the framework asserts on live in the embedded sessionCaps of
// tracingSessionsFull, because Go decides capability by method set: a wrapper
// that always answered the WindowService assertion would turn the framework's
// "no window support, use the full transcript" fallback into a runtime error.
type tracingSessions struct {
	inner   session.Service
	backend Backend
}

// trackEventReader mirrors the framework's unexported anchor-window reader
// interface. Go interface satisfaction is structural, so forwarding this method
// keeps the framework's own type assertion working through the decorator.
type trackEventReader interface {
	GetTrackEvents(ctx context.Context, key session.Key, track session.Track, opts ...session.Option) (*session.TrackEvents, error)
}

// sessionCaps forwards the optional session capabilities (anchor event windows,
// track events) and traces them like every other storage operation.
type sessionCaps struct {
	backend Backend
	window  session.WindowService
	track   session.TrackService
	reader  trackEventReader
}

// tracingSessionsFull is the decorator for a backend that can serve event
// windows and track events — every backend in backendTable can today, and the
// capability tests pin that.
type tracingSessionsFull struct {
	*tracingSessions
	sessionCaps
}

// withTracing wraps a session service so its operations appear in the trace
// without hiding any capability the underlying backend offers.
func withTracingSessions(inner session.Service, backend Backend) session.Service {
	if inner == nil {
		return nil
	}
	base := &tracingSessions{inner: inner, backend: backend}
	window, okWindow := inner.(session.WindowService)
	track, okTrack := inner.(session.TrackService)
	reader, okReader := inner.(trackEventReader)
	if !okWindow || !okTrack || !okReader {
		// The backend cannot serve these, so the decorator must not advertise
		// them either (see the type comment). Warn loudly: a backend that
		// silently loses event windows would otherwise look like a framework
		// mystery rather than a missing capability.
		slog.Warn("storage: session backend lacks optional capabilities, leaving them unadvertised",
			"backend", backend, "event_window", okWindow, "track_events", okTrack, "track_reader", okReader)
		return base
	}
	return &tracingSessionsFull{
		tracingSessions: base,
		sessionCaps:     sessionCaps{backend: backend, window: window, track: track, reader: reader},
	}
}

// startSessionSpan opens a span for one session operation.
func (s *tracingSessions) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return startStoreSpan(ctx, "session", op, s.backend, attrs...)
}

// finish closes a span, recording the error (if any) so a failing backend shows
// up as an error span rather than just a slow one.
func finish(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// keyAttrs describes a session key for span attributes.
func keyAttrs(key session.Key) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("session.app_name", key.AppName),
		attribute.String("session.user_id", key.UserID),
		attribute.String("session.id", key.SessionID),
	}
}

// userKeyAttrs describes a user-scoped key for span attributes.
func userKeyAttrs(key session.UserKey) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("session.app_name", key.AppName),
		attribute.String("session.user_id", key.UserID),
	}
}

func (s *tracingSessions) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	ctx, span := s.start(ctx, "create", keyAttrs(key)...)
	sess, err := s.inner.CreateSession(ctx, key, state, options...)
	finish(span, err)
	return sess, err
}

func (s *tracingSessions) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	ctx, span := s.start(ctx, "get", keyAttrs(key)...)
	sess, err := s.inner.GetSession(ctx, key, options...)
	if sess != nil {
		// Event count is the cheapest signal that the shared state layer is
		// actually returning history (a silent empty session is a real bug).
		span.SetAttributes(attribute.Int("session.events", len(sess.Events)))
	}
	finish(span, err)
	return sess, err
}

func (s *tracingSessions) ListSessions(ctx context.Context, userKey session.UserKey, options ...session.Option) ([]*session.Session, error) {
	ctx, span := s.start(ctx, "list", userKeyAttrs(userKey)...)
	out, err := s.inner.ListSessions(ctx, userKey, options...)
	if err == nil {
		span.SetAttributes(attribute.Int("session.count", len(out)))
	}
	finish(span, err)
	return out, err
}

func (s *tracingSessions) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	ctx, span := s.start(ctx, "delete", keyAttrs(key)...)
	err := s.inner.DeleteSession(ctx, key, options...)
	finish(span, err)
	return err
}

func (s *tracingSessions) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	ctx, span := s.start(ctx, "update_app_state", attribute.String("session.app_name", appName))
	err := s.inner.UpdateAppState(ctx, appName, state)
	finish(span, err)
	return err
}

func (s *tracingSessions) DeleteAppState(ctx context.Context, appName string, key string) error {
	ctx, span := s.start(ctx, "delete_app_state",
		attribute.String("session.app_name", appName), attribute.String("session.state_key", key))
	err := s.inner.DeleteAppState(ctx, appName, key)
	finish(span, err)
	return err
}

func (s *tracingSessions) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	ctx, span := s.start(ctx, "list_app_states", attribute.String("session.app_name", appName))
	out, err := s.inner.ListAppStates(ctx, appName)
	finish(span, err)
	return out, err
}

func (s *tracingSessions) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	ctx, span := s.start(ctx, "update_user_state", userKeyAttrs(userKey)...)
	err := s.inner.UpdateUserState(ctx, userKey, state)
	finish(span, err)
	return err
}

func (s *tracingSessions) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	ctx, span := s.start(ctx, "list_user_states", userKeyAttrs(userKey)...)
	out, err := s.inner.ListUserStates(ctx, userKey)
	finish(span, err)
	return out, err
}

func (s *tracingSessions) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	ctx, span := s.start(ctx, "delete_user_state",
		append(userKeyAttrs(userKey), attribute.String("session.state_key", key))...)
	err := s.inner.DeleteUserState(ctx, userKey, key)
	finish(span, err)
	return err
}

func (s *tracingSessions) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	ctx, span := s.start(ctx, "update_session_state", keyAttrs(key)...)
	err := s.inner.UpdateSessionState(ctx, key, state)
	finish(span, err)
	return err
}

func (s *tracingSessions) AppendEvent(ctx context.Context, sess *session.Session, ev *event.Event, options ...session.Option) error {
	attrs := []attribute.KeyValue{}
	if ev != nil {
		attrs = append(attrs,
			attribute.String("event.id", ev.ID),
			attribute.String("event.author", ev.Author),
			attribute.String("event.request_id", ev.RequestID),
		)
	}
	if sess != nil {
		attrs = append(attrs,
			attribute.String("session.id", sess.ID),
			attribute.String("session.app_name", sess.AppName),
			attribute.String("session.user_id", sess.UserID),
		)
	}
	ctx, span := s.start(ctx, "append_event", attrs...)
	err := s.inner.AppendEvent(ctx, sess, ev, options...)
	finish(span, err)
	return err
}

func (s *tracingSessions) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	ctx, span := s.start(ctx, "create_summary",
		attribute.String("session.id", sessionIDOf(sess)),
		attribute.String("session.filter_key", filterKey),
		attribute.Bool("session.force", force))
	err := s.inner.CreateSessionSummary(ctx, sess, filterKey, force)
	finish(span, err)
	return err
}

func (s *tracingSessions) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	ctx, span := s.start(ctx, "enqueue_summary",
		attribute.String("session.id", sessionIDOf(sess)),
		attribute.String("session.filter_key", filterKey),
		attribute.Bool("session.force", force))
	err := s.inner.EnqueueSummaryJob(ctx, sess, filterKey, force)
	finish(span, err)
	return err
}

func (s *tracingSessions) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	ctx, span := s.start(ctx, "summary_text", attribute.String("session.id", sessionIDOf(sess)))
	text, ok := s.inner.GetSessionSummaryText(ctx, sess, opts...)
	span.SetAttributes(attribute.Bool("session.summary_found", ok), attribute.Int("session.summary_len", len(text)))
	finish(span, nil)
	return text, ok
}

func (s *tracingSessions) Close() error { return s.inner.Close() }

// GetEventWindow loads the events around one anchor, for the framework's
// long-session recovery path.
func (c sessionCaps) GetEventWindow(ctx context.Context, req session.EventWindowRequest) (*session.EventWindow, error) {
	ctx, span := startStoreSpan(ctx, "session", "event_window", c.backend, keyAttrs(req.Key)...)
	span.SetAttributes(
		attribute.String("session.anchor_event_id", req.AnchorEventID),
		attribute.Int("session.window_before", req.Before),
		attribute.Int("session.window_after", req.After),
	)
	out, err := c.window.GetEventWindow(ctx, req)
	if err == nil && out != nil {
		span.SetAttributes(attribute.Int("session.window_events", len(out.Entries)))
	}
	finish(span, err)
	return out, err
}

// AppendTrackEvent records one track event (a durable side-channel about the
// session, e.g. a delivery receipt), so it is visible and attributable.
func (c sessionCaps) AppendTrackEvent(ctx context.Context, sess *session.Session, trackEvent *session.TrackEvent, opts ...session.Option) error {
	attrs := []attribute.KeyValue{attribute.String("session.id", sessionIDOf(sess))}
	if trackEvent != nil {
		attrs = append(attrs, attribute.String("session.track", string(trackEvent.Track)))
	}
	ctx, span := startStoreSpan(ctx, "session", "append_track_event", c.backend, attrs...)
	err := c.track.AppendTrackEvent(ctx, sess, trackEvent, opts...)
	finish(span, err)
	return err
}

// GetTrackEvents reads the track events of a session.
func (c sessionCaps) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, opts ...session.Option) (*session.TrackEvents, error) {
	ctx, span := startStoreSpan(ctx, "session", "track_events", c.backend,
		append(keyAttrs(key), attribute.String("session.track", string(track)))...)
	out, err := c.reader.GetTrackEvents(ctx, key, track, opts...)
	if err == nil && out != nil {
		span.SetAttributes(attribute.Int("session.track_events", len(out.Events)))
	}
	finish(span, err)
	return out, err
}

// sessionIDOf returns the session id, tolerating a nil session.
func sessionIDOf(sess *session.Session) string {
	if sess == nil {
		return ""
	}
	return sess.ID
}
