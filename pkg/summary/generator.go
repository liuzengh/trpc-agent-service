package summary

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	transcriptReadPage = 2048
	transcriptReadMax  = 131072
	// session/redis and session/postgres v1.11 default to this window. A
	// response of exactly the default while requesting a larger page is
	// ambiguous: it may be a truncated long session. Never turn it into a
	// false absolute sequence.
	upstreamDefaultEventWindow = 1000
)

// Transcript carries a filtered immutable Session view together with the
// unfiltered append sequence it covers. Branch filtering can reduce the number
// of visible events without changing that durable checkpoint.
type Transcript struct {
	Session              *session.Session
	CoveredEventSequence int64
}

// TranscriptReader returns the immutable committed event prefix represented by
// sequence. Implementations must enforce tenant, app, owner and session scope
// before returning data from a shared Session backend.
type TranscriptReader interface {
	ReadTranscript(context.Context, Key, int64) (Transcript, error)
}

// SessionGetter is the read-only part of session.Service required to build a
// stable transcript. Both the framework services and strict fenced adapters
// satisfy this interface.
type SessionGetter interface {
	GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error)
}

// AppNameResolver maps durable logical tenant/app identity to the physical
// shared-backend namespace used by Runner.
type AppNameResolver func(Key) (string, error)

// TRPCSessionTranscriptReader reads a committed tRPC Session and freezes the
// exact append prefix requested by a durable Summary job.
type TRPCSessionTranscriptReader struct {
	sessions SessionGetter
	resolve  AppNameResolver
}

// TRPCSessionTargetResolver converts the deferred zero marker into an exact
// committed Session event count. A production composition may wrap this
// resolver in the same distributed Session lock used by Agent Workers.
type TRPCSessionTargetResolver struct {
	sessions SessionGetter
	resolve  AppNameResolver
}

func NewTRPCSessionTargetResolver(sessions SessionGetter, resolve AppNameResolver) (*TRPCSessionTargetResolver, error) {
	if nilInterface(sessions) || resolve == nil {
		return nil, ErrTargetResolverUnavailable
	}
	return &TRPCSessionTargetResolver{sessions: sessions, resolve: resolve}, nil
}

func (r *TRPCSessionTargetResolver) ResolveTarget(ctx context.Context, job Job) (int64, error) {
	if r == nil || nilInterface(r.sessions) || r.resolve == nil {
		return 0, ErrTargetResolverUnavailable
	}
	if err := job.Validate(); err != nil || (job.TargetEventSequence != 0 && job.TargetResolutionLeaseVersion == 0) {
		return 0, ErrTranscriptIncomplete
	}
	ctx = nonNilContext(ctx)
	appName, err := r.resolve(job.Key)
	if err != nil || !validScopedText(appName, 255, false) {
		return 0, ErrTranscriptIncomplete
	}
	value, err := readCompleteSession(ctx, r.sessions, session.Key{
		AppName: appName, UserID: job.SessionOwnerID, SessionID: job.SessionID,
	})
	if err != nil {
		return 0, err
	}
	if value == nil || value.AppName != appName || value.UserID != job.SessionOwnerID || value.ID != job.SessionID {
		return 0, ErrTranscriptIncomplete
	}
	if id, err := storage.SessionIncarnationID(value); err != nil || id != job.SessionIncarnationID {
		return 0, ErrTranscriptIncomplete
	}
	count := value.GetEventCount()
	if count <= 0 {
		return 0, ErrTranscriptIncomplete
	}
	return int64(count), nil
}

func NewTRPCSessionTranscriptReader(sessions SessionGetter, resolve AppNameResolver) (*TRPCSessionTranscriptReader, error) {
	if nilInterface(sessions) || resolve == nil {
		return nil, ErrGeneratorUnavailable
	}
	return &TRPCSessionTranscriptReader{sessions: sessions, resolve: resolve}, nil
}

func (r *TRPCSessionTranscriptReader) ReadTranscript(ctx context.Context, key Key, sequence int64) (Transcript, error) {
	if r == nil || nilInterface(r.sessions) || r.resolve == nil {
		return Transcript{}, ErrGeneratorUnavailable
	}
	if err := key.Validate(); err != nil || sequence <= 0 || uint64(sequence) > uint64(^uint(0)>>1) {
		return Transcript{}, ErrTranscriptIncomplete
	}
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return Transcript{}, err
	}
	appName, err := r.resolve(key)
	if err != nil || !validScopedText(appName, 255, false) {
		return Transcript{}, ErrTranscriptIncomplete
	}
	value, err := readCompleteSession(ctx, r.sessions, session.Key{
		AppName: appName, UserID: key.SessionOwnerID, SessionID: key.SessionID,
	})
	if err != nil {
		return Transcript{}, err
	}
	if value == nil || value.AppName != appName || value.UserID != key.SessionOwnerID || value.ID != key.SessionID {
		return Transcript{}, ErrTranscriptIncomplete
	}
	if id, err := storage.SessionIncarnationID(value); err != nil || id != key.SessionIncarnationID {
		return Transcript{}, ErrTranscriptIncomplete
	}
	events := value.GetEvents()
	if len(events) < int(sequence) {
		return Transcript{}, ErrTranscriptIncomplete
	}
	events = append([]event.Event(nil), events[:int(sequence)]...)
	events = filterTranscriptEvents(events, key.FilterKey)
	frozen := value.Clone()
	frozen.Events = events
	return Transcript{Session: frozen, CoveredEventSequence: sequence}, nil
}

// readCompleteSession requests progressively larger windows until the
// backend proves that it returned the complete append history. A fixed
// default window (the upstream default is 1000) is not an absolute sequence;
// treating it as one silently shifts old summary boundaries after truncation.
// If the bounded completeness proof cannot be made, fail closed.
func readCompleteSession(ctx context.Context, sessions SessionGetter, key session.Key) (*session.Session, error) {
	limit := transcriptReadPage
	for {
		value, err := sessions.GetSession(ctx, key, session.WithEventNum(limit))
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, ErrTranscriptIncomplete
		}
		events := value.GetEvents()
		if limit > upstreamDefaultEventWindow && len(events) == upstreamDefaultEventWindow {
			return nil, ErrTranscriptIncomplete
		}
		if len(events) < limit {
			return value, nil
		}
		if limit >= transcriptReadMax {
			return nil, ErrTranscriptIncomplete
		}
		limit *= 2
	}
}

func filterTranscriptEvents(events []event.Event, filterKey string) []event.Event {
	if filterKey == "" {
		return events
	}
	prefix := filterKey + event.FilterKeyDelimiter
	filtered := make([]event.Event, 0, len(events))
	for _, item := range events {
		effective := item.FilterKey
		if item.Version != event.CurrentVersion {
			effective = item.Branch
		}
		if effective == "" || effective == filterKey || strings.HasPrefix(effective, prefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// SessionSummarizer is the narrow interface used from tRPC-Agent-Go's session
// summarizer. Keeping the larger mutable summarizer interface private to the
// composition root prevents jobs from changing prompts or models at runtime.
type SessionSummarizer interface {
	Summarize(context.Context, *session.Session) (string, error)
}

// ProductionGenerator binds durable Summary jobs to a tenant-scoped Session
// reader and tRPC-Agent-Go summarizer without publishing any state itself.
type ProductionGenerator struct {
	reader     TranscriptReader
	summarizer SessionSummarizer
}

func NewProductionGenerator(reader TranscriptReader, summarizer SessionSummarizer) (*ProductionGenerator, error) {
	if nilInterface(reader) || nilInterface(summarizer) {
		return nil, ErrGeneratorUnavailable
	}
	return &ProductionGenerator{reader: reader, summarizer: summarizer}, nil
}

func (g *ProductionGenerator) Generate(ctx context.Context, job Job) (Candidate, error) {
	if g == nil || nilInterface(g.reader) || nilInterface(g.summarizer) {
		return Candidate{}, ErrGeneratorUnavailable
	}
	if err := job.Validate(); err != nil {
		return Candidate{}, err
	}
	ctx = nonNilContext(ctx)
	transcript, err := g.reader.ReadTranscript(ctx, job.Key, job.TargetEventSequence)
	if err != nil {
		return Candidate{}, fmt.Errorf("read summary transcript: %w", err)
	}
	if transcript.Session == nil || transcript.CoveredEventSequence != job.TargetEventSequence ||
		transcript.Session.UserID != job.SessionOwnerID || transcript.Session.ID != job.SessionID {
		return Candidate{}, ErrTranscriptIncomplete
	}
	content, err := g.summarizer.Summarize(ctx, transcript.Session)
	if err != nil {
		return Candidate{}, fmt.Errorf("summarize transcript: %w", err)
	}
	content = strings.TrimSpace(content)
	if content == "" {
		return Candidate{}, ErrNoSummaryContent
	}
	events := transcript.Session.GetEvents()
	if len(events) == 0 {
		return Candidate{}, ErrTranscriptIncomplete
	}
	last := events[len(events)-1]
	if last.Timestamp.IsZero() || !validScopedText(last.ID, MaxSummaryEventIDBytes, false) {
		return Candidate{}, ErrTranscriptIncomplete
	}
	candidate := Candidate{
		Key: job.Key, EventSequence: job.TargetEventSequence,
		Content: content, ContentSHA256: HashContent(content),
		CutoffAt: last.Timestamp.UTC(), LastEventID: last.ID,
	}
	if err := candidate.Validate(); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
