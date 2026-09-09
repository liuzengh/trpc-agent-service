package summary

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type recordingTranscriptReader struct {
	key      Key
	sequence int64
	session  *session.Session
}

func (r *recordingTranscriptReader) ReadTranscript(_ context.Context, key Key, sequence int64) (Transcript, error) {
	r.key = key
	r.sequence = sequence
	return Transcript{Session: r.session.Clone(), CoveredEventSequence: sequence}, nil
}

type fixedSessionSummarizer struct{ content string }

func (s fixedSessionSummarizer) Summarize(context.Context, *session.Session) (string, error) {
	return s.content, nil
}

type staticSessionGetter struct{ value *session.Session }

func (g staticSessionGetter) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return g.value.Clone(), nil
}

type eventCountSummarizer struct{ count int }

func (s *eventCountSummarizer) Summarize(_ context.Context, value *session.Session) (string, error) {
	s.count = value.GetEventCount()
	return "prefix summary", nil
}

func TestProductionGeneratorTRPCReaderExcludesEventsAfterTargetAndOutsideFilter(t *testing.T) {
	key := Key{
		TenantID: "tenant-a", AgentAppID: "support", SessionOwnerID: "owner-1",
		SessionID: "session-1", FilterKey: "support/tool",
	}
	stored := session.NewSession("tsa1:8:tenant-a:support", key.SessionOwnerID, key.SessionID)
	base := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	stored.Events = []event.Event{
		{ID: "root", Timestamp: base, Version: event.CurrentVersion},
		{ID: "target", Timestamp: base.Add(time.Second), FilterKey: "support/tool", Version: event.CurrentVersion},
		{ID: "other", Timestamp: base.Add(2 * time.Second), FilterKey: "other", Version: event.CurrentVersion},
		{ID: "late", Timestamp: base.Add(3 * time.Second), FilterKey: "support/tool", Version: event.CurrentVersion},
	}
	reader, err := NewTRPCSessionTranscriptReader(staticSessionGetter{value: stored}, func(Key) (string, error) {
		return "tsa1:8:tenant-a:support", nil
	})
	if err != nil {
		t.Fatalf("create transcript reader: %v", err)
	}
	summarizer := &eventCountSummarizer{}
	generator, err := NewProductionGenerator(reader, summarizer)
	if err != nil {
		t.Fatalf("create generator: %v", err)
	}
	_, err = generator.Generate(context.Background(), Job{
		ID: 9, Key: key, AgentVersionID: "version-1", TargetEventSequence: 3, Status: StatusPending, MaxAttempts: DefaultMaxAttempts,
	})
	if err != nil {
		t.Fatalf("generate prefix summary: %v", err)
	}
	if summarizer.count != 2 {
		t.Fatalf("summarizer received %d events, want root + matching target only", summarizer.count)
	}
}

func TestProductionGeneratorReadsExactOwnedSessionPrefix(t *testing.T) {
	key := Key{
		TenantID:       "tenant-a",
		AgentAppID:     "support",
		SessionOwnerID: "wecom:user-42",
		SessionID:      "session-1",
	}
	transcript := session.NewSession("tsa1:8:tenant-a:support", key.SessionOwnerID, key.SessionID)
	firstAt := time.Date(2026, time.August, 30, 1, 0, 0, 0, time.UTC)
	lastAt := firstAt.Add(time.Second)
	transcript.Events = []event.Event{
		{ID: "event-1", Timestamp: firstAt},
		{ID: "event-2", Timestamp: lastAt},
	}
	reader := &recordingTranscriptReader{session: transcript}
	generator, err := NewProductionGenerator(reader, fixedSessionSummarizer{content: "bounded summary"})
	if err != nil {
		t.Fatalf("create generator: %v", err)
	}

	candidate, err := generator.Generate(context.Background(), Job{
		ID:                  7,
		Key:                 key,
		AgentVersionID:      "version-1",
		TargetEventSequence: 2,
		Status:              StatusPending,
		MaxAttempts:         DefaultMaxAttempts,
	})
	if err != nil {
		t.Fatalf("generate summary: %v", err)
	}
	if reader.key != key || reader.sequence != 2 {
		t.Fatalf("reader scope = %#v sequence=%d, want %#v sequence=2", reader.key, reader.sequence, key)
	}
	if candidate.Key != key || candidate.EventSequence != 2 || candidate.Content != "bounded summary" {
		t.Fatalf("candidate = %#v", candidate)
	}
	if candidate.ContentSHA256 != HashContent("bounded summary") {
		t.Fatalf("candidate hash = %q", candidate.ContentSHA256)
	}
	if candidate.LastEventID != "event-2" || !candidate.CutoffAt.Equal(lastAt) {
		t.Fatalf("candidate boundary = (%q, %v), want event-2 at %v", candidate.LastEventID, candidate.CutoffAt, lastAt)
	}
}

func TestProductionGeneratorUsesLastFilteredEventAsBoundary(t *testing.T) {
	key := Key{TenantID: "tenant-a", AgentAppID: "support", SessionOwnerID: "owner-1", SessionID: "session-1", FilterKey: "branch"}
	base := time.Date(2026, time.August, 30, 2, 0, 0, 0, time.UTC)
	stored := session.NewSession("tsa1:8:tenant-a:support", key.SessionOwnerID, key.SessionID)
	stored.Events = []event.Event{
		{ID: "root", Timestamp: base, Version: event.CurrentVersion},
		{ID: "covered-branch", Timestamp: base.Add(time.Second), FilterKey: "branch", Version: event.CurrentVersion},
		{ID: "other-target", Timestamp: base.Add(2 * time.Second), FilterKey: "other", Version: event.CurrentVersion},
	}
	reader, err := NewTRPCSessionTranscriptReader(staticSessionGetter{value: stored}, func(Key) (string, error) {
		return stored.AppName, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewProductionGenerator(reader, fixedSessionSummarizer{content: "branch summary"})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := generator.Generate(context.Background(), Job{
		ID: 1, Key: key, AgentVersionID: "version-1", TargetEventSequence: 3,
		Status: StatusPending, MaxAttempts: DefaultMaxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.LastEventID != "covered-branch" || !candidate.CutoffAt.Equal(base.Add(time.Second)) {
		t.Fatalf("filtered boundary = (%q, %v)", candidate.LastEventID, candidate.CutoffAt)
	}
}

func TestTRPCSessionTargetResolverReadsAuthoritativeEventCount(t *testing.T) {
	key := summaryKey()
	stored := session.NewSession("tsa1:8:tenant-a:support", key.SessionOwnerID, key.SessionID)
	stored.Events = []event.Event{{ID: "event-1"}, {ID: "event-2"}, {ID: "event-3"}, {ID: "event-4"}}
	resolver, err := NewTRPCSessionTargetResolver(staticSessionGetter{value: stored}, func(Key) (string, error) {
		return "tsa1:8:tenant-a:support", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := resolver.ResolveTarget(context.Background(), Job{
		ID: 1, Key: key, AgentVersionID: "version-1", TargetEventSequence: 0,
		Status: StatusProcessing, LeaseOwner: "worker-1", LeaseVersion: 1,
		LeaseUntil: time.Now().Add(time.Minute), Attempts: 1, MaxAttempts: 8,
	})
	if err != nil || sequence != 4 {
		t.Fatalf("resolved sequence=%d err=%v", sequence, err)
	}
}

func TestTRPCSessionReaderFailsClosedOnDefaultWindowTruncation(t *testing.T) {
	key := summaryKey()
	stored := session.NewSession("tsa1:8:tenant-a:support", key.SessionOwnerID, key.SessionID)
	for i := 1; i <= 1000; i++ {
		stored.Events = append(stored.Events, event.Event{ID: fmt.Sprintf("event-%04d", i)})
	}
	reader, err := NewTRPCSessionTranscriptReader(staticSessionGetter{value: stored}, func(Key) (string, error) {
		return stored.AppName, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.ReadTranscript(context.Background(), key, 1000)
	if !errors.Is(err, ErrTranscriptIncomplete) {
		t.Fatalf("truncated default window err=%v, want ErrTranscriptIncomplete", err)
	}
}
