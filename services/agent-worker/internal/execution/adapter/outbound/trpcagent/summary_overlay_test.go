package trpcagent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type overlaySummarizer struct {
	mu     sync.Mutex
	calls  int
	run    func(context.Context, *session.Session) (string, error)
	should bool
}

func (m *overlaySummarizer) ShouldSummarize(*session.Session) bool { return m.should }
func (m *overlaySummarizer) Summarize(ctx context.Context, s *session.Session) (string, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.run != nil {
		return m.run(ctx, s)
	}
	return "summary", nil
}
func (m *overlaySummarizer) SetPrompt(string)         {}
func (m *overlaySummarizer) SetModel(model.Model)     {}
func (m *overlaySummarizer) Metadata() map[string]any { return nil }
func (m *overlaySummarizer) count() int               { m.mu.Lock(); defer m.mu.Unlock(); return m.calls }
func summaryOverlayFixture(t *testing.T, m *overlaySummarizer) (*overlay, *session.Session) {
	t.Helper()
	s, err := newSummaryOverlay("tenant", "summary-session", nil, 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	loaded, err := s.GetSession(context.Background(), s.key)
	if err != nil {
		t.Fatal(err)
	}
	return s, loaded
}
func appendSummaryEvent(t *testing.T, s *overlay, caller *session.Session, id, text, filter string) {
	t.Helper()
	e := event.New(id, "user")
	e.ID = id
	e.Timestamp = caller.CreatedAt.Add(time.Second)
	e.FilterKey = filter
	e.Version = event.CurrentVersion
	e.Response = &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(text)}}}
	if err := s.AppendEvent(context.Background(), caller, e); err != nil {
		t.Fatal(err)
	}
}
func TestSummaryOverlayRoundTripOwnedInputAndBoundary(t *testing.T) {
	m := &overlaySummarizer{should: true}
	m.run = func(_ context.Context, s *session.Session) (string, error) {
		for _, e := range s.Events {
			if e.Response != nil && len(e.Choices) > 0 && strings.Contains(e.Choices[0].Message.Content, "spoof") {
				return "", errors.New("caller contaminated owned input")
			}
		}
		return "remember tea", nil
	}
	s2, caller2 := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s2, caller2, "one", "tea", "root")
	detached, err := s2.GetSession(context.Background(), s2.key)
	if err != nil {
		t.Fatal(err)
	}
	detached.Events[0].Choices[0].Message.Content = "spoof"
	detached.Summaries = map[string]*session.Summary{"root": {Summary: "spoof"}}
	if err = s2.EnqueueSummaryJob(context.Background(), detached, "root", false); err != nil {
		t.Fatal(err)
	}
	if text, ok := s2.GetSessionSummaryText(context.Background(), detached, session.WithSummaryFilterKey("root")); !ok || text != "remember tea" {
		t.Fatal(text, ok)
	}
	bytes, err := s2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = newOverlay("tenant", "summary-session", bytes, 1<<20); !errors.Is(err, ErrSnapshot) {
		t.Fatal("legacy accepted summaries", err)
	}
	restored, err := newSummaryOverlay("tenant", "summary-session", bytes, 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.GetSession(context.Background(), restored.key)
	if err != nil {
		t.Fatal(err)
	}
	sum := got.Summaries["root"]
	if sum == nil || sum.Boundary == nil || sum.Boundary.LastEventID != "one" || len(got.Events) != 1 {
		t.Fatalf("missing SDK structural boundary %+v", sum)
	}
	got.Summaries["root"].Summary = "external"
	got.Summaries["root"].Boundary.LastEventID = "external"
	if err = restored.CreateSessionSummary(context.Background(), got, "root", false); err != nil {
		t.Fatal(err)
	}
	if m.count() != 1 {
		t.Fatalf("no delta should not resummarize: %d", m.count())
	}
	if text, _ := restored.GetSessionSummaryText(context.Background(), got, session.WithSummaryFilterKey("root")); text != "remember tea" {
		t.Fatal("alias", text)
	}
}
func TestSummaryOverlayConcurrentAppendAndJobs(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	m := &overlaySummarizer{should: true, run: func(ctx context.Context, s *session.Session) (string, error) {
		once.Do(func() { close(entered); <-release })
		var texts []string
		for _, e := range s.Events {
			if e.Response != nil && len(e.Choices) > 0 {
				texts = append(texts, e.Choices[0].Message.Content)
			}
		}
		return strings.Join(texts, ";"), ctx.Err()
	}}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "first", "root")
	first := make(chan error, 1)
	go func() { first <- s.CreateSessionSummary(context.Background(), caller, "root", false) }()
	<-entered
	appended := make(chan struct{})
	go func() { appendSummaryEvent(t, s, caller, "two", "second", "root"); close(appended) }()
	select {
	case <-appended:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("model call held overlay mutex")
	}
	second := make(chan error, 1)
	go func() { second <- s.EnqueueSummaryJob(context.Background(), caller, "root", false) }()
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	state, err := s.GetSession(context.Background(), s.key)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Events) != 2 || state.Summaries["root"].Boundary.LastEventID != "two" || !strings.Contains(state.Summaries["root"].Summary, "second") || m.count() != 2 {
		t.Fatalf("lost concurrent events/summary: %+v calls=%d", state.Summaries, m.count())
	}
}
func TestSummaryOverlayErrorsAreSticky(t *testing.T) {
	for _, mode := range []string{"scope", "cancel", "model", "capacity", "closed"} {
		t.Run(mode, func(t *testing.T) {
			m := &overlaySummarizer{should: true}
			s, caller := summaryOverlayFixture(t, m)
			appendSummaryEvent(t, s, caller, "one", "tea", "")
			ctx := context.Background()
			switch mode {
			case "scope":
				caller.ID = "other"
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "model":
				m.run = func(context.Context, *session.Session) (string, error) { return "", errors.New("model failed") }
			case "capacity":
				m.run = func(context.Context, *session.Session) (string, error) { return strings.Repeat("x", 1<<20), nil }
			case "closed":
				s.Close()
			}
			if err := s.EnqueueSummaryJob(ctx, caller, "", true); err == nil {
				t.Fatal("expected error")
			}
			if s.Err() == nil {
				t.Fatal("error not sticky")
			}
			if _, err := s.Snapshot(); err == nil {
				t.Fatal("failed candidate escaped")
			}
		})
	}
}
func TestSummaryOverlayCanceledModelCannotPublish(t *testing.T) {
	entered := make(chan struct{})
	m := &overlaySummarizer{should: true, run: func(ctx context.Context, _ *session.Session) (string, error) {
		close(entered)
		<-ctx.Done()
		return "ignored late result", nil
	}}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "tea", "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.EnqueueSummaryJob(ctx, caller, "", true) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err == nil {
		t.Fatal("canceled summary exported")
	}
}
func TestSummaryOverlayLegacyNoopAndForce(t *testing.T) {
	legacy, err := newOverlay("tenant", "legacy", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = legacy.EnqueueSummaryJob(context.Background(), nil, "root", true); err != nil {
		t.Fatal(err)
	}
	if text, ok := legacy.GetSessionSummaryText(context.Background(), nil); ok || text != "" {
		t.Fatal("legacy summary enabled")
	}
	m := &overlaySummarizer{should: false}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "tea", "")
	if err = s.CreateSessionSummary(context.Background(), caller, "", false); err != nil {
		t.Fatal(err)
	}
	if m.count() != 0 {
		t.Fatal("ignored SDK trigger")
	}
	if err = s.CreateSessionSummary(context.Background(), caller, "", true); err != nil {
		t.Fatal(err)
	}
	if m.count() != 1 {
		t.Fatal("force not forwarded")
	}
}

func TestSummaryOverlaySDKFilterViews(t *testing.T) {
	m := &overlaySummarizer{should: true, run: func(_ context.Context, s *session.Session) (string, error) {
		var parts []string
		for _, e := range s.Events {
			if e.Response != nil && len(e.Choices) > 0 {
				parts = append(parts, e.Choices[0].Message.Content)
			}
		}
		return strings.Join(parts, ";"), nil
	}}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "selected", "root/a")
	appendSummaryEvent(t, s, caller, "two", "sibling", "root/b")
	if err := s.EnqueueSummaryJob(context.Background(), caller, "root/a", false); err != nil {
		t.Fatal(err)
	}
	if text, ok := s.GetSessionSummaryText(context.Background(), caller, session.WithSummaryFilterKey("root/a")); !ok || text != "selected" {
		t.Fatalf("SDK filter wrong: %q %v", text, ok)
	}
	if text, ok := s.GetSessionSummaryText(context.Background(), caller, session.WithSummaryFilterKey("root/b")); ok || text != "" {
		t.Fatal("sibling summary leaked", text, ok)
	}
	if err := s.CreateSessionSummary(context.Background(), caller, "", false); err != nil {
		t.Fatal(err)
	}
	if text, ok := s.GetSessionSummaryText(context.Background(), caller); !ok || !strings.Contains(text, "selected") || !strings.Contains(text, "sibling") {
		t.Fatal("full-session view wrong", text, ok)
	}
}
func TestSummaryOverlayQueuedCancellationAndGetterScope(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	m := &overlaySummarizer{should: true, run: func(context.Context, *session.Session) (string, error) {
		close(entered)
		<-release
		return "summary", nil
	}}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "tea", "")
	first := make(chan error, 1)
	go func() { first <- s.CreateSessionSummary(context.Background(), caller, "", true) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.EnqueueSummaryJob(ctx, caller, "", true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-first; err == nil {
		t.Fatal("sticky cancellation not respected on merge")
	}
	other, otherCaller := summaryOverlayFixture(t, &overlaySummarizer{should: true})
	otherCaller.ID = "spoof"
	if _, ok := other.GetSessionSummaryText(context.Background(), otherCaller); ok || other.Err() == nil {
		t.Fatal("getter scope failure not sticky")
	}
}

func TestSummaryOverlaySnapshotRejectsInFlightResult(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	m := &overlaySummarizer{should: true, run: func(context.Context, *session.Session) (string, error) {
		close(entered)
		<-release
		return "late summary", nil
	}}
	s, caller := summaryOverlayFixture(t, m)
	appendSummaryEvent(t, s, caller, "one", "tea", "")
	job := make(chan error, 1)
	go func() { job <- s.CreateSessionSummary(context.Background(), caller, "", true) }()
	<-entered
	type snapshotResult struct {
		body []byte
		err  error
	}
	snapshotDone := make(chan snapshotResult, 1)
	go func() { body, err := s.Snapshot(); snapshotDone <- snapshotResult{body, err} }()
	select {
	case result := <-snapshotDone:
		if result.body != nil || !errors.Is(result.err, ErrSummaryInProgress) {
			close(release)
			t.Fatalf("busy snapshot escaped: %s %v", result.body, result.err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("Snapshot blocked on model")
	}
	close(release)
	if err := <-job; !errors.Is(err, ErrSummaryInProgress) {
		t.Fatalf("late result ignored sticky boundary: %v", err)
	}
	if body, err := s.Snapshot(); body != nil || !errors.Is(err, ErrSummaryInProgress) {
		t.Fatal("failed Attempt candidate escaped", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stored.Summaries) != 0 {
		t.Fatal("late summary imported into failed attempt")
	}
}
func TestSummaryOverlayClosedSnapshotAndLegacyCompatibility(t *testing.T) {
	s, _ := summaryOverlayFixture(t, &overlaySummarizer{should: true})
	s.Close()
	if body, err := s.Snapshot(); body != nil || !errors.Is(err, ErrSummaryClosed) {
		t.Fatal("closed summary candidate escaped", err)
	}
	legacy, err := newOverlay("tenant", "legacy", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Close()
	if body, err := legacy.Snapshot(); err != nil || len(body) == 0 {
		t.Fatal("legacy Close/Snapshot changed", err)
	}
}
