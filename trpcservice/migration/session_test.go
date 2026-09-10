package migration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestCopyAndVerifySession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = source.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}
	sourceSession, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")})
	if err != nil {
		t.Fatalf("create source session: %v", err)
	}
	if err := source.AppendEvent(ctx, sourceSession, event.NewResponseEvent("invocation-a", "user", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewUserMessage("summarize billing"),
		}},
	})); err != nil {
		t.Fatalf("append source user event: %v", err)
	}
	if err := source.AppendEvent(ctx, sourceSession, event.NewResponseEvent("invocation-b", "assistant", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewAssistantMessage("billing reply"),
		}},
	})); err != nil {
		t.Fatalf("append source event: %v", err)
	}

	copier := migration.RedisPostgresCopier{Source: source, Target: target}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy session: %v", err)
	}
	if err := copier.VerifySession(ctx, key); err != nil {
		t.Fatalf("verify copied session: %v", err)
	}
}

func TestCopySessionRequiresSummaryImporter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sourceBase := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sourceBase.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}
	sourceSession := session.NewSession(key.AppName, key.UserID, key.SessionID)
	sourceSession.Summaries[""] = &session.Summary{
		Summary:   "billing history",
		UpdatedAt: time.Now().UTC(),
	}
	source := summarySessionService{Service: sourceBase, value: sourceSession}

	withoutImporter := migration.RedisPostgresCopier{Source: source, Target: target}
	if err := withoutImporter.CopySession(ctx, key); !errors.Is(err, migration.ErrSummaryImportRequired) {
		t.Fatalf("copy session error = %v, want summary importer", err)
	}

	importer := &summaryImportSpy{}
	copier := migration.RedisPostgresCopier{
		Source:    source,
		Target:    target,
		Summaries: importer,
	}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy session with importer: %v", err)
	}
	if importer.key != key {
		t.Fatalf("import key = %#v, want %#v", importer.key, key)
	}
	if got := importer.summaries[""]; got == nil || got.Summary != "billing history" {
		t.Fatalf("imported summary = %#v", got)
	}
}

func TestCopySessionDoesNotIgnoreUnavailableSummaries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sourceBase := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sourceBase.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-summary-error"}
	sourceSession := session.NewSession(key.AppName, key.UserID, key.SessionID,
		session.WithSessionEvents([]event.Event{{
			ID:       "event-1",
			Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("history")}}},
		}}),
	)
	source := summarySessionService{
		Service:    sourceBase,
		value:      sourceSession,
		summaryErr: migration.ErrSummaryImportRequired,
	}

	err := (migration.RedisPostgresCopier{Source: source, Target: target}).CopySession(ctx, key)
	if !errors.Is(err, migration.ErrSummaryImportRequired) {
		t.Fatalf("copy session error = %v, want summary import error", err)
	}
	if value, getErr := target.GetSession(ctx, key, session.WithEventNum(math.MaxInt)); getErr != nil || value != nil {
		t.Fatalf("target after summary error = %#v, %v; want no committed target", value, getErr)
	}
}

func TestCopyZeroEventSessionUsesExplicitSummarySource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sourceBase := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sourceBase.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-zero"}
	if _, err := sourceBase.CreateSession(ctx, key, nil); err != nil {
		t.Fatalf("create source session: %v", err)
	}
	summary := &session.Summary{Summary: "retained summary", UpdatedAt: time.Now().UTC()}
	source := explicitSummarySource{
		Service:   sourceBase,
		summaries: map[string]*session.Summary{"": summary},
	}
	importer := &summaryImportSpy{}
	copier := migration.RedisPostgresCopier{Source: source, Target: target, Summaries: importer}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy zero-event session: %v", err)
	}
	if got := importer.summaries[""]; got == nil || got.Summary != summary.Summary {
		t.Fatalf("imported zero-event summary = %#v", got)
	}
}

func TestRedisPostgresCopierRequestsCompleteEventHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sourceBase := inmemory.NewSessionService(inmemory.WithSessionEventLimit(0))
	targetBase := inmemory.NewSessionService(inmemory.WithSessionEventLimit(0))
	t.Cleanup(func() { _ = sourceBase.Close() })
	t.Cleanup(func() { _ = targetBase.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}
	const wantEvents = 3
	sourceSession, err := sourceBase.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("create source session: %v", err)
	}
	for i := 0; i < wantEvents; i++ {
		value := event.New(fmt.Sprintf("invocation-%d", i), "user")
		value.ID = fmt.Sprintf("event-%d", i)
		value.Response = &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleUser, Content: fmt.Sprintf("event-%d", i)},
		}}}
		if err := sourceBase.AppendEvent(ctx, sourceSession, value); err != nil {
			t.Fatalf("append source event %d: %v", i, err)
		}
	}
	stored, err := sourceBase.GetSession(ctx, key, session.WithEventNum(math.MaxInt))
	if err != nil {
		t.Fatalf("get stored source session: %v", err)
	}
	if got := len(stored.Events); got != wantEvents {
		t.Fatalf("stored source events = %d, want %d", got, wantEvents)
	}
	if stored.Events[0].ID != "event-0" || stored.Events[wantEvents-1].ID != "event-2" {
		t.Fatalf("source event endpoints = %q, %q", stored.Events[0].ID, stored.Events[wantEvents-1].ID)
	}
	source := &recordingSessionService{Service: sourceBase}
	target := &recordingSessionService{Service: targetBase}
	copier := migration.RedisPostgresCopier{Source: source, Target: target}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy session: %v", err)
	}
	if err := copier.VerifySession(ctx, key); err != nil {
		t.Fatalf("verify session: %v", err)
	}
	if source.eventNum != math.MaxInt || target.eventNum != math.MaxInt {
		t.Fatalf("event limits = source %d target %d, want %d", source.eventNum, target.eventNum, math.MaxInt)
	}
	copied, err := targetBase.GetSession(ctx, key, session.WithEventNum(math.MaxInt))
	if err != nil {
		t.Fatalf("get copied session: %v", err)
	}
	if got := len(copied.Events); got != wantEvents {
		t.Fatalf("copied events = %d, want %d", got, wantEvents)
	}
	for i, value := range copied.Events {
		wantID := fmt.Sprintf("event-%d", i)
		wantContent := fmt.Sprintf("event-%d", i)
		if value.ID != wantID || value.Response == nil || len(value.Choices) != 1 ||
			value.Choices[0].Message.Content != wantContent {
			var content string
			if value.Response != nil && len(value.Choices) > 0 {
				content = value.Choices[0].Message.Content
			}
			t.Fatalf("copied event %d = id %q content %q, want id %q content %q", i,
				value.ID, content, wantID, wantContent)
		}
	}
}

func TestRedisPostgresCopierVerifyUsesSemanticProjection(t *testing.T) {
	t.Parallel()
	now := time.Now()
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-semantic"}
	left := session.NewSession(key.AppName, key.UserID, key.SessionID,
		session.WithSessionEvents([]event.Event{{
			ID:         "event-1",
			Version:    1,
			Timestamp:  now,
			StateDelta: nil,
			Response:   &model.Response{ID: "response-1", Timestamp: now.Add(-time.Minute)},
		}}),
		session.WithSessionSummaries(map[string]*session.Summary{
			"": &session.Summary{Summary: "summary", Topics: []string{"agent", "user"}, UpdatedAt: now},
		}),
	)
	left.Tracks = map[session.Track]*session.TrackEvents{
		"trace": {Track: "trace", Events: []session.TrackEvent{{
			Track: "trace", Payload: json.RawMessage(`{"a":1,"b":[2]}`), Timestamp: now,
		}}},
	}
	right := left.Clone()
	right.Events[0].Timestamp = time.Unix(0, now.UnixNano()).In(time.FixedZone("offset", 8*60*60))
	right.Events[0].Response.Timestamp = now.Add(time.Hour)
	right.Events[0].StateDelta = map[string][]byte{}
	right.Tracks["trace"].Events[0].Payload = json.RawMessage(` { "b": [2], "a": 1 } `)
	right.Tracks["trace"].Events[0].Timestamp = right.Events[0].Timestamp
	right.Summaries[""].Topics = []string{"user", "agent"}
	right.Summaries[""].UpdatedAt = right.Events[0].Timestamp
	copier := migration.RedisPostgresCopier{
		Source: summarySessionService{value: left},
		Target: summarySessionService{value: right},
	}
	if err := copier.VerifySession(context.Background(), key); err != nil {
		t.Fatalf("verify semantically equivalent session: %v", err)
	}
}

type summaryImportSpy struct {
	key       session.Key
	summaries map[string]*session.Summary
}

type summarySessionService struct {
	session.Service
	value      *session.Session
	summaryErr error
}

type explicitSummarySource struct {
	session.Service
	summaries map[string]*session.Summary
}

func (s explicitSummarySource) GetSessionSummaries(
	context.Context,
	session.Key,
) (map[string]*session.Summary, error) {
	return cloneSummaryMap(s.summaries), nil
}

type recordingSessionService struct {
	session.Service
	eventNum int
}

func (s *recordingSessionService) GetSession(
	ctx context.Context,
	key session.Key,
	options ...session.Option,
) (*session.Session, error) {
	values := &session.Options{}
	for _, option := range options {
		option(values)
	}
	s.eventNum = values.EventNum
	return s.Service.GetSession(ctx, key, options...)
}

func (*recordingSessionService) GetSessionSummaries(
	context.Context,
	session.Key,
) (map[string]*session.Summary, error) {
	return nil, nil
}

func (s summarySessionService) GetSession(
	_ context.Context,
	_ session.Key,
	_ ...session.Option,
) (*session.Session, error) {
	return s.value.Clone(), nil
}

func (s summarySessionService) GetSessionSummaries(
	context.Context,
	session.Key,
) (map[string]*session.Summary, error) {
	return nil, s.summaryErr
}

func (s *summaryImportSpy) ReplaceSessionSummaries(
	_ context.Context,
	key session.Key,
	summaries map[string]*session.Summary,
) error {
	s.key = key
	s.summaries = summaries
	return nil
}

func cloneSummaryMap(source map[string]*session.Summary) map[string]*session.Summary {
	if source == nil {
		return nil
	}
	target := make(map[string]*session.Summary, len(source))
	for key, value := range source {
		target[key] = value.Clone()
	}
	return target
}
