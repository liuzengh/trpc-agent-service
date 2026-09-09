package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

var errSummaryDelegated = errors.New("summary delegated")

type summaryTrackingService struct {
	session.Service
	createCalls  int
	enqueueCalls int
}

type summaryFixtureService struct {
	session.Service
	current *session.Session
}

func (s *summaryFixtureService) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return s.current, nil
}

func (s *summaryFixtureService) ListSessions(context.Context, session.UserKey, ...session.Option) ([]*session.Session, error) {
	return []*session.Session{s.current}, nil
}

func (s *summaryTrackingService) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	s.createCalls++
	return errSummaryDelegated
}

func (s *summaryTrackingService) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	s.enqueueCalls++
	return errSummaryDelegated
}

func TestSQLStagingSessionDoesNotWriteBaseBeforePrepare(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	defer base.Close()
	staging := newSQLStagingSession(base, sessionfence.Limits{MaxTurnEvents: 10, MaxTurnBytes: 4096}, false)
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	turn := staging.StartTurn(key, sessionfence.Fence{TaskID: "task", SessionCoord: "coord", SessionSeq: 1})
	ctx := sessionfence.WithTurn(context.Background(), turn)
	sess, err := staging.CreateSession(ctx, key, session.StateMap{"initial": []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	current := event.New("invocation", "author")
	current.Response = &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "answer"}}}}
	if err := staging.AppendEvent(ctx, sess, current); err != nil {
		t.Fatal(err)
	}
	if stored, err := base.GetSession(context.Background(), key); err != nil || stored != nil {
		t.Fatalf("base was written before SQL commit: (%#v, %v)", stored, err)
	}
	commit, err := staging.Prepare(turn)
	if err != nil {
		t.Fatal(err)
	}
	if commit.SessionCoord != "coord" || len(commit.Events) != 1 || string(commit.FinalState["initial"]) != "value" {
		t.Fatalf("unexpected staged commit: %#v", commit)
	}
}

func TestPostgresSummaryGuardAsiaShanghai(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	defer base.Close()
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	oldLocal := time.Local
	time.Local = shanghai
	defer func() { time.Local = oldLocal }()
	fixture := &summaryFixtureService{Service: base, current: session.NewSession("app", "user", "session", session.WithSessionSummaries(map[string]*session.Summary{
		"": {Summary: "must not escape", UpdatedAt: time.Date(2026, 9, 4, 12, 0, 0, 0, shanghai)},
	}))}
	staging := newSQLStagingSession(fixture, sessionfence.Limits{MaxTurnEvents: 10, MaxTurnBytes: 4096}, true)
	if err := staging.CreateSessionSummary(context.Background(), session.NewSession("app", "user", "session"), "", true); !errors.Is(err, persistence.ErrPostgresSummaryDisabled) {
		t.Fatalf("CreateSessionSummary error = %v", err)
	}
	if err := staging.EnqueueSummaryJob(context.Background(), session.NewSession("app", "user", "session"), "", true); !errors.Is(err, persistence.ErrPostgresSummaryDisabled) {
		t.Fatalf("EnqueueSummaryJob error = %v", err)
	}
	stored, err := staging.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	stored.SummariesMu.RLock()
	summaryCount := len(stored.Summaries)
	stored.SummariesMu.RUnlock()
	if summaryCount != 0 {
		t.Fatalf("PostgreSQL Summary guard retained %d summaries", summaryCount)
	}
	if text, ok := staging.GetSessionSummaryText(context.Background(), nil); text != "" || ok {
		t.Fatalf("summary guard returned (%q,%v)", text, ok)
	}
}

func TestMySQLSummaryDelegatesToOfficialService(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	defer base.Close()
	tracked := &summaryTrackingService{Service: base}
	staging := newSQLStagingSession(tracked, sessionfence.Limits{MaxTurnEvents: 10, MaxTurnBytes: 4096}, false)
	if err := staging.CreateSessionSummary(context.Background(), nil, "", true); !errors.Is(err, errSummaryDelegated) {
		t.Fatalf("CreateSessionSummary error = %v", err)
	}
	if err := staging.EnqueueSummaryJob(context.Background(), nil, "", true); !errors.Is(err, errSummaryDelegated) {
		t.Fatalf("EnqueueSummaryJob error = %v", err)
	}
	if tracked.createCalls != 1 || tracked.enqueueCalls != 1 {
		t.Fatalf("summary delegate calls = (%d,%d)", tracked.createCalls, tracked.enqueueCalls)
	}
}
