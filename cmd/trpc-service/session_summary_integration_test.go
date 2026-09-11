package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

func TestPostgresSessionServicePersistsFrameworkSummary(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	summarizer := sessionsummary.NewSummarizer(testutil.NewFakeModel("框架摘要"))
	service, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithSummarizer(summarizer),
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	id := fmt.Sprintf("summary-%d", time.Now().UnixNano())
	key := agentsession.Key{AppName: "integration/summary", UserID: "user-summary", SessionID: id}
	sess, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	t.Cleanup(func() { _ = service.DeleteSession(context.Background(), key) })
	if err := service.AppendEvent(context.Background(), sess, &event.Event{
		ID: id + "-user", Author: "user", Timestamp: time.Now().UTC(),
		Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("请记住这次讨论的结论")}}},
	}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if err := service.EnqueueSummaryJob(context.Background(), sess, agentsession.SummaryFilterKeyAllContents, true); err != nil {
		t.Fatalf("EnqueueSummaryJob() error = %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		fresh, getErr := service.GetSession(context.Background(), key)
		if getErr != nil {
			t.Fatalf("GetSession() error = %v", getErr)
		}
		if summary, ok := service.GetSessionSummaryText(context.Background(), fresh); ok {
			if summary != "框架摘要" {
				t.Fatalf("summary = %q, want framework output", summary)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("framework summary was not persisted before deadline")
}

func TestPostgresSessionServiceCreatesFrameworkSummarySynchronously(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	summarizer := sessionsummary.NewSummarizer(testutil.NewFakeModel("框架摘要"))
	service, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithSummarizer(summarizer),
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	id := fmt.Sprintf("summary-sync-%d", time.Now().UnixNano())
	key := agentsession.Key{AppName: "integration/summary", UserID: "user-summary", SessionID: id}
	sess, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	t.Cleanup(func() { _ = service.DeleteSession(context.Background(), key) })
	if err := service.AppendEvent(context.Background(), sess, &event.Event{
		ID: id + "-user", Author: "user", Timestamp: time.Now().UTC(),
		Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("请记住这次讨论的结论")}}},
	}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(sess.Events) != 1 {
		t.Fatalf("session events = %d, want 1", len(sess.Events))
	}
	if err := service.CreateSessionSummary(context.Background(), sess, agentsession.SummaryFilterKeyAllContents, true); err != nil {
		t.Fatalf("CreateSessionSummary() error = %v", err)
	}
	if summary, ok := service.GetSessionSummaryText(context.Background(), sess); !ok || summary != "框架摘要" {
		t.Fatalf("in-memory summary = %q, %v; want framework output", summary, ok)
	}

	fresh, err := service.GetSession(context.Background(), key)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if len(fresh.Events) != 1 {
		t.Fatalf("fresh session events = %d, want 1", len(fresh.Events))
	}
	if len(fresh.Summaries) != 1 {
		t.Fatalf("fresh session summaries = %d, want 1 (created_at=%s, original_created_at=%s)", len(fresh.Summaries), fresh.CreatedAt, sess.CreatedAt)
	}
	summary, ok := service.GetSessionSummaryText(context.Background(), fresh)
	if !ok || summary != "框架摘要" {
		t.Fatalf("GetSessionSummaryText() = %q, %v; want framework output", summary, ok)
	}
}
