package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type requestCaptureModel struct {
	model.Model
	requests chan []model.Message
}

func (m *requestCaptureModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	m.requests <- append([]model.Message(nil), req.Messages...)
	return m.Model.GenerateContent(ctx, req)
}

type markerSummarizer struct{}

func (markerSummarizer) ShouldSummarize(*session.Session) bool { return true }
func (markerSummarizer) Summarize(context.Context, *session.Session) (string, error) {
	return "SUMMARY_MARKER", nil
}
func (markerSummarizer) SetPrompt(string)         {}
func (markerSummarizer) SetModel(model.Model)     {}
func (markerSummarizer) Metadata() map[string]any { return nil }

func TestRuntimeUsesPersistedSummaryOnlyWhenConfigured(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			data := controlplane.DefaultBootstrapData()
			frequency := 0
			if enabled {
				frequency = 1
			}
			data.Revisions[0].AgentConfig = json.RawMessage(fmt.Sprintf(`{"name":"tutorial-agent","instruction":"help","summary_every_turns":%d}`, frequency))
			data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
			repo := controlplane.NewMemoryRepository(data)
			defer func() { _ = repo.Close() }()
			m := &requestCaptureModel{Model: NewTutorialModel(), requests: make(chan []model.Message, 4)}
			compiler, err := NewRevisionCompiler(repo, m, false)
			if err != nil {
				t.Fatal(err)
			}
			sessions, err := storage.NewSessionRouter(repo, secret.StaticStore{}, inmemory.NewSessionService(), markerSummarizer{})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewRuntimeWithCompilerServices(m, compiler, sessions, coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = runtime.Close() }()
			scope := runtimecontext.TutorialScope()
			ctx := runtimecontext.WithStorageScope(context.Background(), scope.StorageScope)
			key := session.Key{AppName: scope.StorageScope, UserID: "user", SessionID: "summary"}
			sess, err := sessions.CreateSession(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			evt := &event.Event{ID: "old-event", Timestamp: time.Now(), FilterKey: scope.StorageScope, Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("OLD_EVENT_MARKER")}}}}
			if err := sessions.AppendEvent(ctx, sess, evt); err != nil {
				t.Fatal(err)
			}
			if err := sessions.CreateSessionSummary(ctx, sess, "", true); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.ChatWithScope(context.Background(), ChatInput{Scope: scope, ChatType: "direct", UserID: key.UserID, SessionID: key.SessionID, MessageID: "next", Text: "NEW_MESSAGE_MARKER"}); err != nil {
				t.Fatal(err)
			}
			request := <-m.requests
			var contents strings.Builder
			for _, message := range request {
				contents.WriteString(message.Content)
				contents.WriteByte('\n')
			}
			text := contents.String()
			if strings.Contains(text, "SUMMARY_MARKER") != enabled || strings.Contains(text, "OLD_EVENT_MARKER") == enabled || !strings.Contains(text, "NEW_MESSAGE_MARKER") {
				t.Fatalf("unexpected summary projection (enabled=%t): %s", enabled, text)
			}
		})
	}
}
