package background

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type accountedBackgroundModel struct {
	purpose string
	calls   int
}

func (m *accountedBackgroundModel) Info() model.Info {
	return model.Info{Name: "background-usage-test"}
}
func (m *accountedBackgroundModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	m.calls++
	if m.purpose == "memory" {
		responses, err := (extractionModel{}).GenerateContent(ctx, request)
		if err != nil {
			return nil, err
		}
		out := make(chan *model.Response, 1)
		for r := range responses {
			r.Usage = &model.Usage{PromptTokens: 10, CompletionTokens: 2}
			out <- r
		}
		close(out)
		return out, nil
	}
	out := make(chan *model.Response, 1)
	out <- &model.Response{Done: true, Usage: &model.Usage{PromptTokens: 10, CompletionTokens: 2}, Choices: []model.Choice{{Message: model.NewAssistantMessage("summary of this test conversation")}}}
	close(out)
	return out, nil
}
func TestBackgroundJobsResolveRevisionModelAndChargeUsage(t *testing.T) {
	for _, purpose := range []string{"summary", "memory"} {
		t.Run(purpose, func(t *testing.T) {
			data := controlplane.DefaultBootstrapData()
			data.Revisions[0].AgentConfig = json.RawMessage(`{"name":"tutorial","instruction":"help","summary_every_turns":1}`)
			data.Revisions[0].MemoryConfig = json.RawMessage(`{"auto_extract":true,"every_turns":1}`)
			data.Revisions[0].ModelConfig = json.RawMessage(`{"source":"startup_env","max_completion_tokens":8}`)
			data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
			data.Tenants[0].QuotaConfig = json.RawMessage(`{"daily_completion_tokens":8}`)
			control := controlplane.NewMemoryRepository(data)
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(control)
			g, _ := tenant.NewGuard(context.Background(), control, config.QuotaConfig{Backend: "local"})
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(g)
			base := &accountedBackgroundModel{purpose: purpose}
			compiler, err := agentmodel.NewRevisionCompiler(control, base, false, agentmodel.WithModelBudget(g))
			if err != nil {
				t.Fatal(err)
			}
			jobs := NewMemoryRepository()
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(jobs)
			sessions := inmemory.NewSessionService(inmemory.WithSummarizer(NewJobSummarizer()))
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(sessions)
			memories, _ := platformstorage.NewMemoryRouter(control, secret.StaticStore{})
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(memories)
			kb, _ := platformstorage.NewKnowledgeRouter(control, secret.StaticStore{})
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(kb)
			key := session.Key{AppName: runtimecontext.TutorialScope().StorageScope, UserID: "synthetic-user", SessionID: purpose}
			ctx := context.Background()
			sess, err := sessions.CreateSession(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := sessions.AppendEvent(ctx, sess, &event.Event{Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("I prefer tea")}}}}); err != nil {
				t.Fatal(err)
			}
			processor, err := NewProcessor(jobs, control, sessions, memories, kb, agentmodel.DisabledModel{}, nil, ProcessorOptions{WorkerID: "test", ClaimLease: 10 * time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond, ModelResolver: compiler.ModelForRevision})
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(SessionJobPayload{StorageScope: key.AppName, UserID: key.UserID, SessionID: key.SessionID, TurnSeq: 1})
			typ := JobSummary
			if purpose == "memory" {
				typ = JobMemoryExtract
			}
			if _, err := jobs.Enqueue(ctx, EnqueueRequest{TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1", Type: typ, DedupeKey: purpose, Payload: payload}); err != nil {
				t.Fatal(err)
			}
			if ok, err := processor.ProcessOne(ctx); err != nil || !ok {
				t.Fatalf("process=%t %v", ok, err)
			}
			if base.calls != 1 {
				t.Fatalf("resolved model calls=%d", base.calls)
			}
			if _, err := g.ReserveModel(ctx, "tutorial-tenant", 0, 7, 0); !errors.Is(err, tenant.ErrBudgetExceeded) {
				t.Fatal("background usage was not settled against tenant")
			}
		})
	}
}
