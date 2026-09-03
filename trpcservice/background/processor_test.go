package background

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentmodel "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

type staticSummarizer struct{}

func (staticSummarizer) ShouldSummarize(*session.Session) bool { return true }
func (staticSummarizer) Summarize(context.Context, *session.Session) (string, error) {
	return "durable session summary", nil
}
func (staticSummarizer) SetPrompt(string)         {}
func (staticSummarizer) SetModel(model.Model)     {}
func (staticSummarizer) Metadata() map[string]any { return map[string]any{} }

type extractionModel struct{}

func (extractionModel) Info() model.Info { return model.Info{Name: "extractor-test"} }
func (extractionModel) GenerateContent(
	context.Context,
	*model.Request,
) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{Choices: []model.Choice{{Message: model.Message{
		Role: model.RoleAssistant,
		ToolCalls: []model.ToolCall{{
			ID: "memory-call", Type: "function",
			Function: model.FunctionDefinitionParam{
				Name:      memory.AddToolName,
				Arguments: []byte(`{"memory":"User likes tea","topics":["preference"]}`),
			},
		}},
	}}}}
	close(responses)
	return responses, nil
}

func TestProcessorRunsKnowledgeJob(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{
		ID: "knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"dimensions":32}`),
	})
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"chunk_size":100,
        "embedding":{"provider":"hash","dimensions":32}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	control := controlplane.NewMemoryRepository(data)
	jobs := NewMemoryRepository()
	sessions := inmemory.NewSessionService()
	memories, _ := platformstorage.NewMemoryRouter(control, secret.StaticStore{})
	knowledgeRouter, _ := platformstorage.NewKnowledgeRouter(control, secret.StaticStore{})
	t.Cleanup(func() {
		_ = knowledgeRouter.Close()
		_ = memories.Close()
		_ = sessions.Close()
		_ = jobs.Close()
		_ = control.Close()
	})
	processor, err := NewProcessor(
		jobs, control, sessions, memories, knowledgeRouter,
		agentmodel.NewTutorialModel(), nil,
		ProcessorOptions{
			WorkerID: "jobs", ClaimLease: time.Second,
			PollInterval: time.Millisecond, RetryDelay: time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	payload, _ := json.Marshal(KnowledgeUpsertPayload{Document: platformstorage.KnowledgeDocument{
		ID: "policy", Name: "Policy", Content: "refunds are available within thirty days",
	}})
	if _, err := jobs.Enqueue(context.Background(), EnqueueRequest{
		TenantID: "tutorial-tenant", AppID: "tutorial-app",
		RevisionID: "tutorial-revision-1", Type: JobKnowledgeUpsert,
		DedupeKey: "policy:v1", Payload: payload,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	processed, err := processor.ProcessOne(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%t err=%v", processed, err)
	}
	kb, _, err := knowledgeRouter.KnowledgeForRevision(
		context.Background(), runtimecontext.TutorialScope(), data.Revisions[0],
	)
	if err != nil {
		t.Fatalf("knowledge: %v", err)
	}
	result, err := kb.Search(context.Background(), &knowledge.SearchRequest{Query: "refunds"})
	if err != nil || result.Document == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestProcessorCreatesDurableSummary(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{
        "name":"tutorial-agent","instruction":"help",
        "summary_every_turns":1
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	control := controlplane.NewMemoryRepository(data)
	jobs := NewMemoryRepository()
	sessions := inmemory.NewSessionService(inmemory.WithSummarizer(staticSummarizer{}))
	memories, _ := platformstorage.NewMemoryRouter(control, secret.StaticStore{})
	knowledgeRouter, _ := platformstorage.NewKnowledgeRouter(control, secret.StaticStore{})
	t.Cleanup(func() {
		_ = knowledgeRouter.Close()
		_ = memories.Close()
		_ = sessions.Close()
		_ = jobs.Close()
		_ = control.Close()
	})
	key := session.Key{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "summary",
	}
	sess, err := sessions.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := sessions.AppendEvent(context.Background(), sess, &event.Event{
		Timestamp: time.Now(), Response: &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage("hello")}},
		},
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	processor, _ := NewProcessor(
		jobs, control, sessions, memories, knowledgeRouter,
		agentmodel.NewTutorialModel(), nil,
		ProcessorOptions{WorkerID: "jobs", ClaimLease: time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond},
	)
	payload, _ := json.Marshal(SessionJobPayload{
		StorageScope: key.AppName, UserID: key.UserID, SessionID: key.SessionID, TurnSeq: 1,
	})
	_, _ = jobs.Enqueue(context.Background(), EnqueueRequest{
		TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1",
		Type: JobSummary, DedupeKey: "summary:1", Payload: payload,
	})
	if processed, err := processor.ProcessOne(context.Background()); err != nil || !processed {
		t.Fatalf("processed=%t err=%v", processed, err)
	}
	stored, err := sessions.GetSession(context.Background(), key)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	text, ok := sessions.GetSessionSummaryText(context.Background(), stored)
	if !ok || text != "durable session summary" {
		t.Fatalf("summary=%q ok=%t", text, ok)
	}
}

func TestProcessorExtractsMemoryAndAdvancesWatermark(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].MemoryConfig = json.RawMessage(`{"auto_extract":true,"every_turns":1}`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	control := controlplane.NewMemoryRepository(data)
	jobs := NewMemoryRepository()
	sessions := inmemory.NewSessionService()
	memories, _ := platformstorage.NewMemoryRouter(control, secret.StaticStore{})
	knowledgeRouter, _ := platformstorage.NewKnowledgeRouter(control, secret.StaticStore{})
	t.Cleanup(func() {
		_ = knowledgeRouter.Close()
		_ = memories.Close()
		_ = sessions.Close()
		_ = jobs.Close()
		_ = control.Close()
	})
	key := session.Key{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "memory",
	}
	sess, _ := sessions.CreateSession(context.Background(), key, nil)
	if err := sessions.AppendEvent(context.Background(), sess, &event.Event{
		Timestamp: time.Now(), Response: &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage("I like tea")}},
		},
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	processor, _ := NewProcessor(
		jobs, control, sessions, memories, knowledgeRouter, extractionModel{}, nil,
		ProcessorOptions{WorkerID: "jobs", ClaimLease: time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond},
	)
	payload, _ := json.Marshal(SessionJobPayload{
		StorageScope: key.AppName, UserID: key.UserID, SessionID: key.SessionID, TurnSeq: 1,
	})
	_, _ = jobs.Enqueue(context.Background(), EnqueueRequest{
		TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1",
		Type: JobMemoryExtract, DedupeKey: "memory:1", Payload: payload,
	})
	if processed, err := processor.ProcessOne(context.Background()); err != nil || !processed {
		t.Fatalf("processed=%t err=%v", processed, err)
	}
	entries, err := memories.ReadMemories(context.Background(), memory.UserKey{
		AppName: key.AppName, UserID: key.UserID,
	}, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "User likes tea" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	stored, _ := sessions.GetSession(context.Background(), key)
	if len(stored.State[memory.SessionStateKeyAutoMemoryLastExtractAt]) == 0 {
		t.Fatal("memory extraction watermark was not stored")
	}
}

var _ summary.SessionSummarizer = staticSummarizer{}
var _ model.Model = extractionModel{}
