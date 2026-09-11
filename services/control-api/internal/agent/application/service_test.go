package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

const validSpec = `{
  "schema_version":"v1",
  "root":"assistant",
  "requirements":{
    "models":{"primary":{"capabilities":["chat"]}},
    "tools":{},
    "knowledge":{}
  },
  "nodes":{
    "assistant":{
      "kind":"llm",
      "instruction":"Answer accurately.",
      "model_slot":"primary",
      "tool_slots":[],
      "knowledge_slots":[]
    }
  }
}`

func TestAgentV1LifecycleAndTenantBoundary(t *testing.T) {
	store := newMemoryStore()
	clock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	id := 0
	service := application.NewService(application.Dependencies{
		Store:        store,
		TenantAccess: accessStub{members: map[string]bool{"tnt_a/usr_author": true}},
		NewAgentID:   func() (string, error) { return "agt_1", nil },
		NewVersionID: func() (string, error) {
			id++
			return fmt.Sprintf("agv_%d", id), nil
		},
		Now: func() time.Time { return clock },
	})
	ctx := context.Background()

	created, err := service.CreateAgent(ctx, application.CreateAgentCommand{
		TenantID: "tnt_a", ActorUserID: "usr_author", Name: "  Support  ",
		Description: " First agent ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Agent.Name != "Support" || created.Draft.Revision != 1 || string(created.Draft.Spec) != `{}` {
		t.Fatalf("created = %#v", created)
	}

	if _, err := service.GetAgent(ctx, "tnt_a", "agt_1", "usr_outsider"); !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("cross-tenant authorization error = %v", err)
	}

	incomplete, _, err := service.SaveDraft(ctx, application.SaveDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author",
		ExpectedRevision: 1, Spec: json.RawMessage(`{"nodes":{}}`),
	})
	if err != nil || incomplete.Revision != 2 {
		t.Fatalf("save incomplete draft = %#v, %v", incomplete, err)
	}
	report, err := service.ValidateDraft(ctx, application.ValidateDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author", ExpectedRevision: 2,
	})
	if err != nil || report.Valid {
		t.Fatalf("validate incomplete = %#v, %v", report, err)
	}

	draft, _, err := service.SaveDraft(ctx, application.SaveDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author",
		ExpectedRevision: 2, Spec: json.RawMessage(validSpec),
	})
	if err != nil || draft.Revision != 3 {
		t.Fatalf("save valid draft = %#v, %v", draft, err)
	}
	if _, _, err := service.SaveDraft(ctx, application.SaveDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author",
		ExpectedRevision: 2, Spec: json.RawMessage(validSpec),
	}); !errors.Is(err, application.ErrDraftRevisionConflict) {
		t.Fatalf("stale save error = %v", err)
	}

	first, err := service.PublishAgentVersion(ctx, application.PublishVersionCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author", ExpectedRevision: 3,
	})
	if err != nil || !first.Created || first.Version.VersionNumber != 1 || !first.Report.Valid {
		t.Fatalf("first publication = %#v, %v", first, err)
	}
	second, err := service.PublishAgentVersion(ctx, application.PublishVersionCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author", ExpectedRevision: 3,
	})
	if err != nil || second.Created || second.Version.ID != first.Version.ID {
		t.Fatalf("idempotent publication = %#v, %v", second, err)
	}

	_, _, err = service.SaveDraft(ctx, application.SaveDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author",
		ExpectedRevision: 3, Spec: json.RawMessage(`{"nodes":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := service.GetAgentVersion(ctx, "tnt_a", "agt_1", "usr_author", 1)
	if err != nil || string(published.Spec) != string(first.Version.Spec) {
		t.Fatalf("published version changed with draft: %#v, %v", published, err)
	}
}

func TestSaveDraftRejectsUnsafeDocumentWithoutMutation(t *testing.T) {
	store := newMemoryStore()
	store.agents["tnt_a/agt_1"] = domain.Agent{ID: "agt_1", TenantID: "tnt_a"}
	store.drafts["tnt_a/agt_1"] = domain.AgentDraft{
		TenantID: "tnt_a", AgentID: "agt_1", Revision: 1, Spec: json.RawMessage(`{}`),
	}
	service := application.NewService(application.Dependencies{
		Store: store, TenantAccess: accessStub{members: map[string]bool{"tnt_a/usr_author": true}},
		NewAgentID:   func() (string, error) { return "agt_2", nil },
		NewVersionID: func() (string, error) { return "agv_1", nil },
		Now:          time.Now,
	})
	_, report, err := service.SaveDraft(context.Background(), application.SaveDraftCommand{
		TenantID: "tnt_a", AgentID: "agt_1", ActorUserID: "usr_author",
		ExpectedRevision: 1, Spec: json.RawMessage(`{"api_key":"secret"}`),
	})
	if !errors.Is(err, application.ErrAgentSpecInvalid) || report.Valid {
		t.Fatalf("unsafe save report = %#v, err = %v", report, err)
	}
	if got := store.drafts["tnt_a/agt_1"].Revision; got != 1 {
		t.Fatalf("revision = %d, want 1", got)
	}
}

type accessStub struct {
	members map[string]bool
}

func (stub accessStub) IsActiveMember(_ context.Context, tenantID, userID string) (bool, error) {
	return stub.members[tenantID+"/"+userID], nil
}

type memoryStore struct {
	mu       sync.Mutex
	agents   map[string]domain.Agent
	drafts   map[string]domain.AgentDraft
	versions map[string][]domain.AgentVersion
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		agents: make(map[string]domain.Agent), drafts: make(map[string]domain.AgentDraft),
		versions: make(map[string][]domain.AgentVersion),
	}
}

func resourceKey(tenantID, agentID string) string { return tenantID + "/" + agentID }

func (store *memoryStore) CreateAgent(_ context.Context, agent domain.Agent, draft domain.AgentDraft) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(agent.TenantID, agent.ID)
	store.agents[key] = agent
	store.drafts[key] = draft.Clone()
	return nil
}

func (store *memoryStore) GetAgent(_ context.Context, tenantID, agentID string) (domain.Agent, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	agent, ok := store.agents[resourceKey(tenantID, agentID)]
	if !ok {
		return domain.Agent{}, application.ErrAgentNotFound
	}
	return agent, nil
}

func (store *memoryStore) ListAgents(_ context.Context, tenantID string, page application.Page) (application.AgentPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var agents []domain.Agent
	for _, agent := range store.agents {
		if agent.TenantID == tenantID {
			agents = append(agents, agent)
		}
	}
	return application.AgentPage{Agents: agents, Total: len(agents)}, nil
}

func (store *memoryStore) UpdateAgent(_ context.Context, agent domain.Agent) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(agent.TenantID, agent.ID)
	if _, ok := store.agents[key]; !ok {
		return application.ErrAgentNotFound
	}
	store.agents[key] = agent
	return nil
}

func (store *memoryStore) GetDraft(_ context.Context, tenantID, agentID string) (domain.AgentDraft, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	draft, ok := store.drafts[resourceKey(tenantID, agentID)]
	if !ok {
		return domain.AgentDraft{}, application.ErrAgentNotFound
	}
	return draft.Clone(), nil
}

func (store *memoryStore) SaveDraft(_ context.Context, draft domain.AgentDraft, expectedRevision int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(draft.TenantID, draft.AgentID)
	current, ok := store.drafts[key]
	if !ok {
		return application.ErrAgentNotFound
	}
	if current.Revision != expectedRevision {
		return application.ErrDraftRevisionConflict
	}
	store.drafts[key] = draft.Clone()
	return nil
}

func (store *memoryStore) PublishVersion(_ context.Context, candidate domain.AgentVersion, expectedRevision int64) (domain.AgentVersion, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(candidate.TenantID, candidate.AgentID)
	draft, ok := store.drafts[key]
	if !ok {
		return domain.AgentVersion{}, false, application.ErrAgentNotFound
	}
	if draft.Revision != expectedRevision {
		return domain.AgentVersion{}, false, application.ErrDraftRevisionConflict
	}
	for _, version := range store.versions[key] {
		if version.SourceDraftRevision == expectedRevision {
			return version.Clone(), false, nil
		}
	}
	candidate.VersionNumber = int64(len(store.versions[key]) + 1)
	store.versions[key] = append(store.versions[key], candidate.Clone())
	agent := store.agents[key]
	agent.LatestVersionNumber = &candidate.VersionNumber
	store.agents[key] = agent
	return candidate.Clone(), true, nil
}

func (store *memoryStore) GetVersion(_ context.Context, tenantID, agentID string, number int64) (domain.AgentVersion, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, version := range store.versions[resourceKey(tenantID, agentID)] {
		if version.VersionNumber == number {
			return version.Clone(), nil
		}
	}
	return domain.AgentVersion{}, application.ErrAgentVersionNotFound
}

func (store *memoryStore) ListVersions(_ context.Context, tenantID, agentID string, page application.Page) (application.VersionPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(tenantID, agentID)
	if _, ok := store.agents[key]; !ok {
		return application.VersionPage{}, application.ErrAgentNotFound
	}
	versions := append([]domain.AgentVersion(nil), store.versions[key]...)
	return application.VersionPage{Versions: versions, Total: len(versions)}, nil
}

var _ application.Store = (*memoryStore)(nil)
