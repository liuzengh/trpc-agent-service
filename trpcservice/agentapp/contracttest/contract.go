// Package contracttest defines backend-neutral Agent App repository behavior.
package contracttest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
)

type Factory func(testing.TB, string) agentapp.Repository

var tenantIDs = []string{
	"t_01ARZ3NDEKTSV4RRFFQ69G5FAA",
	"t_01ARZ3NDEKTSV4RRFFQ69G5FAB",
	"t_01ARZ3NDEKTSV4RRFFQ69G5FAC",
	"t_01ARZ3NDEKTSV4RRFFQ69G5FAD",
}

func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("publish_failure_preserves_current_revision", func(t *testing.T) {
		repository := factory(t, tenantIDs[0])
		app, draft := createAppAndDraft(t, repository, tenantIDs[0])
		_, err := repository.Publish(context.Background(), agentapp.PublishInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion + 1, ChangeMetadata: metadata()})
		if !errors.Is(err, agentapp.ErrVersionConflict) {
			t.Fatalf("publish err=%v", err)
		}
		current, err := repository.Get(context.Background(), app.TenantID, app.AgentAppID)
		if err != nil || current.CurrentRevision != 0 || current.Version != app.Version+1 {
			t.Fatalf("current=%#v err=%v", current, err)
		}
	})

	t.Run("published_revision_is_immutable_and_historical_rollback_works", func(t *testing.T) {
		repository := factory(t, tenantIDs[1])
		app, firstDraft := createAppAndDraft(t, repository, tenantIDs[1])
		first, err := repository.Publish(context.Background(), agentapp.PublishInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, Revision: firstDraft.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: metadata()})
		if err != nil {
			t.Fatal(err)
		}
		firstDraft.Instruction = "mutated"
		if _, err := repository.UpdateDraft(context.Background(), agentapp.UpdateDraftInput{Revision: firstDraft, ExpectedDraftVersion: 1, ChangeMetadata: metadata()}); !errors.Is(err, agentapp.ErrImmutable) {
			t.Fatalf("published update=%v", err)
		}
		secondDraft, err := repository.CreateDraft(context.Background(), agentapp.CreateDraftInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, ExpectedAppVersion: first.App.Version, Revision: revision("second"), ChangeMetadata: metadata()})
		if err != nil {
			t.Fatal(err)
		}
		second, err := repository.Publish(context.Background(), agentapp.PublishInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, Revision: secondDraft.Revision, ExpectedAppVersion: first.App.Version + 1, ExpectedDraftVersion: 1, ChangeMetadata: metadata()})
		if err != nil {
			t.Fatal(err)
		}
		rolled, err := repository.Rollback(context.Background(), agentapp.RollbackInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, TargetRevision: firstDraft.Revision, ExpectedAppVersion: second.App.Version, ChangeMetadata: metadata()})
		if err != nil || rolled.App.CurrentRevision != firstDraft.Revision || rolled.App.Version <= second.App.Version {
			t.Fatalf("rollback=%#v err=%v", rolled, err)
		}
	})

	t.Run("concurrent_draft_and_publish_have_one_cas_winner", func(t *testing.T) {
		repository := factory(t, tenantIDs[2])
		app, err := repository.Create(context.Background(), agentapp.CreateInput{App: appValue(tenantIDs[2]), ChangeMetadata: metadata()})
		if err != nil {
			t.Fatal(err)
		}
		results := runTwo(func() error {
			_, err := repository.CreateDraft(context.Background(), agentapp.CreateDraftInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, ExpectedAppVersion: app.Version, Revision: revision("race"), ChangeMetadata: metadata()})
			return err
		})
		assertOneWinner(t, results)
		current, err := repository.Get(context.Background(), app.TenantID, app.AgentAppID)
		if err != nil {
			t.Fatal(err)
		}
		draft, err := repository.GetRevision(context.Background(), app.TenantID, app.AgentAppID, 1)
		if err != nil {
			t.Fatal(err)
		}
		results = runTwo(func() error {
			_, err := repository.Publish(context.Background(), agentapp.PublishInput{TenantID: app.TenantID, AgentAppID: app.AgentAppID, Revision: draft.Revision, ExpectedAppVersion: current.Version, ExpectedDraftVersion: draft.DraftVersion, ChangeMetadata: metadata()})
			return err
		})
		assertOneWinner(t, results)
	})

	t.Run("nil_json_objects_are_canonical", func(t *testing.T) {
		repository := factory(t, tenantIDs[3])
		_, draft := createAppAndDraft(t, repository, tenantIDs[3])
		if draft.GenerationConfig == nil || draft.RuntimePolicy == nil {
			t.Fatalf("draft=%#v", draft)
		}
	})
}

func createAppAndDraft(t *testing.T, repository agentapp.Repository, tenantID string) (agentapp.AgentApp, agentapp.Revision) {
	t.Helper()
	app, err := repository.Create(context.Background(), agentapp.CreateInput{App: appValue(tenantID), ChangeMetadata: metadata()})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repository.CreateDraft(context.Background(), agentapp.CreateDraftInput{TenantID: tenantID, AgentAppID: app.AgentAppID, ExpectedAppVersion: app.Version, Revision: revision("first"), ChangeMetadata: metadata()})
	if err != nil {
		t.Fatal(err)
	}
	return app, draft
}

func appValue(tenantID string) agentapp.AgentApp {
	return agentapp.AgentApp{TenantID: tenantID, AgentAppID: "app_" + tenantID[2:], AgentAppKey: "contract-app", DisplayName: "Contract App"}
}

func revision(instruction string) agentapp.Revision {
	return agentapp.Revision{AgentKind: "llm", Instruction: instruction, ModelProfileID: "model", ModelProfileVersion: 1}
}

func metadata() agentapp.ChangeMetadata {
	return agentapp.ChangeMetadata{ActorType: "test", ActorID: "contract", Reason: "contract", CorrelationID: "contract", TraceID: "contract"}
}

func runTwo(call func() error) []error {
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results <- call()
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	return []error{<-results, <-results}
}

func assertOneWinner(t *testing.T, results []error) {
	t.Helper()
	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, agentapp.ErrVersionConflict), errors.Is(err, agentapp.ErrImmutable):
			conflicts++
		default:
			t.Fatalf("unexpected result=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d results=%v", successes, conflicts, results)
	}
}
