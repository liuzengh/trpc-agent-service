package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

const (
	tenantOne = "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	tenantTwo = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
)

func TestCreateScopesKeysByTenantAndReturnsCopies(t *testing.T) {
	r := NewRepository()
	first := createApp(t, r, tenantOne, "support")
	if first.Status != appmodel.StatusDraft || first.Version != 1 {
		t.Fatalf("unexpected app root: %+v", first)
	}
	if _, err := r.Create(context.Background(), createInput(tenantOne, "support")); !errors.Is(err, appmodel.ErrDuplicateKey) {
		t.Fatalf("expected tenant-local duplicate rejection, got %v", err)
	}
	second := createApp(t, r, tenantTwo, "support")
	if second.AppID == first.AppID {
		t.Fatal("generated app identities must differ")
	}
	if _, err := r.Get(context.Background(), tenantTwo, first.AppID); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("cross-tenant lookup must not find app, got %v", err)
	}

	first.DisplayName = "mutated"
	stored, err := r.Get(context.Background(), tenantOne, first.AppID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DisplayName != "Support" {
		t.Fatalf("caller mutation leaked into repository: %+v", stored)
	}
}

func TestDraftCRUDUsesAppAndDraftVersions(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "drafts")
	first := createDraft(t, r, app, draftConfiguration("first"))
	second := createDraft(t, r, app, draftConfiguration("second"))
	if first.Revision != 1 || second.Revision != 2 {
		t.Fatalf("revision allocation must be App-local and monotonic: %d %d", first.Revision, second.Revision)
	}

	updated, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("updated"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.DraftVersion != 2 || updated.Instruction != "updated instruction" {
		t.Fatalf("unexpected updated draft: %+v", updated)
	}
	if _, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("stale"),
	}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("expected stale draft conflict, got %v", err)
	}

	updated.Tools[0].ToolID = "leaked"
	*updated.Generation.Temperature = 1.7
	stored, err := r.GetRevision(context.Background(), tenantOne, app.AppID, first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Tools[0].ToolID == "leaked" || *stored.Generation.Temperature == 1.7 {
		t.Fatal("returned draft did not have defensive slice and pointer copies")
	}

	metadata, err := r.UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version,
		DisplayName: " Updated ", Description: " Changed ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Version != 2 || metadata.DisplayName != "Updated" || metadata.Description != "Changed" {
		t.Fatalf("unexpected metadata update: %+v", metadata)
	}
	if _, err := r.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: draftConfiguration("stale-app"),
	}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("expected stale App version conflict, got %v", err)
	}
	if _, err := r.GetRevision(context.Background(), tenantTwo, app.AppID, first.Revision); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("cross-tenant revision lookup must fail, got %v", err)
	}
}

func TestPublishRollbackAndLifecycleReturnCompleteEvents(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "lifecycle")
	first := createDraft(t, r, app, draftConfiguration("first"))
	app, first, event := assertInitialPublication(t, r, app, first)
	app, first, second := assertSecondPublicationAndRollback(t, r, app, first)
	assertLifecycleTransitions(t, r, app, first, second, event)
}

func TestSetCanarySelectsPublishedRevisionAndRollbackClearsIt(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "canary")
	first := createDraft(t, r, app, draftConfiguration("first"))
	var err error
	app, first, _, err = r.Publish(context.Background(), publishInput(app, first))
	if err != nil {
		t.Fatal(err)
	}
	second := createDraft(t, r, app, draftConfiguration("second"))
	app, second, _, err = r.Publish(context.Background(), publishInput(app, second))
	if err != nil {
		t.Fatal(err)
	}
	third := createDraft(t, r, app, draftConfiguration("third"))
	app, _, _, err = r.Publish(context.Background(), publishInput(app, third))
	if err != nil {
		t.Fatal(err)
	}
	candidate := second.Revision
	selected, event, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, CandidateRevision: &candidate, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: changeMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	if selected.CanaryRevision == nil || *selected.CanaryRevision != candidate || event.EventType != appmodel.ChangeCanaryStarted || event.PreviousRevision != nil || event.CurrentRevision == nil || *event.CurrentRevision != candidate {
		t.Fatalf("canary selection = app=%+v event=%+v", selected, event)
	}
	if _, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, CandidateRevision: &candidate, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale canary version error = %v", err)
	}
	newDraft := createDraft(t, r, selected, draftConfiguration("after-canary"))
	publishedAfterCanary, _, _, err := r.Publish(context.Background(), publishInput(selected, newDraft))
	if err != nil {
		t.Fatal(err)
	}
	if publishedAfterCanary.CanaryRevision != nil {
		t.Fatalf("publishing a new stable revision retained canary: %+v", publishedAfterCanary)
	}
	rolledBack, rollbackEvent, err := r.Rollback(context.Background(), appmodel.RollbackInput{TenantID: tenantOne, AppID: app.AppID, TargetRevision: first.Revision, ExpectedAppVersion: publishedAfterCanary.Version, Metadata: changeMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.CanaryRevision != nil || rollbackEvent.EventType != appmodel.ChangeRolledBack {
		t.Fatalf("rollback did not clear canary: app=%+v event=%+v", rolledBack, rollbackEvent)
	}
}

func TestSetCanaryRejectsInactiveTenantAppAndDraftCandidate(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "canary-invalid")
	draft := createDraft(t, r, app, draftConfiguration("candidate"))
	for name, mutate := range map[string]func(*appmodel.SetCanaryInput){
		"inactive tenant": func(input *appmodel.SetCanaryInput) { input.TenantActive = false },
		"draft app":       func(input *appmodel.SetCanaryInput) {},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := draft.Revision
			input := appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, CandidateRevision: &candidate, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: changeMetadata()}
			mutate(&input)
			if _, _, err := r.SetCanary(context.Background(), input); !errors.Is(err, appmodel.ErrInvalid) {
				t.Fatalf("SetCanary error = %v", err)
			}
		})
	}
}

func TestSetCanaryClearsSelectionAndRejectsInvalidCandidates(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "canary-clear")
	first := createDraft(t, r, app, draftConfiguration("first"))
	var err error
	app, first, _, err = r.Publish(context.Background(), publishInput(app, first))
	if err != nil {
		t.Fatal(err)
	}
	second := createDraft(t, r, app, draftConfiguration("second"))
	app, second, _, err = r.Publish(context.Background(), publishInput(app, second))
	if err != nil {
		t.Fatal(err)
	}
	candidate := first.Revision
	selected, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{
		TenantID: tenantOne, AppID: app.AppID, CandidateRevision: &candidate, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	cleared, event, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: selected.Version, TenantActive: true, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.CanaryRevision != nil || event.EventType != appmodel.ChangeCanaryStopped || event.PreviousRevision == nil || *event.PreviousRevision != candidate || event.CurrentRevision != nil || event.ContentDigest != "" {
		t.Fatalf("canary clear = app=%+v event=%+v", cleared, event)
	}
	stable := second.Revision
	cases := []struct {
		name  string
		input appmodel.SetCanaryInput
	}{
		{name: "zero revision", input: appmodel.SetCanaryInput{CandidateRevision: int64Pointer(0)}},
		{name: "stable revision", input: appmodel.SetCanaryInput{CandidateRevision: &stable}},
		{name: "missing revision", input: appmodel.SetCanaryInput{CandidateRevision: int64Pointer(99)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.input
			input.TenantID, input.AppID, input.ExpectedAppVersion = tenantOne, app.AppID, cleared.Version
			input.TenantActive, input.Metadata = true, changeMetadata()
			if _, _, err := r.SetCanary(context.Background(), input); !errors.Is(err, appmodel.ErrInvalid) && !errors.Is(err, appmodel.ErrNotFound) {
				t.Fatalf("SetCanary error = %v", err)
			}
		})
	}
	invalidMetadata := appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: cleared.Version, TenantActive: true}
	if _, _, err := r.SetCanary(context.Background(), invalidMetadata); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("invalid metadata error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := r.SetCanary(ctx, appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: cleared.Version, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v", err)
	}
}

func assertInitialPublication(t *testing.T, r *InMemoryRepository, app *appmodel.App, first *appmodel.Revision) (*appmodel.App, *appmodel.Revision, appmodel.ChangeEvent) {
	t.Helper()

	inactive := publishInput(app, first)
	inactive.TenantActive = false
	if _, _, _, err := r.Publish(context.Background(), inactive); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected inactive tenant publication rejection, got %v", err)
	}
	unchanged, _ := r.Get(context.Background(), tenantOne, app.AppID)
	if unchanged.Version != app.Version || unchanged.Status != appmodel.StatusDraft {
		t.Fatalf("failed publication changed root: %+v", unchanged)
	}

	publishedApp, publishedFirst, event, err := r.Publish(context.Background(), publishInput(app, first))
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, event, appmodel.ChangePublished, 1, 2, nil, int64Pointer(1), publishedFirst.ContentDigest)
	if publishedApp.Status != appmodel.StatusActive || publishedApp.CurrentRevision == nil || *publishedApp.CurrentRevision != 1 {
		t.Fatalf("first publication did not activate App: %+v", publishedApp)
	}
	if _, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: publishedApp.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("immutable"),
	}); !errors.Is(err, appmodel.ErrImmutableRevision) {
		t.Fatalf("published revision accepted draft update, got %v", err)
	}
	return publishedApp, publishedFirst, event
}

func assertSecondPublicationAndRollback(t *testing.T, r *InMemoryRepository, app *appmodel.App, publishedFirst *appmodel.Revision) (*appmodel.App, *appmodel.Revision, *appmodel.Revision) {
	t.Helper()

	second := createDraft(t, r, app, draftConfiguration("second"))
	secondApp, publishedSecond, secondEvent, err := r.Publish(context.Background(), publishInput(app, second))
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, secondEvent, appmodel.ChangePublished, 2, 3, int64Pointer(1), int64Pointer(2), publishedSecond.ContentDigest)

	rolledBack, rollbackEvent, err := r.Rollback(context.Background(), appmodel.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: secondApp.Version, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, rollbackEvent, appmodel.ChangeRolledBack, 3, 4, int64Pointer(2), int64Pointer(1), publishedFirst.ContentDigest)
	storedFirst, _ := r.GetRevision(context.Background(), tenantOne, app.AppID, 1)
	if storedFirst.ContentDigest != publishedFirst.ContentDigest || storedFirst.Revision != 1 {
		t.Fatal("rollback copied or mutated immutable target revision")
	}
	return rolledBack, publishedFirst, publishedSecond
}

func assertLifecycleTransitions(t *testing.T, r *InMemoryRepository, rolledBack *appmodel.App, publishedFirst, publishedSecond *appmodel.Revision, event appmodel.ChangeEvent) {
	t.Helper()

	suspended, suspendEvent, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: rolledBack.AppID, ExpectedVersion: rolledBack.Version, NextStatus: appmodel.StatusSuspended, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, suspendEvent, appmodel.ChangeSuspended, 4, 5, int64Pointer(1), int64Pointer(1), publishedFirst.ContentDigest)
	resumed, resumeEvent, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: rolledBack.AppID, ExpectedVersion: suspended.Version, NextStatus: appmodel.StatusActive, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumeEvent.EventType != appmodel.ChangeResumed || resumed.Status != appmodel.StatusActive {
		t.Fatalf("unexpected resume result: %+v %+v", resumed, resumeEvent)
	}
	disabled, disabledEvent, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: rolledBack.AppID, ExpectedVersion: resumed.Version, NextStatus: appmodel.StatusDisabled, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if disabledEvent.EventType != appmodel.ChangeDisabled || disabled.Status != appmodel.StatusDisabled {
		t.Fatalf("unexpected disable result: %+v %+v", disabled, disabledEvent)
	}
	if _, err := r.UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: tenantOne, AppID: rolledBack.AppID, ExpectedVersion: disabled.Version, DisplayName: "No", Description: ""}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("terminal App accepted mutation, got %v", err)
	}

	event.CurrentRevision = int64Pointer(99)
	if rolledBack.CurrentRevision == nil || *rolledBack.CurrentRevision != 1 {
		t.Fatal("event pointer mutation leaked into returned App")
	}
}

func TestPublishAndDraftUpdateHaveSingleConcurrentWinner(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "publish-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		input := publishInput(app, draft)
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, _, err := r.Publish(context.Background(), input)
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})

	t.Run("draft update", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "draft-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				_, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
					TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision,
					ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
					Configuration: draftConfiguration("race-update"),
				})
				errorsCh <- err
			}(i)
		}
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})

	t.Run("publish versus draft update", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "mixed-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, _, err := r.Publish(context.Background(), publishInput(app, draft))
			errorsCh <- err
		}()
		go func() {
			defer wg.Done()
			_, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
				TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision,
				ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
				Configuration: draftConfiguration("mixed-update"),
			})
			errorsCh <- err
		}()
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})
}

func TestAuditValidationAndRollbackGuards(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "guards")
	draft := createDraft(t, r, app, draftConfiguration("guard"))
	invalid := publishInput(app, draft)
	invalid.Metadata.Reason = ""
	if _, _, _, err := r.Publish(context.Background(), invalid); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected audit metadata rejection, got %v", err)
	}
	if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: draft.Revision,
		ExpectedAppVersion: app.Version, Metadata: changeMetadata(),
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected unpublished rollback target rejection, got %v", err)
	}
	if _, _, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version,
		NextStatus: appmodel.StatusSuspended, Metadata: changeMetadata(),
	}); !errors.Is(err, appmodel.ErrInvalidTransition) {
		t.Fatalf("expected draft-to-suspended transition rejection, got %v", err)
	}
}

func TestRepositoryRejectsInvalidStaleAndMissingMutations(t *testing.T) {
	r := NewRepository()
	if _, err := r.Create(context.Background(), appmodel.CreateInput{TenantID: "bad", AppKey: "bad", DisplayName: "Bad"}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected invalid create rejection, got %v", err)
	}
	app := createApp(t, r, tenantOne, "errors")
	draft := createDraft(t, r, app, draftConfiguration("errors"))

	if _, err := r.UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version + 1, DisplayName: "Stale",
	}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
	if _, err := r.UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "",
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected invalid metadata rejection, got %v", err)
	}
	invalidConfiguration := draftConfiguration("invalid")
	invalidConfiguration.Instruction = ""
	if _, err := r.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: invalidConfiguration,
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected invalid draft rejection, got %v", err)
	}
	if _, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: 99,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1, Configuration: draftConfiguration("missing"),
	}); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("expected missing draft rejection, got %v", err)
	}
	if _, err := r.UpdateDraft(context.Background(), appmodel.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, Configuration: invalidConfiguration,
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected invalid draft update rejection, got %v", err)
	}
	if _, err := r.GetRevision(context.Background(), tenantOne, app.AppID, 99); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("expected missing revision lookup rejection, got %v", err)
	}

	stalePublish := publishInput(app, draft)
	stalePublish.ExpectedDraftVersion++
	if _, _, _, err := r.Publish(context.Background(), stalePublish); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("expected stale publication conflict, got %v", err)
	}
	publishedApp, _, _, err := r.Publish(context.Background(), publishInput(app, draft))
	if err != nil {
		t.Fatal(err)
	}
	republish := publishInput(publishedApp, draft)
	if _, _, _, err := r.Publish(context.Background(), republish); !errors.Is(err, appmodel.ErrImmutableRevision) {
		t.Fatalf("expected immutable re-publication rejection, got %v", err)
	}
	if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: draft.Revision,
		ExpectedAppVersion: publishedApp.Version, Metadata: changeMetadata(),
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected same-target rollback rejection, got %v", err)
	}
	if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: 99,
		ExpectedAppVersion: publishedApp.Version, Metadata: changeMetadata(),
	}); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("expected missing rollback target rejection, got %v", err)
	}
	if _, _, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: publishedApp.Version + 1,
		NextStatus: appmodel.StatusSuspended, Metadata: changeMetadata(),
	}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("expected stale transition conflict, got %v", err)
	}
	tooLong := changeMetadata()
	tooLong.Reason = string(make([]rune, 1001))
	if _, _, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: publishedApp.Version,
		NextStatus: appmodel.StatusSuspended, Metadata: tooLong,
	}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("expected oversized audit reason rejection, got %v", err)
	}
}

func TestOperationsHonorContextCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "cancel")
	if err := r.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Get(ctx, tenantOne, app.AppID)
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation did not observe cancellation while waiting for lock")
	}
}

func TestReadLocksRemainConcurrent(t *testing.T) {
	r := NewRepository()
	if err := r.mu.rlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.runlock()
	acquired := make(chan error, 1)
	go func() {
		err := r.mu.rlock(context.Background())
		if err == nil {
			r.mu.runlock()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent readers were serialized")
	}
}

func TestEveryOperationChecksAlreadyCancelledContext(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "cancelled")
	draft := createDraft(t, r, app, draftConfiguration("cancelled"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	operations := []struct {
		name string
		run  func() error
	}{
		{name: "create", run: func() error { _, err := r.Create(ctx, createInput(tenantOne, "cancelled-create")); return err }},
		{name: "get", run: func() error { _, err := r.Get(ctx, tenantOne, app.AppID); return err }},
		{name: "metadata", run: func() error {
			_, err := r.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Name"})
			return err
		}},
		{name: "create draft", run: func() error {
			_, err := r.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: draftConfiguration("new")})
			return err
		}},
		{name: "update draft", run: func() error {
			_, err := r.UpdateDraft(ctx, appmodel.UpdateDraftInput{TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, Configuration: draftConfiguration("updated")})
			return err
		}},
		{name: "get revision", run: func() error { _, err := r.GetRevision(ctx, tenantOne, app.AppID, draft.Revision); return err }},
		{name: "publish", run: func() error { _, _, _, err := r.Publish(ctx, publishInput(app, draft)); return err }},
		{name: "rollback", run: func() error {
			_, _, err := r.Rollback(ctx, appmodel.RollbackInput{TenantID: tenantOne, AppID: app.AppID, TargetRevision: draft.Revision, ExpectedAppVersion: app.Version, Metadata: changeMetadata()})
			return err
		}},
		{name: "transition", run: func() error {
			_, _, err := r.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusDisabled, Metadata: changeMetadata()})
			return err
		}},
		{name: "canary", run: func() error {
			_, _, err := r.SetCanary(ctx, appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: changeMetadata()})
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation, got %v", err)
			}
		})
	}
}

func TestWriterLockHonorsCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	if err := r.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.mu.lock(ctx) }()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected writer cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting writer did not observe cancellation")
	}
}

func TestInternalDefensiveBranches(t *testing.T) {
	if cloneApp(nil) != nil || cloneRevision(nil) != nil {
		t.Fatal("nil domain values must remain nil across clone boundaries")
	}
	future := time.Now().UTC().Add(time.Hour)
	if got := nextTime(future); !got.Equal(future) {
		t.Fatalf("monotonic clock guard moved backwards: %v", got)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var mutex contextRWMutex
	if err := mutex.rlock(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read lock returned %v", err)
	}
	if err := mutex.lock(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write lock returned %v", err)
	}

	assertPanics(t, func() { mutex.unlock() })
	assertPanics(t, func() { mutex.runlock() })

	if err := mutex.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	readerAcquired := make(chan error, 1)
	go func() {
		err := mutex.rlock(context.Background())
		if err == nil {
			mutex.runlock()
		}
		readerAcquired <- err
	}()
	time.Sleep(25 * time.Millisecond)
	mutex.unlock()
	select {
	case err := <-readerAcquired:
		if err != nil {
			t.Fatalf("reader failed after writer wakeup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not reacquire after writer release")
	}
}

func TestMutationOperationsObserveCancellationWhileWaitingForWriter(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*InMemoryRepository) func(context.Context) error
	}{
		{name: "publish", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "publish-lock")
			draft := createDraft(t, r, app, draftConfiguration("publish-lock"))
			input := publishInput(app, draft)
			return func(ctx context.Context) error {
				_, _, _, err := r.Publish(ctx, input)
				return err
			}
		}},
		{name: "rollback", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "rollback-lock")
			first := createDraft(t, r, app, draftConfiguration("rollback-first"))
			publishedApp, _, _, err := r.Publish(context.Background(), publishInput(app, first))
			if err != nil {
				t.Fatal(err)
			}
			second := createDraft(t, r, publishedApp, draftConfiguration("rollback-second"))
			current, _, _, err := r.Publish(context.Background(), publishInput(publishedApp, second))
			if err != nil {
				t.Fatal(err)
			}
			input := appmodel.RollbackInput{TenantID: tenantOne, AppID: current.AppID, TargetRevision: first.Revision, ExpectedAppVersion: current.Version, Metadata: changeMetadata()}
			return func(ctx context.Context) error {
				_, _, err := r.Rollback(ctx, input)
				return err
			}
		}},
		{name: "canary", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "canary-lock")
			draft := createDraft(t, r, app, draftConfiguration("canary-lock"))
			active, _, _, err := r.Publish(context.Background(), publishInput(app, draft))
			if err != nil {
				t.Fatal(err)
			}
			input := appmodel.SetCanaryInput{TenantID: tenantOne, AppID: active.AppID, ExpectedAppVersion: active.Version, TenantActive: true, Metadata: changeMetadata()}
			return func(ctx context.Context) error {
				_, _, err := r.SetCanary(ctx, input)
				return err
			}
		}},
		{name: "transition", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "transition-lock")
			input := appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusDisabled, Metadata: changeMetadata()}
			return func(ctx context.Context) error {
				_, _, err := r.TransitionStatus(ctx, input)
				return err
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRepository()
			operation := tc.setup(r)
			if err := r.mu.lock(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			time.Sleep(25 * time.Millisecond)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("waiting operation error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation did not observe cancellation while waiting for writer")
			}
			r.mu.unlock()
		})
	}
}

func TestMutationOperationsObserveCancellationAfterWriterLock(t *testing.T) {
	cases := []struct {
		name string
		call func(*InMemoryRepository, context.Context) error
	}{
		{name: "publish", call: func(r *InMemoryRepository, ctx context.Context) error {
			_, _, _, err := r.Publish(ctx, appmodel.PublishInput{TenantID: tenantOne, AppID: "app", Revision: 1, ExpectedAppVersion: 1, ExpectedDraftVersion: 1, TenantActive: true, Metadata: changeMetadata()})
			return err
		}},
		{name: "rollback", call: func(r *InMemoryRepository, ctx context.Context) error {
			_, _, err := r.Rollback(ctx, appmodel.RollbackInput{TenantID: tenantOne, AppID: "app", TargetRevision: 1, ExpectedAppVersion: 1, Metadata: changeMetadata()})
			return err
		}},
		{name: "canary", call: func(r *InMemoryRepository, ctx context.Context) error {
			_, _, err := r.SetCanary(ctx, appmodel.SetCanaryInput{TenantID: tenantOne, AppID: "app", ExpectedAppVersion: 1, TenantActive: true, Metadata: changeMetadata()})
			return err
		}},
		{name: "transition", call: func(r *InMemoryRepository, ctx context.Context) error {
			_, _, err := r.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: "app", ExpectedVersion: 1, NextStatus: appmodel.StatusDisabled, Metadata: changeMetadata()})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCancelAfterDoneContext(3)
			if err := tc.call(NewRepository(), ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("post-lock cancellation error = %v", err)
			}
		})
	}
}

func TestMutationOperationsObserveCancellationAfterBuildingEvent(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*InMemoryRepository) func(context.Context) error
	}{
		{name: "publish", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "publish-event-cancel")
			draft := createDraft(t, r, app, draftConfiguration("publish-event-cancel"))
			input := publishInput(app, draft)
			return func(ctx context.Context) error {
				_, _, _, err := r.Publish(ctx, input)
				return err
			}
		}},
		{name: "rollback", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "rollback-event-cancel")
			first := createDraft(t, r, app, draftConfiguration("rollback-event-first"))
			active, _, _, err := r.Publish(context.Background(), publishInput(app, first))
			if err != nil {
				t.Fatal(err)
			}
			second := createDraft(t, r, active, draftConfiguration("rollback-event-second"))
			current, _, _, err := r.Publish(context.Background(), publishInput(active, second))
			if err != nil {
				t.Fatal(err)
			}
			input := appmodel.RollbackInput{TenantID: tenantOne, AppID: current.AppID, TargetRevision: first.Revision, ExpectedAppVersion: current.Version, Metadata: changeMetadata()}
			return func(ctx context.Context) error {
				_, _, err := r.Rollback(ctx, input)
				return err
			}
		}},
		{name: "transition", setup: func(r *InMemoryRepository) func(context.Context) error {
			app := createApp(t, r, tenantOne, "transition-event-cancel")
			input := appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusDisabled, Metadata: changeMetadata()}
			return func(ctx context.Context) error {
				_, _, err := r.TransitionStatus(ctx, input)
				return err
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.setup(NewRepository())(newCancelAfterDoneContext(4)); !errors.Is(err, context.Canceled) {
				t.Fatalf("post-event cancellation error = %v", err)
			}
		})
	}
}

//nolint:gocyclo // This table-driven test deliberately covers each mutation boundary error.
func TestMutationOperationsCoverRepositoryBoundaryBranches(t *testing.T) {
	t.Run("publish missing revision", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "publish-missing")
		if _, _, _, err := r.Publish(context.Background(), appmodel.PublishInput{TenantID: tenantOne, AppID: app.AppID, Revision: 99, ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrNotFound) {
			t.Fatalf("missing publish revision error = %v", err)
		}
	})

	t.Run("publish invalid draft", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "publish-invalid-draft")
		draft := createDraft(t, r, app, draftConfiguration("publish-invalid-draft"))
		stored := r.revisions[appScope{tenantID: tenantOne, appID: app.AppID}][draft.Revision]
		stored.CreatedAt = time.Time{}
		if _, _, _, err := r.Publish(context.Background(), publishInput(app, draft)); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid draft publication error = %v", err)
		}
	})

	t.Run("publish invalid updated app", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "publish-invalid-app")
		draft := createDraft(t, r, app, draftConfiguration("publish-invalid-app"))
		r.apps[appScope{tenantID: tenantOne, appID: app.AppID}].AppKey = ""
		if _, _, _, err := r.Publish(context.Background(), publishInput(app, draft)); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid updated app publication error = %v", err)
		}
	})

	t.Run("rollback invalid metadata", func(t *testing.T) {
		r := NewRepository()
		if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{Metadata: appmodel.ChangeMetadata{ActorType: "test"}}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid rollback metadata error = %v", err)
		}
	})

	t.Run("rollback disabled app", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "rollback-disabled")
		r.apps[appScope{tenantID: tenantOne, appID: app.AppID}].Status = appmodel.StatusDisabled
		if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{TenantID: tenantOne, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrDisabled) {
			t.Fatalf("disabled rollback error = %v", err)
		}
	})

	t.Run("rollback invalid updated app", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "rollback-invalid-app")
		first := createDraft(t, r, app, draftConfiguration("rollback-invalid-first"))
		active, _, _, err := r.Publish(context.Background(), publishInput(app, first))
		if err != nil {
			t.Fatal(err)
		}
		second := createDraft(t, r, active, draftConfiguration("rollback-invalid-second"))
		current, _, _, err := r.Publish(context.Background(), publishInput(active, second))
		if err != nil {
			t.Fatal(err)
		}
		r.apps[appScope{tenantID: tenantOne, appID: app.AppID}].AppKey = ""
		if _, _, err := r.Rollback(context.Background(), appmodel.RollbackInput{TenantID: tenantOne, AppID: app.AppID, TargetRevision: first.Revision, ExpectedAppVersion: current.Version, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid updated app rollback error = %v", err)
		}
	})

	t.Run("canary draft candidate", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "canary-draft")
		stable := createDraft(t, r, app, draftConfiguration("canary-stable"))
		active, _, _, err := r.Publish(context.Background(), publishInput(app, stable))
		if err != nil {
			t.Fatal(err)
		}
		candidate := createDraft(t, r, active, draftConfiguration("canary-draft"))
		if _, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: active.AppID, CandidateRevision: int64Pointer(candidate.Revision), ExpectedAppVersion: active.Version, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("draft canary candidate error = %v", err)
		}
	})

	t.Run("canary unchanged", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "canary-unchanged")
		stable := createDraft(t, r, app, draftConfiguration("canary-unchanged-stable"))
		active, _, _, err := r.Publish(context.Background(), publishInput(app, stable))
		if err != nil {
			t.Fatal(err)
		}
		candidateDraft := createDraft(t, r, active, draftConfiguration("canary-unchanged-candidate"))
		current, _, _, err := r.Publish(context.Background(), publishInput(active, candidateDraft))
		if err != nil {
			t.Fatal(err)
		}
		selected, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: current.AppID, CandidateRevision: int64Pointer(stable.Revision), ExpectedAppVersion: current.Version, TenantActive: true, Metadata: changeMetadata()})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: current.AppID, CandidateRevision: int64Pointer(stable.Revision), ExpectedAppVersion: selected.Version, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("unchanged canary error = %v", err)
		}
	})

	t.Run("canary invalid updated app", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "canary-invalid-app")
		stable := createDraft(t, r, app, draftConfiguration("canary-invalid-stable"))
		active, _, _, err := r.Publish(context.Background(), publishInput(app, stable))
		if err != nil {
			t.Fatal(err)
		}
		candidateDraft := createDraft(t, r, active, draftConfiguration("canary-invalid-candidate"))
		current, _, _, err := r.Publish(context.Background(), publishInput(active, candidateDraft))
		if err != nil {
			t.Fatal(err)
		}
		selected, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: current.AppID, CandidateRevision: int64Pointer(stable.Revision), ExpectedAppVersion: current.Version, TenantActive: true, Metadata: changeMetadata()})
		if err != nil {
			t.Fatal(err)
		}
		r.apps[appScope{tenantID: tenantOne, appID: app.AppID}].AppKey = ""
		if _, _, err := r.SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: selected.Version, TenantActive: true, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid updated app canary error = %v", err)
		}
	})

	t.Run("transition missing current revision", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "transition-missing-revision")
		stored := r.apps[appScope{tenantID: tenantOne, appID: app.AppID}]
		stored.Status = appmodel.StatusActive
		stored.CurrentRevision = int64Pointer(99)
		if _, _, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrNotFound) {
			t.Fatalf("missing transition revision error = %v", err)
		}
	})

	t.Run("transition invalid updated app", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "transition-invalid-app")
		draft := createDraft(t, r, app, draftConfiguration("transition-invalid-app"))
		active, _, _, err := r.Publish(context.Background(), publishInput(app, draft))
		if err != nil {
			t.Fatal(err)
		}
		r.apps[appScope{tenantID: tenantOne, appID: app.AppID}].AppKey = ""
		if _, _, err := r.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: active.Version, NextStatus: appmodel.StatusSuspended, Metadata: changeMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("invalid updated app transition error = %v", err)
		}
	})
}

type cancelAfterDoneContext struct {
	context.Context
	done     chan struct{}
	cancelAt int
	mu       sync.Mutex
	calls    int
	closed   bool
}

func newCancelAfterDoneContext(cancelAt int) *cancelAfterDoneContext {
	return &cancelAfterDoneContext{Context: context.Background(), done: make(chan struct{}), cancelAt: cancelAt}
}

func (c *cancelAfterDoneContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if !c.closed && c.calls >= c.cancelAt {
		close(c.done)
		c.closed = true
	}
	return c.done
}

func (c *cancelAfterDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func assertPanics(t *testing.T, operation func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected operation to panic")
		}
	}()
	operation()
}

func createInput(tenantID, key string) appmodel.CreateInput {
	return appmodel.CreateInput{TenantID: tenantID, AppKey: key, DisplayName: "Support", Description: "Answers questions"}
}

func createApp(t *testing.T, r *InMemoryRepository, tenantID, key string) *appmodel.App {
	t.Helper()
	app, err := r.Create(context.Background(), createInput(tenantID, key))
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func draftConfiguration(name string) appmodel.DraftConfiguration {
	return appmodel.DraftConfiguration{
		Description: name, Instruction: name + " instruction", ModelProfileID: "model-primary",
		Generation: appmodel.GenerationConfig{Temperature: float64Pointer(0.2), MaxOutputTokens: intPointer(1024)},
		Runtime:    appmodel.DefaultRuntimePolicy(), Tools: []appmodel.ToolAuthorization{{ToolID: "search", Required: true}},
	}
}

func createDraft(t *testing.T, r *InMemoryRepository, app *appmodel.App, configuration appmodel.DraftConfiguration) *appmodel.Revision {
	t.Helper()
	draft, err := r.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return draft
}

func publishInput(app *appmodel.App, revision *appmodel.Revision) appmodel.PublishInput {
	return appmodel.PublishInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: revision.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: revision.DraftVersion,
		TenantActive: true, Metadata: changeMetadata(),
	}
}

func changeMetadata() appmodel.ChangeMetadata {
	return appmodel.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "requested change", CorrelationID: "corr-1"}
}

func assertEvent(t *testing.T, event appmodel.ChangeEvent, eventType appmodel.ChangeEventType, previousVersion, nextVersion int64, previousRevision, currentRevision *int64, digest string) {
	t.Helper()
	if event.EventType != eventType || event.TenantID != tenantOne || event.AppID == "" || event.PreviousVersion != previousVersion || event.NextVersion != nextVersion || event.ContentDigest != digest || event.ActorType != "admin" || event.ActorID != "user-1" || event.Reason != "requested change" || event.CorrelationID != "corr-1" || event.OccurredAt.IsZero() {
		t.Fatalf("incomplete event: %+v", event)
	}
	if !sameOptionalInt(event.PreviousRevision, previousRevision) || !sameOptionalInt(event.CurrentRevision, currentRevision) {
		t.Fatalf("unexpected event revision movement: %+v", event)
	}
}

func assertOneWinner(t *testing.T, results <-chan error) {
	t.Helper()
	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, appmodel.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one winner and one conflict, got %d and %d", successes, conflicts)
	}
}

func sameOptionalInt(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func float64Pointer(value float64) *float64 { return &value }
func intPointer(value int) *int             { return &value }
