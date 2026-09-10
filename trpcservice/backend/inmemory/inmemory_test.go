package inmemory_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
)

const (
	tenantOne = "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	tenantTwo = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
)

func TestRepositoryContractTenantIsolationAndCreatedEvent(t *testing.T) {
	catalog := testCatalog(t)
	repository := inmemory.NewRepository(catalog)
	var _ backend.Repository = repository

	input := createInput(tenantOne, "primary")
	created, event, err := repository.Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != backend.EventCreated || event.TenantID != tenantOne || event.ProfileID != created.ProfileID ||
		event.PreviousStatus != "" || event.CurrentStatus != created.Status || event.PreviousDigest != "" ||
		event.CurrentDigest != created.ContentDigest || event.PreviousVersion != 0 || event.NextVersion != 1 ||
		event.ActorType != "admin" || event.Reason != "bootstrap" || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("unexpected created event: %+v", event)
	}
	if _, _, err := repository.Create(context.Background(), createInput(tenantOne, "PRIMARY")); !errors.Is(err, backend.ErrDuplicateKey) {
		t.Fatalf("duplicate normalized key error = %v", err)
	}
	if _, _, err := repository.Create(context.Background(), createInput(tenantTwo, "primary")); err != nil {
		t.Fatalf("same key across tenants must be valid: %v", err)
	}
	if _, err := repository.Get(context.Background(), tenantTwo, created.ProfileID); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
}

func TestRepositoryReturnsDeepCopies(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	input := createInput(tenantOne, "copies")
	created, _, err := repository.Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input.Bindings[0].Options["namespace"] = "input-mutated"
	created.Bindings[0].Options["namespace"] = "result-mutated"
	created.Bindings = nil

	stored, err := repository.Get(context.Background(), tenantOne, created.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Bindings) != 1 || stored.Bindings[0].Options["namespace"] != "session" {
		t.Fatalf("repository leaked caller-owned configuration: %+v", stored.Bindings)
	}
	stored.Bindings[0].Options["namespace"] = "get-mutated"
	again, err := repository.Get(context.Background(), tenantOne, stored.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Bindings[0].Options["namespace"] != "session" {
		t.Fatalf("Get leaked stored configuration: %+v", again.Bindings)
	}
}

func TestUpdateConfigurationUsesOptimisticLockAndEmitsEvent(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	created, _, err := repository.Create(context.Background(), createInput(tenantOne, "update"))
	if err != nil {
		t.Fatal(err)
	}
	input := updateInput(created, "Updated", []backend.CapabilityBinding{
		sessionBinding("updated-session"),
		{Capability: backend.CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"}},
	})
	updated, event, err := repository.UpdateConfiguration(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if updated.ProfileKey != created.ProfileKey || updated.DisplayName != "Updated" || updated.Version != created.Version+1 || len(updated.Bindings) != 2 {
		t.Fatalf("unexpected updated Profile: %+v", updated)
	}
	if event.EventType != backend.EventConfigurationUpdated || event.PreviousVersion != created.Version || event.NextVersion != updated.Version ||
		event.PreviousDigest != created.ContentDigest || event.CurrentDigest != updated.ContentDigest || event.PreviousStatus != created.Status || event.CurrentStatus != created.Status {
		t.Fatalf("unexpected configuration event: %+v", event)
	}
	input.Bindings[0].Options["namespace"] = "caller-mutated"
	updated.Bindings[0].Options["namespace"] = "result-mutated"
	stored, err := repository.Get(context.Background(), tenantOne, created.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Bindings[0].Options["namespace"] != "updated-session" {
		t.Fatalf("update leaked caller-owned options: %+v", stored.Bindings)
	}
	if _, _, err := repository.UpdateConfiguration(context.Background(), updateInput(created, "stale", sessionBindings())); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	missing := updateInput(updated, "missing", sessionBindings())
	missing.TenantID = tenantTwo
	if _, _, err := repository.UpdateConfiguration(context.Background(), missing); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("cross-tenant update error = %v", err)
	}
}

func TestLifecycleTransitionsAndResumeValidation(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	created, _, err := repository.Create(context.Background(), createInput(tenantOne, "lifecycle"))
	if err != nil {
		t.Fatal(err)
	}
	suspended, event, err := repository.TransitionStatus(context.Background(), transitionInput(created, backend.StatusSuspended))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != backend.EventSuspended || event.PreviousDigest != event.CurrentDigest || suspended.CanAcceptExecution() {
		t.Fatalf("unexpected suspend result: %+v %+v", suspended, event)
	}
	resumed, event, err := repository.TransitionStatus(context.Background(), transitionInput(suspended, backend.StatusActive))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != backend.EventResumed || !resumed.CanAcceptExecution() {
		t.Fatalf("unexpected resume result: %+v %+v", resumed, event)
	}
	if _, _, err := repository.TransitionStatus(context.Background(), transitionInput(resumed, backend.StatusActive)); !errors.Is(err, backend.ErrInvalidTransition) {
		t.Fatalf("same-state transition error = %v", err)
	}
	disabled, event, err := repository.TransitionStatus(context.Background(), transitionInput(resumed, backend.StatusDisabled))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != backend.EventDisabled || disabled.Status != backend.StatusDisabled {
		t.Fatalf("unexpected disable result: %+v %+v", disabled, event)
	}
	if _, _, err := repository.UpdateConfiguration(context.Background(), updateInput(disabled, "blocked", sessionBindings())); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("disabled update error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(context.Background(), transitionInput(disabled, backend.StatusActive)); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("disabled transition error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(context.Background(), updateInput(resumed, "stale-disabled", sessionBindings())); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("stale disabled update error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(context.Background(), transitionInput(resumed, backend.StatusSuspended)); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("stale disabled transition error = %v", err)
	}

	suspendedOnly, _, err := repository.Create(context.Background(), backend.CreateInput{
		TenantID: tenantOne, ProfileKey: "no-session", DisplayName: "No Session", Status: backend.StatusSuspended,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilityMemory, Provider: "inmemory"}}, Metadata: metadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.TransitionStatus(context.Background(), transitionInput(suspendedOnly, backend.StatusActive)); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("resume without Session error = %v", err)
	}
}

func TestMutationMetadataBoundaries(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	invalid := createInput(tenantOne, "invalid-metadata")
	invalid.Metadata.CorrelationID = " "
	if _, _, err := repository.Create(context.Background(), invalid); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("invalid create metadata error = %v", err)
	}
	tooLong := createInput(tenantOne, "long-reason")
	tooLong.Metadata.Reason = strings.Repeat("界", 1001)
	if _, _, err := repository.Create(context.Background(), tooLong); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("oversized reason error = %v", err)
	}
	boundary := createInput(tenantOne, "reason-boundary")
	boundary.Metadata.Reason = strings.Repeat("界", 1000)
	if _, _, err := repository.Create(context.Background(), boundary); err != nil {
		t.Fatalf("1000-character reason error = %v", err)
	}
}

func TestRepositoryHonorsCancelledContext(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	created, _, err := repository.Create(context.Background(), createInput(tenantOne, "context"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := repository.Create(ctx, createInput(tenantOne, "cancelled-create")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Create error = %v", err)
	}
	if _, err := repository.Get(ctx, tenantOne, created.ProfileID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Get error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(ctx, updateInput(created, "cancelled", sessionBindings())); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Update error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, transitionInput(created, backend.StatusSuspended)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Transition error = %v", err)
	}
}

func TestConcurrentUpdatesHaveOneWinner(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	created, _, err := repository.Create(context.Background(), createInput(tenantOne, "race"))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	results := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _, err := repository.UpdateConfiguration(context.Background(), updateInput(created, "winner", sessionBindings()))
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, backend.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent update error: %v", err)
		}
	}
	if successes != 1 || conflicts != workers-1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func testCatalog(t *testing.T) *backend.ProviderCatalog {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory",
		Capabilities: []backend.Capability{
			backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilityKnowledge,
			backend.CapabilityArtifact, backend.CapabilityAudit,
		},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func metadata() backend.ChangeMetadata {
	return backend.ChangeMetadata{ActorType: " admin ", ActorID: " user-1 ", Reason: " bootstrap ", CorrelationID: " request-1 "}
}

func createInput(tenantID, key string) backend.CreateInput {
	return backend.CreateInput{
		TenantID: tenantID, ProfileKey: key, DisplayName: "Primary", Bindings: sessionBindings(), Metadata: metadata(),
	}
}

func sessionBinding(namespace string) backend.CapabilityBinding {
	return backend.CapabilityBinding{
		Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": namespace},
	}
}

func sessionBindings() []backend.CapabilityBinding {
	return []backend.CapabilityBinding{sessionBinding("session")}
}

func updateInput(profile *backend.Profile, displayName string, bindings []backend.CapabilityBinding) backend.UpdateConfigurationInput {
	return backend.UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: displayName, Description: "updated", SchemaVersion: 1, Bindings: bindings, Metadata: metadata(),
	}
}

func transitionInput(profile *backend.Profile, next backend.Status) backend.TransitionStatusInput {
	return backend.TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: next, Metadata: metadata(),
	}
}
