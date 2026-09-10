package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestRepositoryTenantIsolationLifecycleAndDefensiveCopies(t *testing.T) {
	repository := NewRepository(inmemoryTestCatalog(t))
	created := assertTenantScopedProfileCreation(t, repository)
	updated := assertProfileCopiesAndConfigurationUpdate(t, repository, created)
	assertProfileLifecycleAndDisabledMutation(t, repository, updated)
}

func assertTenantScopedProfileCreation(t *testing.T, repository *InMemoryRepository) *modelprofile.Profile {
	t.Helper()
	created, event, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "primary"))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != modelprofile.EventCreated || event.TenantID != created.TenantID || event.ProfileID != created.ProfileID || event.NextVersion != 1 {
		t.Fatalf("unexpected created event: %+v", event)
	}
	if _, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "PRIMARY")); !errors.Is(err, modelprofile.ErrDuplicateKey) {
		t.Fatalf("same-tenant duplicate error = %v", err)
	}
	other, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-two", "primary"))
	if err != nil {
		t.Fatal(err)
	}
	if other.ProfileKey != created.ProfileKey || other.TenantID == created.TenantID {
		t.Fatalf("cross-tenant key was not isolated: %+v", other)
	}
	if _, err := repository.Get(context.Background(), "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", created.ProfileID); !errors.Is(err, modelprofile.ErrNotFound) {
		t.Fatalf("cross-tenant read error = %v", err)
	}
	return created
}

func assertProfileCopiesAndConfigurationUpdate(t *testing.T, repository *InMemoryRepository, created *modelprofile.Profile) *modelprofile.Profile {
	t.Helper()
	created.Configuration.Options["mode"] = "fast"
	fetched, err := repository.Get(context.Background(), created.TenantID, created.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Configuration.Options["mode"] != "safe" {
		t.Fatal("repository returned a mutable stored Profile")
	}
	fetched.Configuration.Options["mode"] = "fast"
	fetchedAgain, err := repository.Get(context.Background(), created.TenantID, created.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if fetchedAgain.Configuration.Options["mode"] != "safe" {
		t.Fatal("repository Get leaked nested map state")
	}

	updated, updateEvent, err := repository.UpdateConfiguration(context.Background(), modelprofile.UpdateConfigurationInput{
		TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
		DisplayName: "Updated", Description: "Updated description", SchemaVersion: modelprofile.SchemaVersionV1,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updateEvent.EventType != modelprofile.EventConfigurationUpdated || !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("unexpected update: profile=%+v event=%+v", updated, updateEvent)
	}
	if _, _, err := repository.UpdateConfiguration(context.Background(), modelprofile.UpdateConfigurationInput{
		TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
		DisplayName: "Stale", SchemaVersion: modelprofile.SchemaVersionV1,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	}); !errors.Is(err, modelprofile.ErrConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	return updated
}

func assertProfileLifecycleAndDisabledMutation(t *testing.T, repository *InMemoryRepository, updated *modelprofile.Profile) {
	t.Helper()
	suspended, transitionEvent, err := repository.TransitionStatus(context.Background(), modelprofile.TransitionStatusInput{
		TenantID: updated.TenantID, ProfileID: updated.ProfileID, ExpectedVersion: updated.Version,
		NextStatus: modelprofile.StatusSuspended, Metadata: inmemoryMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if suspended.Status != modelprofile.StatusSuspended || transitionEvent.EventType != modelprofile.EventSuspended {
		t.Fatalf("unexpected suspension: profile=%+v event=%+v", suspended, transitionEvent)
	}
	disabled, _, err := repository.TransitionStatus(context.Background(), modelprofile.TransitionStatusInput{
		TenantID: suspended.TenantID, ProfileID: suspended.ProfileID, ExpectedVersion: suspended.Version,
		NextStatus: modelprofile.StatusDisabled, Metadata: inmemoryMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.UpdateConfiguration(context.Background(), modelprofile.UpdateConfigurationInput{
		TenantID: disabled.TenantID, ProfileID: disabled.ProfileID, ExpectedVersion: disabled.Version,
		DisplayName: "Rejected", SchemaVersion: modelprofile.SchemaVersionV1,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	}); !errors.Is(err, modelprofile.ErrDisabled) {
		t.Fatalf("disabled update error = %v", err)
	}
}

func TestRepositoryConcurrentOptimisticUpdatesHaveOneWinner(t *testing.T) {
	repository := NewRepository(inmemoryTestCatalog(t))
	created, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "concurrent"))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	var waitGroup sync.WaitGroup
	for index := 0; index < 8; index++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			_, _, updateErr := repository.UpdateConfiguration(context.Background(), modelprofile.UpdateConfigurationInput{
				TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
				DisplayName: fmt.Sprintf("Updated %d", index), SchemaVersion: modelprofile.SchemaVersionV1,
				Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
			})
			results <- updateErr
		}(index)
	}
	waitGroup.Wait()
	close(results)
	winners := 0
	conflicts := 0
	for err := range results {
		if err == nil {
			winners++
		} else if errors.Is(err, modelprofile.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent update error = %v", err)
		}
	}
	if winners != 1 || conflicts != 7 {
		t.Fatalf("winners=%d conflicts=%d, want one winner and seven conflicts", winners, conflicts)
	}
	fetched, err := repository.Get(context.Background(), created.TenantID, created.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Version != 2 {
		t.Fatalf("stored version = %d, want 2", fetched.Version)
	}
}

func TestRepositoryOperationsCancelWhileWaitingForLock(t *testing.T) {
	repository := NewRepository(inmemoryTestCatalog(t))
	created, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "waiting"))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repository.unlock()

	contexts := make([]context.Context, 4)
	cancels := make([]context.CancelFunc, 4)
	for index := range contexts {
		contexts[index], cancels[index] = context.WithCancel(context.Background())
	}
	results := make(chan error, len(contexts))
	go func() {
		_, _, err := repository.Create(contexts[0], inmemoryCreateInput("tenant-one", "waiting-create"))
		results <- err
	}()
	go func() {
		_, err := repository.Get(contexts[1], created.TenantID, created.ProfileID)
		results <- err
	}()
	go func() {
		_, _, err := repository.UpdateConfiguration(contexts[2], modelprofile.UpdateConfigurationInput{
			TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
			DisplayName: "Waiting", SchemaVersion: modelprofile.SchemaVersionV1,
			Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
		})
		results <- err
	}()
	go func() {
		_, _, err := repository.TransitionStatus(contexts[3], modelprofile.TransitionStatusInput{
			TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
			NextStatus: modelprofile.StatusSuspended, Metadata: inmemoryMetadata(),
		})
		results <- err
	}()
	time.Sleep(25 * time.Millisecond)
	for _, cancel := range cancels {
		cancel()
	}
	for range contexts {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting operation error = %v", err)
		}
	}
}

func TestRepositoryContextAndMissingIdentityBoundaries(t *testing.T) {
	repository := NewRepository(inmemoryTestCatalog(t))
	var nilContext context.Context
	created, _, err := repository.Create(nilContext, inmemoryCreateInput("tenant-one", "nil-context"))
	if err != nil {
		t.Fatal(err)
	}
	if fetched, err := repository.Get(nilContext, created.TenantID, created.ProfileID); err != nil || fetched == nil {
		t.Fatalf("nil-context Get = %+v, %v", fetched, err)
	}
	updated, _, err := repository.UpdateConfiguration(nilContext, modelprofile.UpdateConfigurationInput{
		TenantID: created.TenantID, ProfileID: created.ProfileID, ExpectedVersion: created.Version,
		DisplayName: "Nil Context Updated", SchemaVersion: modelprofile.SchemaVersionV1,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.TransitionStatus(nilContext, modelprofile.TransitionStatusInput{
		TenantID: updated.TenantID, ProfileID: updated.ProfileID, ExpectedVersion: updated.Version,
		NextStatus: modelprofile.StatusActive, Metadata: inmemoryMetadata(),
	}); !errors.Is(err, modelprofile.ErrInvalidTransition) {
		t.Fatalf("same-status transition error = %v", err)
	}
	missingID := "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if _, _, err := repository.UpdateConfiguration(context.Background(), modelprofile.UpdateConfigurationInput{
		TenantID: created.TenantID, ProfileID: missingID, ExpectedVersion: 1,
		DisplayName: "Missing", SchemaVersion: modelprofile.SchemaVersionV1,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	}); !errors.Is(err, modelprofile.ErrNotFound) {
		t.Fatalf("missing update error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(context.Background(), modelprofile.TransitionStatusInput{
		TenantID: created.TenantID, ProfileID: missingID, ExpectedVersion: 1,
		NextStatus: modelprofile.StatusSuspended, Metadata: inmemoryMetadata(),
	}); !errors.Is(err, modelprofile.ErrNotFound) {
		t.Fatalf("missing transition error = %v", err)
	}
	if cloneProfile(nil) != nil {
		t.Fatal("cloneProfile(nil) returned a value")
	}
	if checkContext(nilContext) != nil {
		t.Fatal("nil context was rejected")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(checkContext(cancelled), context.Canceled) {
		t.Fatalf("cancelled context error = %v", checkContext(cancelled))
	}
}

func TestContextRWMutexDirectCancellationAndMisusePaths(t *testing.T) {
	mutex := contextRWMutex{}
	var nilContext context.Context
	if err := mutex.lock(nilContext); err != nil {
		t.Fatal(err)
	}
	mutex.unlock()
	if err := mutex.rlock(nilContext); err != nil {
		t.Fatal(err)
	}
	mutex.runlock()
	closed := make(chan struct{})
	close(closed)
	if err := waitContext(nilContext, closed); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(waitContext(cancelled, make(chan struct{})), context.Canceled) {
		t.Fatalf("waitContext cancellation error = %v", waitContext(cancelled, make(chan struct{})))
	}
	if !errors.Is(mutex.lock(cancelled), context.Canceled) {
		t.Fatalf("cancelled writer lock error = %v", mutex.lock(cancelled))
	}
	if !errors.Is(mutex.rlock(cancelled), context.Canceled) {
		t.Fatalf("cancelled reader lock error = %v", mutex.rlock(cancelled))
	}
	assertPanics(t, mutex.unlock)
	assertPanics(t, mutex.runlock)
}

func assertPanics(t *testing.T, function func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	function()
}

func inmemoryTestCatalog(t *testing.T) *modelprofile.ProviderCatalog {
	t.Helper()
	defaultMode := "safe"
	catalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: modelprofile.FieldForbidden,
		SecretRefPolicy: modelprofile.FieldForbidden, Options: map[string]modelprofile.OptionSpec{
			"mode": {Kind: modelprofile.OptionEnum, DefaultValue: &defaultMode, AllowedValues: []string{"fast", "safe"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func inmemoryCreateInput(tenantKey, profileKey string) modelprofile.CreateInput {
	tenantID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if tenantKey == "tenant-two" {
		tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	}
	return modelprofile.CreateInput{
		TenantID: tenantID, ProfileKey: profileKey, DisplayName: "Model Profile",
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: inmemoryMetadata(),
	}
}

func inmemoryMetadata() modelprofile.ChangeMetadata {
	return modelprofile.ChangeMetadata{ActorType: "admin", ActorID: "test-user", Reason: "test mutation", CorrelationID: "test-correlation"}
}
