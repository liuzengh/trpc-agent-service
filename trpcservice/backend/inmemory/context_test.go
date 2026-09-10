package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestOperationsCancelWhileWaitingForLock(t *testing.T) {
	repository := NewRepository(contextCatalog(t))
	created, _, err := repository.Create(context.Background(), contextCreateInput("waiting"))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repository.unlock()

	type result struct{ err error }
	results := make(chan result, 4)
	contexts := make([]context.Context, 4)
	cancels := make([]context.CancelFunc, 4)
	for i := range contexts {
		contexts[i], cancels[i] = context.WithCancel(context.Background())
	}
	go func() {
		_, _, err := repository.Create(contexts[0], contextCreateInput("waiting-create"))
		results <- result{err}
	}()
	go func() {
		_, err := repository.Get(contexts[1], created.TenantID, created.ProfileID)
		results <- result{err}
	}()
	go func() {
		_, _, err := repository.UpdateConfiguration(contexts[2], contextUpdateInput(created))
		results <- result{err}
	}()
	go func() {
		_, _, err := repository.TransitionStatus(contexts[3], contextTransitionInput(created))
		results <- result{err}
	}()
	time.Sleep(25 * time.Millisecond)
	for _, cancel := range cancels {
		cancel()
	}
	for range contexts {
		if err := (<-results).err; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting operation error = %v", err)
		}
	}
}

func TestReadLocksRemainConcurrentAndHelpersCoverNil(t *testing.T) {
	repository := NewRepository(contextCatalog(t))
	if err := repository.rLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repository.rUnlock()
	acquired := make(chan error, 1)
	go func() {
		err := repository.rLock(context.Background())
		if err == nil {
			repository.rUnlock()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reader was serialized")
	}
	if cloneProfile(nil) != nil {
		t.Fatal("nil Profile clone must remain nil")
	}
}

func contextCatalog(t *testing.T) *backend.ProviderCatalog {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func contextMetadata() backend.ChangeMetadata {
	return backend.ChangeMetadata{ActorType: "admin", ActorID: "u1", Reason: "test", CorrelationID: "c1"}
}

func contextCreateInput(key string) backend.CreateInput {
	return backend.CreateInput{
		TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", ProfileKey: key, DisplayName: "Profile",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: contextMetadata(),
	}
}

func contextUpdateInput(profile *backend.Profile) backend.UpdateConfigurationInput {
	return backend.UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: "Updated", SchemaVersion: 1,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: contextMetadata(),
	}
}

func contextTransitionInput(profile *backend.Profile) backend.TransitionStatusInput {
	return backend.TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: backend.StatusSuspended, Metadata: contextMetadata(),
	}
}
