package inmemory_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func createInput(key string) tenant.CreateInput {
	return tenant.CreateInput{TenantKey: key, DisplayName: "Example", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
}

func updateInput(id string, version int64, name string) tenant.UpdateConfigurationInput {
	return tenant.UpdateConfigurationInput{TenantID: id, ExpectedVersion: version, DisplayName: name, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
}

func transitionInput(id string, version int64, next tenant.Status) tenant.TransitionStatusInput {
	return tenant.TransitionStatusInput{TenantID: id, ExpectedVersion: version, NextStatus: next, Metadata: tenant.TransitionMetadata{ActorType: "admin", ActorID: "u1", Reason: "maintenance", CorrelationID: "c1"}}
}

func TestRepositoryContractAndTenantIsolation(t *testing.T) {
	r := inmemory.NewRepository()
	var _ tenant.Repository = r
	first, err := r.Create(context.Background(), createInput("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Create(context.Background(), createInput("second"))
	if err != nil {
		t.Fatal(err)
	}
	if first.TenantID == second.TenantID {
		t.Fatal("generated tenant IDs must differ")
	}
	if _, err := r.Get(context.Background(), "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"); !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := r.Get(context.Background(), first.TenantID); err != nil {
		t.Fatalf("tenant scope lookup failed: %v", err)
	}
}

func TestRepositoryRejectsDuplicateIdentity(t *testing.T) {
	r := inmemory.NewRepository()
	first, err := r.Create(context.Background(), createInput("duplicate"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Create(context.Background(), createInput("DUPLICATE")); !errors.Is(err, tenant.ErrDuplicateKey) {
		t.Fatalf("expected duplicate normalized key, got %v", err)
	}
	if first.TenantID == "" {
		t.Fatal("repository must generate tenant IDs")
	}
}

func TestRepositoryReturnsDeepCopies(t *testing.T) {
	r := inmemory.NewRepository()
	rate := int64(2)
	appID := "app-1"
	input := createInput("copies")
	input.RateLimitRPM = &rate
	input.DefaultAgentAppID = &appID
	created, err := r.Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	*created.RateLimitRPM = 99
	*created.DefaultAgentAppID = "mutated"
	got, err := r.Get(context.Background(), created.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if *got.RateLimitRPM != 2 || *got.DefaultAgentAppID != "app-1" {
		t.Fatalf("repository leaked nested pointers: %+v", got)
	}
}

func TestUpdateConfigurationUsesOptimisticLockAndDefaults(t *testing.T) {
	r := inmemory.NewRepository()
	created, err := r.Create(context.Background(), createInput("update"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := r.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: " Updated "})
	if err != nil {
		t.Fatal(err)
	}
	if updated.DisplayName != "Updated" || updated.Version != created.Version+1 || updated.AuditRetentionDays != 90 || updated.LogMaskingLevel != tenant.MaskingBasic {
		t.Fatalf("unexpected update: %+v", updated)
	}
	if _, err := r.UpdateConfiguration(context.Background(), updateInput(created.TenantID, created.Version, "conflict")); !errors.Is(err, tenant.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if _, err := r.UpdateConfiguration(context.Background(), updateInput("t_01J1K9ZQTVE4PAWF1TSB2WMHNP", 1, "missing")); !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("expected missing tenant, got %v", err)
	}
}

func TestTransitionStateMachineAndAuditEvent(t *testing.T) {
	r := inmemory.NewRepository()
	created, err := r.Create(context.Background(), createInput("lifecycle"))
	if err != nil {
		t.Fatal(err)
	}
	suspended, event, err := r.TransitionStatus(context.Background(), transitionInput(created.TenantID, created.Version, tenant.StatusSuspended))
	if err != nil {
		t.Fatal(err)
	}
	if suspended.CanAcceptExecution() || event.PreviousVersion != created.Version || event.NextVersion != suspended.Version || event.OccurredAt.IsZero() {
		t.Fatalf("unexpected transition result: %+v %+v", suspended, event)
	}
	if _, _, err := r.TransitionStatus(context.Background(), transitionInput(suspended.TenantID, suspended.Version, tenant.StatusSuspended)); !errors.Is(err, tenant.ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	disabled, _, err := r.TransitionStatus(context.Background(), transitionInput(suspended.TenantID, suspended.Version, tenant.StatusDisabled))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.UpdateConfiguration(context.Background(), updateInput(disabled.TenantID, disabled.Version, "blocked")); !errors.Is(err, tenant.ErrDisabled) {
		t.Fatalf("expected disabled update rejection, got %v", err)
	}
	if _, _, err := r.TransitionStatus(context.Background(), transitionInput(disabled.TenantID, disabled.Version, tenant.StatusActive)); !errors.Is(err, tenant.ErrDisabled) {
		t.Fatalf("expected disabled transition rejection, got %v", err)
	}
}

func TestTransitionRejectsInvalidMetadata(t *testing.T) {
	r := inmemory.NewRepository()
	created, err := r.Create(context.Background(), createInput("metadata"))
	if err != nil {
		t.Fatal(err)
	}
	input := transitionInput(created.TenantID, created.Version, tenant.StatusSuspended)
	input.Metadata.CorrelationID = " "
	if _, _, err := r.TransitionStatus(context.Background(), input); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("expected invalid metadata, got %v", err)
	}
}

func TestTransitionAuditReasonCharacterLimit(t *testing.T) {
	tests := []struct {
		name    string
		reason  string
		wantErr bool
	}{
		{name: "exactly 1000 characters", reason: strings.Repeat("a", 1000)},
		{name: "1001 characters", reason: strings.Repeat("a", 1001), wantErr: true},
		{name: "multibyte characters", reason: strings.Repeat("界", 500)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := inmemory.NewRepository()
			created, err := r.Create(context.Background(), createInput("reason-limit-"+strings.ReplaceAll(test.name, " ", "-")))
			if err != nil {
				t.Fatal(err)
			}
			input := transitionInput(created.TenantID, created.Version, tenant.StatusSuspended)
			input.Metadata.Reason = test.reason
			_, _, err = r.TransitionStatus(context.Background(), input)
			if test.wantErr && !errors.Is(err, tenant.ErrInvalid) {
				t.Fatalf("expected invalid transition metadata, got %v", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("expected valid transition metadata, got %v", err)
			}
		})
	}
}

func TestRepositoryHonorsCancelledContextForEveryOperation(t *testing.T) {
	r := inmemory.NewRepository()
	created, err := r.Create(context.Background(), createInput("cancelled"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Create(ctx, createInput("cancelled-create")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled create, got %v", err)
	}
	if _, err := r.Get(ctx, created.TenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled get, got %v", err)
	}
	if _, err := r.UpdateConfiguration(ctx, updateInput(created.TenantID, created.Version, "cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled update, got %v", err)
	}
	if _, _, err := r.TransitionStatus(ctx, transitionInput(created.TenantID, created.Version, tenant.StatusSuspended)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled transition, got %v", err)
	}
}

func TestConcurrentUpdatesHaveOneWinner(t *testing.T) {
	r := inmemory.NewRepository()
	created, err := r.Create(context.Background(), createInput("race"))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.UpdateConfiguration(context.Background(), updateInput(created.TenantID, created.Version, "race-update"))
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, tenant.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent update error: %v", err)
		}
	}
	if successes != 1 || conflicts != workers-1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}
