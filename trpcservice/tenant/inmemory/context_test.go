package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestLockHonorsCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	if err := r.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.lock(ctx)
	}()
	select {
	case <-time.After(25 * time.Millisecond):
		cancel()
	case err := <-done:
		t.Fatalf("lock unexpectedly acquired before cancellation: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellable lock did not return after cancellation")
	}
}

func TestInternalContextAndCloneBranches(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository()
	if err := r.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.unlock()
	}()
	if err := r.rLock(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled read lock, got %v", err)
	}

	value := int64(1)
	text := "value"
	if cloneInt64(&value) == nil || cloneString(&text) == nil {
		t.Fatal("non-nil clone helpers must return copies")
	}
	if cloneTenant(nil) != nil {
		t.Fatal("nil tenant clone must remain nil")
	}
	var _ tenant.Repository = r
}

func TestOperationsCancelWhileWaitingForLock(t *testing.T) {
	r := NewRepository()
	created, err := r.Create(context.Background(), contextCreateInput("waiting"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.unlock()
	}()

	type result struct{ err error }
	results := make(chan result, 4)
	contexts := make([]context.Context, 4)
	cancels := make([]context.CancelFunc, 4)
	for i := range contexts {
		contexts[i], cancels[i] = context.WithCancel(context.Background())
	}
	go func() { _, err := r.Create(contexts[0], contextCreateInput("waiting-create")); results <- result{err} }()
	go func() { _, err := r.Get(contexts[1], created.TenantID); results <- result{err} }()
	go func() {
		_, err := r.UpdateConfiguration(contexts[2], contextUpdateInput(created.TenantID, created.Version, "waiting-update"))
		results <- result{err}
	}()
	go func() {
		_, _, err := r.TransitionStatus(contexts[3], contextTransitionInput(created.TenantID, created.Version, tenant.StatusSuspended))
		results <- result{err}
	}()
	time.Sleep(25 * time.Millisecond)
	for _, cancel := range cancels {
		cancel()
	}
	for range contexts {
		if err := (<-results).err; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation while waiting for lock, got %v", err)
		}
	}
}

func TestReadLocksRemainConcurrent(t *testing.T) {
	r := NewRepository()
	if err := r.rLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.rUnlock()

	acquired := make(chan error, 1)
	go func() {
		err := r.rLock(context.Background())
		if err == nil {
			r.rUnlock()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("second reader failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reader was serialized behind the first reader")
	}
}

func contextCreateInput(key string) tenant.CreateInput {
	return tenant.CreateInput{TenantKey: key, DisplayName: "Example", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
}

func contextUpdateInput(id string, version int64, name string) tenant.UpdateConfigurationInput {
	return tenant.UpdateConfigurationInput{TenantID: id, ExpectedVersion: version, DisplayName: name, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
}

func contextTransitionInput(id string, version int64, next tenant.Status) tenant.TransitionStatusInput {
	return tenant.TransitionStatusInput{TenantID: id, ExpectedVersion: version, NextStatus: next, Metadata: tenant.TransitionMetadata{ActorType: "admin", ActorID: "u1", Reason: "maintenance", CorrelationID: "c1"}}
}
