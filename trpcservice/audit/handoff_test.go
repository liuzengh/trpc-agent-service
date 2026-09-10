package audit

import (
	"context"
	"errors"
	"testing"
)

func TestInMemoryHandoffReserveFinalizeIdempotencyAndIsolation(t *testing.T) {
	s := NewInMemoryHandoffStore()
	p := ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", RequestID: "request", State: HandoffPending}
	got, err := s.Reserve(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != HandoffPending {
		t.Fatal(got.State)
	}
	if _, err := s.Reserve(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(context.Background(), ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", RequestID: "other", State: HandoffPending}); !errors.Is(err, ErrHandoffConflict) {
		t.Fatal(err)
	}
	f, err := s.Finalize(context.Background(), ExecutionHandoff{TenantID: "tenant-a", HandoffID: "handoff-1", State: HandoffFinalized, Result: ResultSuccess})
	if err != nil {
		t.Fatal(err)
	}
	if f.RequestID != "request" || f.State != HandoffFinalized {
		t.Fatalf("%+v", f)
	}
	if _, err := s.Get(context.Background(), "tenant-b", "handoff-1"); !errors.Is(err, ErrHandoffNotFound) {
		t.Fatal(err)
	}
}

func TestInMemoryHandoffValidationAndFinalizeBranches(t *testing.T) {
	s := NewInMemoryHandoffStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		value ExecutionHandoff
		want  error
	}{
		{"canceled", ctx, ExecutionHandoff{}, context.Canceled},
		{"invalid reserve", context.Background(), ExecutionHandoff{TenantID: "t", HandoffID: "h", State: HandoffFinalized}, ErrInvalid},
		{"invalid finalize", context.Background(), ExecutionHandoff{TenantID: "t", HandoffID: "h", State: HandoffPending}, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.name == "invalid finalize" {
				_, err = s.Finalize(tc.ctx, tc.value)
			} else {
				_, err = s.Reserve(tc.ctx, tc.value)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
		})
	}
	if _, err := s.Finalize(context.Background(), ExecutionHandoff{TenantID: "missing", HandoffID: "h", State: HandoffFinalized}); !errors.Is(err, ErrHandoffNotFound) {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), "t", "missing"); !errors.Is(err, ErrHandoffNotFound) {
		t.Fatal(err)
	}
}
