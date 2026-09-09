package pipeline

import (
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func TestStablePipelineErrorDoesNotExposeProviderDetails(t *testing.T) {
	secret := errors.New("dial postgres://user:password@db.internal/agent?token=secret")
	if got := stablePipelineError(secret); got != "internal_error" {
		t.Fatalf("unknown provider error code=%q, want internal_error", got)
	}
	if got := stablePipelineError(errors.Join(errors.New("wrapped"), reliable.ErrStaleLease)); got != "stale_lease" {
		t.Fatalf("wrapped lease error code=%q, want stale_lease", got)
	}
	if got := stablePipelineError(errors.Join(errors.New("wrapped"), reliable.ErrOutboxConflict)); got != "outbox_conflict" {
		t.Fatalf("wrapped outbox conflict code=%q, want outbox_conflict", got)
	}
}

func TestStablePipelineErrorClassifiesUnknownDelivery(t *testing.T) {
	if got := stablePipelineError(channel.UnknownDeliveryError(errors.New("provider body secret"))); got != "delivery_outcome_unknown" {
		t.Fatalf("unknown delivery code=%q, want delivery_outcome_unknown", got)
	}
}
