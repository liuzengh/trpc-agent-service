package domain_test

import (
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"testing"
)

func TestReplaySourceRequiresCompleteRetainedHistory(t *testing.T) {
	source := domain.ReplaySource{StreamName: "CONTROL_ROUTES", StreamID: "2026-09-05T00:00:00Z", FirstSequence: 1, LastSequence: 3, MessageCount: 3}
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
	source.MessageCount = 2
	if err := source.Validate(); !errors.Is(err, domain.ErrHistoryGap) {
		t.Fatal(err)
	}
	source.MessageCount = 3
	source.FirstSequence = 2
	if err := source.Validate(); !errors.Is(err, domain.ErrHistoryGap) {
		t.Fatal(err)
	}
	source.FirstSequence = 0
	source.LastSequence = 0
	source.MessageCount = 0
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestProjectionHealthFailureClasses(t *testing.T) {
	tests := []struct {
		health domain.ProjectionHealth
		want   error
	}{{domain.ProjectionHealth{}, domain.ErrNotInitialized}, {domain.ProjectionHealth{Stale: true}, domain.ErrProjectionStale}, {domain.ProjectionHealth{BlockedReason: "invalid_schema"}, domain.ErrProjectionBlocked}, {domain.ProjectionHealth{Initialized: true}, nil}}
	for _, test := range tests {
		if err := test.health.RequireInitialized(); !errors.Is(err, test.want) {
			t.Fatalf("%#v => %v, want %v", test.health, err, test.want)
		}
	}
}

func TestProjectionHealthApplyLagIsIndependentOfObservationAndInitialization(t *testing.T) {
	tests := []struct {
		name   string
		health domain.ProjectionHealth
		want   error
	}{
		{"complete but lagging", domain.ProjectionHealth{Initialized: true, ApplyLagExceeded: true}, domain.ErrProjectionApplyLag},
		{"incomplete and lagging", domain.ProjectionHealth{ApplyLagExceeded: true}, domain.ErrProjectionApplyLag},
		{"short lag within grace", domain.ProjectionHealth{Initialized: true, HighestSequence: 2, ContiguousSequence: 1}, nil},
		{"source expired independently", domain.ProjectionHealth{Initialized: true, Stale: true, ApplyLagExceeded: true}, domain.ErrProjectionStale},
		{"quarantined independently", domain.ProjectionHealth{BlockedReason: "invalid_schema", ApplyLagExceeded: true}, domain.ErrProjectionBlocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.health.RequireInitialized(); !errors.Is(err, tt.want) {
				t.Fatalf("RequireInitialized()=%v, want %v", err, tt.want)
			}
		})
	}
}
