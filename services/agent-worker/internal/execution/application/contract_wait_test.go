package application

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type contractWaitLedger struct {
	Ledger
	terminalized []string
}

func (l *contractWaitLedger) Terminalize(_ context.Context, _, _, reason string) (bool, error) {
	l.terminalized = append(l.terminalized, reason)
	return false, nil
}

type contractWaitManifest struct{}

func (contractWaitManifest) Resolve(context.Context, domain.Route) (domain.Plan, error) {
	return domain.Plan{}, ErrManifestContractMismatch
}

// Release skew must leave the Run durably waiting for a matching release: no
// Attempt, no terminal completion, and a distinct low-cardinality signal.
func TestAdvanceWaitsOnReleaseSkewInsteadOfTerminating(t *testing.T) {
	run := domain.Run{Request: domain.Requested{RunID: "run", Route: domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: "sha256:" + "1f27128199f89", DeploymentRevisionID: "revision"}}}
	ledger := &contractWaitLedger{}
	events := &observations{}
	processor, err := NewProcessor(ledger, contractWaitManifest{}, &observationRuntime{}, "worker", 1, events)
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Advance(context.Background(), run); !errors.Is(err, ErrManifestContractMismatch) {
		t.Fatal(err)
	}
	for _, reason := range ledger.terminalized {
		if reason != "" {
			t.Fatalf("release skew terminalized the Run with %q", reason)
		}
	}
	seen := false
	for _, e := range events.events {
		if e.Operation != "manifest" {
			continue
		}
		seen = true
		if e.Result != "manifest_contract_wait" {
			t.Fatalf("manifest signal = %q", e.Result)
		}
	}
	if !seen {
		t.Fatal("release skew emitted no manifest signal")
	}
}
