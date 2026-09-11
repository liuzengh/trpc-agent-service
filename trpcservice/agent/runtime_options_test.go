package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type coverageRateLimiter struct{}

func (*coverageRateLimiter) Take(context.Context, string, string, int) error { return nil }

type coverageRoleResolver struct{}

func (*coverageRoleResolver) RoleFor(context.Context, string, string) (identity.Role, error) {
	return identity.RoleMember, nil
}

type coverageCostCalculator struct{}

func (*coverageCostCalculator) CostMicros(config.ModelConfig, PricedTokenUsage) int64 { return 0 }

type coverageUsageGovernor struct {
	renewErr error
	renewed  chan struct{}
}

func (*coverageUsageGovernor) Reserve(context.Context, governance.UsageReservationRequest) (governance.UsageReservation, error) {
	return governance.UsageReservation{}, nil
}

func (g *coverageUsageGovernor) Renew(context.Context, governance.UsageReservation, time.Duration) error {
	if g.renewed != nil {
		select {
		case g.renewed <- struct{}{}:
		default:
		}
	}
	return g.renewErr
}

func (*coverageUsageGovernor) SettleKnown(context.Context, governance.UsageReservation, governance.SettledUsage) error {
	return nil
}

func (*coverageUsageGovernor) SettleUnknown(context.Context, governance.UsageReservation) error {
	return nil
}

type coverageDeltaPublisher struct{}

func (*coverageDeltaPublisher) PublishDelta(context.Context, string, string, string, string) error {
	return nil
}

type coverageIMDeltaPublisher struct{}

func (*coverageIMDeltaPublisher) PublishProgress(context.Context, messaging.IMProgressEvent) error {
	return nil
}

type coverageDocumentExtractor struct{}

func (*coverageDocumentExtractor) SupportsDocument(string) bool { return true }

func (*coverageDocumentExtractor) ExtractDocument(context.Context, string, []byte) (string, error) {
	return "text", nil
}

func TestRuntimeDependencyOptionsRejectNil(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		option RuntimeOption
	}{
		{name: "tenant rate limiter", option: WithTenantRateLimiter(nil)},
		{name: "tenant role resolver", option: WithTenantRoleResolver(nil)},
		{name: "model cost calculator", option: WithModelCostCalculator(nil)},
		{name: "usage governor", option: WithUsageGovernor(nil)},
		{name: "invocation factory", option: WithInvocationFactory(nil)},
		{name: "observer", option: WithObserver(nil)},
		{name: "execution dedup", option: WithExecutionDedup(nil)},
		{name: "reply delta publisher", option: WithReplyDeltaPublisher(nil)},
		{name: "IM reply delta publisher", option: WithIMReplyDeltaPublisher(nil)},
		{name: "artifact provider", option: WithArtifactProvider(nil)},
		{name: "model input validator", option: WithModelInputValidator(nil)},
		{name: "document input extractor", option: WithDocumentInputExtractor(nil)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.option(&Runtime{}); err == nil {
				t.Fatal("option accepted a nil dependency")
			}
		})
	}
}

func TestRuntimeContextHelpersHandleMissingValues(t *testing.T) {
	t.Parallel()
	if snapshot, ok := snapshotFromContext(nil); ok || snapshot.Config.TenantID != "" {
		t.Fatalf("snapshotFromContext(nil) = %+v, %v", snapshot, ok)
	}
	if snapshot, ok := snapshotFromContext(context.Background()); ok || snapshot.Config.TenantID != "" {
		t.Fatalf("snapshotFromContext(background) = %+v, %v", snapshot, ok)
	}
	invalidSnapshotContext := context.WithValue(context.Background(), snapshotContextKey{}, tenant.Snapshot{})
	if _, ok := snapshotFromContext(invalidSnapshotContext); ok {
		t.Fatal("snapshotFromContext() accepted incomplete snapshot")
	}
	validSnapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", ConfigVersion: 1}}
	if snapshot, ok := snapshotFromContext(WithConfigurationSnapshot(context.Background(), validSnapshot)); !ok || snapshot.Config.ConfigVersion != 1 {
		t.Fatalf("snapshotFromContext(valid) = %+v, %v", snapshot, ok)
	}
	if key := resolvedSessionKeyFromContext(nil); key != "" {
		t.Fatalf("resolvedSessionKeyFromContext(nil) = %q", key)
	}
	if key := resolvedSessionKeyFromContext(context.Background()); key != "" {
		t.Fatalf("resolvedSessionKeyFromContext(background) = %q", key)
	}
	ctx := WithResolvedSessionKey(context.Background(), "  tenant-a/support/session/1  ")
	if key := resolvedSessionKeyFromContext(ctx); key != "tenant-a/support/session/1" {
		t.Fatalf("resolved session key = %q", key)
	}
}

func TestRuntimeDependencyOptionsInstallDependencies(t *testing.T) {
	t.Parallel()
	rateLimiter := &coverageRateLimiter{}
	roleResolver := &coverageRoleResolver{}
	costCalculator := &coverageCostCalculator{}
	usageGovernor := &coverageUsageGovernor{}
	deltaPublisher := &coverageDeltaPublisher{}
	imDeltaPublisher := &coverageIMDeltaPublisher{}
	documentExtractor := &coverageDocumentExtractor{}
	runtime := &Runtime{}

	options := []RuntimeOption{
		WithTenantRateLimiter(rateLimiter),
		WithTenantRoleResolver(roleResolver),
		WithModelCostCalculator(costCalculator),
		WithUsageGovernor(usageGovernor),
		WithReplyDeltaPublisher(deltaPublisher),
		WithIMReplyDeltaPublisher(imDeltaPublisher),
		WithDocumentInputExtractor(documentExtractor),
	}
	for _, option := range options {
		if err := option(runtime); err != nil {
			t.Fatalf("install option: %v", err)
		}
	}
	if runtime.rateLimiter != rateLimiter || runtime.roleResolver != roleResolver ||
		runtime.costCalculator != costCalculator || runtime.usageGovernor != usageGovernor ||
		runtime.deltaPublisher != deltaPublisher || runtime.imDeltaPublisher != imDeltaPublisher ||
		runtime.documentInputs != documentExtractor {
		t.Fatal("one or more Runtime dependencies were not installed")
	}
}

func TestUsageReservationHeartbeatStopsOnRenewFailure(t *testing.T) {
	renewErr := errors.New("usage lease lost")
	governor := &coverageUsageGovernor{renewErr: renewErr, renewed: make(chan struct{}, 1)}
	heartbeat := startUsageReservationHeartbeat(
		context.Background(),
		governor,
		governance.UsageReservation{ID: "reservation-1"},
		time.Millisecond,
	)
	select {
	case <-governor.renewed:
	case <-time.After(2 * time.Second):
		t.Fatal("usage reservation was not renewed")
	}
	heartbeat.Stop()
	if !errors.Is(heartbeat.Err(), renewErr) {
		t.Fatalf("heartbeat error = %v, want %v", heartbeat.Err(), renewErr)
	}
}

func TestNilUsageReservationHeartbeatIsSafe(t *testing.T) {
	t.Parallel()
	var heartbeat *usageReservationHeartbeat
	if err := heartbeat.Err(); err != nil {
		t.Fatalf("nil heartbeat error = %v", err)
	}
	heartbeat.Stop()
}
