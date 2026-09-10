package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestAgentExecutionSnapshotFreezesFactoryInputAndCacheIdentity(t *testing.T) {
	tenantRoot, tenantSnapshot, app, revision := executionFixture(t)
	snapshot, err := NewAgentExecutionSnapshot(tenantSnapshot, app, revision)
	if err != nil {
		t.Fatal(err)
	}
	assertExecutionCacheIdentity(t, snapshot, tenantRoot, app, revision)
	input := assertExecutionFactoryInput(t, snapshot, tenantRoot, app, revision)
	assertExecutionSnapshotIsFrozen(t, snapshot, tenantRoot, app, revision)
	assertExecutionFactoryInputIsDefensive(t, snapshot, input)
}

func assertExecutionCacheIdentity(t *testing.T, snapshot AgentExecutionSnapshot, tenantRoot *tenant.Tenant, app *appmodel.App, revision *appmodel.Revision) {
	t.Helper()
	key, err := snapshot.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != tenantRoot.TenantID || key.TenantVersion != tenantRoot.Version || key.AppID != app.AppID || key.AppVersion != app.Version || key.Revision != revision.Revision || key.ContentDigest != revision.ContentDigest {
		t.Fatalf("unexpected cache identity: %+v", key)
	}
}

func assertExecutionFactoryInput(t *testing.T, snapshot AgentExecutionSnapshot, tenantRoot *tenant.Tenant, app *appmodel.App, revision *appmodel.Revision) LLMAgentFactoryInput {
	t.Helper()
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if input.Name != app.AppKey || input.DisplayName != app.DisplayName || input.Description != revision.Description || input.ModelProfileID != revision.ModelProfileID || input.Revision != revision.Revision || input.ContentDigest != revision.ContentDigest {
		t.Fatalf("unexpected Factory mapping: %+v", input)
	}
	if input.TenantVersion != tenantRoot.Version || input.AppVersion != app.Version || input.Kind != appmodel.KindLLM || input.SchemaVersion != appmodel.SchemaVersionV1 || len(input.Tools) != 2 {
		t.Fatalf("Factory input lost fixed versions or executable configuration: %+v", input)
	}
	return input
}

func assertExecutionSnapshotIsFrozen(t *testing.T, snapshot AgentExecutionSnapshot, tenantRoot *tenant.Tenant, app *appmodel.App, revision *appmodel.Revision) {
	t.Helper()
	tenantRoot.DisplayName = "source tenant mutation"
	app.DisplayName = "source App mutation"
	revision.Tools[0].ToolID = "source-tool-mutation"
	*revision.Generation.Temperature = 1.9
	if snapshot.Tenant().DisplayName == "source tenant mutation" || snapshot.App().DisplayName == "source App mutation" {
		t.Fatal("snapshot retained mutable Tenant or App source")
	}
	frozenRevision := snapshot.Revision()
	if frozenRevision.Tools[0].ToolID == "source-tool-mutation" || *frozenRevision.Generation.Temperature == 1.9 {
		t.Fatal("snapshot retained mutable Revision source")
	}
}

func assertExecutionFactoryInputIsDefensive(t *testing.T, snapshot AgentExecutionSnapshot, input LLMAgentFactoryInput) {
	t.Helper()
	input.Tools[0].ToolID = "caller mutation"
	*input.Generation.Temperature = 1.8
	again, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if again.Tools[0].ToolID == "caller mutation" || *again.Generation.Temperature == 1.8 {
		t.Fatal("Factory input exposed mutable snapshot state")
	}
	clone := again.Clone()
	clone.Tools[0].ToolID = "clone mutation"
	*clone.Generation.Temperature = 1.7
	if again.Tools[0].ToolID == "clone mutation" || *again.Generation.Temperature == 1.7 {
		t.Fatal("Factory input clone leaked pointer or slice mutation")
	}
}

func TestAgentExecutionSnapshotContextIsSealedAndDefensive(t *testing.T) {
	_, tenantSnapshot, app, revision := executionFixture(t)
	snapshot, err := NewAgentExecutionSnapshot(tenantSnapshot, app, revision)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAgentExecutionSnapshot(context.Background(), snapshot)
	fromContext, ok := AgentExecutionSnapshotFromContext(ctx)
	if !ok {
		t.Fatal("valid snapshot was not carried by context")
	}
	mutable := fromContext.Revision()
	mutable.Tools[0].ToolID = "context mutation"
	again, ok := AgentExecutionSnapshotFromContext(ctx)
	if !ok || again.Revision().Tools[0].ToolID == "context mutation" {
		t.Fatal("context exposed mutable execution state")
	}

	ctx = WithAgentExecutionSnapshot(ctx, AgentExecutionSnapshot{})
	if invalid, ok := AgentExecutionSnapshotFromContext(ctx); ok || invalid.App().AppID != "" {
		t.Fatalf("zero snapshot entered trusted context: %+v", invalid)
	}
	if _, ok := AgentExecutionSnapshotFromContext(context.Background()); ok {
		t.Fatal("context without snapshot was accepted")
	}
}

func TestAgentExecutionSnapshotRejectsInvalidAdmissionState(t *testing.T) {
	_, tenantSnapshot, app, revision := executionFixture(t)
	otherApp, err := appmodel.NewApp(appmodel.CreateInput{TenantID: app.TenantID, AppKey: "other", DisplayName: "Other"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		snapshot tenant.ConfigurationSnapshot
		app      *appmodel.App
		revision *appmodel.Revision
		mutate   func(*appmodel.App, *appmodel.Revision)
	}{
		{name: "zero tenant snapshot", snapshot: tenant.ConfigurationSnapshot{}, app: app, revision: revision},
		{name: "nil App", snapshot: tenantSnapshot, app: nil, revision: revision},
		{name: "nil Revision", snapshot: tenantSnapshot, app: app, revision: nil},
		{name: "draft App", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, _ *appmodel.Revision) {
			app.Status = appmodel.StatusDraft
			app.CurrentRevision = nil
		}},
		{name: "suspended App", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, _ *appmodel.Revision) { app.Status = appmodel.StatusSuspended }},
		{name: "disabled App", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, _ *appmodel.Revision) { app.Status = appmodel.StatusDisabled }},
		{name: "draft Revision", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(_ *appmodel.App, revision *appmodel.Revision) {
			revision.State = appmodel.RevisionStateDraft
			revision.ContentDigest = ""
			revision.PublishedAt = nil
		}},
		{name: "tenant mismatch", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, revision *appmodel.Revision) {
			app.TenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
			revision.TenantID = app.TenantID
		}},
		{name: "App mismatch", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(_ *appmodel.App, revision *appmodel.Revision) { revision.AppID = otherApp.AppID }},
		{name: "non-current Revision", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, _ *appmodel.Revision) { app.CurrentRevision = int64Pointer(2) }},
		{name: "digest mismatch", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(_ *appmodel.App, revision *appmodel.Revision) { revision.ContentDigest = "bad" }},
		{name: "invalid App root", snapshot: tenantSnapshot, app: app, revision: revision, mutate: func(app *appmodel.App, _ *appmodel.Revision) { app.Version = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var appCopy *appmodel.App
			if test.app != nil {
				value := test.app.Clone()
				appCopy = &value
			}
			var revisionCopy *appmodel.Revision
			if test.revision != nil {
				value := test.revision.Clone()
				revisionCopy = &value
			}
			if test.mutate != nil {
				test.mutate(appCopy, revisionCopy)
			}
			if _, err := NewAgentExecutionSnapshot(test.snapshot, appCopy, revisionCopy); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected admission rejection, got %v", err)
			}
		})
	}

	inactiveTenant := tenantSnapshot.Tenant()
	inactiveTenant.Status = tenant.StatusSuspended
	if err := validateExecutionState(inactiveTenant, app, revision); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inactive tenant execution state error = %v", err)
	}
}

func TestAgentExecutionSnapshotRequiresActiveTenantBoundary(t *testing.T) {
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "inactive", DisplayName: "Inactive", Status: tenant.StatusSuspended,
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.NewConfigurationSnapshot(root); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("inactive Tenant entered protected snapshot boundary: %v", err)
	}
}

func TestZeroExecutionSnapshotCannotProduceFactoryState(t *testing.T) {
	var snapshot AgentExecutionSnapshot
	if snapshot.Tenant().TenantID != "" || snapshot.App().AppID != "" || snapshot.Revision().Revision != 0 {
		t.Fatal("zero snapshot accessors returned trusted-looking state")
	}
	if _, err := snapshot.CacheKey(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot produced cache key: %v", err)
	}
	if _, err := snapshot.FactoryInput(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot produced Factory input: %v", err)
	}
	if cloneTools(nil) != nil {
		t.Fatal("nil tool set did not remain nil after cloning")
	}
}

func executionFixture(t *testing.T) (*tenant.Tenant, tenant.ConfigurationSnapshot, *appmodel.App, *appmodel.Revision) {
	t.Helper()
	tenantRoot, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "execution", DisplayName: "Execution Tenant",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	app, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantRoot.TenantID, AppKey: "support", DisplayName: "Support", Description: "Support UI metadata"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: tenantRoot.TenantID, AppID: app.AppID, Revision: 1,
		Configuration: appmodel.DraftConfiguration{
			Description: "Immutable Agent description", Instruction: "Answer accurately.",
			GlobalInstruction: "Follow policy.", ModelProfileID: "model-primary",
			Generation: appmodel.GenerationConfig{Temperature: float64Pointer(0.2), TopP: float64Pointer(0.8), MaxOutputTokens: intPointer(2048)},
			Runtime:    appmodel.DefaultRuntimePolicy(),
			Tools:      []appmodel.ToolAuthorization{{ToolID: "calculator", Required: true}, {ToolID: "search"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedAt := draft.UpdatedAt.Add(time.Second)
	published, err := draft.Publish(publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	app.Status = appmodel.StatusActive
	app.CurrentRevision = int64Pointer(published.Revision)
	app.Version++
	app.UpdatedAt = publishedAt
	if err := app.Validate(); err != nil {
		t.Fatal(err)
	}
	return tenantRoot, tenantSnapshot, app, &published
}

func int64Pointer(value int64) *int64 { return &value }

func float64Pointer(value float64) *float64 { return &value }

func intPointer(value int) *int { return &value }
