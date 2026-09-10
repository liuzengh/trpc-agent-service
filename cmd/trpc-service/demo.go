package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	configdomain "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/governance/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

const (
	demoTenantID         = "t_01ARZ3NDEKTSV4RRFFQ69G5FAY"
	demoTenantKey        = "demo"
	demoAgentAppID       = "app_01ARZ3NDEKTSV4RRFFQ69G5FAY"
	demoModelProfileID   = "model-demo-fake"
	demoBackendProfileID = "backend-demo-postgres"
	demoProfileVersion   = int64(1)
	demoPolicyVersion    = int64(1)
	demoRevision         = int64(1)
)

var demoOpaqueIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,127}$`)

type demoIDs struct {
	TenantID, TenantKey, AgentAppID, ModelProfileID, BackendProfileID   string
	ModelProfileVersion, BackendProfileVersion, PolicyVersion, Revision int64
}

func fixedDemoIDs() demoIDs {
	return demoIDs{
		TenantID: demoTenantID, TenantKey: demoTenantKey, AgentAppID: demoAgentAppID,
		ModelProfileID: demoModelProfileID, BackendProfileID: demoBackendProfileID,
		ModelProfileVersion: demoProfileVersion, BackendProfileVersion: demoProfileVersion,
		PolicyVersion: demoPolicyVersion, Revision: demoRevision,
	}
}

// Validate keeps the fixed bootstrap identifiers in the same safe format as
// control-plane IDs. It is intentionally called before both preview and apply
// so a future constant change cannot create an invalid partial bootstrap.
func (ids demoIDs) Validate() error {
	if !strings.HasPrefix(ids.TenantID, "t_") || len(ids.TenantID) != 28 ||
		!strings.HasPrefix(ids.AgentAppID, "app_") || len(ids.AgentAppID) != 30 ||
		!demoOpaqueIDPattern.MatchString(ids.TenantKey) || !demoOpaqueIDPattern.MatchString(ids.ModelProfileID) ||
		!demoOpaqueIDPattern.MatchString(ids.BackendProfileID) || ids.ModelProfileVersion < 1 ||
		ids.BackendProfileVersion < 1 || ids.PolicyVersion < 1 || ids.Revision < 1 {
		return errors.New("demo bootstrap identifiers are invalid")
	}
	return nil
}

func runDemo(ctx context.Context, args []string, output io.Writer, getenv func(string) string) error {
	if ctx == nil || output == nil || getenv == nil {
		return errors.New("invalid demo dependencies")
	}
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	confirm := flags.Bool("confirm", false, "apply the demo bootstrap")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("usage: demo --confirm: %w", err)
	}
	if flags.NArg() != 0 {
		return errors.New("usage: demo --confirm")
	}
	ids := fixedDemoIDs()
	if err := ids.Validate(); err != nil {
		return err
	}
	if !*confirm {
		fmt.Fprintln(output, "demo plan (no database writes; rerun with --confirm to apply):")
		writeDemoIDs(output, ids, 0)
		return nil
	}
	dsn := strings.TrimSpace(getenv("TRPC_POSTGRES_DSN"))
	if dsn == "" {
		return errors.New("TRPC_POSTGRES_DSN is required for demo --confirm")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return errors.New("postgres unavailable")
	}
	// Demo is a disposable first-delivery path. It deliberately applies the
	// embedded migration contract before creating any control-plane records;
	// production services continue to require schema-migrate separately.
	runner := migrations.NewRunner(db)
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("demo schema migration failed: %w", err)
	}
	if err := runner.Ready(ctx); err != nil {
		return fmt.Errorf("demo schema verification failed: %w", err)
	}
	if err := bootstrapDemo(ctx, db, ids); err != nil {
		return fmt.Errorf("demo bootstrap failed: %w", err)
	}
	fmt.Fprintln(output, "demo bootstrap complete:")
	writeDemoIDs(output, ids, 1)
	return nil
}

func writeDemoIDs(output io.Writer, ids demoIDs, configVersion int64) {
	fmt.Fprintf(output, "tenant_id=%s\n", ids.TenantID)
	fmt.Fprintf(output, "tenant_key=%s\n", ids.TenantKey)
	fmt.Fprintf(output, "agent_app_id=%s\n", ids.AgentAppID)
	fmt.Fprintf(output, "model_profile_id=%s\nmodel_profile_version=%d\n", ids.ModelProfileID, ids.ModelProfileVersion)
	fmt.Fprintf(output, "backend_profile_id=%s\nbackend_profile_version=%d\n", ids.BackendProfileID, ids.BackendProfileVersion)
	fmt.Fprintf(output, "policy_version=%d\nrevision=%d\n", ids.PolicyVersion, ids.Revision)
	if configVersion > 0 {
		fmt.Fprintf(output, "config_version=%d\n", configVersion)
	}
}

func bootstrapDemo(ctx context.Context, db *sql.DB, ids demoIDs) error {
	catalog, err := provider.NewCatalog(provider.FakeModelSchema(), provider.FakeEmbeddingSchema(), provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2())
	if err != nil {
		return err
	}
	tenants := tenantpostgres.New(db)
	apps := agentpostgres.New(db)
	configs := configpostgres.New(db, tenants)
	providers := providerpostgres.New(db, catalog)
	governanceStore := governancepostgres.New(db)
	metadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "demo-bootstrap", ReasonCode: "demo_bootstrap", CorrelationID: "demo-bootstrap", TraceID: "demo-bootstrap"}
	root, err := ensureDemoTenant(ctx, tenants, ids, metadata)
	if err != nil {
		return err
	}
	if _, err = providers.PublishModel(ctx, demoModelProfile(ids)); err != nil {
		return err
	}
	if _, err = providers.PublishBackend(ctx, demoBackendProfile(ids)); err != nil {
		return err
	}
	if err = ensureDemoPolicy(ctx, governanceStore, ids); err != nil {
		return err
	}
	if err = ensureDemoApp(ctx, apps, ids); err != nil {
		return err
	}
	root, err = ensureDemoDefaultBackend(ctx, tenants, root, ids, metadata)
	if err != nil {
		return err
	}
	return ensureDemoConfig(ctx, configs, root, ids, metadata)
}

func ensureDemoTenant(ctx context.Context, tenants *tenantpostgres.Repository, ids demoIDs, metadata tenant.ChangeMetadata) (tenant.Tenant, error) {
	root, err := tenants.Get(ctx, ids.TenantID)
	if errors.Is(err, tenant.ErrNotFound) {
		return tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: ids.TenantID, TenantKey: ids.TenantKey, DisplayName: "TRPC Demo"}, ChangeMetadata: metadata})
	}
	if err != nil {
		return tenant.Tenant{}, err
	}
	if root.TenantKey != ids.TenantKey || root.DisplayName != "TRPC Demo" || root.Status != tenant.StatusActive {
		return tenant.Tenant{}, errors.New("existing demo tenant is incompatible")
	}
	return root, nil
}

func demoModelProfile(ids demoIDs) provider.ModelProfileSnapshot {
	return provider.ModelProfileSnapshot{TenantID: ids.TenantID, ProfileID: ids.ModelProfileID, ProfileKey: "demo-fake",
		DisplayName: "Demo Fake Model", Status: "active", SchemaVersion: 1, Provider: "fake", Model: "fake-deterministic-v1",
		Options: map[string]string{"response": "demo: deterministic fake response", "stream_deltas": `["demo: ","deterministic fake response"]`}, Version: ids.ModelProfileVersion}
}

func demoBackendProfile(ids demoIDs) provider.BackendProfileSnapshot {
	return provider.BackendProfileSnapshot{TenantID: ids.TenantID, ProfileID: ids.BackendProfileID, ProfileKey: "demo-postgres",
		// Keep the long-lived demo bootstrap on the immutable v1 default-plane
		// contract so an existing demo database remains idempotent. Named
		// Session data planes are opt-in through a separate v2 Profile.
		DisplayName: "Demo PostgreSQL", Status: "active", SchemaVersion: 1, Provider: "postgres", Version: ids.BackendProfileVersion,
		Capabilities: provider.CapabilitySet{"atomic_turn_commit": true, "strong_ryw": true, "summary_cas": true}}
}

func ensureDemoPolicy(ctx context.Context, store *governancepostgres.Store, ids demoIDs) error {
	policy := governance.PolicyV1{SchemaVersion: governance.CurrentPolicySchemaVersion, DefaultAction: governance.ActionAllow,
		AllowedModels: []governance.VersionedRef{{ID: ids.ModelProfileID, Version: ids.ModelProfileVersion}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled}
	digest, _, err := governance.PolicyDigest(policy)
	if err != nil {
		return err
	}
	return store.PublishPolicy(ctx, governance.PolicySnapshot{TenantID: ids.TenantID, Version: ids.PolicyVersion,
		SchemaVersion: governance.CurrentPolicySchemaVersion, Policy: policy, ContentDigest: digest, PublishedAt: time.Now().UTC()})
}

func ensureDemoApp(ctx context.Context, apps *agentpostgres.Repository, ids demoIDs) error {
	metadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "demo-bootstrap", Reason: "demo_bootstrap", CorrelationID: "demo-bootstrap", TraceID: "demo-bootstrap"}
	app, err := apps.Get(ctx, ids.TenantID, ids.AgentAppID)
	if errors.Is(err, agentapp.ErrNotFound) {
		app, err = apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: ids.TenantID, AgentAppID: ids.AgentAppID,
			AgentAppKey: "assistant", DisplayName: "TRPC Demo Assistant", Description: "Deterministic fake-model demo"}, ChangeMetadata: metadata})
	}
	if err != nil {
		return err
	}
	if app.AgentAppKey != "assistant" || app.DisplayName != "TRPC Demo Assistant" ||
		(app.Status != agentapp.StatusActive && !(app.Status == agentapp.StatusDraft && app.CurrentRevision == 0)) {
		return errors.New("existing demo agent app is incompatible")
	}
	if app.CurrentRevision > 0 {
		if app.CurrentRevision != ids.Revision {
			return errors.New("existing demo agent app revision is incompatible")
		}
		return verifyDemoRevision(ctx, apps, ids, agentapp.RevisionPublished)
	}
	draft, err := apps.GetRevision(ctx, ids.TenantID, ids.AgentAppID, ids.Revision)
	if errors.Is(err, agentapp.ErrNotFound) {
		draft, err = apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: ids.TenantID, AgentAppID: ids.AgentAppID,
			ExpectedAppVersion: app.Version, Revision: demoRevisionDefinition(ids), ChangeMetadata: metadata})
	}
	if err != nil {
		return err
	}
	if draft.Revision != ids.Revision || draft.State != agentapp.RevisionDraft || !sameDemoRevision(draft, ids) {
		return errors.New("existing demo draft is incompatible")
	}
	app, err = apps.Get(ctx, ids.TenantID, ids.AgentAppID)
	if err != nil {
		return err
	}
	_, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: ids.TenantID, AgentAppID: ids.AgentAppID, Revision: ids.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, ChangeMetadata: metadata})
	return err
}

func demoRevisionDefinition(ids demoIDs) agentapp.Revision {
	return agentapp.Revision{AgentKind: agentapp.AgentKindLLM, SchemaVersion: 1, Instruction: "You are the deterministic TRPC demo assistant.",
		ModelProfileID: ids.ModelProfileID, ModelProfileVersion: ids.ModelProfileVersion, FallbackModelRefs: []agentapp.VersionedRef{}}
}

func verifyDemoRevision(ctx context.Context, apps *agentpostgres.Repository, ids demoIDs, state agentapp.RevisionState) error {
	revision, err := apps.GetRevision(ctx, ids.TenantID, ids.AgentAppID, ids.Revision)
	if err != nil {
		return err
	}
	if revision.State != state || !sameDemoRevision(revision, ids) {
		return errors.New("existing demo revision is incompatible")
	}
	return nil
}

func sameDemoRevision(value agentapp.Revision, ids demoIDs) bool {
	actualDigest, actualErr := value.ComputeContentDigest()
	desiredDigest, desiredErr := demoRevisionDefinition(ids).ComputeContentDigest()
	return actualErr == nil && desiredErr == nil && actualDigest == desiredDigest
}

func ensureDemoDefaultBackend(ctx context.Context, tenants *tenantpostgres.Repository, root tenant.Tenant, ids demoIDs, metadata tenant.ChangeMetadata) (tenant.Tenant, error) {
	if root.DefaultBackendProfileID == ids.BackendProfileID {
		return root, nil
	}
	if root.DefaultBackendProfileID != "" {
		return tenant.Tenant{}, errors.New("existing demo default backend is incompatible")
	}
	updated := root
	updated.DefaultBackendProfileID = ids.BackendProfileID
	result, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: updated, ExpectedVersion: root.Version, ChangeMetadata: metadata})
	if err != nil {
		return tenant.Tenant{}, err
	}
	return result.Tenant, nil
}

func ensureDemoConfig(ctx context.Context, configs *configpostgres.Repository, root tenant.Tenant, ids demoIDs, metadata tenant.ChangeMetadata) error {
	payload := configdomain.ConfigV1{SchemaVersion: configdomain.CurrentSchemaVersion, DefaultAgentAppID: ids.AgentAppID, PolicyVersion: ids.PolicyVersion,
		BackendBindings: []configdomain.BackendBinding{
			{Domain: "memory", BackendProfileID: ids.BackendProfileID, BackendVersion: ids.BackendProfileVersion, Required: []string{"strong_ryw"}},
			{Domain: "artifact", BackendProfileID: ids.BackendProfileID, BackendVersion: ids.BackendProfileVersion, Required: []string{"strong_ryw"}},
			{Domain: "session", BackendProfileID: ids.BackendProfileID, BackendVersion: ids.BackendProfileVersion, Required: []string{"atomic_turn_commit"}},
			{Domain: "summary", BackendProfileID: ids.BackendProfileID, BackendVersion: ids.BackendProfileVersion, Required: []string{"summary_cas"}},
		}}
	wantDigest, _, err := configdomain.ContentDigest(payload)
	if err != nil {
		return err
	}
	current, err := configs.GetCurrent(ctx, ids.TenantID)
	if errors.Is(err, configdomain.ErrNotFound) {
		_, err = configs.Publish(ctx, configdomain.PublishInput{TenantID: ids.TenantID, ExpectedTenantVersion: root.Version, Payload: payload, Metadata: metadata})
		return err
	}
	if err != nil {
		return err
	}
	if current.ContentDigest != wantDigest || current.State != configdomain.StatePublished {
		return errors.New("existing demo configuration is incompatible")
	}
	return nil
}
