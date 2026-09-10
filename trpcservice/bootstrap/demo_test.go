package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type demoModelRepoStub struct {
	profile       *modelprofile.Profile
	getErr        error
	createErr     error
	transitionErr error
}

type demoTenantRepoStub struct {
	root      *tenant.Tenant
	getErr    error
	updateErr error
}

func (r *demoTenantRepoStub) Create(context.Context, tenant.CreateInput) (*tenant.Tenant, error) {
	return r.root, nil
}
func (r *demoTenantRepoStub) Get(context.Context, string) (*tenant.Tenant, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.root, nil
}
func (r *demoTenantRepoStub) UpdateConfiguration(_ context.Context, input tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	if r.updateErr != nil {
		return nil, r.updateErr
	}
	updated := r.root.Clone()
	updated.DefaultAgentAppID = stringPointer(*input.DefaultAgentAppID)
	updated.DefaultBackendProfileID = stringPointer(*input.DefaultBackendProfileID)
	updated.Version++
	r.root = &updated
	return r.root, nil
}
func (r *demoTenantRepoStub) TransitionStatus(context.Context, tenant.TransitionStatusInput) (*tenant.Tenant, tenant.StatusChangeEvent, error) {
	return r.root, tenant.StatusChangeEvent{}, nil
}

func (r *demoModelRepoStub) Create(context.Context, modelprofile.CreateInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	if r.createErr != nil {
		return nil, modelprofile.ChangeEvent{}, r.createErr
	}
	return r.profile, modelprofile.ChangeEvent{}, nil
}
func (r *demoModelRepoStub) Get(context.Context, string, string) (*modelprofile.Profile, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.profile, nil
}
func (r *demoModelRepoStub) UpdateConfiguration(context.Context, modelprofile.UpdateConfigurationInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	return r.profile, modelprofile.ChangeEvent{}, nil
}
func (r *demoModelRepoStub) TransitionStatus(_ context.Context, _ modelprofile.TransitionStatusInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	if r.transitionErr != nil {
		return nil, modelprofile.ChangeEvent{}, r.transitionErr
	}
	copy := r.profile.Clone()
	copy.Status = modelprofile.StatusActive
	r.profile = &copy
	return r.profile, modelprofile.ChangeEvent{}, nil
}

type demoBackendRepoStub struct {
	profile       *backend.Profile
	getErr        error
	createErr     error
	transitionErr error
}

type demoAgentRepoStub struct {
	app            *appmodel.App
	revision       *appmodel.Revision
	getErr         error
	getRevisionErr error
	createDraftErr error
	publishErr     error
	transitionErr  error
}

func demoAgentMetadata() appmodel.ChangeMetadata {
	return appmodel.ChangeMetadata{ActorType: demoActorType, ActorID: demoActorID, Reason: demoReason, CorrelationID: demoCorrelationID}
}

func (r *demoAgentRepoStub) Create(context.Context, appmodel.CreateInput) (*appmodel.App, error) {
	return r.app, nil
}
func (r *demoAgentRepoStub) Get(context.Context, string, string) (*appmodel.App, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.app, nil
}
func (r *demoAgentRepoStub) UpdateMetadata(context.Context, appmodel.UpdateMetadataInput) (*appmodel.App, error) {
	return r.app, nil
}
func (r *demoAgentRepoStub) CreateDraft(context.Context, appmodel.CreateDraftInput) (*appmodel.Revision, error) {
	if r.createDraftErr != nil {
		return nil, r.createDraftErr
	}
	return r.revision, nil
}
func (r *demoAgentRepoStub) UpdateDraft(context.Context, appmodel.UpdateDraftInput) (*appmodel.Revision, error) {
	return r.revision, nil
}
func (r *demoAgentRepoStub) GetRevision(context.Context, string, string, int64) (*appmodel.Revision, error) {
	if r.getRevisionErr != nil {
		return nil, r.getRevisionErr
	}
	return r.revision, nil
}
func (r *demoAgentRepoStub) Publish(context.Context, appmodel.PublishInput) (*appmodel.App, *appmodel.Revision, appmodel.ChangeEvent, error) {
	if r.publishErr != nil {
		return nil, nil, appmodel.ChangeEvent{}, r.publishErr
	}
	return r.app, r.revision, appmodel.ChangeEvent{}, nil
}
func (r *demoAgentRepoStub) Rollback(context.Context, appmodel.RollbackInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	return r.app, appmodel.ChangeEvent{}, nil
}
func (r *demoAgentRepoStub) SetCanary(context.Context, appmodel.SetCanaryInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	return r.app, appmodel.ChangeEvent{}, nil
}
func (r *demoAgentRepoStub) TransitionStatus(_ context.Context, _ appmodel.TransitionStatusInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if r.transitionErr != nil {
		return nil, appmodel.ChangeEvent{}, r.transitionErr
	}
	copy := r.app.Clone()
	copy.Status = appmodel.StatusActive
	r.app = &copy
	return r.app, appmodel.ChangeEvent{}, nil
}

func (r *demoBackendRepoStub) Create(context.Context, backend.CreateInput) (*backend.Profile, backend.ChangeEvent, error) {
	if r.createErr != nil {
		return nil, backend.ChangeEvent{}, r.createErr
	}
	return r.profile, backend.ChangeEvent{}, nil
}
func (r *demoBackendRepoStub) Get(context.Context, string, string) (*backend.Profile, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.profile, nil
}
func (r *demoBackendRepoStub) UpdateConfiguration(context.Context, backend.UpdateConfigurationInput) (*backend.Profile, backend.ChangeEvent, error) {
	return r.profile, backend.ChangeEvent{}, nil
}
func (r *demoBackendRepoStub) TransitionStatus(_ context.Context, _ backend.TransitionStatusInput) (*backend.Profile, backend.ChangeEvent, error) {
	if r.transitionErr != nil {
		return nil, backend.ChangeEvent{}, r.transitionErr
	}
	copy := r.profile.Clone()
	copy.Status = backend.StatusActive
	r.profile = &copy
	return r.profile, backend.ChangeEvent{}, nil
}

func TestDefaultDemoConfigAndValidation(t *testing.T) {
	config := DefaultDemoConfig()
	if config.TenantKey != demoTenantKey || config.AppKey != demoAppKey || config.ModelProfileKey != demoModelProfileKey || config.BackendProfileKey != demoBackendProfileKey {
		t.Fatalf("default demo config = %+v", config)
	}
	if err := config.validate(); err != nil {
		t.Fatalf("default demo config is invalid: %v", err)
	}
	for _, mutate := range []func(*DemoConfig){
		func(value *DemoConfig) { value.TenantKey = "bad key" },
		func(value *DemoConfig) { value.AppKey = "bad key" },
		func(value *DemoConfig) { value.ModelProfileKey = "bad key" },
		func(value *DemoConfig) { value.BackendProfileKey = "bad key" },
	} {
		invalid := config
		mutate(&invalid)
		if err := invalid.validate(); !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrDemoState) {
			t.Fatalf("invalid demo config error = %v", err)
		}
	}
	trimmed := normalizeDemoConfig(DemoConfig{TenantKey: "  Tenant-Demo  ", AppKey: "  Assistant-Demo  ", ModelProfileKey: " DemoModel ", BackendProfileKey: " LocalStore "})
	if trimmed.TenantKey != "tenant-demo" || trimmed.AppKey != "assistant-demo" || trimmed.ModelProfileKey != "demomodel" || trimmed.BackendProfileKey != "localstore" || trimmed.AppDisplayName == "" {
		t.Fatalf("normalized demo config = %+v", trimmed)
	}
	if got := normalizeDemoConfig(DemoConfig{}); got != config {
		t.Fatalf("empty config defaults = %+v, want %+v", got, config)
	}
}

func TestDemoCatalogLoaderErrors(t *testing.T) {
	failing := func(environmentConfig) (*modelprofile.ProviderCatalog, *backend.ProviderCatalog, error) {
		return nil, nil, errors.New("catalog unavailable")
	}
	if _, _, _, _, err := newDemoRepositories(nil, failing); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("repository catalog error = %v", err)
	}
	if _, err := initializeDemoAfterInit(context.Background(), nil, DefaultDemoConfig(), InitResult{}, failing); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("post-init catalog error = %v", err)
	}
	if err := DefaultDemoConfig().validateWithCatalogs(failing); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("validation catalog error = %v", err)
	}
	if _, _, _, _, err := newDemoRepositories(nil, environmentCatalogs); err != nil {
		t.Fatalf("valid repository catalogs = %v", err)
	}
}

func TestWriteDemoResultOmitsSecrets(t *testing.T) {
	result := DemoResult{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackendProfileID: "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Revision: 1, Created: true,
	}
	var output strings.Builder
	if err := WriteDemoResult(&output, result); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, expected := range []string{"TRPC_DEMO_MODE='true'", "TRPC_MODEL_PROVIDER='fake'", "TRPC_TENANT_ID='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'", "TRPC_APP_ID='app_01ARZ3NDEKTSV4RRFFQ69G5FAV'"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("demo output missing %q: %s", expected, text)
		}
	}
	for _, secret := range []string{"postgres://", "api-token", "model-secret", "secretref", "TRPC_POSTGRES_DSN", "TRPC_API_TOKEN", "TRPC_MODEL_API_KEY"} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(secret)) {
			t.Fatalf("demo output leaked %q: %s", secret, text)
		}
	}
	if err := WriteDemoResult(io.Discard, DemoResult{}); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("invalid demo result error = %v", err)
	}
}

func TestDeterministicModelContract(t *testing.T) {
	model := deterministicModel{model: demoModelName}
	if model.Info().Name != demoModelName {
		t.Fatalf("model info = %+v", model.Info())
	}
	if _, err := model.GenerateContent(nil, &trpcmodel.Request{}); err == nil {
		t.Fatal("nil context was accepted")
	}
	if _, err := model.GenerateContent(context.Background(), nil); err == nil {
		t.Fatal("nil request was accepted")
	}
	responses, err := model.GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	response, ok := <-responses
	if !ok || response == nil || response.Error != nil || !response.Done || len(response.Choices) != 1 || response.Choices[0].Message.Content != deterministicDemoResponse {
		t.Fatalf("deterministic response = %#v, open=%v", response, ok)
	}
	if _, open := <-responses; open {
		t.Fatal("deterministic response channel remained open")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	responses, err = model.GenerateContent(canceled, &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, open := <-responses; open {
		t.Fatal("canceled deterministic model emitted a response")
	}
}

func TestDemoEnvironmentIsExplicitAndCredentialFree(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envModelNames, demoModelName)
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !config.demoMode || config.modelProvider != demoModelProvider || config.secretRef != "" || config.modelAPIKey != "" || len(config.modelNames) != 1 || config.modelNames[0] != demoModelName {
		t.Fatalf("demo environment = %+v", config)
	}
	catalog, _, err := environmentCatalogs(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (environmentModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{Provider: demoModelProvider, Model: demoModelName}, modelprofile.SecretValue{}); err != nil {
		t.Fatalf("credential-free demo model = %v", err)
	}
	if _, err := catalog.NormalizeConfiguration(modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}); err != nil {
		t.Fatalf("demo model catalog = %v", err)
	}

	t.Setenv(envDemoMode, "false")
	t.Setenv(envModelProvider, defaultModelProvider)
	t.Setenv(envModelAPIKey, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("production environment without model key = %v", err)
	}
	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, defaultModelProvider)
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with production provider = %v", err)
	}
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envControlPlaneDriver, string(ControlPlaneDriverMySQL))
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with MySQL control plane = %v", err)
	}
	t.Setenv(envControlPlaneDriver, string(ControlPlaneDriverPostgres))
	t.Setenv(envWeComCallbackToken, "callback")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo environment with WeCom credentials = %v", err)
	}
}

func TestEnsureDemoDefaultsFailsClosed(t *testing.T) {
	repo := tenantmemory.NewRepository()
	root, err := repo.Create(context.Background(), tenant.CreateInput{TenantKey: "demo", DisplayName: "Demo"})
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := ensureDemoDefaults(context.Background(), repo, root, "app_demo", "backend_demo")
	if err != nil || !changed || updated.DefaultAgentAppID == nil || updated.DefaultBackendProfileID == nil {
		t.Fatalf("initial defaults update = %+v, changed=%v, err=%v", updated, changed, err)
	}
	stable, changed, err := ensureDemoDefaults(context.Background(), repo, updated, "app_demo", "backend_demo")
	if err != nil || changed || stable.DefaultAgentAppID == nil || *stable.DefaultAgentAppID != "app_demo" {
		t.Fatalf("matching defaults = %+v, changed=%v, err=%v", stable, changed, err)
	}
	wrong := "app_other"
	stable.DefaultAgentAppID = &wrong
	if _, _, err := ensureDemoDefaults(context.Background(), repo, stable, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("incompatible defaults error = %v", err)
	}
	suspended := *root
	suspended.Status = tenant.StatusSuspended
	if _, _, err := ensureDemoDefaults(context.Background(), repo, &suspended, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("suspended tenant error = %v", err)
	}
	if _, _, err := ensureDemoDefaults(context.Background(), repo, nil, "app_demo", "backend_demo"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("nil tenant error = %v", err)
	}
}

func TestEnsureDemoRevisionRejectsUnrunnableState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusActive}
	canary := int64(2)
	app.CanaryRevision = &canary
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, root, app, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("canary state error = %v", err)
	}
	app.CanaryRevision = nil
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, root, app, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("active app without revision error = %v", err)
	}
	suspended := *root
	suspended.Status = tenant.StatusSuspended
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, &suspended, app, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("suspended tenant error = %v", err)
	}
	if _, _, _, err := ensureDemoRevision(context.Background(), db, nil, root, nil, "model"); !errors.Is(err, ErrDemoState) {
		t.Fatalf("nil app error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDemoSQLLookupBranches(t *testing.T) {
	t.Run("profile lookup", func(t *testing.T) {
		tests := []struct {
			name  string
			kind  string
			rows  *sqlmock.Rows
			err   error
			want  error
			found bool
		}{
			{name: "unknown kind", kind: "other", want: ErrDemoInitialization},
			{name: "query error", kind: "model_profile", err: sql.ErrConnDone, want: ErrDemoInitialization},
			{name: "empty", kind: "model_profile", rows: sqlmock.NewRows([]string{"profile_id"})},
			{name: "one", kind: "backend_profile", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("bp_demo"), found: true},
			{name: "duplicate", kind: "model_profile", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_1").AddRow("mp_2"), want: ErrDemoState},
			{name: "scan error", kind: "model_profile", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow(nil), want: ErrDemoInitialization},
			{name: "row error", kind: "model_profile", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_1").RowError(0, sql.ErrConnDone), want: ErrDemoInitialization},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				var found bool
				if test.kind == "other" {
					_, _, err = findProfileID(context.Background(), db, test.kind, "tenant", "key")
				} else if test.err != nil {
					mock.ExpectQuery("SELECT profile_id").WillReturnError(test.err)
					_, found, err = findProfileID(context.Background(), db, test.kind, "tenant", "key")
				} else {
					mock.ExpectQuery("SELECT profile_id").WillReturnRows(test.rows)
					_, found, err = findProfileID(context.Background(), db, test.kind, "tenant", "key")
				}
				if found != test.found {
					t.Fatalf("found = %v, want %v", found, test.found)
				}
				if test.want != nil && !errors.Is(err, test.want) {
					t.Fatalf("error = %v, want %v", err, test.want)
				}
				if test.want == nil && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("revision lookup", func(t *testing.T) {
		tests := []struct {
			name     string
			rows     *sqlmock.Rows
			queryErr error
			wantErr  bool
		}{
			{name: "empty", rows: sqlmock.NewRows([]string{"revision"})},
			{name: "values", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).AddRow(int64(2))},
			{name: "query error", queryErr: sql.ErrConnDone, wantErr: true},
			{name: "scan error", rows: sqlmock.NewRows([]string{"revision"}).AddRow("bad"), wantErr: true},
			{name: "row error", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).RowError(0, sql.ErrConnDone), wantErr: true},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				if test.queryErr != nil {
					mock.ExpectQuery("SELECT revision").WillReturnError(test.queryErr)
				} else {
					mock.ExpectQuery("SELECT revision").WillReturnRows(test.rows)
				}
				values, err := findRevisionNumbers(context.Background(), db, "tenant", "app")
				if test.wantErr != (err != nil) {
					t.Fatalf("error = %v, wantErr=%v", err, test.wantErr)
				}
				if !test.wantErr && len(values) != 2 && test.name == "values" {
					t.Fatalf("values = %v", values)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestDemoErrorAndValueHelpers(t *testing.T) {
	if demoDependencyError(nil) != nil {
		t.Fatal("nil dependency error not nil")
	}
	if demoDependencyError(context.Canceled) != context.Canceled || demoDependencyError(context.DeadlineExceeded) != context.DeadlineExceeded {
		t.Fatal("context error was not preserved")
	}
	if !errors.Is(demoDependencyError(errors.New("secret")), ErrDemoInitialization) {
		t.Fatal("dependency error not wrapped")
	}
	if demoStepError("step", nil) != nil || demoStepError("step", context.Canceled) != context.Canceled || demoStepError("step", ErrDemoState) != ErrDemoState {
		t.Fatal("step passthrough failed")
	}
	if !strings.Contains(demoStepError("model", errors.New("bad")).Error(), "model") {
		t.Fatal("step context missing")
	}
	if cloneInt64Pointer(nil) != nil {
		t.Fatal("nil pointer clone not nil")
	}
	value := int64(4)
	clone := cloneInt64Pointer(&value)
	if clone == nil || *clone != value || clone == &value {
		t.Fatal("pointer not cloned")
	}
	if !emptyModelGeneration(modelprofile.GenerationConfig{}) || !emptyAgentGeneration(appmodel.GenerationConfig{}) {
		t.Fatal("empty generation not detected")
	}
	temperature := 0.2
	if emptyModelGeneration(modelprofile.GenerationConfig{Temperature: &temperature}) {
		t.Fatal("model generation incorrectly empty")
	}
	if demoRevisionMatches(nil, "model") {
		t.Fatal("nil revision should not match")
	}
	revision := &appmodel.Revision{Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "model", Runtime: appmodel.DefaultRuntimePolicy()}
	if !demoRevisionMatches(revision, "model") {
		t.Fatal("valid revision did not match")
	}
	revision.Tools = []appmodel.ToolAuthorization{{ToolID: "tool"}}
	if demoRevisionMatches(revision, "model") {
		t.Fatal("tool revision incorrectly matched")
	}
}

func TestEnsureDemoModelCreateBranches(t *testing.T) {
	t.Run("model create and dependency failures", func(t *testing.T) {
		profile := &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusActive}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
		id, created, err := ensureDemoModel(context.Background(), db, &demoModelRepoStub{profile: profile}, testInitTenantID, demoModelProfileKey)
		if err != nil || !created || id != profile.ProfileID {
			t.Fatalf("create = id:%s created:%v err:%v", id, created, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db, mock, _ = sqlmock.New()
		defer db.Close()
		mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
		if _, _, err := ensureDemoModel(context.Background(), db, &demoModelRepoStub{profile: profile, createErr: sql.ErrConnDone}, testInitTenantID, demoModelProfileKey); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("create error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}

		db, mock, _ = sqlmock.New()
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
		if _, _, err := ensureDemoModel(context.Background(), db, &demoModelRepoStub{profile: profile}, testInitTenantID, demoModelProfileKey); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

}

func TestEnsureDemoModelExistingBranches(t *testing.T) {
	t.Run("model existing lifecycle and mismatch", func(t *testing.T) {
		cases := []struct {
			name                  string
			profile               *modelprofile.Profile
			getErr, transitionErr error
			want                  error
			wantActive            bool
		}{
			{name: "active", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusActive}},
			{name: "suspended", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusSuspended}, wantActive: true},
			{name: "disabled", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusDisabled}, want: ErrDemoState},
			{name: "mismatch", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: "other", Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusActive}, want: ErrDemoState},
			{name: "get error", profile: &modelprofile.Profile{ProfileID: "mp_demo"}, getErr: sql.ErrConnDone, want: ErrDemoInitialization},
			{name: "transition error", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusSuspended}, transitionErr: sql.ErrConnDone, want: ErrDemoInitialization},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"))
				repo := &demoModelRepoStub{profile: test.profile, getErr: test.getErr, transitionErr: test.transitionErr}
				id, _, err := ensureDemoModel(context.Background(), db, repo, testInitTenantID, demoModelProfileKey)
				if test.want != nil && !errors.Is(err, test.want) {
					t.Fatalf("error = %v, want %v", err, test.want)
				}
				if test.want == nil && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if test.want == nil && id != "mp_demo" {
					t.Fatalf("id = %s", id)
				}
				if test.wantActive && repo.profile.Status != modelprofile.StatusActive {
					t.Fatalf("status = %s", repo.profile.Status)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestEnsureDemoBackendBranches(t *testing.T) {
	binding := backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "inmemory"}
	profile := &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusActive}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
	id, created, err := ensureDemoBackend(context.Background(), db, &demoBackendRepoStub{profile: profile}, testInitTenantID, demoBackendProfileKey)
	if err != nil || !created || id != profile.ProfileID {
		t.Fatalf("create = id:%s created:%v err:%v", id, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
	if _, _, err := ensureDemoBackend(context.Background(), db, &demoBackendRepoStub{profile: profile, createErr: sql.ErrConnDone}, testInitTenantID, demoBackendProfileKey); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("create error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	for _, status := range []backend.Status{backend.StatusActive, backend.StatusSuspended, backend.StatusDisabled} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("bp_demo"))
		repo := &demoBackendRepoStub{profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: status}}
		_, _, err = ensureDemoBackend(context.Background(), db, repo, testInitTenantID, demoBackendProfileKey)
		if status == backend.StatusDisabled && !errors.Is(err, ErrDemoState) {
			t.Fatalf("disabled error = %v", err)
		}
		if status != backend.StatusDisabled && err != nil {
			t.Fatalf("status %s error = %v", status, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name string
		repo *demoBackendRepoStub
		want error
	}{
		{name: "mismatch", repo: &demoBackendRepoStub{profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: "other", Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusActive}}, want: ErrDemoState},
		{name: "get error", repo: &demoBackendRepoStub{profile: profile, getErr: sql.ErrConnDone}, want: ErrDemoInitialization},
		{name: "transition error", repo: &demoBackendRepoStub{profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusSuspended}, transitionErr: sql.ErrConnDone}, want: ErrDemoInitialization},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("bp_demo"))
			_, _, err = ensureDemoBackend(context.Background(), db, test.repo, testInitTenantID, demoBackendProfileKey)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitializeDemoEarlyReturns(t *testing.T) {
	if _, err := InitializeDemo(nil, nil, DefaultDemoConfig()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InitializeDemo(ctx, nil, DefaultDemoConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if _, err := InitializeDemo(context.Background(), nil, DefaultDemoConfig()); !errors.Is(err, ErrDemoInitialization) {
		t.Fatalf("nil db error = %v", err)
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	invalid := DefaultDemoConfig()
	invalid.TenantKey = "bad key"
	if _, err := InitializeDemo(context.Background(), db, invalid); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", err)
	}
}

func TestLoadDemoRootAppBranches(t *testing.T) {
	ctx := context.Background()
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	root, err := tenants.Create(ctx, tenant.CreateInput{TenantKey: "demo", DisplayName: "Demo"})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Create(ctx, appmodel.CreateInput{TenantID: root.TenantID, AppKey: "assistant", DisplayName: "Assistant"})
	if err != nil {
		t.Fatal(err)
	}
	initial := InitResult{TenantID: root.TenantID, AppID: app.AppID}
	config := DefaultDemoConfig()
	if gotRoot, gotApp, err := loadDemoRootApp(ctx, tenants, apps, initial, config); err != nil || gotRoot.TenantID != root.TenantID || gotApp.AppID != app.AppID {
		t.Fatalf("valid root/app = %+v %+v, err=%v", gotRoot, gotApp, err)
	}
	if _, _, err := loadDemoRootApp(ctx, tenants, apps, InitResult{TenantID: "missing", AppID: app.AppID}, config); err == nil {
		t.Fatal("missing tenant accepted")
	}
	if _, _, err := loadDemoRootApp(ctx, tenants, apps, InitResult{TenantID: root.TenantID, AppID: "missing"}, config); err == nil {
		t.Fatal("missing app accepted")
	}
	wrong := config
	wrong.TenantKey = "other"
	if _, _, err := loadDemoRootApp(ctx, tenants, apps, initial, wrong); !errors.Is(err, ErrDemoState) {
		t.Fatalf("key mismatch error = %v", err)
	}
	suspendedTenants := tenantmemory.NewRepository()
	suspendedApps := appmemory.NewRepository()
	suspendedRoot, err := suspendedTenants.Create(ctx, tenant.CreateInput{TenantKey: demoTenantKey, DisplayName: "Paused", Status: tenant.StatusSuspended})
	if err != nil {
		t.Fatal(err)
	}
	suspendedApp, err := suspendedApps.Create(ctx, appmodel.CreateInput{TenantID: suspendedRoot.TenantID, AppKey: "assistant", DisplayName: "Assistant"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadDemoRootApp(ctx, suspendedTenants, suspendedApps, InitResult{TenantID: suspendedRoot.TenantID, AppID: suspendedApp.AppID}, config); !errors.Is(err, ErrDemoState) {
		t.Fatalf("inactive tenant error = %v", err)
	}
}

func TestPreflightDemoAppBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	app.CanaryRevision = new(int64)
	if err := preflightDemoApp(context.Background(), db, nil, root, app, demoModelProfileKey); !errors.Is(err, ErrDemoState) {
		t.Fatalf("canary error = %v", err)
	}
	app.CanaryRevision = nil
	app.Status = appmodel.StatusActive
	mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
	if err := preflightDemoApp(context.Background(), db, nil, root, app, demoModelProfileKey); !errors.Is(err, ErrDemoState) {
		t.Fatalf("active without revision error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	app.Status = appmodel.StatusDraft
	mock2.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).AddRow(int64(2)))
	if err := preflightDemoApp(context.Background(), db2, nil, root, app, demoModelProfileKey); !errors.Is(err, ErrDemoState) {
		t.Fatalf("multiple draft error = %v", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateExistingDemoRevisionBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID}
	valid := &appmodel.Revision{TenantID: root.TenantID, AppID: app.AppID, Revision: 1, State: appmodel.RevisionStatePublished, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
	for _, test := range []struct {
		name        string
		profileRows *sqlmock.Rows
		revision    *appmodel.Revision
		getErr      error
		want        error
	}{
		{name: "unknown model", profileRows: sqlmock.NewRows([]string{"profile_id"}), want: ErrDemoState},
		{name: "profile lookup error", profileRows: nil, want: ErrDemoInitialization},
		{name: "get revision error", profileRows: sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"), getErr: sql.ErrConnDone, want: ErrDemoInitialization},
		{name: "mismatch", profileRows: sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"), revision: &appmodel.Revision{State: appmodel.RevisionStatePublished}, want: ErrDemoState},
		{name: "valid", profileRows: sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"), revision: valid},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if test.name == "profile lookup error" {
				mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
			} else {
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(test.profileRows)
			}
			stub := &demoAgentRepoStub{revision: test.revision, getRevisionErr: test.getErr}
			err = validateExistingDemoRevision(context.Background(), db, stub, root, app, 1, appmodel.RevisionStatePublished, demoModelProfileKey)
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type failAfterFirstWriter struct{ writes int }

func (w *failAfterFirstWriter) Write(value []byte) (int, error) {
	if w.writes > 0 {
		return 0, errors.New("write failed")
	}
	w.writes++
	return len(value), nil
}

func TestWriteDemoResultOutputBranches(t *testing.T) {
	result := DemoResult{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackendProfileID: "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Revision: 1}
	var output strings.Builder
	if err := WriteDemoResult(&output, result); err != nil || !strings.Contains(output.String(), "found the existing") {
		t.Fatalf("existing output err=%v text=%q", err, output.String())
	}
	if err := WriteDemoResult(failingWriter{}, result); err == nil {
		t.Fatal("writer failure was ignored")
	}
	if err := WriteDemoResult(&failAfterFirstWriter{}, result); err == nil {
		t.Fatal("final output writer failure was ignored")
	}
	result.Created = true
	if err := WriteDemoResult(&failAfterFirstWriter{}, result); err == nil {
		t.Fatal("created output writer failure was ignored")
	}
	if err := WriteDemoResult(failingWriter{}, result); err == nil {
		t.Fatal("created output first write failure was ignored")
	}
}

func TestDemoProfilePreflightBranches(t *testing.T) {
	modelCases := []struct {
		name    string
		profile *modelprofile.Profile
		getErr  error
		want    error
	}{
		{name: "missing", want: nil},
		{name: "valid", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusActive}},
		{name: "suspended", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusSuspended}},
		{name: "disabled", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusDisabled}, want: ErrDemoState},
		{name: "mismatch", profile: &modelprofile.Profile{ProfileID: "mp_demo", ProfileKey: "other", Configuration: modelprofile.Configuration{Provider: "other", Model: "other"}, Status: modelprofile.StatusActive}, want: ErrDemoState},
		{name: "get error", profile: &modelprofile.Profile{ProfileID: "mp_demo"}, getErr: sql.ErrConnDone, want: ErrDemoInitialization},
	}
	for _, test := range modelCases {
		t.Run("model/"+test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			repo := &demoModelRepoStub{profile: test.profile, getErr: test.getErr}
			if test.name == "missing" {
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
			} else {
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"))
			}
			err = preflightDemoModel(context.Background(), db, repo, testInitTenantID, demoModelProfileKey)
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}

	binding := backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "inmemory"}
	backendCases := []struct {
		name    string
		profile *backend.Profile
		getErr  error
		want    error
	}{
		{name: "missing", want: nil},
		{name: "valid", profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusActive}},
		{name: "suspended", profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusSuspended}},
		{name: "disabled", profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{binding}, Status: backend.StatusDisabled}, want: ErrDemoState},
		{name: "mismatch", profile: &backend.Profile{ProfileID: "bp_demo", ProfileKey: "other", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "other"}}, Status: backend.StatusActive}, want: ErrDemoState},
		{name: "get error", profile: &backend.Profile{ProfileID: "bp_demo"}, getErr: sql.ErrConnDone, want: ErrDemoInitialization},
	}
	for _, test := range backendCases {
		t.Run("backend/"+test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			repo := &demoBackendRepoStub{profile: test.profile, getErr: test.getErr}
			if test.name == "missing" {
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
			} else {
				mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("bp_demo"))
			}
			err = preflightDemoBackend(context.Background(), db, repo, testInitTenantID, demoBackendProfileKey)
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEnsureDemoRevisionBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	modelID := "mp_demo"
	validRevision := &appmodel.Revision{TenantID: root.TenantID, AppID: testInitAppID, Revision: 1, DraftVersion: 1, State: appmodel.RevisionStateDraft, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: modelID, Runtime: appmodel.DefaultRuntimePolicy()}
	activeRevision := validRevision.Clone()
	activeRevision.State = appmodel.RevisionStatePublished

	t.Run("published existing", func(t *testing.T) {
		current := int64(1)
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusActive, CurrentRevision: &current}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		stub := &demoAgentRepoStub{app: app, revision: &activeRevision}
		gotApp, gotRevision, created, err := ensureDemoRevision(context.Background(), db, stub, root, app, modelID)
		if err != nil || created || gotApp != app || gotRevision != &activeRevision {
			t.Fatalf("published result = %+v %+v created=%v err=%v", gotApp, gotRevision, created, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("draft create and publish", func(t *testing.T) {
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft, Version: 1}
		draft := validRevision.Clone()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
		stub := &demoAgentRepoStub{app: app, revision: &draft}
		gotApp, gotRevision, created, err := ensureDemoRevision(context.Background(), db, stub, root, app, modelID)
		if err != nil || !created || gotRevision != &draft {
			t.Fatalf("draft result = %+v %+v created=%v err=%v", gotApp, gotRevision, created, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("existing draft", func(t *testing.T) {
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft, Version: 1}
		draft := validRevision.Clone()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		stub := &demoAgentRepoStub{app: app, revision: &draft}
		_, _, created, err := ensureDemoRevision(context.Background(), db, stub, root, app, modelID)
		if err != nil || created {
			t.Fatalf("existing draft result created=%v err=%v", created, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("dependency errors", func(t *testing.T) {
		current := int64(1)
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusActive, CurrentRevision: &current}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		if _, _, _, err := ensureDemoRevision(context.Background(), db, &demoAgentRepoStub{app: app, revision: &activeRevision}, root, app, modelID); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("revision lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestInitializeDemoRejectsControlPlaneFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
	if _, err := InitializeDemo(context.Background(), db, DefaultDemoConfig()); !errors.Is(err, ErrInitialization) {
		t.Fatalf("control-plane initialization error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeDemoBuildsRepositoriesAfterInitialization(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(testInitTenantID, testInitAppID))
	mock.ExpectRollback()
	mock.ExpectQuery("SELECT tenant_id, tenant_key").WithArgs(testInitTenantID).WillReturnError(sql.ErrConnDone)
	if _, err := InitializeDemo(context.Background(), db, DefaultDemoConfig()); err == nil {
		t.Fatal("repository lookup failure was ignored")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightDemoAppCompleteBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	config := DefaultDemoConfig()

	t.Run("revision lookup error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft}
		if err := preflightDemoApp(context.Background(), db, nil, root, app, config.ModelProfileKey); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("empty draft history", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft}
		if err := preflightDemoApp(context.Background(), db, nil, root, app, config.ModelProfileKey); err != nil {
			t.Fatalf("empty history error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("published revision validation", func(t *testing.T) {
		current := int64(1)
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusActive, CurrentRevision: &current}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"))
		valid := &appmodel.Revision{TenantID: root.TenantID, AppID: app.AppID, Revision: 1, State: appmodel.RevisionStatePublished, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
		if err := preflightDemoApp(context.Background(), db, &demoAgentRepoStub{revision: valid}, root, app, config.ModelProfileKey); err != nil {
			t.Fatalf("published validation error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("draft revision validation", func(t *testing.T) {
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("mp_demo"))
		valid := &appmodel.Revision{TenantID: root.TenantID, AppID: app.AppID, Revision: 1, State: appmodel.RevisionStateDraft, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
		if err := preflightDemoApp(context.Background(), db, &demoAgentRepoStub{revision: valid}, root, app, config.ModelProfileKey); err != nil {
			t.Fatalf("draft validation error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("current revision history mismatch", func(t *testing.T) {
		current := int64(1)
		app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusActive, CurrentRevision: &current}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(2)))
		if err := preflightDemoApp(context.Background(), db, nil, root, app, config.ModelProfileKey); !errors.Is(err, ErrDemoState) {
			t.Fatalf("history mismatch error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEnsureDemoPublishedRevisionBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	current := int64(1)
	newApp := func(status appmodel.Status) *appmodel.App {
		return &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: status, CurrentRevision: &current, Version: 2}
	}
	valid := &appmodel.Revision{TenantID: root.TenantID, AppID: testInitAppID, Revision: 1, State: appmodel.RevisionStatePublished, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}

	t.Run("revision lookup failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		if _, _, _, err := ensureDemoPublishedRevision(context.Background(), db, nil, root, newApp(appmodel.StatusActive), "mp_demo", demoAgentMetadata()); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("history mismatch", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).AddRow(int64(2)))
		if _, _, _, err := ensureDemoPublishedRevision(context.Background(), db, nil, root, newApp(appmodel.StatusActive), "mp_demo", demoAgentMetadata()); !errors.Is(err, ErrDemoState) {
			t.Fatalf("history mismatch error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("revision dependency failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		if _, _, _, err := ensureDemoPublishedRevision(context.Background(), db, &demoAgentRepoStub{getRevisionErr: sql.ErrConnDone}, root, newApp(appmodel.StatusActive), "mp_demo", demoAgentMetadata()); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("revision dependency error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("graph mismatch", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		mismatch := valid.Clone()
		mismatch.State = appmodel.RevisionStateDraft
		if _, _, _, err := ensureDemoPublishedRevision(context.Background(), db, &demoAgentRepoStub{revision: &mismatch}, root, newApp(appmodel.StatusActive), "mp_demo", demoAgentMetadata()); !errors.Is(err, ErrDemoState) {
			t.Fatalf("graph mismatch error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEnsureDemoPublishedRevisionLifecycle(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	current := int64(1)
	newApp := func(status appmodel.Status) *appmodel.App {
		return &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: status, CurrentRevision: &current, Version: 2}
	}
	valid := &appmodel.Revision{TenantID: root.TenantID, AppID: testInitAppID, Revision: 1, State: appmodel.RevisionStatePublished, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
	t.Run("suspended app resumes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		app := newApp(appmodel.StatusSuspended)
		stub := &demoAgentRepoStub{app: app, revision: valid}
		got, revision, created, err := ensureDemoPublishedRevision(context.Background(), db, stub, root, app, "mp_demo", demoAgentMetadata())
		if err != nil || created || revision != valid || got == app || got.Status != appmodel.StatusActive {
			t.Fatalf("resumed result = %+v revision=%+v created=%v err=%v", got, revision, created, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("resume failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		stub := &demoAgentRepoStub{app: newApp(appmodel.StatusSuspended), revision: valid, transitionErr: sql.ErrConnDone}
		if _, _, _, err := ensureDemoPublishedRevision(context.Background(), db, stub, root, stub.app, "mp_demo", demoAgentMetadata()); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("resume failure = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEnsureDemoDraftRevisionBranches(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	metadata := demoAgentMetadata()
	newApp := func() *appmodel.App {
		return &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft, Version: 1}
	}
	valid := &appmodel.Revision{TenantID: root.TenantID, AppID: testInitAppID, Revision: 1, DraftVersion: 1, State: appmodel.RevisionStateDraft, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}

	t.Run("revision lookup failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, nil, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("create failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
		stub := &demoAgentRepoStub{createDraftErr: sql.ErrConnDone}
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, stub, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("create error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("multiple revisions", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).AddRow(int64(2)))
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, nil, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoState) {
			t.Fatalf("multiple revision error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("existing revision dependency failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		stub := &demoAgentRepoStub{getRevisionErr: sql.ErrConnDone}
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, stub, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("get revision error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("existing revision mismatch", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		mismatch := valid.Clone()
		mismatch.ModelProfileID = "other"
		stub := &demoAgentRepoStub{revision: &mismatch}
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, stub, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoState) {
			t.Fatalf("mismatch error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEnsureDemoDraftRevisionLifecycleFailures(t *testing.T) {
	root := &tenant.Tenant{TenantID: testInitTenantID, Status: tenant.StatusActive}
	metadata := demoAgentMetadata()
	newApp := func() *appmodel.App {
		return &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, Status: appmodel.StatusDraft, Version: 1}
	}
	valid := &appmodel.Revision{TenantID: root.TenantID, AppID: testInitAppID, Revision: 1, DraftVersion: 1, State: appmodel.RevisionStateDraft, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
	t.Run("app reload failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
		stub := &demoAgentRepoStub{revision: valid, getErr: sql.ErrConnDone}
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, stub, root, newApp(), "mp_demo", metadata); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("reload error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("publish failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
		stub := &demoAgentRepoStub{app: newApp(), revision: valid, publishErr: sql.ErrConnDone}
		if _, _, _, err := ensureDemoDraftRevision(context.Background(), db, stub, root, stub.app, "mp_demo", metadata); !errors.Is(err, ErrDemoInitialization) {
			t.Fatalf("publish error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func newDemoGraphFixture() (DemoConfig, InitResult, *demoTenantRepoStub, *demoAgentRepoStub, *demoModelRepoStub, *demoBackendRepoStub) {
	root := &tenant.Tenant{TenantID: testInitTenantID, TenantKey: demoTenantKey, Status: tenant.StatusActive, Version: 1}
	app := &appmodel.App{TenantID: root.TenantID, AppID: testInitAppID, AppKey: demoAppKey, Status: appmodel.StatusDraft, Version: 1}
	revision := &appmodel.Revision{TenantID: root.TenantID, AppID: app.AppID, Revision: 1, DraftVersion: 1, State: appmodel.RevisionStateDraft, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: demoInstruction, ModelProfileID: "mp_demo", Runtime: appmodel.DefaultRuntimePolicy()}
	model := &modelprofile.Profile{TenantID: root.TenantID, ProfileID: "mp_demo", ProfileKey: demoModelProfileKey, Configuration: modelprofile.Configuration{Provider: demoModelProvider, Model: demoModelName}, Status: modelprofile.StatusActive}
	backendProfile := &backend.Profile{TenantID: root.TenantID, ProfileID: "bp_demo", ProfileKey: demoBackendProfileKey, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Status: backend.StatusActive}
	return DefaultDemoConfig(), InitResult{TenantID: root.TenantID, AppID: app.AppID, Created: true},
		&demoTenantRepoStub{root: root}, &demoAgentRepoStub{app: app, revision: revision},
		&demoModelRepoStub{profile: model}, &demoBackendRepoStub{profile: backendProfile}
}

func expectEmptyDemoRevision(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT revision").WillReturnRows(sqlmock.NewRows([]string{"revision"}))
}

func expectEmptyDemoProfile(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT profile_id").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}))
}

func expectDemoGraphPreflight(mock sqlmock.Sqlmock) {
	expectEmptyDemoRevision(mock)
	expectEmptyDemoProfile(mock)
	expectEmptyDemoProfile(mock)
}

func TestInitializeDemoGraphSuccess(t *testing.T) {
	config, initial, tenants, apps, models, backends := newDemoGraphFixture()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectDemoGraphPreflight(mock)
	expectEmptyDemoProfile(mock)
	expectEmptyDemoProfile(mock)
	expectEmptyDemoRevision(mock)
	result, err := initializeDemoGraph(demoGraphDependencies{
		ctx: context.Background(), db: db, config: config, initial: initial,
		tenantRepo: tenants, appRepo: apps, modelRepo: models, backendRepo: backends,
	})
	if err != nil {
		t.Fatalf("graph error = %v", err)
	}
	if result.TenantID != initial.TenantID || result.AppID != initial.AppID || result.ModelProfileID != "mp_demo" || result.BackendProfileID != "bp_demo" || result.Revision != 1 || !result.Created {
		t.Fatalf("graph result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeDemoGraphLoadFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		setErr func(*demoTenantRepoStub, *demoAgentRepoStub)
	}{{
		name: "tenant", setErr: func(tenants *demoTenantRepoStub, _ *demoAgentRepoStub) { tenants.getErr = sql.ErrConnDone },
	}, {
		name: "app", setErr: func(_ *demoTenantRepoStub, apps *demoAgentRepoStub) { apps.getErr = sql.ErrConnDone },
	}} {
		t.Run(test.name, func(t *testing.T) {
			config, initial, tenants, apps, models, backends := newDemoGraphFixture()
			test.setErr(tenants, apps)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := initializeDemoGraph(demoGraphDependencies{
				ctx: context.Background(), db: db, config: config, initial: initial,
				tenantRepo: tenants, appRepo: apps, modelRepo: models, backendRepo: backends,
			}); !errors.Is(err, sql.ErrConnDone) {
				t.Fatalf("load error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitializeDemoGraphPreflightFailures(t *testing.T) {
	for _, test := range []struct {
		name       string
		expectFunc func(sqlmock.Sqlmock)
	}{{
		name: "app", expectFunc: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		},
	}, {
		name: "model", expectFunc: func(mock sqlmock.Sqlmock) {
			expectEmptyDemoRevision(mock)
			mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
		},
	}, {
		name: "backend", expectFunc: func(mock sqlmock.Sqlmock) {
			expectEmptyDemoRevision(mock)
			expectEmptyDemoProfile(mock)
			mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
		},
	}} {
		t.Run(test.name, func(t *testing.T) {
			config, initial, tenants, apps, models, backends := newDemoGraphFixture()
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			test.expectFunc(mock)
			if _, err := initializeDemoGraph(demoGraphDependencies{
				ctx: context.Background(), db: db, config: config, initial: initial,
				tenantRepo: tenants, appRepo: apps, modelRepo: models, backendRepo: backends,
			}); !errors.Is(err, ErrDemoInitialization) {
				t.Fatalf("preflight error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitializeDemoGraphEnsureFailures(t *testing.T) {
	for _, test := range []struct {
		name       string
		configure  func(*demoTenantRepoStub, *demoAgentRepoStub, *demoModelRepoStub, *demoBackendRepoStub)
		expectFunc func(sqlmock.Sqlmock)
	}{{
		name: "model lookup", configure: func(_ *demoTenantRepoStub, _ *demoAgentRepoStub, _ *demoModelRepoStub, _ *demoBackendRepoStub) {}, expectFunc: func(mock sqlmock.Sqlmock) {
			expectDemoGraphPreflight(mock)
			mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
		},
	}, {
		name: "backend lookup", configure: func(_ *demoTenantRepoStub, _ *demoAgentRepoStub, _ *demoModelRepoStub, _ *demoBackendRepoStub) {}, expectFunc: func(mock sqlmock.Sqlmock) {
			expectDemoGraphPreflight(mock)
			expectEmptyDemoProfile(mock)
			mock.ExpectQuery("SELECT profile_id").WillReturnError(sql.ErrConnDone)
		},
	}, {
		name: "revision lookup", configure: func(_ *demoTenantRepoStub, _ *demoAgentRepoStub, _ *demoModelRepoStub, _ *demoBackendRepoStub) {}, expectFunc: func(mock sqlmock.Sqlmock) {
			expectDemoGraphPreflight(mock)
			expectEmptyDemoProfile(mock)
			expectEmptyDemoProfile(mock)
			mock.ExpectQuery("SELECT revision").WillReturnError(sql.ErrConnDone)
		},
	}, {
		name: "tenant defaults", configure: func(tenants *demoTenantRepoStub, _ *demoAgentRepoStub, _ *demoModelRepoStub, _ *demoBackendRepoStub) {
			tenants.updateErr = sql.ErrConnDone
		}, expectFunc: func(mock sqlmock.Sqlmock) {
			expectDemoGraphPreflight(mock)
			expectEmptyDemoProfile(mock)
			expectEmptyDemoProfile(mock)
			expectEmptyDemoRevision(mock)
		},
	}} {
		t.Run(test.name, func(t *testing.T) {
			config, initial, tenants, apps, models, backends := newDemoGraphFixture()
			test.configure(tenants, apps, models, backends)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			test.expectFunc(mock)
			if _, err := initializeDemoGraph(demoGraphDependencies{
				ctx: context.Background(), db: db, config: config, initial: initial,
				tenantRepo: tenants, appRepo: apps, modelRepo: models, backendRepo: backends,
			}); !errors.Is(err, ErrDemoInitialization) {
				t.Fatalf("ensure error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
