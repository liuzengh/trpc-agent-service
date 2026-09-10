package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPostgreSQLRepositoriesRejectTerminalAndInvalidDomainMutations(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)

	t.Run("tenant disabled update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewTenantRepository(db)
		current := mockTenant(t)
		current.Status = tenant.StatusDisabled
		if err := current.Validate(); err != nil {
			t.Fatal(err)
		}
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(current.TenantID).WillReturnRows(mockTenantRows(current))
		mock.ExpectRollback()
		_, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
			TenantID: current.TenantID, ExpectedVersion: current.Version, DisplayName: current.DisplayName,
			AuditRetentionDays: current.AuditRetentionDays, LogMaskingLevel: current.LogMaskingLevel, TraceSamplingRate: current.TraceSamplingRate,
		})
		assertDomainFailure(t, err, tenant.ErrDisabled, mock)
	})

	t.Run("tenant terminal and invalid transitions", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			status tenant.Status
			next   tenant.Status
			want   error
		}{
			{name: "disabled", status: tenant.StatusDisabled, next: tenant.StatusActive, want: tenant.ErrDisabled},
			{name: "active to active", status: tenant.StatusActive, next: tenant.StatusActive, want: tenant.ErrInvalidTransition},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewTenantRepository(db)
				current := mockTenant(t)
				current.Status = test.status
				if err := current.Validate(); err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				mock.ExpectQuery(".*").WithArgs(current.TenantID).WillReturnRows(mockTenantRows(current))
				mock.ExpectRollback()
				_, _, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{
					TenantID: current.TenantID, ExpectedVersion: current.Version, NextStatus: test.next, Metadata: mockTenantMetadata(),
				})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("model disabled and invalid transitions", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			status model.Status
			next   model.Status
			want   error
		}{
			{name: "disabled", status: model.StatusDisabled, next: model.StatusActive, want: model.ErrDisabled},
			{name: "active to active", status: model.StatusActive, next: model.StatusActive, want: model.ErrInvalidTransition},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewModelRepository(db, modelCatalog)
				current := mockModel(t, modelCatalog)
				current.Status = test.status
				if err := current.Validate(modelCatalog); err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				mock.ExpectQuery(".*").WithArgs(current.TenantID, current.ProfileID).WillReturnRows(mockModelRows(t, current))
				mock.ExpectRollback()
				_, _, err := repo.TransitionStatus(ctx, model.TransitionStatusInput{
					TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version, NextStatus: test.next, Metadata: mockModelMetadata(),
				})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("model disabled update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		current.Status = model.StatusDisabled
		if err := current.Validate(modelCatalog); err != nil {
			t.Fatal(err)
		}
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(current.TenantID, current.ProfileID).WillReturnRows(mockModelRows(t, current))
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: current.DisplayName, SchemaVersion: current.SchemaVersion, Configuration: current.Configuration, Metadata: mockModelMetadata(),
		})
		assertDomainFailure(t, err, model.ErrDisabled, mock)
	})

	t.Run("backend disabled and invalid transitions", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			status backend.Status
			next   backend.Status
			want   error
		}{
			{name: "disabled", status: backend.StatusDisabled, next: backend.StatusActive, want: backend.ErrDisabled},
			{name: "active to active", status: backend.StatusActive, next: backend.StatusActive, want: backend.ErrInvalidTransition},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewBackendRepository(db, backendCatalog)
				current := mockBackend(t, backendCatalog)
				current.Status = test.status
				if err := current.Validate(backendCatalog); err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				expectBackendProfile(mock, current)
				mock.ExpectRollback()
				_, _, err := repo.TransitionStatus(ctx, backend.TransitionStatusInput{
					TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version, NextStatus: test.next, Metadata: mockBackendMetadata(),
				})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("backend disabled update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewBackendRepository(db, backendCatalog)
		current := mockBackend(t, backendCatalog)
		current.Status = backend.StatusDisabled
		if err := current.Validate(backendCatalog); err != nil {
			t.Fatal(err)
		}
		mock.ExpectBegin()
		expectBackendProfile(mock, current)
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: current.DisplayName, SchemaVersion: current.SchemaVersion, Bindings: current.Bindings, Metadata: mockBackendMetadata(),
		})
		assertDomainFailure(t, err, backend.ErrDisabled, mock)
	})

	t.Run("channel disabled and invalid transitions", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			status channels.Status
			next   channels.Status
			want   error
		}{
			{name: "disabled", status: channels.StatusDisabled, next: channels.StatusActive, want: channels.ErrDisabled},
			{name: "active to active", status: channels.StatusActive, next: channels.StatusActive, want: channels.ErrInvalidTransition},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewChannelRepository(db)
				current := mockBinding(t, mockAppID)
				current.Status = test.status
				if err := current.Validate(); err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				mock.ExpectQuery(".*").WithArgs(current.TenantID, current.BindingID).WillReturnRows(mockBindingRows(current))
				mock.ExpectRollback()
				_, _, err := repo.TransitionStatus(ctx, channels.TransitionStatusInput{
					TenantID: current.TenantID, BindingID: current.BindingID, ExpectedVersion: current.Version, NextStatus: test.next, Metadata: mockChannelMetadata(),
				})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("channel disabled update and empty lookup", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, mockAppID)
		current.Status = channels.StatusDisabled
		if err := current.Validate(); err != nil {
			t.Fatal(err)
		}
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(current.TenantID, current.BindingID).WillReturnRows(mockBindingRows(current))
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{
			TenantID: current.TenantID, BindingID: current.BindingID, ExpectedVersion: current.Version,
			ProviderAccountID: current.ProviderAccountID, PublicRouteKeyDigest: current.PublicRouteKeyDigest,
			AppID: current.AppID, SecretRef: current.SecretRef, Protocol: current.Protocol, Metadata: mockChannelMetadata(),
		})
		assertDomainFailure(t, err, channels.ErrDisabled, mock)

		db, mock = newSQLMock(t)
		repo = NewChannelRepository(db)
		mock.ExpectQuery(".*").WithArgs(string(channels.ChannelTelegram), current.PublicRouteKeyDigest).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "binding_id", "version", "config_digest"}))
		if _, err := repo.LookupCandidates(ctx, channels.ChannelTelegram, current.PublicRouteKeyDigest); !errors.Is(err, channels.ErrCandidateUnavailable) {
			t.Fatalf("empty candidate lookup error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPostgreSQLAgentRepositoryDomainGuards(t *testing.T) {
	ctx := context.Background()
	draftApp := mockApp(t, 1)
	draft := mockRevision(t, draftApp, 2, false)
	published := mockRevision(t, draftApp, 1, true)

	t.Run("disabled app rejects mutable operations", func(t *testing.T) {
		disabled := mockApp(t, 1)
		disabled.Status = appmodel.StatusDisabled
		if err := disabled.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			name   string
			action func(*AgentRepository) error
		}{
			{name: "metadata", action: func(repo *AgentRepository) error {
				_, err := repo.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{TenantID: disabled.TenantID, AppID: disabled.AppID, ExpectedVersion: disabled.Version, DisplayName: disabled.DisplayName})
				return err
			}},
			{name: "draft", action: func(repo *AgentRepository) error {
				_, err := repo.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: disabled.TenantID, AppID: disabled.AppID, ExpectedAppVersion: disabled.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Configuration: appmodel.DraftConfiguration{Instruction: "disabled", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()}})
				return err
			}},
			{name: "draft update", action: func(repo *AgentRepository) error {
				_, err := repo.UpdateDraft(ctx, appmodel.UpdateDraftInput{TenantID: disabled.TenantID, AppID: disabled.AppID, Revision: 1, ExpectedAppVersion: disabled.Version, ExpectedDraftVersion: 1, Configuration: appmodel.DraftConfiguration{Instruction: "disabled", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()}})
				return err
			}},
			{name: "transition", action: func(repo *AgentRepository) error {
				_, _, err := repo.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: disabled.TenantID, AppID: disabled.AppID, ExpectedVersion: disabled.Version, NextStatus: appmodel.StatusActive, Metadata: mockAgentMetadata()})
				return err
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewAgentRepository(db)
				mock.ExpectBegin()
				expectAgentApp(mock, disabled)
				mock.ExpectRollback()
				assertDomainFailure(t, test.action(repo), appmodel.ErrDisabled, mock)
			})
		}
	})

	t.Run("draft mutation rejects immutable revision and stale version", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			revision *appmodel.Revision
			expected int64
			want     error
		}{
			{name: "published", revision: published, expected: published.DraftVersion, want: appmodel.ErrImmutableRevision},
			{name: "stale", revision: draft, expected: draft.DraftVersion + 1, want: appmodel.ErrConflict},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewAgentRepository(db)
				mock.ExpectBegin()
				expectAgentApp(mock, draftApp)
				expectAgentRevision(mock, test.revision)
				mock.ExpectRollback()
				_, err := repo.UpdateDraft(ctx, appmodel.UpdateDraftInput{
					TenantID: draftApp.TenantID, AppID: draftApp.AppID, Revision: test.revision.Revision,
					ExpectedAppVersion: draftApp.Version, ExpectedDraftVersion: test.expected,
					Configuration: appmodel.DraftConfiguration{Instruction: "updated", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()},
				})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("publication rejects inactive tenant and immutable revision", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		if _, _, _, err := repo.Publish(ctx, appmodel.PublishInput{TenantActive: false, Metadata: mockAgentMetadata()}); !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("inactive publication input error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}

		db, mock = newSQLMock(t)
		repo = NewAgentRepository(db)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("suspended"))
		mock.ExpectRollback()
		_, _, _, err := repo.Publish(ctx, appmodel.PublishInput{TenantID: draftApp.TenantID, AppID: draftApp.AppID, Revision: draft.Revision, ExpectedAppVersion: draftApp.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: mockAgentMetadata()})
		assertDomainFailure(t, err, appmodel.ErrInvalid, mock)

		db, mock = newSQLMock(t)
		repo = NewAgentRepository(db)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
		expectAgentApp(mock, draftApp)
		expectAgentRevision(mock, published)
		mock.ExpectRollback()
		_, _, _, err = repo.Publish(ctx, appmodel.PublishInput{TenantID: draftApp.TenantID, AppID: draftApp.AppID, Revision: published.Revision, ExpectedAppVersion: draftApp.Version, ExpectedDraftVersion: published.DraftVersion, TenantActive: true, Metadata: mockAgentMetadata()})
		assertDomainFailure(t, err, appmodel.ErrImmutableRevision, mock)
	})

	t.Run("rollback rejects draft and current targets", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			app    *appmodel.App
			target *appmodel.Revision
			want   error
		}{
			{name: "draft target", app: mockApp(t, 2), target: draft, want: appmodel.ErrInvalid},
			{name: "current target", app: mockApp(t, 1), target: published, want: appmodel.ErrInvalid},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				repo := NewAgentRepository(db)
				mock.ExpectBegin()
				expectAgentApp(mock, test.app)
				expectAgentRevision(mock, test.target)
				mock.ExpectRollback()
				_, _, err := repo.Rollback(ctx, appmodel.RollbackInput{TenantID: test.app.TenantID, AppID: test.app.AppID, TargetRevision: test.target.Revision, ExpectedAppVersion: test.app.Version, Metadata: mockAgentMetadata()})
				assertDomainFailure(t, err, test.want, mock)
			})
		}
	})

	t.Run("transition rejects self transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, draftApp)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: draftApp.TenantID, AppID: draftApp.AppID, ExpectedVersion: draftApp.Version, NextStatus: appmodel.StatusActive, Metadata: mockAgentMetadata()})
		assertDomainFailure(t, err, appmodel.ErrInvalidTransition, mock)
	})
}

func expectBackendProfile(mock sqlmock.Sqlmock, value *backend.Profile) {
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.ProfileID).WillReturnRows(mockBackendRootRows(value))
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.ProfileID).WillReturnRows(mockBackendBindingRows(value))
}

func assertDomainFailure(t *testing.T, err, want error, mock sqlmock.Sqlmock) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
