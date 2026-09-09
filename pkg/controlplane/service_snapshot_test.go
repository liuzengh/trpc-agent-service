package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestControlPlaneClassifiesPostgresConstraintClasses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "unique constraint", err: &pq.Error{Code: "23505"}, want: ErrControlPlaneConflict},
		{name: "foreign key constraint", err: &pq.Error{Code: "23503"}, want: ErrControlPlaneConflict},
		{name: "data exception", err: &pq.Error{Code: "22001"}, want: ErrInvalidControlPlaneRequest},
		{name: "infrastructure failure", err: errors.New("connection reset"), want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := wrapControlPlaneDBError("operation", test.err)
			if test.want == nil {
				if errors.Is(got, ErrControlPlaneConflict) || errors.Is(got, ErrInvalidControlPlaneRequest) {
					t.Fatalf("infrastructure error misclassified: %v", got)
				}
				return
			}
			if !errors.Is(got, test.want) {
				t.Fatalf("classified error=%v, want sentinel=%v", got, test.want)
			}
		})
	}
}

func TestCreateVersionRequiresTrustedSnapshotPreparer(t *testing.T) {
	service := NewService(nil, nil)
	_, err := service.CreateVersion(
		context.Background(),
		"tenant-a",
		"support",
		"operator-a",
		validServiceSnapshot(),
	)
	if err == nil || !strings.Contains(err.Error(), "snapshot preparer") {
		t.Fatalf("CreateVersion bypassed trusted snapshot preparation: %v", err)
	}
}

func TestCreateVersionPropagatesSnapshotPreparationFailureBeforeDatabaseAccess(t *testing.T) {
	want := errors.New("operator model catalog rejected snapshot")
	called := false
	service := NewService(nil, func(_ context.Context, tenantID string, snapshot *VersionSnapshot) error {
		called = true
		if tenantID != "tenant-a" || snapshot == nil {
			t.Fatalf("preparer input tenant=%q snapshot=%v", tenantID, snapshot)
		}
		return want
	})

	_, err := service.CreateVersion(
		context.Background(),
		"tenant-a",
		"support",
		"operator-a",
		validServiceSnapshot(),
	)
	if !called || !errors.Is(err, want) {
		t.Fatalf("snapshot preparation called=%v err=%v", called, err)
	}
}

func TestCreateVersionRejectsSnapshotForAnotherAgentAppBeforeDatabaseAccess(t *testing.T) {
	service := NewService(nil, func(context.Context, string, *VersionSnapshot) error { return nil })

	_, err := service.CreateVersion(
		context.Background(),
		"tenant-a",
		"support",
		"operator-a",
		validServiceSnapshotForAgent("billing"),
	)
	if err == nil || !strings.Contains(err.Error(), "does not match agent app") {
		t.Fatalf("CreateVersion accepted a snapshot for another app: %v", err)
	}
}

func TestCreateVersionRejectsPreparerRebindingSnapshotBeforeDatabaseAccess(t *testing.T) {
	service := NewService(nil, func(_ context.Context, _ string, snapshot *VersionSnapshot) error {
		snapshot.Agent.Name = "billing"
		return nil
	})

	_, err := service.CreateVersion(
		context.Background(),
		"tenant-a",
		"support",
		"operator-a",
		validServiceSnapshot(),
	)
	if err == nil || !strings.Contains(err.Error(), "does not match agent app") {
		t.Fatalf("CreateVersion accepted a preparer-rebound snapshot: %v", err)
	}
}

func validServiceSnapshot() VersionSnapshot {
	return validServiceSnapshotForAgent("support")
}

func validServiceSnapshotForAgent(agentName string) VersionSnapshot {
	return VersionSnapshot{
		Agent: tenant.AgentConfig{Name: agentName, DefaultModel: "gpt-4", MaxLLMCalls: 1},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", MaxTokens: 1_000},
	}
}

func TestControlPlaneRejectsInvalidIdentifiersBeforeDatabaseAccess(t *testing.T) {
	service := NewService(nil, func(context.Context, string, *VersionSnapshot) error { return nil })
	tests := []struct {
		name string
		call func() error
	}{
		{"app too long", func() error {
			_, err := service.CreateApp(context.Background(), "tenant-a", strings.Repeat("a", tenant.MaxAgentAppNameBytes+1), "", "operator-a")
			return err
		}},
		{"app control character", func() error {
			_, err := service.CreateVersion(context.Background(), "tenant-a", "support\nadmin", "operator-a", validServiceSnapshot())
			return err
		}},
		{"invalid tenant", func() error {
			_, err := service.Deploy(context.Background(), "tenant a", "support", "version-1", "", 0, "operator-a")
			return err
		}},
		{"invalid actor", func() error {
			return service.PublishVersion(context.Background(), "tenant-a", "version-1", "operator\nforged")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || strings.Contains(err.Error(), "database") {
				t.Fatalf("identifier was not rejected before database access: %v", err)
			}
		})
	}
}

func TestPublishVersionRejectsLegacyDraftWithoutCurrentPreflightAndLeavesItDraft(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snapshot := validServiceSnapshot()
	snapshot.Agent.Type = tenant.AgentTypeGraph
	snapshot.Agent.Runtime = &tenant.AgentRuntimeConfig{
		Nodes: []tenant.AgentRuntimeNode{{Name: "answer"}},
		Entry: "answer",
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	preflightErr := errors.New("agent runtime factory unavailable")
	service := NewService(db, func(_ context.Context, tenantID string, received *VersionSnapshot) error {
		if tenantID != "tenant-a" || received == nil || received.Agent.Type != tenant.AgentTypeGraph {
			t.Fatalf("preflight input tenant=%q snapshot=%#v", tenantID, received)
		}
		return preflightErr
	})

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT status
		FROM tenants
		WHERE id=$1
		FOR UPDATE`)).
		WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT av.status, av.config_snapshot, av.config_hash, aa.name
		FROM agent_versions av
		JOIN agent_apps aa ON aa.id=av.agent_app_id
		WHERE av.id=$1 AND aa.tenant_id=$2 AND aa.status='active'
		FOR UPDATE OF av`)).
		WithArgs("version-1", "tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "config_snapshot", "config_hash", "name"}).
			AddRow("draft", encoded, hex.EncodeToString(digest[:]), "support"))
	mock.ExpectRollback()

	err = service.PublishVersion(context.Background(), "tenant-a", "version-1", "operator-a")
	if !errors.Is(err, preflightErr) {
		t.Fatalf("PublishVersion error=%v, want preflight rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishVersionRejectsPreflightThatRewritesImmutableDraft(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snapshot := validServiceSnapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	service := NewService(db, func(_ context.Context, _ string, received *VersionSnapshot) error {
		received.ModelCatalogRevision = "new-catalog"
		received.ModelContextWindow = 16_384
		return nil
	})

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT status
		FROM tenants
		WHERE id=$1
		FOR UPDATE`)).
		WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT av.status, av.config_snapshot, av.config_hash, aa.name
		FROM agent_versions av
		JOIN agent_apps aa ON aa.id=av.agent_app_id
		WHERE av.id=$1 AND aa.tenant_id=$2 AND aa.status='active'
		FOR UPDATE OF av`)).
		WithArgs("version-1", "tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "config_snapshot", "config_hash", "name"}).
			AddRow("draft", encoded, hex.EncodeToString(digest[:]), "support"))
	mock.ExpectRollback()

	err = service.PublishVersion(context.Background(), "tenant-a", "version-1", "operator-a")
	if !errors.Is(err, ErrInvalidControlPlaneRequest) || !strings.Contains(err.Error(), "no longer matches") {
		t.Fatalf("PublishVersion error=%v, want immutable draft rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeployRejectsTheSameVersionAsStableAndCanary(t *testing.T) {
	service := NewService(nil, nil)
	_, err := service.Deploy(context.Background(), "tenant-a", "support", "version-1", "version-1", 100, "operator-a")
	if !errors.Is(err, ErrInvalidControlPlaneRequest) || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("Deploy error=%v, want same-version rejection", err)
	}
}

func TestControlPlaneLifecycleWritesRejectInactiveTenant(t *testing.T) {
	activeTenantQuery := regexp.QuoteMeta(`
		SELECT status
		FROM tenants
		WHERE id=$1
		FOR UPDATE`)
	tests := []struct {
		name string
		call func(*Service) error
	}{
		{
			name: "create app",
			call: func(service *Service) error {
				_, err := service.CreateApp(context.Background(), "tenant-a", "support", "", "operator-a")
				return err
			},
		},
		{
			name: "create version",
			call: func(service *Service) error {
				_, err := service.CreateVersion(context.Background(), "tenant-a", "support", "operator-a", validServiceSnapshot())
				return err
			},
		},
		{
			name: "publish version",
			call: func(service *Service) error {
				return service.PublishVersion(context.Background(), "tenant-a", "version-1", "operator-a")
			},
		},
		{
			name: "deploy",
			call: func(service *Service) error {
				_, err := service.Deploy(context.Background(), "tenant-a", "support", "version-1", "", 0, "operator-a")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			prepare := func(context.Context, string, *VersionSnapshot) error { return nil }
			service := NewService(db, prepare)
			mock.ExpectBegin()
			mock.ExpectQuery(activeTenantQuery).
				WithArgs("tenant-a").
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("suspended"))
			mock.ExpectRollback()
			if err := test.call(service); !errors.Is(err, ErrTenantInactive) {
				t.Fatalf("error=%v, want ErrTenantInactive", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
