package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLRepositoryUpdateRejectsStorageRebinding(t *testing.T) {
	current := StorageConfig{
		SessionBackend: "redis", SessionProfile: "session-source",
		MemoryBackend: "postgres", MemoryProfile: "memory-source",
		KnowledgeBackend: "qdrant", KnowledgeProfile: "knowledge-source",
		ArtifactBackend: "s3", ArtifactProfile: "artifact-source",
	}
	tests := []struct {
		name   string
		change func(*StorageConfig)
	}{
		{"session backend", func(s *StorageConfig) { s.SessionBackend = "postgres" }},
		{"session profile", func(s *StorageConfig) { s.SessionProfile = "session-target" }},
		{"memory backend", func(s *StorageConfig) { s.MemoryBackend = "redis" }},
		{"memory profile", func(s *StorageConfig) { s.MemoryProfile = "memory-target" }},
		{"knowledge profile", func(s *StorageConfig) { s.KnowledgeProfile = "knowledge-target" }},
		{"artifact profile", func(s *StorageConfig) { s.ArtifactProfile = "artifact-target" }},
		{"disable session", func(s *StorageConfig) { s.SessionBackend, s.SessionProfile = "", "" }},
		{"disable memory", func(s *StorageConfig) { s.MemoryBackend, s.MemoryProfile = "", "" }},
		{"disable knowledge", func(s *StorageConfig) { s.KnowledgeBackend, s.KnowledgeProfile = "", "" }},
		{"disable artifact", func(s *StorageConfig) { s.ArtifactBackend, s.ArtifactProfile = "", "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			next := current
			test.change(&next)
			value := &Tenant{ID: "tenant-a", Name: "Acme", Status: TenantStatusActive, ConfigVersion: 8, Storage: next}
			config, err := json.Marshal(map[string]StorageConfig{"storage": current})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectBegin()
			mock.ExpectQuery(regexp.QuoteMeta("SELECT config FROM tenants WHERE id = $1 AND config_version = $2 AND status <> $3 FOR UPDATE")).
				WithArgs("tenant-a", int64(8), TenantStatusDeleted).
				WillReturnRows(sqlmock.NewRows([]string{"config"}).AddRow(config))
			mock.ExpectRollback()
			err = (&SQLRepository{db: db}).Update(ContextWithAuditActor(context.Background(), "operator"), value)
			if !errors.Is(err, ErrStorageBindingChange) || !errors.Is(err, ErrInvalidTenantConfig) {
				t.Fatalf("Update error = %v, want typed storage binding rejection", err)
			}
			if value.ConfigVersion != 8 || !value.UpdatedAt.IsZero() {
				t.Fatalf("rejected update published metadata: %+v", value)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLRepositoryUpdateAllowsUnchangedBindingsAndFirstConfiguration(t *testing.T) {
	for _, firstBinding := range []bool{false, true} {
		t.Run(map[bool]string{false: "policy update", true: "first binding"}[firstBinding], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			next := StorageConfig{SessionBackend: "redis", SessionProfile: "session", MemoryBackend: "postgres", MemoryProfile: "memory"}
			current := next
			if firstBinding {
				current = StorageConfig{}
			}
			next.SessionConfig = map[string]string{"session_ttl": "24h"}
			value := &Tenant{ID: "tenant-a", Name: "New name", Status: TenantStatusSuspended, ConfigVersion: 8, Storage: next}
			config, err := json.Marshal(map[string]StorageConfig{"storage": current})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectBegin()
			mock.ExpectQuery(regexp.QuoteMeta("SELECT config FROM tenants")).
				WithArgs("tenant-a", int64(8), TenantStatusDeleted).
				WillReturnRows(sqlmock.NewRows([]string{"config"}).AddRow(config))
			mock.ExpectQuery(regexp.QuoteMeta("UPDATE tenants")).
				WithArgs("New name", TenantStatusSuspended, sqlmock.AnyArg(), sqlmock.AnyArg(), "tenant-a", int64(8), TenantStatusDeleted).
				WillReturnRows(sqlmock.NewRows([]string{"config_version"}).AddRow(int64(9)))
			mock.ExpectExec(regexp.QuoteMeta("DELETE FROM tenant_channels WHERE tenant_id=$1")).
				WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta("INSERT INTO control_plane_audit")).
				WithArgs("tenant-a", "operator", "tenant.update", "tenant-a", sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			if err := (&SQLRepository{db: db}).Update(ContextWithAuditActor(context.Background(), "operator"), value); err != nil {
				t.Fatal(err)
			}
			if value.ConfigVersion != 9 || value.UpdatedAt.IsZero() {
				t.Fatalf("committed update did not publish metadata: %+v", value)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorageBindingUpdateCannotOverwriteCompletedMigration(t *testing.T) {
	current := StorageConfig{SessionBackend: "postgres", SessionProfile: "migration-target"}
	next := StorageConfig{SessionBackend: "postgres", SessionProfile: "unrelated-target"}
	if err := validateStorageBindingUpdate(current, next); !errors.Is(err, ErrStorageBindingChange) {
		t.Fatalf("completed migration binding was mutable: %v", err)
	}
	if err := validateStorageBindingUpdate(current, StorageConfig{}); !errors.Is(err, ErrStorageBindingChange) {
		t.Fatalf("completed migration binding could be disabled: %v", err)
	}
}
