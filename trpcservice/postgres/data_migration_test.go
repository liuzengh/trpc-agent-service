package postgres

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateSupportedDataMigration(t *testing.T) {
	base := tenant.BackendConfig{
		Session: tenant.BackendRef{
			Kind:     tenant.BackendRedis,
			Provider: "redis",
			Name:     "source-session",
		},
		Memory: tenant.BackendRef{
			Kind:     tenant.BackendExternal,
			Provider: "tencentdb",
			Name:     "memory",
		},
		Artifact: tenant.BackendRef{
			Kind:     tenant.BackendObject,
			Provider: "cos",
			Name:     "artifact",
		},
	}
	target := base.Clone()
	target.Session = tenant.BackendRef{
		Kind:     tenant.BackendSQL,
		Provider: "postgres",
		Name:     "target-session",
	}

	if err := validateSupportedDataMigration(base, target); err != nil {
		t.Fatalf("validate supported migration: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*tenant.BackendConfig, *tenant.BackendConfig)
		wantErr string
	}{
		{
			name: "memory change",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Memory.Name = "other-memory"
			},
			wantErr: "only supports session backend changes",
		},
		{
			name: "artifact change",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Artifact.Name = "other-artifact"
			},
			wantErr: "only supports session backend changes",
		},
		{
			name: "knowledge change",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Knowledge.Name = "other-knowledge"
			},
			wantErr: "only supports session backend changes",
		},
		{
			name: "source is not redis",
			mutate: func(source, _ *tenant.BackendConfig) {
				source.Session.Kind = tenant.BackendSQL
			},
			wantErr: "source session backend must use redis",
		},
		{
			name: "target is not postgres",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Session.Kind = tenant.BackendRedis
			},
			wantErr: "target session backend must use postgres",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, destination := base.Clone(), target.Clone()
			tt.mutate(&source, &destination)
			err := validateSupportedDataMigration(source, destination)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate migration error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateMigrationCutoverProgressRejectsPartialTarget(t *testing.T) {
	if err := validateMigrationCutoverProgress(3, 3, 2); err == nil {
		t.Fatal("partial target was allowed to cut over")
	}
	if err := validateMigrationCutoverProgress(3, 2, 3); err == nil {
		t.Fatal("partially copied target was allowed to cut over")
	}
	if err := validateMigrationCutoverProgress(3, 3, 3); err != nil {
		t.Fatalf("complete target was rejected: %v", err)
	}
}

func TestSameMigrationBehaviorIncludesGovernancePolicies(t *testing.T) {
	base := tenant.AppConfig{
		TenantID:         "tenant-a",
		AppID:            "app-a",
		Model:            tenant.ModelConfig{Provider: "openai", Model: "model-a"},
		Tools:            tenant.ToolPolicy{VisibleTools: []string{"todo_write"}},
		KnowledgeBaseIDs: []string{"kb-a"},
		SecretRefs:       []tenant.SecretRef{{Name: "model-key", Version: "v1"}},
		ChannelBinding:   []string{"binding-a"},
	}
	for _, test := range []struct {
		name   string
		mutate func(*tenant.AppConfig)
	}{
		{
			name: "im access",
			mutate: func(config *tenant.AppConfig) {
				config.IMAccess.AllowedUsers = []string{"user-a"}
			},
		},
		{
			name: "budget",
			mutate: func(config *tenant.AppConfig) {
				config.Budget.MaxTokensPerExecution = 1024
			},
		},
		{
			name: "audit",
			mutate: func(config *tenant.AppConfig) {
				config.Audit.Enabled = true
				config.Audit.RecordExecutions = true
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := base.Clone()
			test.mutate(&target)
			if sameMigrationBehavior(base, target) {
				t.Fatal("behavior change was accepted")
			}
		})
	}
}

func TestValidateMigrationCutoverConfigRechecksBehaviorAndDomain(t *testing.T) {
	baseBackend := tenant.BackendConfig{
		Session:   tenant.BackendRef{Kind: tenant.BackendRedis, Provider: "redis", Name: "session-source"},
		Memory:    tenant.BackendRef{Kind: tenant.BackendExternal, Provider: "memory", Name: "memory"},
		Knowledge: tenant.BackendRef{Kind: tenant.BackendVector, Provider: "qdrant", Name: "knowledge"},
		Artifact:  tenant.BackendRef{Kind: tenant.BackendObject, Provider: "cos", Name: "artifact"},
	}
	source := tenant.AppConfig{
		TenantID: "tenant-a", AppID: "app-a",
		Model:         tenant.ModelConfig{Provider: "openai", Model: "model-a"},
		BackendConfig: baseBackend,
	}
	target := source.Clone()
	target.BackendConfig.Session = tenant.BackendRef{Kind: tenant.BackendSQL, Provider: "postgres", Name: "session-target"}
	record := migration.Record{Domain: migration.DomainSession}
	if err := validateMigrationCutoverConfig(record, source, target); err != nil {
		t.Fatalf("valid session cutover config: %v", err)
	}

	behaviorChanged := target.Clone()
	behaviorChanged.Model.Model = "model-b"
	if err := validateMigrationCutoverConfig(record, source, behaviorChanged); err == nil {
		t.Fatal("behavior change was accepted at cutover")
	}

	backendChanged := target.Clone()
	backendChanged.BackendConfig.Knowledge.Name = "knowledge-target"
	if err := validateMigrationCutoverConfig(record, source, backendChanged); err == nil {
		t.Fatal("knowledge backend change was accepted by session cutover")
	}
}

func TestSameMigrationBehaviorAllowsBackendOnlyChange(t *testing.T) {
	base := tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "app-a",
		BackendConfig: tenant.BackendConfig{
			Name: "backend-a",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendRedis,
				Provider: "redis",
				Name:     "session-a",
			},
		},
	}
	target := base.Clone()
	target.Version = "v2"
	target.BackendConfig.Name = "backend-b"
	target.BackendConfig.Session = tenant.BackendRef{
		Kind:     tenant.BackendSQL,
		Provider: "postgres",
		Name:     "session-b",
	}
	if !sameMigrationBehavior(base, target) {
		t.Fatal("backend-only change was rejected")
	}
}
