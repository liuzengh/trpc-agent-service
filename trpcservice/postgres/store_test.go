package postgres

import (
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNewRejectsNilPool(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("new store succeeded with a nil pool")
	}
}

func TestEmbeddedMigrationsAreOrdered(t *testing.T) {
	migrations, err := loadSchemaMigrations()
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no embedded migrations")
	}
	for i, migration := range migrations {
		if migration.version <= 0 || migration.name == "" || migration.contents == "" {
			t.Fatalf("migration %d is incomplete: %#v", i, migration)
		}
		if i > 0 && migrations[i-1].version >= migration.version {
			t.Fatalf(
				"migration versions are not increasing: %d then %d",
				migrations[i-1].version,
				migration.version,
			)
		}
	}
}

func TestAppConfigPersistenceDocumentRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  tenant.AppConfig
	}{
		{
			name: "nil optional values",
			cfg: tenant.AppConfig{
				TenantID: "tenant-a",
				AppID:    "support",
				Version:  "v1",
				Model: tenant.ModelConfig{
					Provider:  "openai",
					APIKeyRef: tenant.SecretRef{Name: "model-key"},
					Model:     "gpt-4.1-mini",
				},
				BackendConfig: tenant.BackendConfig{
					Name: "shared",
					Session: tenant.BackendRef{
						Kind:     tenant.BackendSQL,
						Provider: "postgres",
						Name:     "session-postgres",
					},
				},
			},
		},
		{
			name: "configured optional values",
			cfg: tenant.AppConfig{
				TenantID: "tenant-b",
				AppID:    "research",
				Version:  "v7",
				Model: tenant.ModelConfig{
					Provider:               "openai",
					Model:                  "gpt-4.1",
					APIKeyRef:              tenant.SecretRef{Name: "model-key", Version: "3"},
					Parameters:             map[string]string{"temperature": "0.2"},
					AttachmentCapabilities: tenant.ModelAttachmentCapabilities{Image: true, Audio: true},
				},
				Tools: tenant.ToolPolicy{
					VisibleTools:        []string{"search"},
					ExecutableTools:     []string{"search"},
					ReviewRequiredTools: []string{"search"},
				},
				IMAccess: tenant.IMAccessPolicy{
					AllowedUsers:         []string{"user-1"},
					AllowedConversations: []string{"conversation-1"},
				},
				Budget: tenant.BudgetPolicy{MaxTokensPerExecution: 4096},
				BackendConfig: tenant.BackendConfig{
					Name: "isolated",
					Session: tenant.BackendRef{
						Kind:     tenant.BackendSQL,
						Provider: "postgres",
						Name:     "tenant-postgres",
						SecretRef: tenant.SecretRef{
							Name: "session-dsn",
						},
						Options: map[string]string{"schema": "agent"},
					},
					Memory: tenant.BackendRef{
						Kind:     tenant.BackendRedis,
						Provider: "redis",
						Name:     "memory-redis",
					},
				},
				Audit: tenant.AuditPolicy{
					Enabled:       true,
					RetentionDays: 90,
					RedactPII:     true,
				},
				SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "3"}},
				ChannelBinding: []string{
					"wecom-support",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelConfig, toolPolicy, backendConfig, auditPolicy, secretRefs, bindings, knowledgeBaseIDs, err :=
				marshalAppConfig(tt.cfg)
			if err != nil {
				t.Fatalf("marshal app config: %v", err)
			}
			got, err := unmarshalAppConfig(
				tt.cfg.TenantID,
				tt.cfg.AppID,
				tt.cfg.Version,
				appConfigColumns{
					modelConfig:       modelConfig,
					toolPolicy:        toolPolicy,
					backendConfig:     backendConfig,
					auditPolicy:       auditPolicy,
					secretRefs:        secretRefs,
					channelBindingIDs: bindings,
					knowledgeBaseIDs:  knowledgeBaseIDs,
				},
			)
			if err != nil {
				t.Fatalf("unmarshal app config: %v", err)
			}
			if !reflect.DeepEqual(got, tt.cfg) {
				t.Fatalf("round-trip config = %#v, want %#v", got, tt.cfg)
			}
		})
	}
}
