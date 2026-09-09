package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPresetRepositoryResolvesActiveCatalog(t *testing.T) {
	catalog := testCatalog()
	catalog.StorageProfiles = append(catalog.StorageProfiles, StorageProfile{TenantID: "tenant-a", ID: "memory-v2", Kind: StorageKindInMemory})
	catalog.ConfigVersions = append(catalog.ConfigVersions, ConfigVersion{
		TenantID: "tenant-a", AgentAppID: "assistant", Version: "v2", StorageProfileID: "memory-v2",
		Instruction: "test v2", Model: catalog.ConfigVersions[0].Model,
	})
	repository, err := NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := repository.ResolveBinding(context.Background(), "demo", "binding-a")
	if err != nil {
		t.Fatal(err)
	}
	if binding.TenantID != "tenant-a" || binding.AgentAppID != "assistant" {
		t.Fatalf("unexpected binding: %#v", binding)
	}
	profiles, err := repository.ListActiveStorageProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || profiles[0].ID != "memory-v1" || profiles[1].ID != "memory-v2" {
		t.Fatalf("active profiles = %#v", profiles)
	}
	profiles[0].ID = "mutated"
	again, _ := repository.ListActiveStorageProfiles(context.Background())
	if again[0].ID != "memory-v1" {
		t.Fatal("repository returned mutable profile storage")
	}
}

func TestPresetRepositoryRejectsInvalidReferencesAndDuplicates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Catalog)
	}{
		{name: "duplicate tenant", mutate: func(c *Catalog) { c.Tenants = append(c.Tenants, c.Tenants[0]) }},
		{name: "unknown profile tenant", mutate: func(c *Catalog) { c.StorageProfiles[0].TenantID = "missing" }},
		{name: "unknown active version", mutate: func(c *Catalog) { c.AgentApps[0].ActiveConfigVersion = "v2" }},
		{name: "cross tenant profile", mutate: func(c *Catalog) { c.ConfigVersions[0].StorageProfileID = "missing" }},
		{name: "duplicate binding", mutate: func(c *Catalog) { c.ChannelBindings = append(c.ChannelBindings, c.ChannelBindings[0]) }},
		{name: "duplicate binding across channels", mutate: func(c *Catalog) {
			duplicate := c.ChannelBindings[0]
			duplicate.Channel = "telegram"
			c.ChannelBindings = append(c.ChannelBindings, duplicate)
		}},
		{name: "redis missing credential", mutate: func(c *Catalog) {
			c.StorageProfiles[0].Kind = StorageKindRedis
			c.StorageProfiles[0].KeyPrefix = "prefix"
		}},
		{name: "invalid id", mutate: func(c *Catalog) { c.Tenants[0].ID = "tenant/a" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := testCatalog()
			test.mutate(&catalog)
			if _, err := NewPresetRepository(catalog); err == nil {
				t.Fatal("expected catalog validation error")
			}
		})
	}
}

func TestChannelBindingCredentialContracts(t *testing.T) {
	tests := []struct {
		name    string
		binding ChannelBinding
	}{
		{name: "unsupported channel", binding: ChannelBinding{Channel: "unknown"}},
		{name: "demo with token", binding: ChannelBinding{Channel: "demo", CredentialRef: "env:TOKEN"}},
		{name: "telegram without token", binding: ChannelBinding{Channel: "telegram"}},
		{name: "telegram with wecom reference", binding: ChannelBinding{Channel: "telegram", CredentialRef: "env:TOKEN", BotIDRef: "env:BOT_ID"}},
		{name: "wecom missing secret", binding: ChannelBinding{Channel: "wecom_aibot", BotIDRef: "env:BOT_ID"}},
		{name: "wecom with telegram token", binding: ChannelBinding{Channel: "wecom_aibot", CredentialRef: "env:TOKEN", BotIDRef: "env:BOT_ID", BotSecretRef: "env:BOT_SECRET"}},
		{name: "feishu missing secret", binding: ChannelBinding{Channel: "feishu", BotIDRef: "env:APP_ID"}},
		{name: "feishu with generic token", binding: ChannelBinding{Channel: "feishu", CredentialRef: "env:TOKEN", BotIDRef: "env:APP_ID", BotSecretRef: "env:APP_SECRET"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := testCatalog()
			test.binding.ID = "binding-a"
			test.binding.ExternalAccountID = "account-a"
			test.binding.TenantID = "tenant-a"
			test.binding.AgentAppID = "assistant"
			test.binding.Enabled = true
			catalog.ChannelBindings[0] = test.binding
			if _, err := NewPresetRepository(catalog); err == nil {
				t.Fatal("invalid channel credential contract was accepted")
			}
		})
	}

	validFeishu := testCatalog()
	validFeishu.ChannelBindings[0] = ChannelBinding{
		ID: "feishu-a", Channel: "feishu", ExternalAccountID: "cli_app",
		BotIDRef: "env:APP_ID", BotSecretRef: "env:APP_SECRET",
		TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
	}
	if _, err := NewPresetRepository(validFeishu); err != nil {
		t.Fatalf("valid Feishu binding was rejected: %v", err)
	}

	catalog := testCatalog()
	catalog.ChannelBindings[0] = ChannelBinding{ID: "telegram-a", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TOKEN_A", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true}
	catalog.ChannelBindings = append(catalog.ChannelBindings, ChannelBinding{ID: "telegram-b", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TOKEN_B", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true})
	if _, err := NewPresetRepository(catalog); err == nil {
		t.Fatal("duplicate enabled external account was accepted")
	}
}

func TestPresetRepositoryHidesDisabledAndUnknownBindings(t *testing.T) {
	catalog := testCatalog()
	catalog.ChannelBindings[0].Enabled = false
	repository, err := NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, bindingID := range []string{"binding-a", "missing"} {
		if _, err := repository.ResolveBinding(context.Background(), "demo", bindingID); !errors.Is(err, ErrBindingNotFound) {
			t.Fatalf("ResolveBinding(%q) error = %v", bindingID, err)
		}
	}
}

func TestValidateIDAndAppName(t *testing.T) {
	if err := ValidateID("id", "safe-ID_1.0"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateID("id", "not safe"); err == nil {
		t.Fatal("expected invalid ID error")
	}
	if got := AppName("tenant-a", "assistant"); got != "tenant/tenant-a/app/assistant" {
		t.Fatalf("AppName() = %q", got)
	}
}

func TestNormalizeSQLStorageProfiles(t *testing.T) {
	postgres, err := NormalizeStorageProfile(StorageProfile{
		TenantID: "tenant-a", ID: "pg", Kind: StorageKindPostgres,
		CredentialRef: "env:PG_DSN", TablePrefix: "tenant_a", SkipDBInit: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if postgres.TablePrefix != "tenant_a_" || postgres.Schema != "public" || !postgres.SkipDBInit {
		t.Fatalf("normalized postgres profile = %#v", postgres)
	}
	mysql, err := NormalizeStorageProfile(StorageProfile{
		TenantID: "tenant-a", ID: "mysql", Kind: StorageKindMySQL,
		CredentialRef: "env:MYSQL_DSN", TablePrefix: "tenant_a_",
	})
	if err != nil || mysql.TablePrefix != "tenant_a_" || mysql.Schema != "" {
		t.Fatalf("normalized mysql profile = (%#v, %v)", mysql, err)
	}
}

func TestNormalizeStorageProfileRejectsCrossKindFields(t *testing.T) {
	tests := []StorageProfile{
		{ID: "redis", Kind: StorageKindRedis, CredentialRef: "env:REDIS", KeyPrefix: "x", TablePrefix: "sql"},
		{ID: "pg", Kind: StorageKindPostgres, CredentialRef: "env:PG", TablePrefix: "bad-prefix"},
		{ID: "mysql", Kind: StorageKindMySQL, CredentialRef: "env:MYSQL", TablePrefix: "ok", Schema: "public"},
		{ID: "memory", Kind: StorageKindInMemory, SkipDBInit: true},
	}
	for _, profile := range tests {
		if _, err := NormalizeStorageProfile(profile); err == nil {
			t.Fatalf("NormalizeStorageProfile(%#v) unexpectedly succeeded", profile)
		}
	}
}

func TestNormalizeSQLStorageProfileEnforcesOfficialIndexNameLimits(t *testing.T) {
	postgres := StorageProfile{ID: "pg", Kind: StorageKindPostgres, CredentialRef: "env:PG", Schema: "public", TablePrefix: strings.Repeat("a", 20) + "_"}
	if _, err := NormalizeStorageProfile(postgres); err != nil {
		t.Fatalf("PostgreSQL boundary prefix rejected: %v", err)
	}
	postgres.TablePrefix = strings.Repeat("a", 21) + "_"
	if _, err := NormalizeStorageProfile(postgres); err == nil {
		t.Fatal("PostgreSQL prefix producing a truncated official index was accepted")
	}
	mysql := StorageProfile{ID: "mysql", Kind: StorageKindMySQL, CredentialRef: "env:MYSQL", TablePrefix: strings.Repeat("a", 28) + "_"}
	if _, err := NormalizeStorageProfile(mysql); err != nil {
		t.Fatalf("MySQL boundary prefix rejected: %v", err)
	}
	mysql.TablePrefix = strings.Repeat("a", 29) + "_"
	if _, err := NormalizeStorageProfile(mysql); err == nil {
		t.Fatal("MySQL prefix producing a truncated official index was accepted")
	}
}

func testCatalog() Catalog {
	return Catalog{
		Tenants:         []Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []StorageProfile{{TenantID: "tenant-a", ID: "memory-v1", Kind: StorageKindInMemory}},
		AgentApps:       []AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []ConfigVersion{{
			TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1",
			Instruction: "test", Model: ModelConfig{
				Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL_KEY",
				RequestTimeout: time.Second, MaxOutputTokens: 128,
			},
		}},
		ChannelBindings: []ChannelBinding{{
			ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
}
