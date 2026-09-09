package config

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestControlPlaneDefaultsAndRoleScopedAdminToken(t *testing.T) {
	db := 4
	raw := &controlPlaneConfigFile{
		RedisEndpoint: "redis://control.example:6379", RedisCredentialRef: "env:CONTROL_REDIS",
		LogicalDB: &db, KeyPrefix: "phase6-control", AdminTokenRef: "env:ADMIN_TOKEN",
	}
	resolver := NewStaticCredentialResolver(map[string]string{
		"env:CONTROL_REDIS": "redis://control-user:control-secret@control.example:6379/4",
		"env:ADMIN_TOKEN":   "platform-admin-secret",
	})
	workerConfig, err := parseControlPlaneFile(raw, resolver, RoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	if workerConfig.AdminToken != "" {
		t.Fatal("worker resolved the platform administrator token")
	}
	serveConfig, err := parseControlPlaneFile(raw, resolver, RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if serveConfig.AdminToken != "platform-admin-secret" || serveConfig.PolicyCacheTTL != 5*time.Minute ||
		serveConfig.AuditRetention != 30*24*time.Hour || serveConfig.MetricRetention != 7*24*time.Hour ||
		serveConfig.NodeHeartbeatInterval != 10*time.Second || serveConfig.NodeOfflineAfter != 30*time.Second ||
		serveConfig.NodeAssignmentWaitBackoff != 250*time.Millisecond {
		t.Fatalf("unexpected control plane defaults: %#v", serveConfig)
	}
	formatted := fmt.Sprintf("%v %#v", serveConfig, serveConfig)
	for _, secret := range []string{"control.example", "control-user", "control-secret", "CONTROL_REDIS", "ADMIN_TOKEN", "platform-admin-secret"} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("formatted control plane config leaked %q: %s", secret, formatted)
		}
	}
}

func TestControlPlaneFeatureFlagsAreBound(t *testing.T) {
	base := validControlPlaneConfigForTest()
	base.TraceDigestV2Enabled = true
	if err := base.Validate(); err == nil {
		t.Fatal("trace-only control plane configuration unexpectedly passed")
	}
	base.NodeAssignmentEnabled = true
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	base.TraceDigestV2Enabled = false
	if err := base.Validate(); err == nil {
		t.Fatal("assignment-only control plane configuration unexpectedly passed")
	}
}

func TestControlPlaneIsolationProductionAndDevelopment(t *testing.T) {
	control := validControlPlaneConfigForTest()
	messaging := MessagingConfig{RedisURL: "redis://localhost:6379/0", KeyPrefix: "messaging"}
	if err := validateControlPlaneIsolation(&control, &messaging, tenant.Catalog{}, NewStaticCredentialResolver(nil), RoleGateway); err == nil {
		t.Fatal("production configuration reused the messaging Redis endpoint")
	}
	control.DevelopmentAllowSharedRedis = true
	if err := validateControlPlaneIsolation(&control, &messaging, tenant.Catalog{}, NewStaticCredentialResolver(nil), RoleGateway); err != nil {
		t.Fatalf("development configuration with a distinct DB was rejected: %v", err)
	}
	messaging.RedisURL = "redis://localhost:6379/4"
	if err := validateControlPlaneIsolation(&control, &messaging, tenant.Catalog{}, NewStaticCredentialResolver(nil), RoleGateway); err == nil {
		t.Fatal("development configuration reused the messaging logical DB")
	}
}

func TestControlPlaneIsolationChecksTenantRedisForWorker(t *testing.T) {
	control := validControlPlaneConfigForTest()
	control.DevelopmentAllowSharedRedis = true
	catalog := tenant.Catalog{StorageProfiles: []tenant.StorageProfile{{
		TenantID: "tenant", ID: "redis", Kind: tenant.StorageKindRedis,
		CredentialRef: "env:TENANT_REDIS", KeyPrefix: "tenant-prefix",
	}}}
	resolver := NewStaticCredentialResolver(map[string]string{"env:TENANT_REDIS": "redis://localhost:6379/4"})
	if err := validateControlPlaneIsolation(&control, nil, catalog, resolver, RoleWorker); err == nil {
		t.Fatal("worker accepted a tenant Redis profile in the control plane DB")
	}
}

func TestControlPlaneStrictJSONAndLoad(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "redis://tenant.example:6379/0")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "redis://messaging.example:6379/0")
	t.Setenv("CONTROL_REDIS_URL", "redis://control-user:control-secret@control.example:6379/4")
	t.Setenv("CONTROL_ADMIN_TOKEN", "admin-secret")
	controlJSON := `"control_plane": {"redis_endpoint":"redis://control.example:6379","redis_credential_ref":"env:CONTROL_REDIS_URL","logical_db":4,"key_prefix":"phase6-control","admin_token_ref":"env:CONTROL_ADMIN_TOKEN"},`
	data := strings.Replace(validCatalogJSON(), `"messaging":`, controlJSON+`"messaging":`, 1)
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, data))
	cfg, err := LoadForRole(RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControlPlane == nil || cfg.ControlPlane.LogicalDB != 4 || cfg.ControlPlane.AdminToken != "admin-secret" {
		t.Fatalf("unexpected control plane config: %#v", cfg.ControlPlane)
	}
	bad := strings.Replace(data, `"key_prefix":"phase6-control"`, `"key_prefix":"phase6-control","unknown":true`, 1)
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, bad))
	if _, err := LoadForRole(RoleServe); err == nil {
		t.Fatal("unknown control plane field unexpectedly passed")
	}
}

func validControlPlaneConfigForTest() ControlPlaneConfig {
	return ControlPlaneConfig{
		RedisEndpoint: "redis://localhost:6379", RedisURL: "redis://localhost:6379/4", LogicalDB: 4,
		KeyPrefix: "phase6-control", PolicyCacheTTL: 5 * time.Minute, AuditRetention: 30 * 24 * time.Hour,
		MetricRetention: 7 * 24 * time.Hour, NodeHeartbeatInterval: 10 * time.Second,
		NodeOfflineAfter: 30 * time.Second, NodeAssignmentWaitBackoff: 250 * time.Millisecond,
	}
}
