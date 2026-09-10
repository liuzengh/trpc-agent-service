package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestLoadProductionConfigRequiresEveryCriticalDependency(t *testing.T) {
	values := validProductionEnvironment()
	config, err := loadProductionConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != ":8080" || config.RedisDB != 2 || !config.S3PathStyle ||
		config.ArtifactOrphanGrace != 48*time.Hour || config.ArtifactPutTimeout >= config.UploadProtection ||
		config.PayloadKeyRef != "secret://messaging/payload" || config.PayloadKeyVersion != 7 {
		t.Fatal("parsed production configuration mismatch")
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_REDIS_ADDRESS", "TRPC_SECRET_ROOT", "TRPC_S3_REGION", "TRPC_S3_BUCKET",
		"TRPC_CLAMAV_ADDRESS", "TRPC_DLP_ENDPOINT", "TRPC_DLP_PROBE_TENANT_ID", "TRPC_DLP_SECRET_REF",
		"TRPC_DLP_SECRET_VERSION", "TRPC_DLP_BACKEND_VERSION", "TRPC_PAYLOAD_KEY_REF", "TRPC_PAYLOAD_KEY_VERSION"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadProductionConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}

func TestLoadAuditRelayConfigRequiresIndependentCompliancePostgreSQL(t *testing.T) {
	values := map[string]string{"TRPC_POSTGRES_DSN": "postgres://service:secret@postgres/service", "TRPC_AUDIT_RELAY_OWNER": "audit-1",
		"TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN": "postgres://compliance:secret@compliance/audit",
		"TRPC_AUDIT_BATCH_SIZE":              "64", "TRPC_AUDIT_CLAIM_TTL": "45s", "TRPC_AUDIT_CLAIM_RENEW": "15s",
		"TRPC_AUDIT_LAG_POLL_INTERVAL": "2s", "TRPC_AUDIT_LAG_ALERT_AGE": "3m", "TRPC_AUDIT_LAG_ALERT_COUNT": "9000"}
	config, err := loadAuditRelayConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.AuditOwner != "audit-1" || config.AuditBatchSize != 64 || config.AuditLagPollInterval != 2*time.Second ||
		config.AuditLagAlertAge != 3*time.Minute || config.AuditLagAlertCount != 9000 || config.RedisAddress != "" || config.SecretRoot != "" {
		t.Fatalf("unexpected audit config: %+v", config)
	}
	for name, value := range map[string]string{"TRPC_AUDIT_BATCH_SIZE": "0", "TRPC_AUDIT_CLAIM_TTL": "10s", "TRPC_AUDIT_CLAIM_RENEW": "45s",
		"TRPC_AUDIT_RELAY_OWNER": "bad\nowner", "TRPC_AUDIT_LAG_POLL_INTERVAL": "0s", "TRPC_AUDIT_LAG_ALERT_AGE": "500ms", "TRPC_AUDIT_LAG_ALERT_COUNT": "0"} {
		copy := cloneEnvironment(values)
		copy[name] = value
		if _, err := loadAuditRelayConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
	}
	copy := cloneEnvironment(values)
	copy["TRPC_AUDIT_LAG_POLL_INTERVAL"] = "5m"
	copy["TRPC_AUDIT_LAG_ALERT_AGE"] = "5m"
	if _, err := loadAuditRelayConfig(mapEnvironment(copy)); err == nil {
		t.Fatal("audit lag poll interval equal to alert age accepted")
	}
	delete(values, "TRPC_POSTGRES_DSN")
	if _, err := loadAuditRelayConfig(mapEnvironment(values)); err == nil {
		t.Fatal("missing postgres accepted")
	}
	values["TRPC_POSTGRES_DSN"] = values["TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN"]
	if _, err := loadAuditRelayConfig(mapEnvironment(values)); err == nil {
		t.Fatal("source database accepted as compliance sink")
	}
}

func TestLoadBusinessAuditPurgeConfigDefaultsSafeAndRejectsUnsafeValues(t *testing.T) {
	values := map[string]string{
		"TRPC_BUSINESS_AUDIT_PURGE_POSTGRES_DSN": "postgres://purger:secret@postgres/service",
		"TRPC_BUSINESS_AUDIT_PURGE_OWNER":        "business-audit-purge-1",
	}
	config, err := loadBusinessAuditPurgeConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if !config.DryRun || config.Retention != 180*24*time.Hour || config.MaxBatchSize != 1000 {
		t.Fatalf("unexpected safe defaults: %+v", config)
	}
	for name, value := range map[string]string{
		"TRPC_BUSINESS_AUDIT_PURGE_DRY_RUN":        "sometimes",
		"TRPC_BUSINESS_AUDIT_RETENTION":            "1h",
		"TRPC_BUSINESS_AUDIT_PURGE_MAX_ATTEMPTS":   "0",
		"TRPC_BUSINESS_AUDIT_PURGE_MAX_BATCH_SIZE": "0",
	} {
		copy := cloneEnvironment(values)
		copy[name] = value
		if _, err := loadBusinessAuditPurgeConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
	}
	for _, name := range []string{"TRPC_BUSINESS_AUDIT_PURGE_POSTGRES_DSN", "TRPC_BUSINESS_AUDIT_PURGE_OWNER"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadBusinessAuditPurgeConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}

func TestLoadProductionConfigRejectsUnsafeLifecycleValuesWithoutLeakingSecrets(t *testing.T) {
	for name, value := range map[string]string{
		"TRPC_ARTIFACT_PUT_TIMEOUT":          "3m",
		"TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE": "1001",
		"TRPC_ARTIFACT_ORPHAN_GRACE":         "1s",
		"TRPC_S3_ALLOW_INSECURE":             "sometimes",
	} {
		values := validProductionEnvironment()
		values[name] = value
		_, err := loadProductionConfig(mapEnvironment(values))
		if err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
		if strings.Contains(err.Error(), "bearer-secret") {
			t.Fatalf("secret leaked in %v", err)
		}
	}
}

func TestLoadProductionConfigRejectsUnversionedPayloadKey(t *testing.T) {
	values := validProductionEnvironment()
	values["TRPC_PAYLOAD_KEY_VERSION"] = "0"
	if _, err := loadProductionConfig(mapEnvironment(values)); err == nil {
		t.Fatal("zero payload key version accepted")
	}
	values = validProductionEnvironment()
	values["TRPC_PAYLOAD_KEY_REF"] = " "
	if _, err := loadProductionConfig(mapEnvironment(values)); err == nil {
		t.Fatal("empty payload key ref accepted")
	}
}

func TestLoadPreprocessConfigDoesNotRequireRedisAndRequiresRetention(t *testing.T) {
	values := validProductionEnvironment()
	delete(values, "TRPC_REDIS_ADDRESS")
	values["TRPC_REDIS_DB"] = "not-used"
	values["TRPC_PREPROCESS_ARTIFACT_RETENTION"] = "24h"
	values["TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS"] = "files.example.test, images.example.test,files.example.test"
	config, err := loadPreprocessConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.RedisAddress != "" || config.ArtifactRetention != 24*time.Hour || len(config.MediaAllowedHosts) != 2 {
		t.Fatalf("unexpected preprocess configuration: %+v", config)
	}
	for _, value := range []string{"", "500ms", "24h500ms"} {
		copy := cloneEnvironment(values)
		if value == "" {
			delete(copy, "TRPC_PREPROCESS_ARTIFACT_RETENTION")
		} else {
			copy["TRPC_PREPROCESS_ARTIFACT_RETENTION"] = value
		}
		if _, err := loadPreprocessConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("retention %q accepted", value)
		}
	}
}

func TestLoadProductionConfigIgnoresPreprocessOnlyOverrides(t *testing.T) {
	values := validProductionEnvironment()
	values["TRPC_PREPROCESS_BATCH_SIZE"] = "not-an-int"
	values["TRPC_PREPROCESS_LEASE_TTL"] = "not-a-duration"
	if _, err := loadProductionConfig(mapEnvironment(values)); err != nil {
		t.Fatalf("artifact role rejected preprocess-only settings: %v", err)
	}
}

func TestLoadChannelConfigAddsRedisOnlyForWebUIProgress(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN":              "postgres://service:secret@postgres/service",
		"TRPC_SECRET_ROOT":               "/var/run/secrets/trpc-agent-service",
		"TRPC_PAYLOAD_KEY_REF":           "secret://messaging/payload",
		"TRPC_PAYLOAD_KEY_VERSION":       "7",
		"TRPC_CHANNEL_PROBE_TENANT_ID":   "probe-tenant",
		"TRPC_CHANNEL_CANDIDATE_TTL":     "45s",
		"TRPC_CHANNEL_CALLBACK_MAX_BODY": "2097152",
		"TRPC_WEBUI_ENABLED":             "true",
		"TRPC_REDIS_ADDRESS":             "redis:6379",
		"TRPC_REDIS_DB":                  "2",
		"TRPC_REDIS_ENVIRONMENT":         "production",
	}
	config, err := loadChannelConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.RedisAddress != "redis:6379" || config.RedisDB != 2 || config.RedisEnvironment != "production" || config.S3Bucket != "" || config.ClamAVAddress != "" || config.DLPEndpoint != "" {
		t.Fatalf("channel role dependencies=%+v", config)
	}
	if config.ChannelProbeTenant != "probe-tenant" || config.ChannelCandidateTTL != 45*time.Second || config.ChannelCallbackMaxBody != 2<<20 || !config.WebUIEnabled {
		t.Fatalf("unexpected channel configuration: %+v", config)
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_SECRET_ROOT", "TRPC_PAYLOAD_KEY_REF", "TRPC_PAYLOAD_KEY_VERSION", "TRPC_CHANNEL_PROBE_TENANT_ID"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadChannelConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
	for _, name := range []string{"TRPC_REDIS_ADDRESS", "TRPC_REDIS_ENVIRONMENT"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadChannelConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("WebUI missing %s accepted", name)
		}
	}
}

func TestLoadChannelConfigRejectsUnsafeCallbackLimits(t *testing.T) {
	base := map[string]string{
		"TRPC_POSTGRES_DSN":            "postgres://service:secret@postgres/service",
		"TRPC_SECRET_ROOT":             "/var/run/secrets/trpc-agent-service",
		"TRPC_PAYLOAD_KEY_REF":         "secret://messaging/payload",
		"TRPC_PAYLOAD_KEY_VERSION":     "7",
		"TRPC_CHANNEL_PROBE_TENANT_ID": "probe-tenant",
	}
	for _, item := range []struct{ name, value string }{
		{name: "TRPC_CHANNEL_CANDIDATE_TTL", value: "500ms"},
		{name: "TRPC_CHANNEL_CANDIDATE_TTL", value: "11m"},
		{name: "TRPC_CHANNEL_CALLBACK_MAX_BODY", value: "0"},
		{name: "TRPC_CHANNEL_CALLBACK_MAX_BODY", value: "16777217"},
		{name: "TRPC_WEBUI_ENABLED", value: "sometimes"},
	} {
		copy := cloneEnvironment(base)
		copy[item.name] = item.value
		if _, err := loadChannelConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("%s=%q accepted", item.name, item.value)
		}
	}
}

func TestLoadChannelDeliveryConfigRequiresOnlyDeliveryDependencies(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://service:secret@postgres/service", "TRPC_REDIS_ADDRESS": "redis:6379", "TRPC_REDIS_DB": "2",
		"TRPC_REDIS_ENVIRONMENT": "production", "TRPC_CHANNEL_DELIVERY_GROUP": "channel-owners", "TRPC_SECRET_ROOT": "/var/run/secrets/trpc-agent-service",
		"TRPC_PAYLOAD_KEY_REF": "secret://messaging/payload", "TRPC_PAYLOAD_KEY_VERSION": "7", "TRPC_CHANNEL_PROBE_TENANT_ID": "probe-tenant",
		"TRPC_CHANNEL_DELIVERY_REFRESH": "3s", "TRPC_CHANNEL_DELIVERY_CLAIM_TTL": "30s", "TRPC_CHANNEL_DELIVERY_CLAIM_RENEW": "10s",
		"TRPC_WEBUI_ENABLED": "true",
	}
	config, err := loadChannelDeliveryConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.RedisEnvironment != "production" || config.ChannelDeliveryGroup != "channel-owners" || config.ChannelDeliveryRefresh != 3*time.Second || config.S3Bucket != "" || !config.WebUIEnabled {
		t.Fatalf("unexpected delivery configuration: %+v", config)
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_REDIS_ADDRESS", "TRPC_REDIS_ENVIRONMENT", "TRPC_CHANNEL_DELIVERY_GROUP", "TRPC_SECRET_ROOT", "TRPC_PAYLOAD_KEY_REF", "TRPC_PAYLOAD_KEY_VERSION", "TRPC_CHANNEL_PROBE_TENANT_ID"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadChannelDeliveryConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}

func TestLoadChannelDeliveryConfigRejectsUnsafeLeaseAndLimits(t *testing.T) {
	base := map[string]string{"TRPC_POSTGRES_DSN": "postgres://service:secret@postgres/service", "TRPC_REDIS_ADDRESS": "redis:6379",
		"TRPC_REDIS_ENVIRONMENT": "production", "TRPC_CHANNEL_DELIVERY_GROUP": "channel-owners", "TRPC_SECRET_ROOT": "/secrets",
		"TRPC_PAYLOAD_KEY_REF": "payload", "TRPC_PAYLOAD_KEY_VERSION": "1", "TRPC_CHANNEL_PROBE_TENANT_ID": "probe"}
	for _, item := range []struct{ name, value string }{{"TRPC_WEBUI_ENABLED", "sometimes"}, {"TRPC_CHANNEL_DELIVERY_CLAIM_TTL", "5s"}, {"TRPC_CHANNEL_DELIVERY_CLAIM_RENEW", "30s"}, {"TRPC_CHANNEL_REPLY_RECLAIM_LIMIT", "0"}, {"TRPC_CHANNEL_DELIVERY_MAX_ATTEMPTS", "1001"}} {
		copy := cloneEnvironment(base)
		copy[item.name] = item.value
		if _, err := loadChannelDeliveryConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("%s=%q accepted", item.name, item.value)
		}
	}
}

func validProductionEnvironment() map[string]string {
	return map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://service:secret@postgres/service",
		"TRPC_REDIS_ADDRESS":              "redis:6379",
		"TRPC_REDIS_PASSWORD":             "bearer-secret",
		"TRPC_REDIS_DB":                   "2",
		"TRPC_SECRET_ROOT":                "/var/run/secrets/trpc-agent-service",
		"TRPC_S3_REGION":                  "us-east-1",
		"TRPC_S3_BUCKET":                  "artifacts",
		"TRPC_S3_ENDPOINT":                "https://s3.example.test",
		"TRPC_S3_PATH_STYLE":              "true",
		"TRPC_CLAMAV_ADDRESS":             "clamav:3310",
		"TRPC_DLP_ENDPOINT":               "https://dlp.example.test/",
		"TRPC_DLP_PROBE_TENANT_ID":        "probe-tenant",
		"TRPC_DLP_SECRET_REF":             "secret://dlp/service",
		"TRPC_DLP_SECRET_VERSION":         "3",
		"TRPC_DLP_BACKEND_VERSION":        "5",
		"TRPC_PAYLOAD_KEY_REF":            "secret://messaging/payload",
		"TRPC_PAYLOAD_KEY_VERSION":        "7",
		"TRPC_ARTIFACT_ORPHAN_GRACE":      "48h",
		"TRPC_ARTIFACT_UPLOAD_PROTECTION": "2m",
		"TRPC_ARTIFACT_PUT_TIMEOUT":       "30s",
	}
}

func mapEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func cloneEnvironment(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func TestLoadWorkerConfig(t *testing.T) {
	environment := workerEnvironment()
	config, err := loadWorkerConfig(mapEnvironment(environment))
	if err != nil {
		t.Fatal(err)
	}
	if config.WorkerShardCount != 4 || len(config.WorkerShards) != 2 || config.WorkerShards[0] != 0 || config.WorkerShards[1] != 3 ||
		config.WorkerLeaseRenew >= config.WorkerLeaseTTL || config.WorkerProbeTenant == "" || config.WorkerGraphCheckpointTTL != 7*24*time.Hour || config.WorkerBacklogPoll != 5*time.Second {
		t.Fatalf("config=%#v", config)
	}
}

func TestLoadWorkerConfigSeparatesMemoryConnectionRegistries(t *testing.T) {
	values := cloneEnvironment(workerEnvironment())
	values["TRPC_SESSION_POSTGRES_CONNECTIONS"] = `{"session":"postgres://session:secret@session/service"}`
	values["TRPC_MEMORY_POSTGRES_CONNECTIONS"] = `{"memory":"postgres://memory:secret@memory/service"}`
	values["TRPC_ARTIFACT_POSTGRES_CONNECTIONS"] = `{"artifact":"postgres://artifact:secret@artifact/service"}`
	values["TRPC_MEMORY_REDIS_CONNECTIONS"] = `{"cache":"rediss://memory:secret@redis.example.test:6380/4"}`
	config, err := loadWorkerConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.MemoryPostgresConnections["memory"] != "postgres://memory:secret@memory/service" ||
		config.ArtifactPostgresConnections["artifact"] != "postgres://artifact:secret@artifact/service" || config.SessionPostgresConnections["memory"] != "" ||
		config.MemoryRedisConnections["cache"] != "rediss://memory:secret@redis.example.test:6380/4" ||
		config.MemoryRedisConnections["default"] == "" {
		t.Fatalf("memory connection registries = %#v / %#v", config.MemoryPostgresConnections, config.MemoryRedisConnections)
	}
	for _, raw := range []string{"[]", `{"default":"redis://redis:6379/0"}`, `{"bad id":"redis://redis:6379/0"}`, `{"cache":"https://redis.example.test"}`} {
		values := cloneEnvironment(workerEnvironment())
		values["TRPC_MEMORY_REDIS_CONNECTIONS"] = raw
		if _, err := loadWorkerConfig(mapEnvironment(values)); err == nil {
			t.Fatalf("accepted invalid memory Redis registry %q", raw)
		}
	}
}

func TestLoadWorkerConfigDoesNotImplicitlyShareNamedSessionConnections(t *testing.T) {
	values := cloneEnvironment(workerEnvironment())
	values["TRPC_SESSION_POSTGRES_CONNECTIONS"] = `{"session":"postgres://session:secret@session/service"}`
	config, err := loadWorkerConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.MemoryPostgresConnections["session"] != "" || config.ArtifactPostgresConnections["session"] != "" ||
		config.MemoryPostgresConnections["default"] == "" || config.ArtifactPostgresConnections["default"] == "" {
		t.Fatalf("implicit cross-domain connection grant: memory=%#v artifact=%#v", config.MemoryPostgresConnections, config.ArtifactPostgresConnections)
	}
}

func TestParseMemoryMem0Connections(t *testing.T) {
	connections, err := parseMemoryMem0Connections(`{"cloud":{"host":"https://mem0.example.test"},"oss":{"host":"http://mem0:8888","self_hosted_oss":true}}`)
	if err != nil || len(connections) != 2 || connections["oss"].Host != "http://mem0:8888" || !connections["oss"].SelfHostedOSS {
		t.Fatalf("connections/error = %#v / %v", connections, err)
	}
	for _, raw := range []string{`{"bad":{"host":"http://mem0"}}`, `{"oss":{"host":"https://mem0/path","self_hosted_oss":true}}`, `[]`} {
		if _, err := parseMemoryMem0Connections(raw); err == nil {
			t.Fatalf("parseMemoryMem0Connections(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestLoadWorkerConfigRestrictsInMemoryToSingleShard(t *testing.T) {
	environment := workerEnvironment()
	environment["TRPC_MEMORY_ALLOW_INMEMORY"] = "true"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("multi-shard in-memory memory accepted")
	}
	environment["TRPC_WORKER_SHARD_COUNT"] = "1"
	environment["TRPC_WORKER_SHARDS"] = "0"
	config, err := loadWorkerConfig(mapEnvironment(environment))
	if err != nil || !config.MemoryAllowInMemory {
		t.Fatalf("single shard in-memory config/error = %#v / %v", config.MemoryAllowInMemory, err)
	}
}

func TestLoadWorkerConfigParsesMCPEndpoints(t *testing.T) {
	values := cloneEnvironment(workerEnvironment())
	values["TRPC_MCP_ENDPOINTS"] = `[{"tenant_id":"t_demo","tool_id":"weather_lookup","version":2,"transport":"streamable","server_url":"https://mcp.test/tools","declaration_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timeout":"10s","secret_ref":"secret://mcp/weather","secret_version":4,"secret_header":"Authorization","secret_prefix":"Bearer "}]`
	config, err := loadWorkerConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.MCPEndpoints) != 1 || config.MCPEndpoints[0].TenantID != "t_demo" || config.MCPEndpoints[0].ToolID != "weather_lookup" ||
		config.MCPEndpoints[0].Version != 2 || config.MCPEndpoints[0].Transport != "streamable" || config.MCPEndpoints[0].DeclarationDigest != strings.Repeat("a", 64) || config.MCPEndpoints[0].Timeout != 10*time.Second ||
		config.MCPEndpoints[0].SecretRef != "secret://mcp/weather" || config.MCPEndpoints[0].SecretVersion != 4 ||
		config.MCPEndpoints[0].SecretHeader != "Authorization" || config.MCPEndpoints[0].SecretPrefix != "Bearer " {
		t.Fatalf("mcp endpoints=%#v", config.MCPEndpoints)
	}
}

func TestLoadWorkerConfigRejectsMalformedMCPEndpoints(t *testing.T) {
	for _, raw := range []string{
		`not-json`,
		`[]`,
		`[{"tool_id":"weather_lookup"}]`,
		`[{"tenant_id":"t_demo","tool_id":"weather_lookup","version":0,"transport":"streamable","server_url":"https://mcp.test","timeout":"10s"}]`,
		`[{"tenant_id":"t_demo","tool_id":"weather_lookup","version":1,"transport":"streamable","server_url":"https://mcp.test","timeout":"10s","secret_ref":"secret://mcp"}]`,
		`[{"tenant_id":"t_demo","tool_id":"weather_lookup","version":1,"transport":"streamable","server_url":"https://mcp.test","timeout":"0s"}]`,
	} {
		values := cloneEnvironment(workerEnvironment())
		values["TRPC_MCP_ENDPOINTS"] = raw
		if _, err := loadWorkerConfig(mapEnvironment(values)); err == nil {
			t.Fatalf("accepted malformed MCP endpoints %q", raw)
		}
	}
}

func TestBuildToolCatalogRegistersMCPEndpointsAndRejectsInvalid(t *testing.T) {
	valid := []mcpEndpoint{{TenantID: "t_demo", ToolID: "weather_lookup", Version: 1, Transport: "streamable",
		ServerURL: "https://mcp.test/tools", DeclarationDigest: strings.Repeat("a", 64), Timeout: 10 * time.Second}}
	catalog, err := buildToolCatalog(valid)
	if err != nil {
		t.Fatalf("valid endpoints rejected: %v", err)
	}
	if catalog == nil {
		t.Fatal("nil catalog")
	}
	invalid := []mcpEndpoint{{TenantID: "t_demo", ToolID: "weather_lookup", Version: 1, Transport: "stdio",
		ServerURL: "https://mcp.test/tools", DeclarationDigest: strings.Repeat("a", 64), Timeout: 10 * time.Second}}
	if _, err := buildToolCatalog(invalid); err == nil {
		t.Fatal("accepted stdio transport")
	}
}

func TestLoadGatewayConfigRequiresScopedAuthAndDurableDependencies(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN":                "postgres://service:secret@postgres/service",
		"TRPC_SECRET_ROOT":                 "/var/run/secrets/trpc-agent-service",
		"TRPC_PAYLOAD_KEY_REF":             "secret://messaging/payload",
		"TRPC_PAYLOAD_KEY_VERSION":         "7",
		"TRPC_GATEWAY_PROBE_TENANT_ID":     "probe-tenant",
		"TRPC_GATEWAY_AUTH_SECRET_REF":     "secret://gateway/auth",
		"TRPC_GATEWAY_AUTH_SECRET_VERSION": "3",
		"TRPC_GATEWAY_PUBLIC_BASE_URL":     "https://gateway.example.test",
		"TRPC_GATEWAY_MAX_BODY":            "2097152",
		"TRPC_GATEWAY_SSE_POLL_INTERVAL":   "2s",
		"TRPC_GATEWAY_SSE_REPLAY_LIMIT":    "128",
		"TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS": "256",
		"TRPC_GATEWAY_PROTOCOL_TIMEOUT":    "3m",
	}
	config, err := loadGatewayConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.GatewayProbeTenant != "probe-tenant" || config.GatewayAuthSecretVersion != 3 || config.GatewayMaxBody != 2<<20 ||
		config.GatewaySSEPollInterval != 2*time.Second || config.GatewaySSEReplayLimit != 128 || config.GatewaySSEMaxSubscribers != 256 || config.GatewayProtocolTimeout != 3*time.Minute {
		t.Fatalf("unexpected gateway configuration: %+v", config)
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_SECRET_ROOT", "TRPC_PAYLOAD_KEY_REF", "TRPC_PAYLOAD_KEY_VERSION", "TRPC_GATEWAY_PROBE_TENANT_ID", "TRPC_GATEWAY_AUTH_SECRET_REF", "TRPC_GATEWAY_AUTH_SECRET_VERSION", "TRPC_GATEWAY_PUBLIC_BASE_URL"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadGatewayConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}

func TestLoadAdminConfigRequiresDedicatedAuthDependencies(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN":              "postgres://service:secret@postgres/service",
		"TRPC_SECRET_ROOT":               "/var/run/secrets/trpc-agent-service",
		"TRPC_ADMIN_PROBE_TENANT_ID":     "probe-tenant",
		"TRPC_ADMIN_AUTH_SECRET_REF":     "secret://admin/auth",
		"TRPC_ADMIN_AUTH_SECRET_VERSION": "3",
		"TRPC_ADMIN_AUTH_CLOCK_SKEW":     "2s",
		"TRPC_SHUTDOWN_TIMEOUT":          "30s",
	}
	config, err := loadAdminConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.AdminProbeTenant != "probe-tenant" || config.AdminAuthSecretVersion != 3 || config.AdminAuthClockSkew != 2*time.Second {
		t.Fatalf("config=%+v", config)
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_SECRET_ROOT", "TRPC_ADMIN_PROBE_TENANT_ID", "TRPC_ADMIN_AUTH_SECRET_REF", "TRPC_ADMIN_AUTH_SECRET_VERSION"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadAdminConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
	values["TRPC_ADMIN_AUTH_CLOCK_SKEW"] = "30s"
	if _, err := loadAdminConfig(mapEnvironment(values)); err == nil {
		t.Fatal("clock skew equal to shutdown timeout accepted")
	}
}

func TestLoadAdminConfigDefaultLifecycleTimingIsValid(t *testing.T) {
	config, err := loadAdminConfig(mapEnvironment(map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://service:secret@postgres/service", "TRPC_SECRET_ROOT": "/secrets",
		"TRPC_ADMIN_PROBE_TENANT_ID": "probe-tenant", "TRPC_ADMIN_AUTH_SECRET_REF": "secret://admin/auth",
		"TRPC_ADMIN_AUTH_SECRET_VERSION": "1",
	}))
	if err != nil || config.AdminAuthClockSkew >= config.ShutdownTimeout {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestLoadAdminConfigOnlyAllowsInsecureSessionCookieExplicitly(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://example", "TRPC_SECRET_ROOT": "/secrets",
		"TRPC_ADMIN_PROBE_TENANT_ID": "probe-tenant", "TRPC_ADMIN_AUTH_SECRET_REF": "file://admin",
		"TRPC_ADMIN_AUTH_SECRET_VERSION": "1", "TRPC_ADMIN_ALLOW_INSECURE_SESSION_COOKIE": "true",
	}
	config, err := loadAdminConfig(mapEnvironment(values))
	if err != nil || !config.AdminAllowInsecureSessionCookie {
		t.Fatalf("config=%#v err=%v", config, err)
	}
	values["TRPC_ADMIN_ALLOW_INSECURE_SESSION_COOKIE"] = "invalid"
	if _, err := loadAdminConfig(mapEnvironment(values)); err == nil {
		t.Fatal("invalid insecure-cookie override accepted")
	}
}

func TestLoadGatewayConfigRejectsUnsafeLimits(t *testing.T) {
	base := map[string]string{"TRPC_POSTGRES_DSN": "postgres://service:secret@postgres/service", "TRPC_SECRET_ROOT": "/secrets",
		"TRPC_PAYLOAD_KEY_REF": "payload", "TRPC_PAYLOAD_KEY_VERSION": "1", "TRPC_GATEWAY_PROBE_TENANT_ID": "probe",
		"TRPC_GATEWAY_AUTH_SECRET_REF": "auth", "TRPC_GATEWAY_AUTH_SECRET_VERSION": "1"}
	base["TRPC_GATEWAY_PUBLIC_BASE_URL"] = "https://gateway.example.test"
	for _, item := range []struct{ name, value string }{
		{"TRPC_GATEWAY_MAX_BODY", "0"}, {"TRPC_GATEWAY_MAX_BODY", "16777217"},
		{"TRPC_GATEWAY_SSE_REPLAY_LIMIT", "257"}, {"TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS", "0"},
		{"TRPC_GATEWAY_AUTH_CLOCK_SKEW", "1m"}, {"TRPC_GATEWAY_SSE_POLL_INTERVAL", "0"},
		{"TRPC_GATEWAY_PROTOCOL_TIMEOUT", "0"},
		{"TRPC_GATEWAY_PROTOCOL_TIMEOUT", "31m"},
		{"TRPC_GATEWAY_PUBLIC_BASE_URL", "http://gateway.example.test"},
	} {
		copy := cloneEnvironment(base)
		copy[item.name] = item.value
		if _, err := loadGatewayConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("%s=%q accepted", item.name, item.value)
		}
	}
}

func TestLoadWorkerConfigRejectsUnsafeTopologyAndTiming(t *testing.T) {
	for name, value := range map[string]string{"duplicate shards": "0,0", "out of range": "0,4"} {
		t.Run(name, func(t *testing.T) {
			environment := workerEnvironment()
			environment["TRPC_WORKER_SHARDS"] = value
			if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	environment := workerEnvironment()
	environment["TRPC_WORKER_LEASE_RENEW"] = "30s"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("expected lease timing rejection")
	}
	environment = workerEnvironment()
	environment["TRPC_WORKER_DRAIN_TIMEOUT"] = "40s"
	environment["TRPC_WORKER_BUNDLE_CLOSE_TIMEOUT"] = "6s"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("expected shutdown budget rejection")
	}
	environment = workerEnvironment()
	environment["TRPC_WORKER_BACKLOG_POLL_INTERVAL"] = "500ms"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("expected backlog poll rejection")
	}
	environment = workerEnvironment()
	environment["TRPC_WORKER_GRAPH_CHECKPOINT_TTL"] = "59m"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("expected graph checkpoint TTL rejection")
	}
}

func TestLoadWorkerConfigDefaultsToAllShards(t *testing.T) {
	environment := workerEnvironment()
	delete(environment, "TRPC_WORKER_SHARDS")
	config, err := loadWorkerConfig(mapEnvironment(environment))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.WorkerShards) != 4 || config.WorkerShards[0] != 0 || config.WorkerShards[3] != 3 {
		t.Fatalf("shards=%v", config.WorkerShards)
	}
}

func TestLoadWorkerConfigOptionalDLPIsAllOrNothing(t *testing.T) {
	environment := workerEnvironment()
	environment["TRPC_DLP_ENDPOINT"] = "https://dlp.example.test/"
	if _, err := loadWorkerConfig(mapEnvironment(environment)); err == nil {
		t.Fatal("partial DLP configuration accepted")
	}
	environment["TRPC_DLP_PROBE_TENANT_ID"] = "tenant-probe"
	environment["TRPC_DLP_SECRET_REF"] = "secret://dlp/service"
	environment["TRPC_DLP_SECRET_VERSION"] = "3"
	environment["TRPC_DLP_BACKEND_VERSION"] = "5"
	config, err := loadWorkerConfig(mapEnvironment(environment))
	if err != nil {
		t.Fatal(err)
	}
	if config.DLPSecretVersion != 3 || config.DLPBackendVersion != 5 || config.DLPEndpoint == "" {
		t.Fatalf("config=%#v", config)
	}
}

func TestRunWorkerRoleRejectsMissingDependenciesBeforeOpeningClients(t *testing.T) {
	err := runWorkerRole(context.Background(), func(string) string { return "" }, testLogger())
	if err == nil || !strings.Contains(err.Error(), "configuration rejected") {
		t.Fatalf("error=%v", err)
	}
}

func TestRunAdminRoleRejectsMissingDependenciesBeforeOpeningClients(t *testing.T) {
	err := runAdminRole(context.Background(), func(string) string { return "" }, testLogger())
	if err == nil || !strings.Contains(err.Error(), "configuration rejected") {
		t.Fatalf("error=%v", err)
	}
}

func workerEnvironment() map[string]string {
	return map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://runtime", "TRPC_REDIS_ADDRESS": "redis:6379", "TRPC_SECRET_ROOT": "/secrets",
		"TRPC_REDIS_ENVIRONMENT": "prod", "TRPC_WORKER_GROUP": "workers", "TRPC_WORKER_CONTROL_GROUP": "worker-control",
		"TRPC_WORKER_PROBE_TENANT_ID": "tenant-probe", "TRPC_PAYLOAD_KEY_REF": "payload/key", "TRPC_PAYLOAD_KEY_VERSION": "1",
		"TRPC_S3_REGION": "ap-shanghai", "TRPC_S3_BUCKET": "artifacts", "TRPC_WORKER_SHARD_COUNT": "4", "TRPC_WORKER_SHARDS": "0,3",
	}
}
