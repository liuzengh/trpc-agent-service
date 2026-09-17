package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const workerHTTPShutdownBudget = 5 * time.Second

type productionConfig struct {
	ListenAddress               string
	PostgresDSN                 string
	SessionPostgresConnections  map[string]string
	MemoryPostgresConnections   map[string]string
	ArtifactPostgresConnections map[string]string
	MemoryRedisConnections      map[string]string
	MemoryMem0Connections       map[string]mem0Connection
	MemoryAllowInMemory         bool
	RedisAddress                string
	RedisPassword               string
	RedisDB                     int
	SecretRoot                  string
	SkillStagingRoot            string

	S3Region, S3Bucket, S3Endpoint string
	S3PathStyle, S3AllowInsecure   bool
	S3MaxBytes                     int64

	ClamAVAddress                             string
	DLPEndpoint, DLPProbeTenant, DLPSecretRef string
	DLPSecretVersion, DLPBackendVersion       int64
	PayloadKeyRef                             string
	PayloadKeyVersion                         int64
	DLPAllowInsecure                          bool

	ProbeTimeout, ProbeInterval, ShutdownTimeout time.Duration
	ArtifactPutTimeout, UploadProtection         time.Duration
	UploadClaimTTL, ArtifactClaimTTL             time.Duration
	UploadPollInterval, ArtifactPollInterval     time.Duration
	ArtifactOrphanGrace                          time.Duration
	LifecycleBatchSize, LifecycleMaxAttempts     int

	PreprocessBatchSize, PreprocessMaxAttempts int
	PreprocessLeaseTTL, PreprocessRetryDelay   time.Duration
	PreprocessPollInterval, ArtifactRetention  time.Duration
	MediaFetchTimeout                          time.Duration
	MediaAllowedHosts                          []string

	ChannelCandidateTTL                                  time.Duration
	ChannelCallbackMaxBody                               int64
	ChannelProbeTenant                                   string
	RedisEnvironment                                     string
	ChannelDeliveryGroup                                 string
	ChannelDeliveryRefresh                               time.Duration
	ChannelReplyReadBlock                                time.Duration
	ChannelReplyReclaimIdle, ChannelReplyReclaimInterval time.Duration
	ChannelDeliveryClaimTTL, ChannelDeliveryClaimRenew   time.Duration
	ChannelDeliveryRetryDelay, ChannelDeliveryMaxRetry   time.Duration
	ChannelProviderTimeout                               time.Duration
	ChannelReplyReclaimLimit, ChannelDeliveryMaxAttempts int
	ChannelDeliveryMaxReconcile                          int
	WebUIEnabled                                         bool

	WorkerID, WorkerGroup, WorkerControlGroup, WorkerProbeTenant string
	WorkerShardCount, WorkerReclaimLimit                         int
	WorkerShards                                                 []uint32
	WorkerLeaseTTL, WorkerLeaseRenew, WorkerRetryWait            time.Duration
	WorkerReclaimInterval, WorkerCancelPoll, WorkerDrainTimeout  time.Duration
	WorkerBacklogPoll                                            time.Duration
	WorkerBundleFailureBackoff, WorkerBundleCloseTimeout         time.Duration
	WorkerGraphCheckpointTTL                                     time.Duration
	MCPEndpoints                                                 []mcpEndpoint
	CodeExecutorWorkspaceRoot                                    string
	CodeExecutors                                                []codeExecutorEndpoint

	GatewayProbeTenant, GatewayAuthSecretRef, GatewayPublicURL           string
	GatewayAuthSecretVersion, GatewayMaxBody, GatewaySSEReplayLimit      int64
	GatewayAuthClockSkew, GatewaySSEPollInterval, GatewayProtocolTimeout time.Duration
	GatewaySSEMaxSubscribers                                             int64

	AdminProbeTenant, AdminAuthSecretRef string
	AdminAuthSecretVersion               int64
	AdminAuthClockSkew                   time.Duration
	AdminAllowInsecureSessionCookie      bool

	AuditOwner                             string
	AuditCompliancePostgresDSN             string
	AuditBatchSize                         int
	AuditLagAlertCount                     int64
	AuditClaimTTL, AuditClaimRenew         time.Duration
	AuditRetryDelay, AuditPollInterval     time.Duration
	AuditLagPollInterval, AuditLagAlertAge time.Duration
}

// mem0Connection contains only deployment routing. Cloud API keys remain in
// tenant-scoped SecretRef values and self-hosted OSS is explicitly marked so
// a private HTTP endpoint cannot accidentally be treated as cloud API.
type mem0Connection struct {
	Host          string `json:"host"`
	SelfHostedOSS bool   `json:"self_hosted_oss"`
}

func loadAuditRelayConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:                strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		AuditCompliancePostgresDSN: strings.TrimSpace(getenv("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN")),
		AuditOwner:                 strings.TrimSpace(getenv("TRPC_AUDIT_RELAY_OWNER")),
		ProbeTimeout:               5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second,
		AuditBatchSize: 100, AuditClaimTTL: 30 * time.Second, AuditClaimRenew: 10 * time.Second,
		AuditRetryDelay: time.Second, AuditPollInterval: 100 * time.Millisecond,
		AuditLagPollInterval: 5 * time.Second, AuditLagAlertAge: 5 * time.Minute, AuditLagAlertCount: 10000}
	var err error
	if config.AuditBatchSize, err = envInt(getenv, "TRPC_AUDIT_BATCH_SIZE", config.AuditBatchSize); err != nil || config.AuditBatchSize < 1 || config.AuditBatchSize > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_AUDIT_BATCH_SIZE")
	}
	if config.AuditLagAlertCount, err = envInt64(getenv, "TRPC_AUDIT_LAG_ALERT_COUNT", config.AuditLagAlertCount); err != nil || config.AuditLagAlertCount < 1 {
		return productionConfig{}, errors.New("invalid TRPC_AUDIT_LAG_ALERT_COUNT")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond}, {"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second}, {"TRPC_AUDIT_CLAIM_TTL", &config.AuditClaimTTL, time.Second},
		{"TRPC_AUDIT_CLAIM_RENEW", &config.AuditClaimRenew, time.Millisecond}, {"TRPC_AUDIT_RETRY_DELAY", &config.AuditRetryDelay, time.Millisecond},
		{"TRPC_AUDIT_POLL_INTERVAL", &config.AuditPollInterval, time.Millisecond},
		{"TRPC_AUDIT_LAG_POLL_INTERVAL", &config.AuditLagPollInterval, time.Millisecond},
		{"TRPC_AUDIT_LAG_ALERT_AGE", &config.AuditLagAlertAge, time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.AuditCompliancePostgresDSN == "" ||
		config.AuditCompliancePostgresDSN == config.PostgresDSN {
		return productionConfig{}, errors.New("required audit relay dependency configuration is missing")
	}
	if len(config.AuditOwner) > 128 || strings.ContainsAny(config.AuditOwner, "\x00\r\n") {
		return productionConfig{}, errors.New("invalid TRPC_AUDIT_RELAY_OWNER")
	}
	if config.AuditClaimRenew >= config.AuditClaimTTL || config.ShutdownTimeout <= config.AuditClaimRenew {
		return productionConfig{}, errors.New("invalid audit relay lifecycle timing")
	}
	if config.AuditLagPollInterval >= config.AuditLagAlertAge || config.AuditLagAlertAge > 7*24*time.Hour {
		return productionConfig{}, errors.New("invalid audit lag timing")
	}
	return config, nil
}

func loadGatewayConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:   strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")), GatewayProbeTenant: strings.TrimSpace(getenv("TRPC_GATEWAY_PROBE_TENANT_ID")),
		GatewayAuthSecretRef: strings.TrimSpace(getenv("TRPC_GATEWAY_AUTH_SECRET_REF")),
		GatewayPublicURL:     strings.TrimSpace(getenv("TRPC_GATEWAY_PUBLIC_BASE_URL")),
		ProbeTimeout:         5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 45 * time.Second,
		GatewayAuthClockSkew: 30 * time.Second, GatewayMaxBody: 1 << 20, GatewaySSEPollInterval: time.Second,
		GatewaySSEReplayLimit: 64, GatewaySSEMaxSubscribers: 128, GatewayProtocolTimeout: 2 * time.Minute,
	}
	var err error
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	if config.GatewayAuthSecretVersion, err = envInt64(getenv, "TRPC_GATEWAY_AUTH_SECRET_VERSION", 0); err != nil || config.GatewayAuthSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_AUTH_SECRET_VERSION")
	}
	if config.GatewayMaxBody, err = envInt64(getenv, "TRPC_GATEWAY_MAX_BODY", config.GatewayMaxBody); err != nil || config.GatewayMaxBody < 1 || config.GatewayMaxBody > 16<<20 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_MAX_BODY")
	}
	if config.GatewaySSEReplayLimit, err = envInt64(getenv, "TRPC_GATEWAY_SSE_REPLAY_LIMIT", config.GatewaySSEReplayLimit); err != nil || config.GatewaySSEReplayLimit < 1 || config.GatewaySSEReplayLimit > 256 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_SSE_REPLAY_LIMIT")
	}
	if config.GatewaySSEMaxSubscribers, err = envInt64(getenv, "TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS", config.GatewaySSEMaxSubscribers); err != nil || config.GatewaySSEMaxSubscribers < 1 || config.GatewaySSEMaxSubscribers > 10000 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_GATEWAY_AUTH_CLOCK_SKEW", &config.GatewayAuthClockSkew, time.Second},
		{"TRPC_GATEWAY_SSE_POLL_INTERVAL", &config.GatewaySSEPollInterval, time.Millisecond},
		{"TRPC_GATEWAY_PROTOCOL_TIMEOUT", &config.GatewayProtocolTimeout, time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.SecretRoot == "" || config.PayloadKeyRef == "" ||
		config.GatewayProbeTenant == "" || config.GatewayAuthSecretRef == "" || config.GatewayPublicURL == "" {
		return productionConfig{}, errors.New("required gateway dependency configuration is missing")
	}
	if config.GatewayAuthClockSkew >= config.ShutdownTimeout {
		return productionConfig{}, errors.New("invalid gateway lifecycle timing")
	}
	if config.GatewayProtocolTimeout > 30*time.Minute {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_PROTOCOL_TIMEOUT")
	}
	publicURL, parseErr := url.Parse(config.GatewayPublicURL)
	if parseErr != nil || publicURL.Scheme != "https" || publicURL.Host == "" || publicURL.User != nil ||
		(publicURL.Path != "" && publicURL.Path != "/") || publicURL.RawQuery != "" || publicURL.Fragment != "" {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_PUBLIC_BASE_URL")
	}
	return config, nil
}

func loadAdminConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress:      valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:        strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		SecretRoot:         strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		AdminProbeTenant:   strings.TrimSpace(getenv("TRPC_ADMIN_PROBE_TENANT_ID")),
		AdminAuthSecretRef: strings.TrimSpace(getenv("TRPC_ADMIN_AUTH_SECRET_REF")),
		ProbeTimeout:       5 * time.Second,
		ProbeInterval:      15 * time.Second,
		ShutdownTimeout:    45 * time.Second,
		AdminAuthClockSkew: 30 * time.Second,
	}
	var err error
	if config.AdminAuthSecretVersion, err = envInt64(getenv, "TRPC_ADMIN_AUTH_SECRET_VERSION", 0); err != nil || config.AdminAuthSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_ADMIN_AUTH_SECRET_VERSION")
	}
	if config.AdminAllowInsecureSessionCookie, err = envBool(getenv, "TRPC_ADMIN_ALLOW_INSECURE_SESSION_COOKIE", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_ADMIN_ALLOW_INSECURE_SESSION_COOKIE")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_ADMIN_AUTH_CLOCK_SKEW", &config.AdminAuthClockSkew, time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.SecretRoot == "" || config.AdminProbeTenant == "" || config.AdminAuthSecretRef == "" {
		return productionConfig{}, errors.New("required admin dependency configuration is missing")
	}
	if config.AdminAuthClockSkew >= config.ShutdownTimeout {
		return productionConfig{}, errors.New("invalid admin lifecycle timing")
	}
	return config, nil
}

func loadWorkerConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:   strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword: getenv("TRPC_REDIS_PASSWORD"), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")), SkillStagingRoot: strings.TrimSpace(getenv("TRPC_SKILL_STAGING_ROOT")),
		RedisEnvironment: strings.TrimSpace(getenv("TRPC_REDIS_ENVIRONMENT")), PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		S3Region: strings.TrimSpace(getenv("TRPC_S3_REGION")), S3Bucket: strings.TrimSpace(getenv("TRPC_S3_BUCKET")), S3Endpoint: strings.TrimSpace(getenv("TRPC_S3_ENDPOINT")),
		WorkerID: strings.TrimSpace(getenv("TRPC_WORKER_ID")), WorkerGroup: strings.TrimSpace(getenv("TRPC_WORKER_GROUP")),
		WorkerControlGroup: strings.TrimSpace(getenv("TRPC_WORKER_CONTROL_GROUP")), WorkerProbeTenant: strings.TrimSpace(getenv("TRPC_WORKER_PROBE_TENANT_ID")),
		ProbeTimeout: 5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 45 * time.Second,
		S3MaxBytes: 16 << 20, WorkerShardCount: 1, WorkerReclaimLimit: 100,
		WorkerLeaseTTL: 30 * time.Second, WorkerLeaseRenew: 10 * time.Second, WorkerRetryWait: 100 * time.Millisecond,
		WorkerReclaimInterval: 5 * time.Second, WorkerCancelPoll: 100 * time.Millisecond, WorkerDrainTimeout: 30 * time.Second,
		WorkerBacklogPoll:          5 * time.Second,
		WorkerBundleFailureBackoff: 250 * time.Millisecond, WorkerBundleCloseTimeout: 5 * time.Second,
		WorkerGraphCheckpointTTL: 7 * 24 * time.Hour,
	}
	config.DLPEndpoint = strings.TrimSpace(getenv("TRPC_DLP_ENDPOINT"))
	config.DLPProbeTenant = strings.TrimSpace(getenv("TRPC_DLP_PROBE_TENANT_ID"))
	config.DLPSecretRef = strings.TrimSpace(getenv("TRPC_DLP_SECRET_REF"))
	var err error
	if config.SessionPostgresConnections, err = parseSessionPostgresConnections(config.PostgresDSN, getenv("TRPC_SESSION_POSTGRES_CONNECTIONS")); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_SESSION_POSTGRES_CONNECTIONS")
	}
	if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
		return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
	}
	memoryPostgresRaw := getenv("TRPC_MEMORY_POSTGRES_CONNECTIONS")
	if strings.TrimSpace(memoryPostgresRaw) == "" {
		config.MemoryPostgresConnections, err = parseSessionPostgresConnections(config.PostgresDSN, "")
	} else if config.MemoryPostgresConnections, err = parseSessionPostgresConnections(config.PostgresDSN, memoryPostgresRaw); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_MEMORY_POSTGRES_CONNECTIONS")
	}
	if err != nil {
		return productionConfig{}, errors.New("invalid TRPC_MEMORY_POSTGRES_CONNECTIONS")
	}
	if config.MemoryRedisConnections, err = parseMemoryRedisConnections(config.RedisAddress, config.RedisPassword, config.RedisDB, getenv("TRPC_MEMORY_REDIS_CONNECTIONS")); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_MEMORY_REDIS_CONNECTIONS")
	}
	if config.MemoryMem0Connections, err = parseMemoryMem0Connections(getenv("TRPC_MEMORY_MEM0_CONNECTIONS")); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_MEMORY_MEM0_CONNECTIONS")
	}
	if config.MemoryAllowInMemory, err = envBool(getenv, "TRPC_MEMORY_ALLOW_INMEMORY", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_MEMORY_ALLOW_INMEMORY")
	}
	artifactPostgresRaw := getenv("TRPC_ARTIFACT_POSTGRES_CONNECTIONS")
	if strings.TrimSpace(artifactPostgresRaw) == "" {
		config.ArtifactPostgresConnections, err = parseSessionPostgresConnections(config.PostgresDSN, "")
	} else if config.ArtifactPostgresConnections, err = parseSessionPostgresConnections(config.PostgresDSN, artifactPostgresRaw); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_POSTGRES_CONNECTIONS")
	}
	if err != nil {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_POSTGRES_CONNECTIONS")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	if config.DLPEndpoint != "" || config.DLPProbeTenant != "" || config.DLPSecretRef != "" {
		if config.DLPEndpoint == "" || config.DLPProbeTenant == "" || config.DLPSecretRef == "" {
			return productionConfig{}, errors.New("incomplete worker DLP configuration")
		}
		if config.DLPSecretVersion, err = envInt64(getenv, "TRPC_DLP_SECRET_VERSION", 0); err != nil || config.DLPSecretVersion < 1 {
			return productionConfig{}, errors.New("invalid TRPC_DLP_SECRET_VERSION")
		}
		if config.DLPBackendVersion, err = envInt64(getenv, "TRPC_DLP_BACKEND_VERSION", 0); err != nil || config.DLPBackendVersion < 1 {
			return productionConfig{}, errors.New("invalid TRPC_DLP_BACKEND_VERSION")
		}
		if config.DLPAllowInsecure, err = envBool(getenv, "TRPC_DLP_ALLOW_INSECURE", false); err != nil {
			return productionConfig{}, errors.New("invalid TRPC_DLP_ALLOW_INSECURE")
		}
	}
	if config.WorkerShardCount, err = envInt(getenv, "TRPC_WORKER_SHARD_COUNT", config.WorkerShardCount); err != nil || config.WorkerShardCount < 1 || config.WorkerShardCount > 4096 {
		return productionConfig{}, errors.New("invalid TRPC_WORKER_SHARD_COUNT")
	}
	if raw := strings.TrimSpace(getenv("TRPC_WORKER_SHARDS")); raw != "" {
		config.WorkerShards, err = parseWorkerShards(raw, config.WorkerShardCount)
		if err != nil {
			return productionConfig{}, errors.New("invalid TRPC_WORKER_SHARDS")
		}
	} else {
		config.WorkerShards = make([]uint32, config.WorkerShardCount)
		for index := range config.WorkerShards {
			config.WorkerShards[index] = uint32(index)
		}
	}
	if config.WorkerReclaimLimit, err = envInt(getenv, "TRPC_WORKER_RECLAIM_LIMIT", config.WorkerReclaimLimit); err != nil || config.WorkerReclaimLimit < 1 || config.WorkerReclaimLimit > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_WORKER_RECLAIM_LIMIT")
	}
	if config.S3MaxBytes, err = envInt64(getenv, "TRPC_ARTIFACT_MAX_BYTES", config.S3MaxBytes); err != nil || config.S3MaxBytes < 1 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_MAX_BYTES")
	}
	for name, target := range map[string]*bool{"TRPC_S3_PATH_STYLE": &config.S3PathStyle, "TRPC_S3_ALLOW_INSECURE": &config.S3AllowInsecure} {
		if *target, err = envBool(getenv, name, false); err != nil {
			return productionConfig{}, errors.New("invalid " + name)
		}
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond}, {"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second}, {"TRPC_WORKER_LEASE_TTL", &config.WorkerLeaseTTL, time.Second},
		{"TRPC_WORKER_LEASE_RENEW", &config.WorkerLeaseRenew, time.Millisecond}, {"TRPC_WORKER_RETRY_WAIT", &config.WorkerRetryWait, time.Millisecond},
		{"TRPC_WORKER_RECLAIM_INTERVAL", &config.WorkerReclaimInterval, time.Millisecond}, {"TRPC_WORKER_CANCEL_POLL", &config.WorkerCancelPoll, time.Millisecond},
		{"TRPC_WORKER_BACKLOG_POLL_INTERVAL", &config.WorkerBacklogPoll, time.Second},
		{"TRPC_WORKER_DRAIN_TIMEOUT", &config.WorkerDrainTimeout, time.Second}, {"TRPC_WORKER_BUNDLE_FAILURE_BACKOFF", &config.WorkerBundleFailureBackoff, time.Millisecond},
		{"TRPC_WORKER_BUNDLE_CLOSE_TIMEOUT", &config.WorkerBundleCloseTimeout, time.Millisecond},
		{"TRPC_WORKER_GRAPH_CHECKPOINT_TTL", &config.WorkerGraphCheckpointTTL, time.Hour},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.WorkerLeaseRenew >= config.WorkerLeaseTTL || config.WorkerDrainTimeout+config.WorkerBundleCloseTimeout+workerHTTPShutdownBudget > config.ShutdownTimeout {
		return productionConfig{}, errors.New("invalid worker lifecycle timing")
	}
	// InMemory is deliberately restricted to an explicitly declared local
	// single-shard worker. It has no shared visibility, so allowing it in the
	// normal multi-shard path would silently introduce sticky-session state.
	if config.MemoryAllowInMemory && (config.WorkerShardCount != 1 || len(config.WorkerShards) != 1 || config.WorkerShards[0] != 0) {
		return productionConfig{}, errors.New("in-memory memory requires one local worker shard")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.RedisAddress == "" || config.SecretRoot == "" ||
		config.RedisEnvironment == "" || config.WorkerGroup == "" || config.WorkerControlGroup == "" || config.WorkerProbeTenant == "" ||
		config.PayloadKeyRef == "" || config.S3Region == "" || config.S3Bucket == "" {
		return productionConfig{}, errors.New("required worker dependency configuration is missing")
	}
	if config.MCPEndpoints, err = parseMCPEndpoints(getenv("TRPC_MCP_ENDPOINTS")); err != nil {
		return productionConfig{}, err
	}
	if config.CodeExecutors, err = parseCodeExecutorEndpoints(getenv("TRPC_CODE_EXECUTORS")); err != nil {
		return productionConfig{}, err
	}
	config.CodeExecutorWorkspaceRoot = strings.TrimSpace(getenv("TRPC_CODE_EXECUTOR_WORKSPACE_ROOT"))
	if len(config.CodeExecutors) != 0 && (config.CodeExecutorWorkspaceRoot == "" ||
		!filepath.IsAbs(config.CodeExecutorWorkspaceRoot) || filepath.Clean(config.CodeExecutorWorkspaceRoot) != config.CodeExecutorWorkspaceRoot ||
		config.CodeExecutorWorkspaceRoot == string(filepath.Separator)) {
		return productionConfig{}, errors.New("invalid TRPC_CODE_EXECUTOR_WORKSPACE_ROOT")
	}
	return config, nil
}

// parseSessionPostgresConnections keeps PostgreSQL credentials in deployment
// configuration rather than in tenant Backend Profiles. The mandatory default
// preserves the single-database deployment; additional JSON entries are named
// data planes selected only through immutable profile connection_id values.
func parseSessionPostgresConnections(defaultDSN, raw string) (map[string]string, error) {
	defaultDSN = strings.TrimSpace(defaultDSN)
	if !validSessionPostgresDSN(defaultDSN) {
		return nil, errors.New("default session postgres DSN is invalid")
	}
	result := map[string]string{"default": defaultDSN}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result, nil
	}
	var declared map[string]string
	if err := json.Unmarshal([]byte(raw), &declared); err != nil || len(declared) == 0 {
		return nil, errors.New("connection registry must be a non-empty JSON object")
	}
	for connectionID, dsn := range declared {
		// v1 Backend Profiles are permanently bound to the original control
		// plane default. A named v2 Profile is required for a different data
		// plane; allowing this registry to replace default would silently alter
		// historical Profile semantics.
		if connectionID == "default" || !validSessionConnectionID(connectionID) || !validSessionPostgresDSN(dsn) {
			return nil, errors.New("invalid session postgres connection")
		}
		result[connectionID] = strings.TrimSpace(dsn)
	}
	return result, nil
}

func validSessionConnectionID(value string) bool {
	if len(value) == 0 || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validSessionPostgresDSN(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && !strings.ContainsAny(value, "\x00\r\n")
}

// parseMemoryRedisConnections mirrors the PostgreSQL deployment registry:
// Backend Profiles select opaque IDs, while Redis URLs (and therefore
// credentials) stay exclusively in deployment configuration. The default
// reuses the Worker coordination plane for small deployments; production may
// provide separate named Memory planes with TRPC_MEMORY_REDIS_CONNECTIONS.
func parseMemoryRedisConnections(address, password string, database int, raw string) (map[string]string, error) {
	if strings.TrimSpace(address) == "" || database < 0 {
		return nil, errors.New("default memory redis connection is invalid")
	}
	defaultURL := url.URL{Scheme: "redis", Host: strings.TrimSpace(address), Path: "/" + strconv.Itoa(database)}
	if password != "" {
		defaultURL.User = url.UserPassword("default", password)
	}
	result := map[string]string{"default": defaultURL.String()}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result, nil
	}
	var declared map[string]string
	if err := json.Unmarshal([]byte(raw), &declared); err != nil || len(declared) == 0 {
		return nil, errors.New("memory redis connection registry must be a non-empty JSON object")
	}
	for connectionID, redisURL := range declared {
		if connectionID == "default" || !validSessionConnectionID(connectionID) || !validMemoryRedisURL(redisURL) {
			return nil, errors.New("invalid memory redis connection")
		}
		result[connectionID] = strings.TrimSpace(redisURL)
	}
	return result, nil
}

func validMemoryRedisURL(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "redis" || parsed.Scheme == "rediss") && parsed.Host != "" && parsed.Fragment == ""
}

// parseMemoryMem0Connections accepts an opt-in JSON registry such as
// {"cloud":{"host":"https://api.mem0.ai"},"oss":{"host":"http://mem0:8888","self_hosted_oss":true}}.
// It intentionally has no default: selecting an external backend must be an
// explicit deployment decision.
func parseMemoryMem0Connections(raw string) (map[string]mem0Connection, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]mem0Connection{}, nil
	}
	var declared map[string]mem0Connection
	if err := json.Unmarshal([]byte(raw), &declared); err != nil || len(declared) == 0 {
		return nil, errors.New("mem0 connection registry must be a non-empty JSON object")
	}
	result := make(map[string]mem0Connection, len(declared))
	for id, connection := range declared {
		connection.Host = strings.TrimSpace(connection.Host)
		endpoint, err := url.Parse(connection.Host)
		if !validSessionConnectionID(id) || err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
			(endpoint.Scheme != "https" && (!connection.SelfHostedOSS || endpoint.Scheme != "http")) || endpoint.Path != "" && endpoint.Path != "/" {
			return nil, errors.New("invalid mem0 connection")
		}
		result[id] = connection
	}
	return result, nil
}

// mcpEndpoint is one reviewed, fixed MCP tool endpoint declared in worker
// configuration. It is not a tenant-submitted payload: the endpoint must map to
// exactly one Tool Catalog registration and be bound to a published revision
// before any Agent can call it. Every field is re-validated by the mcp adapter
// at assembly time so a malformed declaration refuses startup.
type mcpEndpoint struct {
	TenantID          string
	ToolID            string
	Version           int64
	Transport         string
	ServerURL         string
	DeclarationDigest string
	Timeout           time.Duration
	SecretRef         string
	SecretVersion     int64
	SecretHeader      string
	SecretPrefix      string
}

type mcpEndpointJSON struct {
	TenantID          string `json:"tenant_id"`
	ToolID            string `json:"tool_id"`
	Version           int64  `json:"version"`
	Transport         string `json:"transport"`
	ServerURL         string `json:"server_url"`
	DeclarationDigest string `json:"declaration_digest"`
	Timeout           string `json:"timeout"`
	SecretRef         string `json:"secret_ref"`
	SecretVersion     int64  `json:"secret_version"`
	SecretHeader      string `json:"secret_header"`
	SecretPrefix      string `json:"secret_prefix"`
}

func parseMCPEndpoints(raw string) ([]mcpEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var specs []mcpEndpointJSON
	if err := json.Unmarshal([]byte(raw), &specs); err != nil || len(specs) == 0 {
		return nil, errors.New("invalid TRPC_MCP_ENDPOINTS JSON")
	}
	result := make([]mcpEndpoint, 0, len(specs))
	for _, spec := range specs {
		endpoint := mcpEndpoint{TenantID: strings.TrimSpace(spec.TenantID), ToolID: strings.TrimSpace(spec.ToolID),
			Version: spec.Version, Transport: strings.TrimSpace(spec.Transport), ServerURL: strings.TrimSpace(spec.ServerURL),
			DeclarationDigest: strings.TrimSpace(spec.DeclarationDigest),
			SecretRef:         strings.TrimSpace(spec.SecretRef), SecretVersion: spec.SecretVersion,
			SecretHeader: strings.TrimSpace(spec.SecretHeader), SecretPrefix: spec.SecretPrefix}
		if endpoint.TenantID == "" || endpoint.ToolID == "" || endpoint.Version < 1 || endpoint.Transport == "" || endpoint.ServerURL == "" || endpoint.DeclarationDigest == "" {
			return nil, errors.New("invalid TRPC_MCP_ENDPOINTS entry")
		}
		if (endpoint.SecretRef == "") != (endpoint.SecretVersion == 0) {
			return nil, errors.New("invalid TRPC_MCP_ENDPOINTS secret binding")
		}
		var err error
		if endpoint.Timeout, err = time.ParseDuration(spec.Timeout); err != nil || endpoint.Timeout <= 0 {
			return nil, errors.New("invalid TRPC_MCP_ENDPOINTS timeout")
		}
		result = append(result, endpoint)
	}
	return result, nil
}

// codeExecutorEndpoint is one fixed, tenant-scoped sandbox execution tool.
// It is operator configuration rather than tenant input. The digest is the
// reviewed identity of the service-owned sandbox policy and must be carried by
// the published ToolRef before the model can see this tool.
type codeExecutorEndpoint struct {
	TenantID      string
	ToolID        string
	Version       int64
	ContentDigest string
}

type codeExecutorEndpointJSON struct {
	TenantID      string `json:"tenant_id"`
	ToolID        string `json:"tool_id"`
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest"`
}

func parseCodeExecutorEndpoints(raw string) ([]codeExecutorEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var specs []codeExecutorEndpointJSON
	if err := json.Unmarshal([]byte(raw), &specs); err != nil || len(specs) == 0 {
		return nil, errors.New("invalid TRPC_CODE_EXECUTORS JSON")
	}
	seen := make(map[string]struct{}, len(specs))
	result := make([]codeExecutorEndpoint, 0, len(specs))
	for _, spec := range specs {
		endpoint := codeExecutorEndpoint{TenantID: strings.TrimSpace(spec.TenantID), ToolID: strings.TrimSpace(spec.ToolID),
			Version: spec.Version, ContentDigest: strings.TrimSpace(spec.ContentDigest)}
		if endpoint.TenantID == "" || endpoint.ToolID == "" || endpoint.Version < 1 || !validSHA256Digest(endpoint.ContentDigest) {
			return nil, errors.New("invalid TRPC_CODE_EXECUTORS entry")
		}
		key := endpoint.TenantID + "\x00" + endpoint.ToolID + "\x00" + strconv.FormatInt(endpoint.Version, 10)
		if _, exists := seen[key]; exists {
			return nil, errors.New("duplicate TRPC_CODE_EXECUTORS entry")
		}
		seen[key] = struct{}{}
		result = append(result, endpoint)
	}
	return result, nil
}

func validSHA256Digest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func parseWorkerShards(raw string, count int) ([]uint32, error) {
	seen := make(map[uint32]struct{})
	result := make([]uint32, 0)
	for _, part := range strings.Split(raw, ",") {
		value, err := strconv.ParseUint(strings.TrimSpace(part), 10, 32)
		if err != nil || value >= uint64(count) {
			return nil, errors.New("invalid shard")
		}
		shard := uint32(value)
		if _, exists := seen[shard]; exists {
			return nil, errors.New("duplicate shard")
		}
		seen[shard] = struct{}{}
		result = append(result, shard)
	}
	if len(result) == 0 {
		return nil, errors.New("empty shards")
	}
	return result, nil
}

func loadProductionConfig(getenv func(string) string) (productionConfig, error) {
	return loadRoleConfig(getenv, true, false)
}

func loadPreprocessConfig(getenv func(string) string) (productionConfig, error) {
	config, err := loadRoleConfig(getenv, false, true)
	if err != nil {
		return productionConfig{}, err
	}
	config.ArtifactRetention, err = envDuration(getenv, "TRPC_PREPROCESS_ARTIFACT_RETENTION", 0)
	if err != nil {
		return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_ARTIFACT_RETENTION")
	}
	if config.ArtifactRetention < time.Second || config.ArtifactRetention%time.Second != 0 {
		return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_ARTIFACT_RETENTION")
	}
	return config, nil
}

func loadChannelConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		ChannelProbeTenant: strings.TrimSpace(getenv("TRPC_CHANNEL_PROBE_TENANT_ID")),
		ProbeTimeout:       5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second,
		ChannelCandidateTTL: 30 * time.Second, ChannelCallbackMaxBody: 1 << 20}
	var err error
	if config.WebUIEnabled, err = envBool(getenv, "TRPC_WEBUI_ENABLED", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_WEBUI_ENABLED")
	}
	if config.WebUIEnabled {
		config.RedisAddress = strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS"))
		config.RedisPassword = getenv("TRPC_REDIS_PASSWORD")
		config.RedisEnvironment = strings.TrimSpace(getenv("TRPC_REDIS_ENVIRONMENT"))
		if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
			return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
		}
		if config.RedisAddress == "" || config.RedisEnvironment == "" {
			return productionConfig{}, errors.New("WebUI progress requires Redis configuration")
		}
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	config.ProbeTimeout, err = envDuration(getenv, "TRPC_PROBE_TIMEOUT", config.ProbeTimeout)
	if err != nil || config.ProbeTimeout < time.Millisecond {
		return productionConfig{}, errors.New("invalid TRPC_PROBE_TIMEOUT")
	}
	config.ProbeInterval, err = envDuration(getenv, "TRPC_PROBE_INTERVAL", config.ProbeInterval)
	if err != nil || config.ProbeInterval < time.Millisecond {
		return productionConfig{}, errors.New("invalid TRPC_PROBE_INTERVAL")
	}
	config.ShutdownTimeout, err = envDuration(getenv, "TRPC_SHUTDOWN_TIMEOUT", config.ShutdownTimeout)
	if err != nil || config.ShutdownTimeout < time.Second {
		return productionConfig{}, errors.New("invalid TRPC_SHUTDOWN_TIMEOUT")
	}
	config.ChannelCandidateTTL, err = envDuration(getenv, "TRPC_CHANNEL_CANDIDATE_TTL", config.ChannelCandidateTTL)
	if err != nil || config.ChannelCandidateTTL < time.Second || config.ChannelCandidateTTL > 10*time.Minute {
		return productionConfig{}, errors.New("invalid TRPC_CHANNEL_CANDIDATE_TTL")
	}
	if config.ChannelCallbackMaxBody, err = envInt64(getenv, "TRPC_CHANNEL_CALLBACK_MAX_BODY", config.ChannelCallbackMaxBody); err != nil ||
		config.ChannelCallbackMaxBody < 1 || config.ChannelCallbackMaxBody > 16<<20 {
		return productionConfig{}, errors.New("invalid TRPC_CHANNEL_CALLBACK_MAX_BODY")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.SecretRoot == "" || config.ChannelProbeTenant == "" || strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")) == "" {
		return productionConfig{}, errors.New("required channel dependency configuration is missing")
	}
	config.PayloadKeyRef = strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF"))
	return config, nil
}

func loadChannelDeliveryConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword: getenv("TRPC_REDIS_PASSWORD"), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		RedisEnvironment: strings.TrimSpace(getenv("TRPC_REDIS_ENVIRONMENT")), ChannelDeliveryGroup: strings.TrimSpace(getenv("TRPC_CHANNEL_DELIVERY_GROUP")),
		ChannelProbeTenant: strings.TrimSpace(getenv("TRPC_CHANNEL_PROBE_TENANT_ID")), PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		ProbeTimeout: 5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second,
		ChannelDeliveryRefresh: 15 * time.Second, ChannelReplyReadBlock: time.Second, ChannelReplyReclaimIdle: 30 * time.Second,
		ChannelReplyReclaimInterval: 5 * time.Second, ChannelDeliveryClaimTTL: 30 * time.Second, ChannelDeliveryClaimRenew: 10 * time.Second,
		ChannelDeliveryRetryDelay: time.Second, ChannelDeliveryMaxRetry: time.Minute,
		ChannelProviderTimeout:   15 * time.Second,
		ChannelReplyReclaimLimit: 100, ChannelDeliveryMaxAttempts: 8, ChannelDeliveryMaxReconcile: 8}
	var err error
	if config.WebUIEnabled, err = envBool(getenv, "TRPC_WEBUI_ENABLED", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_WEBUI_ENABLED")
	}
	if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
		return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond}, {"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second}, {"TRPC_CHANNEL_DELIVERY_REFRESH", &config.ChannelDeliveryRefresh, time.Millisecond},
		{"TRPC_CHANNEL_REPLY_READ_BLOCK", &config.ChannelReplyReadBlock, time.Millisecond}, {"TRPC_CHANNEL_REPLY_RECLAIM_IDLE", &config.ChannelReplyReclaimIdle, time.Millisecond},
		{"TRPC_CHANNEL_REPLY_RECLAIM_INTERVAL", &config.ChannelReplyReclaimInterval, time.Millisecond}, {"TRPC_CHANNEL_DELIVERY_CLAIM_TTL", &config.ChannelDeliveryClaimTTL, time.Millisecond},
		{"TRPC_CHANNEL_DELIVERY_CLAIM_RENEW", &config.ChannelDeliveryClaimRenew, time.Millisecond}, {"TRPC_CHANNEL_DELIVERY_RETRY_DELAY", &config.ChannelDeliveryRetryDelay, time.Millisecond},
		{"TRPC_CHANNEL_DELIVERY_MAX_RETRY", &config.ChannelDeliveryMaxRetry, time.Millisecond},
		{"TRPC_CHANNEL_PROVIDER_TIMEOUT", &config.ChannelProviderTimeout, time.Millisecond},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"TRPC_CHANNEL_REPLY_RECLAIM_LIMIT", &config.ChannelReplyReclaimLimit}, {"TRPC_CHANNEL_DELIVERY_MAX_ATTEMPTS", &config.ChannelDeliveryMaxAttempts},
		{"TRPC_CHANNEL_DELIVERY_MAX_RECONCILE", &config.ChannelDeliveryMaxReconcile},
	} {
		if *item.target, err = envInt(getenv, item.name, *item.target); err != nil || *item.target < 1 || *item.target > 1000 {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ChannelDeliveryClaimRenew >= config.ChannelDeliveryClaimTTL {
		return productionConfig{}, errors.New("channel delivery claim renew must be shorter than claim TTL")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.RedisAddress == "" || config.SecretRoot == "" || config.RedisEnvironment == "" ||
		config.ChannelDeliveryGroup == "" || config.ChannelProbeTenant == "" || config.PayloadKeyRef == "" {
		return productionConfig{}, errors.New("required channel delivery dependency configuration is missing")
	}
	return config, nil
}

func loadRoleConfig(getenv func(string) string, requireRedis, requirePreprocess bool) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress:          valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:            strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		RedisAddress:           strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword:          getenv("TRPC_REDIS_PASSWORD"),
		SecretRoot:             strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		S3Region:               strings.TrimSpace(getenv("TRPC_S3_REGION")),
		S3Bucket:               strings.TrimSpace(getenv("TRPC_S3_BUCKET")),
		S3Endpoint:             strings.TrimSpace(getenv("TRPC_S3_ENDPOINT")),
		ClamAVAddress:          strings.TrimSpace(getenv("TRPC_CLAMAV_ADDRESS")),
		DLPEndpoint:            strings.TrimSpace(getenv("TRPC_DLP_ENDPOINT")),
		DLPProbeTenant:         strings.TrimSpace(getenv("TRPC_DLP_PROBE_TENANT_ID")),
		DLPSecretRef:           strings.TrimSpace(getenv("TRPC_DLP_SECRET_REF")),
		PayloadKeyRef:          strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		ProbeTimeout:           5 * time.Second,
		ProbeInterval:          15 * time.Second,
		ShutdownTimeout:        30 * time.Second,
		ArtifactPutTimeout:     30 * time.Second,
		UploadProtection:       2 * time.Minute,
		UploadClaimTTL:         time.Minute,
		ArtifactClaimTTL:       time.Minute,
		UploadPollInterval:     time.Minute,
		ArtifactPollInterval:   time.Minute,
		ArtifactOrphanGrace:    24 * time.Hour,
		LifecycleBatchSize:     100,
		LifecycleMaxAttempts:   8,
		S3MaxBytes:             16 << 20,
		PreprocessBatchSize:    100,
		PreprocessMaxAttempts:  8,
		PreprocessLeaseTTL:     30 * time.Second,
		PreprocessRetryDelay:   time.Second,
		PreprocessPollInterval: time.Second,
		MediaFetchTimeout:      15 * time.Second,
	}
	var err error
	if requireRedis {
		if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
			return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
		}
	}
	if config.DLPSecretVersion, err = envInt64(getenv, "TRPC_DLP_SECRET_VERSION", 0); err != nil || config.DLPSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_SECRET_VERSION")
	}
	if config.DLPBackendVersion, err = envInt64(getenv, "TRPC_DLP_BACKEND_VERSION", 0); err != nil || config.DLPBackendVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_BACKEND_VERSION")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	for name, target := range map[string]*bool{
		"TRPC_S3_PATH_STYLE": &config.S3PathStyle, "TRPC_S3_ALLOW_INSECURE": &config.S3AllowInsecure,
		"TRPC_DLP_ALLOW_INSECURE": &config.DLPAllowInsecure,
	} {
		if *target, err = envBool(getenv, name, false); err != nil {
			return productionConfig{}, errors.New("invalid " + name)
		}
	}
	if requirePreprocess {
		config.MediaAllowedHosts = envCSV(getenv, "TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS")
	}
	durations := []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_ARTIFACT_PUT_TIMEOUT", &config.ArtifactPutTimeout, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_PROTECTION", &config.UploadProtection, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_CLAIM_TTL", &config.UploadClaimTTL, time.Second},
		{"TRPC_ARTIFACT_RETENTION_CLAIM_TTL", &config.ArtifactClaimTTL, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_POLL_INTERVAL", &config.UploadPollInterval, time.Millisecond},
		{"TRPC_ARTIFACT_RETENTION_POLL_INTERVAL", &config.ArtifactPollInterval, time.Millisecond},
		{"TRPC_ARTIFACT_ORPHAN_GRACE", &config.ArtifactOrphanGrace, time.Minute},
	}
	if requirePreprocess {
		durations = append(durations,
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_LEASE_TTL", &config.PreprocessLeaseTTL, time.Second},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_RETRY_DELAY", &config.PreprocessRetryDelay, time.Millisecond},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_POLL_INTERVAL", &config.PreprocessPollInterval, time.Millisecond},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_MEDIA_FETCH_TIMEOUT", &config.MediaFetchTimeout, time.Millisecond},
		)
	}
	for _, item := range durations {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.LifecycleBatchSize, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE", config.LifecycleBatchSize); err != nil || config.LifecycleBatchSize < 1 || config.LifecycleBatchSize > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE")
	}
	if requirePreprocess {
		if config.PreprocessBatchSize, err = envInt(getenv, "TRPC_PREPROCESS_BATCH_SIZE", config.PreprocessBatchSize); err != nil || config.PreprocessBatchSize < 1 || config.PreprocessBatchSize > 1000 {
			return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_BATCH_SIZE")
		}
		if config.PreprocessMaxAttempts, err = envInt(getenv, "TRPC_PREPROCESS_MAX_ATTEMPTS", config.PreprocessMaxAttempts); err != nil || config.PreprocessMaxAttempts < 1 || config.PreprocessMaxAttempts > 100 {
			return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_MAX_ATTEMPTS")
		}
	}
	if config.LifecycleMaxAttempts, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS", config.LifecycleMaxAttempts); err != nil || config.LifecycleMaxAttempts < 1 || config.LifecycleMaxAttempts > 100 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS")
	}
	maxBytes, err := envInt64(getenv, "TRPC_ARTIFACT_MAX_BYTES", config.S3MaxBytes)
	if err != nil || maxBytes < 1 || uint64(maxBytes) > uint64(^uint(0)>>1) {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_MAX_BYTES")
	}
	config.S3MaxBytes = maxBytes
	if config.ListenAddress == "" || config.PostgresDSN == "" || (requireRedis && config.RedisAddress == "") || config.SecretRoot == "" || config.S3Region == "" ||
		config.S3Bucket == "" || config.ClamAVAddress == "" ||
		config.DLPEndpoint == "" || config.DLPProbeTenant == "" || config.DLPSecretRef == "" || config.PayloadKeyRef == "" {
		return productionConfig{}, errors.New("required production dependency configuration is missing")
	}
	if config.ArtifactPutTimeout >= config.UploadProtection {
		return productionConfig{}, errors.New("artifact put timeout must be shorter than upload protection")
	}
	return config, nil
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envBool(getenv func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseBool(value)
}

func envDuration(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func envInt(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func envInt64(getenv func(string) string, name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func envCSV(getenv func(string) string, name string) []string {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		result = append(result, part)
	}
	return result
}
