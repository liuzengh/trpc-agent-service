// Package config loads the service's own configuration (port, DB, Redis,
// logging) and provides the Secret Resolver abstraction.
//
// It only covers service-level config; tenant-level config (model_config
// etc.) lives in the PG tenant tables and is owned by the tenant module.
//
// Configuration comes from environment variables only (12-factor / K8s
// friendly), no config files. Secrets never appear in plaintext config —
// only references, resolved at runtime through a SecretResolver.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// AdminTokenDevInsecure is the only accepted way to run the Admin API
// without a real bearer token: an empty value refuses to start, so local
// development must state the intent. The Admin API is internal only and
// must never be reachable unauthenticated by accident.
const AdminTokenDevInsecure = "dev-insecure"

// Config aggregates the service's own configuration.
type Config struct {
	HTTPAddr string // TRPC_HTTP_ADDR: Gateway/HTTP listen address
	// AdminAddr is the Admin API listen address. Every role that serves the
	// Admin API gets this listener of its own, all-in-one included, so the
	// management surface never shares the gateway's public callback address.
	// Bound to loopback by default: the Admin API is internal only and a
	// deployment must opt in explicitly to expose it beyond the host.
	AdminAddr string // TRPC_ADMIN_ADDR
	// MetricsAddr is the internal listener every role serves /metrics on.
	// Deliberately not the gateway's callback address: that listener faces
	// the IM platforms, and the exported series carry per-tenant traffic
	// volumes, token spend and queue depth.
	MetricsAddr string // TRPC_METRICS_ADDR
	PGDSN       string // TRPC_PG_DSN: PostgreSQL DSN (required for worker/admin roles)
	RedisAddr   string // TRPC_REDIS_ADDR: Redis address (required for gateway/worker roles)
	LogLevel    string // TRPC_LOG_LEVEL: debug/info/warn/error
	LogFormat   string // TRPC_LOG_FORMAT: "json" for JSON output, anything else for console
	SecretsDir  string // TRPC_SECRETS_DIR: key directory for the local file-based SecretResolver

	ModelBaseURL string // TRPC_MODEL_BASE_URL: OpenAI-compatible endpoint (DeepSeek default)
	// ModelBaseURLAllow is the platform-level allowlist of model endpoint
	// hosts (comma-separated, exact host match) that tenant and app configs
	// may point at. All user messages travel to that endpoint, so it is
	// platform policy, not tenant policy. Empty means "the host of
	// ModelBaseURL only".
	ModelBaseURLAllow string // TRPC_MODEL_BASE_URL_ALLOW
	ModelName         string // TRPC_MODEL_NAME: model name, e.g. deepseek-v4-flash (cheapest)
	ModelAPIKeyRef    string // TRPC_MODEL_APIKEY_REF: secret ref (NOT the key itself) resolved via SecretResolver
	// ModelTimeout bounds one model run: 60s deadline, retry once, then a
	// busy reply. Go duration syntax.
	ModelTimeout string // TRPC_MODEL_TIMEOUT
	// ModelPrices maps model name to USD per 1M tokens for cost accounting
	// (audit_log.cost), JSON: {"deepseek-v4-flash":[0.1,0.4]}. Empty means
	// cost is not tracked (token counts always are).
	ModelPrices string // TRPC_MODEL_PRICES

	// SessionBackend selects the session store: "redis" (default, hot data)
	// or "postgres" (event journal + snapshot).
	SessionBackend string // TRPC_SESSION_BACKEND
	// AppName is the fallback runner app name for messages the Gateway could
	// not route (tenant routing disabled, e.g. PG down at startup). Routed
	// messages run under their own agent_app ID; the env model config above
	// serves as the lowest-precedence default beneath tenant.model_config and
	// agent_app.config. The default is the seed demo app.
	AppName string // TRPC_APP_NAME

	// WeCom channel: enabled when TRPC_WECOM_CORP_ID is set. All secret
	// material is referenced, resolved via SecretResolver.
	WecomCorpID    string // TRPC_WECOM_CORP_ID
	WecomAgentID   string // TRPC_WECOM_AGENT_ID (integer)
	WecomTokenRef  string // TRPC_WECOM_TOKEN_REF: callback token secret ref
	WecomAESKeyRef string // TRPC_WECOM_AESKEY_REF: EncodingAESKey secret ref
	WecomSecretRef string // TRPC_WECOM_SECRET_REF: corpsecret secret ref
	WecomAPIBase   string // TRPC_WECOM_API_BASE: default https://qyapi.weixin.qq.com

	// WeChat KF channel: enabled when both are set. Same secret-ref
	// discipline; the KF secret is independent of the WeCom corpsecret.
	WxkfCorpID    string // TRPC_WXKF_CORP_ID
	WxkfKfAccount string // TRPC_WXKF_KF_ACCOUNT: open_kfid
	WxkfTokenRef  string // TRPC_WXKF_TOKEN_REF
	WxkfAESKeyRef string // TRPC_WXKF_AESKEY_REF
	WxkfSecretRef string // TRPC_WXKF_SECRET_REF
	WxkfAPIBase   string // TRPC_WXKF_API_BASE

	// WeCom smart-bot WebSocket channel: enabled when TRPC_WECOMWS_ADDR is
	// set (wss://openws.work.weixin.qq.com); unset keeps the channel off
	// without touching the other channels. Bots and their secrets live in
	// channel_binding.config (bot_id + secret_ref), not in env.
	WecomwsAddr           string // TRPC_WECOMWS_ADDR
	WecomwsPingInterval   string // TRPC_WECOMWS_PING_INTERVAL: heartbeat cadence (30s)
	WecomwsLeaderTTL      string // TRPC_WECOMWS_LEADER_TTL: leader lease (15s, renewed every ttl/3)
	WecomwsResyncInterval string // TRPC_WECOMWS_RESYNC_INTERVAL: bindings reconciliation cadence (15s)
	WecomwsSegmentBytes   string // TRPC_WECOMWS_SEGMENT_BYTES: per-frame reply cap (2048)

	// MockChannel enables the built-in mock channel (demo/dev). It is off by
	// default: the mock callback is an unauthenticated message injector, so
	// a deployment must opt in explicitly (internal network only).
	MockChannel string // TRPC_MOCK_CHANNEL: "true" to enable (dev only)

	// AdminToken guards the Admin API (Authorization: Bearer). An unset token
	// is a fatal misconfiguration at startup, not dev mode: the API can
	// repoint a tenant's model endpoint and rewrite its policies. Local dev
	// opts out with the explicit AdminTokenDevInsecure sentinel.
	AdminToken string // TRPC_ADMIN_TOKEN

	// Admin mTLS (split admin role only): when all three are set, the admin
	// listener serves TLS and requires client certificates signed by the
	// given CA.
	AdminTLSCert     string // TRPC_ADMIN_TLS_CERT
	AdminTLSKey      string // TRPC_ADMIN_TLS_KEY
	AdminTLSClientCA string // TRPC_ADMIN_TLS_CLIENT_CA

	// SecretResolverType selects the secret backend: "file" (local dev,
	// default) or "kms" (KMS sidecar / Vault agent at TRPC_KMS_ENDPOINT). The
	// KMS bearer token is itself a secret, resolved from TRPC_KMS_TOKEN_REF
	// through the file resolver. Every backend is wrapped in the short-TTL
	// process cache (TRPC_SECRET_CACHE_TTL). KMSTokenRef's default names the
	// bootstrap token file the deployed KMS sidecar reads, so a dropped env
	// var still resolves.
	SecretResolverType string // TRPC_SECRET_RESOLVER
	KMSEndpoint        string // TRPC_KMS_ENDPOINT
	KMSTokenRef        string // TRPC_KMS_TOKEN_REF
	SecretCacheTTL     string // TRPC_SECRET_CACHE_TTL

	// GatewayRateQPS / GatewayRateBurst are the platform default for the
	// per-tenant admission token bucket; tenant.rate_policy overrides per
	// tenant.
	GatewayRateQPS   string // TRPC_GATEWAY_RATE_QPS
	GatewayRateBurst string // TRPC_GATEWAY_RATE_BURST

	// SendRateQPS / SendRateBurst pace outbound IM sends per
	// {channel, tenant} (IM proactive-send rate limits).
	SendRateQPS   string // TRPC_SEND_RATE_QPS
	SendRateBurst string // TRPC_SEND_RATE_BURST

	// SummaryEventThreshold is the number of uncovered events that triggers
	// session summarization (PG session backend only). ArchiveRetention is
	// how long session_event/audit_log rows stay in the hot tables before the
	// archive sweep moves them; ArchiveInterval is the sweep cadence. Go
	// duration syntax for the two intervals.
	SummaryEventThreshold string // TRPC_SUMMARY_EVENT_THRESHOLD
	ArchiveRetention      string // TRPC_ARCHIVE_RETENTION
	ArchiveInterval       string // TRPC_ARCHIVE_INTERVAL

	// MigrationObserve is the dual-write observation window after a migration
	// read switch (default 24h). Go duration syntax.
	MigrationObserve string // TRPC_MIGRATION_OBSERVE

	// Artifact S3 backend (MinIO / cloud OSS). Secret refs only; disabled when
	// the endpoint is unreachable at startup.
	S3Endpoint     string // TRPC_S3_ENDPOINT
	S3Bucket       string // TRPC_S3_BUCKET
	S3AccessKeyRef string // TRPC_S3_ACCESSKEY_REF
	S3SecretKeyRef string // TRPC_S3_SECRETKEY_REF
	S3Secure       string // TRPC_S3_SECURE: "true" for TLS (cloud OSS)

	// Embedder config for Knowledge (pgvector). Disabled when
	// TRPC_EMBEDDER_MODEL is unset: the default chat endpoint (DeepSeek) has
	// no embeddings API, so point these at an embeddings-capable
	// OpenAI-compatible endpoint.
	EmbedderBaseURL string // TRPC_EMBEDDER_BASE_URL
	EmbedderModel   string // TRPC_EMBEDDER_MODEL
	EmbedderKeyRef  string // TRPC_EMBEDDER_APIKEY_REF: secret ref
	EmbedderDim     string // TRPC_EMBEDDER_DIMENSION: vector size, default 1536
	KnowledgeTable  string // TRPC_KNOWLEDGE_TABLE: pgvector table, default knowledge_embeddings
}

// Load reads configuration from environment variables, filling defaults for
// unset ones. Roles that need PG/Redis (gateway/worker/admin) validate those
// fields at startup and fail fast.
func Load() Config {
	return Config{
		HTTPAddr:  getenv("TRPC_HTTP_ADDR", ":8080"),
		AdminAddr: getenv("TRPC_ADMIN_ADDR", "127.0.0.1:8081"),
		// Loopback by default, like AdminAddr: /metrics carries per-tenant
		// traffic and cost with no auth of its own. Deployments that expose
		// it (k8s readiness probes, cluster scraping) set TRPC_METRICS_ADDR
		// to an all-interfaces bind explicitly.
		MetricsAddr: getenv("TRPC_METRICS_ADDR", "127.0.0.1:8082"),
		PGDSN:       getenv("TRPC_PG_DSN", "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable"),
		RedisAddr:   getenv("TRPC_REDIS_ADDR", "localhost:6380"), // host port 6379 is often taken by other local services
		LogLevel:    getenv("TRPC_LOG_LEVEL", "info"),
		LogFormat:   getenv("TRPC_LOG_FORMAT", "console"),
		SecretsDir:  getenv("TRPC_SECRETS_DIR", "data/secrets"),

		ModelBaseURL:      getenv("TRPC_MODEL_BASE_URL", "https://api.deepseek.com"),
		ModelBaseURLAllow: getenv("TRPC_MODEL_BASE_URL_ALLOW", ""),
		ModelName:         getenv("TRPC_MODEL_NAME", "deepseek-v4-flash"),
		ModelAPIKeyRef:    getenv("TRPC_MODEL_APIKEY_REF", "deepseek-apikey"),
		ModelTimeout:      getenv("TRPC_MODEL_TIMEOUT", "60s"),
		ModelPrices:       getenv("TRPC_MODEL_PRICES", ""),

		SessionBackend: getenv("TRPC_SESSION_BACKEND", "redis"),
		AppName:        getenv("TRPC_APP_NAME", "00000000-0000-0000-0000-000000000101"),

		WecomCorpID:    getenv("TRPC_WECOM_CORP_ID", ""),
		WecomAgentID:   getenv("TRPC_WECOM_AGENT_ID", ""),
		WecomTokenRef:  getenv("TRPC_WECOM_TOKEN_REF", "wecom-token"),
		WecomAESKeyRef: getenv("TRPC_WECOM_AESKEY_REF", "wecom-aeskey"),
		WecomSecretRef: getenv("TRPC_WECOM_SECRET_REF", "wecom-secret"),
		WecomAPIBase:   getenv("TRPC_WECOM_API_BASE", "https://qyapi.weixin.qq.com"),

		WxkfCorpID:    getenv("TRPC_WXKF_CORP_ID", ""),
		WxkfKfAccount: getenv("TRPC_WXKF_KF_ACCOUNT", ""),
		WxkfTokenRef:  getenv("TRPC_WXKF_TOKEN_REF", "wxkf-token"),
		WxkfAESKeyRef: getenv("TRPC_WXKF_AESKEY_REF", "wxkf-aeskey"),
		WxkfSecretRef: getenv("TRPC_WXKF_SECRET_REF", "wxkf-secret"),
		WxkfAPIBase:   getenv("TRPC_WXKF_API_BASE", "https://qyapi.weixin.qq.com"),

		WecomwsAddr:           getenv("TRPC_WECOMWS_ADDR", ""),
		WecomwsPingInterval:   getenv("TRPC_WECOMWS_PING_INTERVAL", "30s"),
		WecomwsLeaderTTL:      getenv("TRPC_WECOMWS_LEADER_TTL", "15s"),
		WecomwsResyncInterval: getenv("TRPC_WECOMWS_RESYNC_INTERVAL", "15s"),
		WecomwsSegmentBytes:   getenv("TRPC_WECOMWS_SEGMENT_BYTES", "2048"),

		MockChannel: getenv("TRPC_MOCK_CHANNEL", "false"),

		AdminToken: getenv("TRPC_ADMIN_TOKEN", ""),
		// Admin mTLS (split admin role): all three must be set to engage.
		AdminTLSCert:     getenv("TRPC_ADMIN_TLS_CERT", ""),
		AdminTLSKey:      getenv("TRPC_ADMIN_TLS_KEY", ""),
		AdminTLSClientCA: getenv("TRPC_ADMIN_TLS_CLIENT_CA", ""),

		SecretResolverType: getenv("TRPC_SECRET_RESOLVER", "file"),
		KMSEndpoint:        getenv("TRPC_KMS_ENDPOINT", ""),
		KMSTokenRef:        getenv("TRPC_KMS_TOKEN_REF", "kms-bootstrap-token"),
		SecretCacheTTL:     getenv("TRPC_SECRET_CACHE_TTL", "1m"),

		GatewayRateQPS:   getenv("TRPC_GATEWAY_RATE_QPS", "50"),
		GatewayRateBurst: getenv("TRPC_GATEWAY_RATE_BURST", "100"),
		SendRateQPS:      getenv("TRPC_SEND_RATE_QPS", "20"),
		SendRateBurst:    getenv("TRPC_SEND_RATE_BURST", "40"),

		SummaryEventThreshold: getenv("TRPC_SUMMARY_EVENT_THRESHOLD", "20"),
		ArchiveRetention:      getenv("TRPC_ARCHIVE_RETENTION", "720h"), // 30 days online
		ArchiveInterval:       getenv("TRPC_ARCHIVE_INTERVAL", "24h"),
		MigrationObserve:      getenv("TRPC_MIGRATION_OBSERVE", "24h"),

		S3Endpoint:     getenv("TRPC_S3_ENDPOINT", "localhost:9000"),
		S3Bucket:       getenv("TRPC_S3_BUCKET", "artifacts"),
		S3AccessKeyRef: getenv("TRPC_S3_ACCESSKEY_REF", "s3-accesskey"),
		S3SecretKeyRef: getenv("TRPC_S3_SECRETKEY_REF", "s3-secretkey"),
		S3Secure:       getenv("TRPC_S3_SECURE", "false"),

		EmbedderBaseURL: getenv("TRPC_EMBEDDER_BASE_URL", ""),
		EmbedderModel:   getenv("TRPC_EMBEDDER_MODEL", ""),
		EmbedderKeyRef:  getenv("TRPC_EMBEDDER_APIKEY_REF", "embedder-apikey"),
		EmbedderDim:     getenv("TRPC_EMBEDDER_DIMENSION", "1536"),
		KnowledgeTable:  getenv("TRPC_KNOWLEDGE_TABLE", "knowledge_embeddings"),
	}
}

// ModelHostAllowlist returns the hosts a tenant or app config may point its
// model endpoint at: TRPC_MODEL_BASE_URL_ALLOW when set, otherwise the host
// of the platform's own default endpoint. Whoever chooses that endpoint reads
// every message the tenant's users send, so it is platform policy and
// defaults to the platform's own choice rather than to "anything".
func (c Config) ModelHostAllowlist() []string {
	if list := strings.TrimSpace(c.ModelBaseURLAllow); list != "" {
		var hosts []string
		for _, h := range strings.Split(list, ",") {
			if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
				hosts = append(hosts, h)
			}
		}
		return hosts
	}
	if u, err := url.Parse(c.ModelBaseURL); err == nil {
		if h := strings.ToLower(u.Hostname()); h != "" {
			return []string{h}
		}
	}
	return nil
}

// MustEnv reads a required environment variable and returns an error if it is
// unset (for startup validation in roles like worker).
func MustEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("required env %s is not set", key)
	}
	return v, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
