package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	agentsessionstore "github.com/XnLemon/trpc-agent-service/trpcservice/agent/sessionstore"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorages3 "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/s3"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type environmentSecretResolver struct {
	reference string
	value     string
}

type environmentWeComCredentialResolver struct {
	tenantID string
	config   environmentWeComConfig
}

func (resolver environmentWeComCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom.Credentials, error) {
	if ctx == nil {
		return wecom.Credentials{}, errors.New("wecom credential resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return wecom.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID || scope.SecretRef != resolver.config.secretRef {
		return wecom.Credentials{}, errors.New("configured WeCom secret reference is unavailable")
	}
	return wecom.Credentials{CallbackToken: resolver.config.callbackToken, EncodingAESKey: resolver.config.encodingAESKey, AppSecret: resolver.config.appSecret}, nil
}

type environmentWeComAIBotCredentialResolver struct {
	tenantID string
	secrets  map[string]string
}

func (resolver environmentWeComAIBotCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom_aibot.Credentials, error) {
	if ctx == nil {
		return wecom_aibot.Credentials{}, errors.New("wecom ai bot credential resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return wecom_aibot.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	secret := resolver.secrets[scope.SecretRef]
	if secret == "" {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	return wecom_aibot.Credentials{BotSecret: secret}, nil
}

func environmentWeComOwner() (string, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "", errors.New("WeCom worker hostname is unavailable")
	}
	return fmt.Sprintf("wecom-%s-%d", hostname, os.Getpid()), nil
}

func (resolver environmentSecretResolver) Resolve(ctx context.Context, scope modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	if ctx == nil {
		return modelprofile.SecretValue{}, errors.New("secret resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if err := scope.Validate(); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if scope.SecretRef != resolver.reference || resolver.value == "" {
		return modelprofile.SecretValue{}, errors.New("configured secret reference is unavailable")
	}
	return modelprofile.NewSecretValue(resolver.value)
}

type environmentModelFactory struct{}

type environmentSessionCapabilityProvider struct {
	delegate  session.Service
	store     environmentStorage
	telemetry observability.Provider
	backend   string
}

type environmentRuntimeCapabilityProvider struct {
	capability            backend.Capability
	delegate              session.Service
	store                 environmentStorage
	telemetry             observability.Provider
	backend               string
	redisEndpoint         string
	redisSecretRef        string
	redisPasswordRequired bool
}

type environmentS3CapabilityProvider struct {
	tenantID  string
	secretRef string
}

func (provider environmentS3CapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if provider.tenantID == "" || input.TenantID != provider.tenantID || binding.Capability != backend.CapabilityArtifact || strings.ToLower(strings.TrimSpace(binding.Provider)) != "s3" || provider.secretRef == "" || binding.SecretRef != provider.secretRef {
		return nil, storagefactory.ErrStorageFactory
	}
	store, err := newEnvironmentS3Store(ctx, provider.tenantID, binding, secret)
	if err != nil || store == nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, storagefactory.ErrStorageFactory
	}
	if err := store.Probe(ctx); err != nil {
		_ = store.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, storagefactory.ErrStorageFactory
	}
	return store, nil
}

func newEnvironmentS3StoreFromConfig(ctx context.Context, tenantID string, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (environmentS3Store, error) {
	if ctx == nil || ctx.Err() != nil || tenantID == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	accessKey, secretKey, err := parseEnvironmentS3Credentials(binding.SecretRef, secret)
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	endpoint := strings.TrimSpace(binding.Endpoint)
	options, err := parseEnvironmentS3Options(binding.Options)
	if err != nil || !validEnvironmentS3Endpoint(endpoint, options.allowInsecure) {
		return nil, storagefactory.ErrStorageFactory
	}
	cfg := awssdk.Config{
		Region:      options.region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	return runtimestorages3.NewFromConfig(cfg, options.bucket, tenantID, endpoint, options.pathStyle, options.allowInsecure, runtimestorages3.Options{
		MaxBytes: options.maxBytes, ConnectTimeout: options.connectTimeout, ReadTimeout: options.readTimeout, WriteTimeout: options.writeTimeout,
	})
}

func parseEnvironmentS3Credentials(secretRef string, secret modelprofile.SecretValue) (string, string, error) {
	if secretRef == "" || secret.Value() == "" {
		return "", "", storagefactory.ErrStorageFactory
	}
	accessKey, secretKey, ok := strings.Cut(secret.Value(), ":")
	if !ok || strings.TrimSpace(accessKey) == "" || secretKey == "" || strings.ContainsAny(accessKey, "\r\n") || strings.ContainsAny(secretKey, "\r\n") {
		return "", "", storagefactory.ErrStorageFactory
	}
	return accessKey, secretKey, nil
}

func validEnvironmentS3Endpoint(endpoint string, allowInsecure bool) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return false
	}
	return parsed.Scheme == "https" || (parsed.Scheme == "http" && allowInsecure)
}

type environmentS3Options struct {
	bucket, region string
	pathStyle      bool
	allowInsecure  bool
	maxBytes       int64
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

func parseEnvironmentS3Options(raw map[string]string) (environmentS3Options, error) {
	for key := range raw {
		switch key {
		case "bucket", "region", "path_style", "allow_insecure", "max_bytes", "connect_timeout_ms", "read_timeout_ms", "write_timeout_ms":
		default:
			return environmentS3Options{}, storagefactory.ErrStorageFactory
		}
	}
	value := func(key, fallback string) string {
		if item := strings.TrimSpace(raw[key]); item != "" {
			return item
		}
		return fallback
	}
	result := environmentS3Options{bucket: value("bucket", ""), region: value("region", "us-east-1")}
	if result.bucket == "" || result.region == "" || len(result.region) > 128 || strings.ContainsAny(result.region, "\r\n\t ") || !validS3Bucket(result.bucket) {
		return environmentS3Options{}, storagefactory.ErrStorageFactory
	}
	var err error
	if result.pathStyle, err = strconv.ParseBool(value("path_style", "false")); err != nil {
		return environmentS3Options{}, storagefactory.ErrStorageFactory
	}
	if result.allowInsecure, err = strconv.ParseBool(value("allow_insecure", "false")); err != nil {
		return environmentS3Options{}, storagefactory.ErrStorageFactory
	}
	maxBytes, err := strconv.ParseInt(value("max_bytes", "33554432"), 10, 64)
	if err != nil || maxBytes < 1 || maxBytes > 1<<30 {
		return environmentS3Options{}, storagefactory.ErrStorageFactory
	}
	result.maxBytes = maxBytes
	for key, target := range map[string]*time.Duration{
		"connect_timeout_ms": &result.connectTimeout,
		"read_timeout_ms":    &result.readTimeout,
		"write_timeout_ms":   &result.writeTimeout,
	} {
		milliseconds, parseErr := strconv.ParseInt(value(key, "15000"), 10, 64)
		if parseErr != nil || milliseconds < 1 || milliseconds > 300000 {
			return environmentS3Options{}, storagefactory.ErrStorageFactory
		}
		*target = time.Duration(milliseconds) * time.Millisecond
	}
	return result, nil
}

func validS3Bucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.ToLower(bucket) != bucket || net.ParseIP(bucket) != nil || strings.Contains(bucket, "..") {
		return false
	}
	for _, label := range strings.Split(bucket, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, value := range label {
			if (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func (provider environmentRuntimeCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := provider.validateRedisBinding(binding, secret); err != nil {
		return nil, err
	}
	return provider.newCapability(ctx, input)
}

func (provider environmentRuntimeCapabilityProvider) validateRedisBinding(binding backend.CapabilityBinding, secret modelprofile.SecretValue) error {
	if provider.backend != "redis" {
		return nil
	}
	if provider.capability != backend.CapabilitySession && provider.capability != backend.CapabilityMemory {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisEndpoint != "" && binding.Endpoint != provider.redisEndpoint {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisSecretRef != "" && binding.SecretRef != "" && binding.SecretRef != provider.redisSecretRef {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisPasswordRequired && secret.Value() == "" {
		return storagefactory.ErrStorageFactory
	}
	return nil
}

func (provider environmentRuntimeCapabilityProvider) newCapability(ctx context.Context, input backend.StorageFactoryInput) (any, error) {
	if provider.capability == backend.CapabilitySession {
		return agentsessionstore.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
	}
	// The runtime store is owned by the environment, not by an individual
	// tenant CapabilitySet. Wrap it so factory cleanup cannot stop shared
	// workers when one runner is torn down.
	switch provider.capability {
	case backend.CapabilityMemory:
		store, ok := provider.store.(runtimestorage.MemoryStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedMemoryStore{MemoryStore: store}, nil
	case backend.CapabilitySummary:
		store, ok := provider.store.(runtimestorage.SummaryStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedSummaryStore{SummaryStore: store}, nil
	case backend.CapabilityKnowledge:
		knowledge, ok := provider.store.(runtimestorage.KnowledgeStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		vector, ok := provider.store.(runtimestorage.VectorStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedKnowledgeStore{KnowledgeStore: knowledge, VectorStore: vector}, nil
	case backend.CapabilityArtifact:
		artifact, ok := provider.store.(runtimestorage.ArtifactStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		object, ok := provider.store.(runtimestorage.ObjectStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedArtifactStore{ArtifactStore: artifact, ObjectStore: object}, nil
	case backend.CapabilityAudit:
		store, ok := provider.store.(runtimestorage.AuditStore)
		if !ok {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedAuditStore{AuditStore: store}, nil
	default:
		return nil, storagefactory.ErrStorageFactory
	}
}

type borrowedMemoryStore struct{ runtimestorage.MemoryStore }
type borrowedSummaryStore struct{ runtimestorage.SummaryStore }
type borrowedKnowledgeStore struct {
	runtimestorage.KnowledgeStore
	runtimestorage.VectorStore
}
type borrowedArtifactStore struct {
	runtimestorage.ArtifactStore
	runtimestorage.ObjectStore
}
type borrowedAuditStore struct{ runtimestorage.AuditStore }

func (borrowedMemoryStore) Close() error    { return nil }
func (borrowedSummaryStore) Close() error   { return nil }
func (borrowedKnowledgeStore) Close() error { return nil }
func (borrowedArtifactStore) Close() error  { return nil }
func (borrowedAuditStore) Close() error     { return nil }

func (provider environmentSessionCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	return agentsessionstore.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
}

func (environmentModelFactory) New(ctx context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	if ctx == nil {
		return nil, errors.New("model factory context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	if provider == demoModelProvider {
		return deterministicModel{model: input.Model}, nil
	}
	apiKey := secret.Value()
	if apiKey == "" {
		return nil, errors.New("model factory secret is required")
	}
	if provider != "" && provider != defaultModelProvider {
		return nil, fmt.Errorf("model factory provider %q is unsupported", input.Provider)
	}
	endpoint := strings.TrimSpace(input.Endpoint)
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1"
	}
	return &responsesModel{apiKey: apiKey, endpoint: endpoint, model: input.Model}, nil
}
