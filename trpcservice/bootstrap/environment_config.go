package bootstrap

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func loadEnvironment() (environmentConfig, error) {
	demoMode, err := environmentBool(envDemoMode)
	if err != nil {
		return environmentConfig{}, err
	}
	config := environmentConfig{
		driver:         ControlPlaneDriver(strings.ToLower(strings.TrimSpace(environmentOrDefault(envControlPlaneDriver, string(ControlPlaneDriverPostgres))))),
		modelProvider:  environmentOrDefault(envModelProvider, defaultModelProvider),
		secretRef:      environmentOrDefault(envModelSecretRef, defaultModelSecretRef),
		subjectID:      environmentOrDefault(envSubjectID, defaultSubjectID),
		runtimeStorage: strings.ToLower(strings.TrimSpace(os.Getenv(envSessionBackend))),
		demoMode:       demoMode,
		telemetry:      observability.NewNoopProvider(),
	}
	loaders := []func() error{config.loadDatabase, config.loadIdentities, config.loadAdmin, config.loadModel, config.loadRuntime, config.loadS3, config.loadWeCom, config.loadWeComAIBots}
	for _, load := range loaders {
		if err := load(); err != nil {
			return environmentConfig{}, err
		}
	}
	if err := config.loadTelemetry(); err != nil {
		return environmentConfig{}, err
	}
	return config, nil
}

func (config *environmentConfig) loadS3() error {
	config.s3AccessKeyID = strings.TrimSpace(os.Getenv(envS3AccessKeyID))
	config.s3SecretKey = os.Getenv(envS3SecretKey)
	config.s3SecretRef = environmentOrDefault(envS3SecretRef, "env/trpc-s3-credentials")
	configured := config.s3AccessKeyID != "" || config.s3SecretKey != ""
	if !configured {
		config.s3SecretRef = ""
		return nil
	}
	if config.s3AccessKeyID == "" || config.s3SecretKey == "" || strings.ContainsAny(config.s3AccessKeyID, "\r\n") || strings.ContainsAny(config.s3SecretKey, "\r\n") {
		return fmt.Errorf("%w: S3 credentials must be configured together", ErrInvalidConfig)
	}
	if _, err := modelprofile.NewSecretValue(config.s3AccessKeyID + ":" + config.s3SecretKey); err != nil {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envS3SecretKey)
	}
	for _, identity := range config.apiIdentities {
		if err := (modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.s3SecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envS3SecretRef)
		}
	}
	return nil
}

func (config *environmentConfig) loadTelemetry() error {
	endpoint := strings.TrimSpace(os.Getenv(envOTLPEndpoint))
	serviceName := strings.TrimSpace(environmentOrDefault(envOTELServiceName, "trpc-agent-service"))
	if strings.ContainsAny(serviceName, "\r\n") || serviceName == "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envOTELServiceName)
	}
	headers, err := parseEnvironmentOTLPHeaders(os.Getenv(envOTLPHeaders))
	if err != nil {
		return err
	}
	insecure := false
	if value := strings.TrimSpace(os.Getenv(envOTLPInsecure)); value != "" {
		insecure, err = strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%w: %s must be true or false", ErrInvalidConfig, envOTLPInsecure)
		}
	}
	config.otlp = observability.OTLPConfig{ServiceName: serviceName, Endpoint: endpoint, Headers: headers, Insecure: insecure}
	return nil
}

func parseEnvironmentOTLPHeaders(value string) (map[string]string, error) {
	if strings.ContainsAny(value, "\r\n") {
		return nil, fmt.Errorf("%w: %s contains an invalid entry", ErrInvalidConfig, envOTLPHeaders)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	result := make(map[string]string)
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		separator := strings.IndexByte(entry, '=')
		if separator <= 0 || separator == len(entry)-1 {
			return nil, fmt.Errorf("%w: %s entries must use key=value", ErrInvalidConfig, envOTLPHeaders)
		}
		key, headerValue := strings.TrimSpace(entry[:separator]), strings.TrimSpace(entry[separator+1:])
		if key == "" || headerValue == "" || strings.ContainsAny(key, "\r\n\t ") || strings.ContainsAny(headerValue, "\r\n") {
			return nil, fmt.Errorf("%w: %s contains an invalid entry", ErrInvalidConfig, envOTLPHeaders)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate keys", ErrInvalidConfig, envOTLPHeaders)
		}
		result[key] = headerValue
	}
	return result, nil
}

func (config *environmentConfig) loadDatabase() error {
	if config.driver != ControlPlaneDriverPostgres && config.driver != ControlPlaneDriverMySQL {
		return fmt.Errorf("%w: %s must be postgres or mysql", ErrInvalidConfig, envControlPlaneDriver)
	}
	dsnName := envPostgresDSN
	if config.driver == ControlPlaneDriverMySQL {
		dsnName = envMySQLDSN
	}
	dsn, err := requiredEnvironment(dsnName)
	if err != nil {
		return err
	}
	config.dsn = dsn
	if config.driver == ControlPlaneDriverMySQL {
		config.migrationDSN, err = requiredEnvironment(envMySQLMigrationDSN)
		if err != nil {
			return err
		}
	}
	return nil
}

func (config *environmentConfig) loadIdentities() error {
	identities := strings.TrimSpace(os.Getenv(envAPIIdentities))
	if identities != "" {
		var err error
		config.apiIdentities, err = parseEnvironmentAPIIdentities(identities)
		if err != nil {
			return err
		}
		if len(config.apiIdentities) == 1 {
			for _, identity := range config.apiIdentities {
				config.tenantID, config.appID = identity.TenantID, identity.AppID
			}
		}
		return nil
	}
	var err error
	if config.apiToken, err = requiredEnvironment(envAPIToken); err != nil {
		return err
	}
	if config.tenantID, err = requiredEnvironment(envTenantID); err != nil {
		return err
	}
	if config.appID, err = requiredEnvironment(envAppID); err != nil {
		return err
	}
	config.apiIdentities = map[string]gateway.APIIdentity{config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID}}
	return nil
}

func (config *environmentConfig) loadAdmin() error {
	var err error
	if config.adminToken, err = requiredEnvironment(envAdminToken); err != nil {
		return err
	}
	adminTenantValue, err := requiredEnvironment(envAdminTenants)
	if err != nil {
		return err
	}
	config.adminTenants, err = environmentList(envAdminTenants, adminTenantValue, false)
	if err != nil {
		return err
	}
	username := strings.TrimSpace(os.Getenv(envAdminUsername))
	password := strings.TrimSpace(os.Getenv(envAdminPassword))
	if (username == "") != (password == "") {
		return fmt.Errorf("%w: %s and %s must be configured together", ErrInvalidConfig, envAdminUsername, envAdminPassword)
	}
	config.adminUsername, config.adminPassword = username, password
	return nil
}

func (config *environmentConfig) loadModel() error {
	if config.demoMode {
		if config.modelProvider != demoModelProvider {
			return fmt.Errorf("%w: %s requires %s provider", ErrInvalidConfig, envDemoMode, demoModelProvider)
		}
		config.modelProvider = demoModelProvider
		config.secretRef = ""
		config.modelAPIKey = ""
		config.modelAPIKeys = nil
		var err error
		if config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, demoModelName), true); err != nil {
			return err
		}
		return nil
	}
	var err error
	if mapped := strings.TrimSpace(os.Getenv(envModelAPIKeys)); mapped != "" {
		config.modelAPIKeys, err = parseEnvironmentModelAPIKeys(mapped)
		if err != nil {
			return err
		}
		for _, identity := range config.apiIdentities {
			if config.modelAPIKeys[identity.TenantID] == "" {
				return fmt.Errorf("%w: %s has no key for tenant", ErrInvalidConfig, envModelAPIKeys)
			}
		}
	} else {
		if len(config.apiIdentities) > 1 {
			return fmt.Errorf("%w: %s is required for multi-tenant bootstrap", ErrInvalidConfig, envModelAPIKeys)
		}
		config.modelAPIKey = strings.TrimSpace(os.Getenv(envModelAPIKey))
		if config.modelAPIKey == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidConfig, envModelAPIKey)
		}
	}
	config.modelProvider = strings.ToLower(strings.TrimSpace(config.modelProvider))
	config.secretRef = strings.TrimSpace(config.secretRef)
	if config.modelProvider == "" || config.secretRef == "" {
		return fmt.Errorf("%w: model provider and secret reference are required", ErrInvalidConfig)
	}
	if config.modelNames, err = environmentList(envModelNames, environmentOrDefault(envModelNames, defaultModelNames), true); err != nil {
		return err
	}
	config.endpointHosts, err = environmentList(envModelEndpointHost, environmentOrDefault(envModelEndpointHost, defaultEndpointHost), true)
	return err
}

func parseEnvironmentModelAPIKeys(value string) (map[string]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("%w: %s is required", ErrInvalidConfig, envModelAPIKeys)
	}
	keys := make(map[string]string)
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" || strings.ContainsAny(item, "\r\n") {
			return nil, fmt.Errorf("%w: %s contains an empty entry", ErrInvalidConfig, envModelAPIKeys)
		}
		separator := strings.IndexByte(item, '=')
		if separator < 1 || separator == len(item)-1 {
			return nil, fmt.Errorf("%w: %s entries must be tenant_id=api_key", ErrInvalidConfig, envModelAPIKeys)
		}
		tenantID := strings.TrimSpace(item[:separator])
		apiKey := strings.TrimSpace(item[separator+1:])
		if tenantID == "" || strings.ContainsAny(tenantID, "\r\n") || apiKey == "" {
			return nil, fmt.Errorf("%w: %s contains an invalid tenant entry", ErrInvalidConfig, envModelAPIKeys)
		}
		if _, exists := keys[tenantID]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate tenant entries", ErrInvalidConfig, envModelAPIKeys)
		}
		keys[tenantID] = apiKey
	}
	return keys, nil
}

func (config *environmentConfig) loadRuntime() error {
	config.subjectID = strings.TrimSpace(config.subjectID)
	switch config.runtimeStorage {
	case "postgres", "inmemory":
	case "redis":
		if config.demoMode {
			return fmt.Errorf("%w: %s cannot use redis in demo mode", ErrInvalidConfig, envSessionBackend)
		}
		if err := config.loadRedis(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %s must be explicitly set to postgres, redis or inmemory", ErrInvalidConfig, envSessionBackend)
	}
	if config.demoMode && (config.driver != ControlPlaneDriverPostgres || config.runtimeStorage != "inmemory") {
		return fmt.Errorf("%w: %s requires PostgreSQL control plane and inmemory session backend", ErrInvalidConfig, envDemoMode)
	}
	if config.driver == ControlPlaneDriverMySQL && config.runtimeStorage == "postgres" {
		return fmt.Errorf("%w: %s=postgres is not available with MySQL control plane; use inmemory until a MySQL runtime adapter is selected", ErrInvalidConfig, envSessionBackend)
	}
	return nil
}

func (config *environmentConfig) loadRedis() error {
	addr, err := requiredEnvironment(envRedisAddr)
	if err != nil {
		return err
	}
	if strings.ContainsAny(addr, "\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisAddr)
	}
	db, err := environmentInteger(envRedisDB, environmentOrDefault(envRedisDB, "0"), 0, maxRedisDB)
	if err != nil {
		return err
	}
	dialTimeout, err := environmentDuration(envRedisDialTimeout)
	if err != nil {
		return err
	}
	readTimeout, err := environmentDuration(envRedisReadTimeout)
	if err != nil {
		return err
	}
	writeTimeout, err := environmentDuration(envRedisWriteTimeout)
	if err != nil {
		return err
	}
	poolSize, err := environmentInteger(envRedisPoolSize, environmentOrDefault(envRedisPoolSize, "0"), 0, 0)
	if err != nil {
		return err
	}
	keyPrefix := environmentOrDefault(envRedisKeyPrefix, "trpc:runtime:v1")
	if strings.ContainsAny(keyPrefix, "\r\n") || strings.TrimSpace(keyPrefix) == "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisKeyPrefix)
	}
	password := os.Getenv(envRedisPassword)
	if strings.ContainsAny(password, "\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisPassword)
	}
	config.redis = runtimestorageredis.Config{Addr: addr, Password: password, DB: db, KeyPrefix: keyPrefix, DialTimeout: dialTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, PoolSize: poolSize}
	config.redisEndpoint = redisEndpoint(addr)
	config.redisSecretRef = environmentOrDefault(envRedisSecretRef, "env/trpc-redis-password")
	if _, err := modelprofile.NewSecretValue(config.redis.Password); err != nil && config.redis.Password != "" {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisPassword)
	}
	for _, identity := range config.apiIdentities {
		if err := (modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.redisSecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envRedisSecretRef)
		}
	}
	return nil
}

func environmentInteger(name, value string, min, max int) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < min || (max > 0 && parsed > max) {
		return 0, fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, name)
	}
	return parsed, nil
}

func environmentDuration(name string) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, name)
	}
	return parsed, nil
}

func (config *environmentConfig) loadWeCom() error {
	values := []string{strings.TrimSpace(os.Getenv(envWeComCallbackToken)), strings.TrimSpace(os.Getenv(envWeComEncodingAESKey)), strings.TrimSpace(os.Getenv(envWeComAppSecret)), strings.TrimSpace(os.Getenv(envWeComSecretRef))}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
		if config.demoMode && configured != 0 {
			return fmt.Errorf("%w: %s cannot be enabled in demo mode", ErrInvalidConfig, envDemoMode)
		}
	}
	if configured != 0 && configured != len(values) {
		return fmt.Errorf("%w: WeCom credentials must be configured together", ErrInvalidConfig)
	}
	if configured == len(values) {
		config.wecom = &environmentWeComConfig{callbackToken: values[0], encodingAESKey: values[1], appSecret: values[2], secretRef: values[3]}
	}
	if config.wecom != nil && len(config.apiIdentities) != 1 {
		return fmt.Errorf("%w: WeCom credentials require exactly one API identity", ErrInvalidConfig)
	}
	return nil
}

func (config *environmentConfig) loadWeComAIBots() error {
	raw := strings.TrimSpace(os.Getenv(envWeComAIBotConnections))
	if raw == "" {
		return nil
	}
	if config.demoMode || len(config.apiIdentities) != 1 {
		return fmt.Errorf("%w: %s requires one non-demo API identity", ErrInvalidConfig, envWeComAIBotConnections)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var values []environmentWeComAIBotConfig
	if err := decoder.Decode(&values); err != nil || len(values) == 0 {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
	}
	seenBindings := make(map[string]struct{}, len(values))
	seenSecretRefs := make(map[string]struct{}, len(values))
	for index := range values {
		values[index].BindingID = strings.TrimSpace(values[index].BindingID)
		values[index].SecretRef = strings.TrimSpace(values[index].SecretRef)
		if values[index].BindingID == "" || values[index].BotSecret == "" || strings.ContainsAny(values[index].BindingID, "\r\n") || strings.ContainsAny(values[index].BotSecret, "\r\n") {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if err := (channels.SecretScope{TenantID: config.tenantID, SecretRef: values[index].SecretRef}).Validate(); err != nil {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if _, exists := seenBindings[values[index].BindingID]; exists {
			return fmt.Errorf("%w: %s contains duplicate binding IDs", ErrInvalidConfig, envWeComAIBotConnections)
		}
		if _, exists := seenSecretRefs[values[index].SecretRef]; exists {
			return fmt.Errorf("%w: %s contains duplicate secret references", ErrInvalidConfig, envWeComAIBotConnections)
		}
		seenBindings[values[index].BindingID] = struct{}{}
		seenSecretRefs[values[index].SecretRef] = struct{}{}
	}
	config.wecomAIBots = values
	return nil
}

func environmentRuntimeStore(kind string, db *sql.DB) (environmentStorage, error) {
	switch kind {
	case "postgres":
		if db == nil {
			return nil, fmt.Errorf("%w: PostgreSQL runtime storage requires a database", ErrInvalidConfig)
		}
		return runtimestoragepostgres.New(db), nil
	case "inmemory":
		return runtimestorageinmemory.New(), nil
	default:
		return nil, fmt.Errorf("%w: unsupported runtime storage", ErrInvalidConfig)
	}
}

func newEnvironmentRuntimeStoreForConfig(ctx context.Context, config environmentConfig, db *sql.DB) (environmentStorage, error) {
	if config.runtimeStorage == "redis" {
		return newEnvironmentRedisRuntimeStore(ctx, config)
	}
	return newEnvironmentRuntimeStore(config.runtimeStorage, db)
}

func newEnvironmentRuntimeStoresForConfig(ctx context.Context, config environmentConfig, db *sql.DB) (environmentRuntimeStores, error) {
	primary, err := newEnvironmentRuntimeStoreForConfig(ctx, config, db)
	if err != nil {
		return environmentRuntimeStores{}, err
	}
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	stores := environmentRuntimeStores{
		primary:   primary,
		providers: map[string]environmentStorage{providerName: primary},
		owned:     []environmentStorage{primary},
	}
	if config.runtimeStorage != "redis" {
		return stores, nil
	}
	fallback := newEnvironmentInMemoryFallback()
	stores.providers["inmemory"] = fallback
	stores.owned = append(stores.owned, fallback)
	return stores, nil
}

func environmentRedisRuntimeStore(ctx context.Context, config environmentConfig) (environmentStorage, error) {
	store, err := runtimestorageredis.NewFromConfig(ctx, config.redis)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func environmentPing(ctx context.Context, driver ControlPlaneDriver, db *sql.DB, runtimePinger interface{ Ping(context.Context) error }) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if driver == ControlPlaneDriverMySQL {
		if err := mysql.Ping(ctx, db); err != nil {
			return err
		}
	} else if err := postgres.Ping(ctx, db); err != nil {
		return err
	}
	if runtimePinger != nil {
		return runtimePinger.Ping(ctx)
	}
	return nil
}

func redisEndpoint(addr string) string {
	return "redis://" + strings.TrimSpace(addr)
}

func environmentCatalogs(config environmentConfig) (*modelprofile.ProviderCatalog, *backend.ProviderCatalog, error) {
	if config.demoMode {
		if config.modelProvider != demoModelProvider {
			return nil, nil, fmt.Errorf("%w: model provider %q is unsupported in demo mode", ErrInvalidConfig, config.modelProvider)
		}
		modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
			Provider:        demoModelProvider,
			Models:          config.modelNames,
			EndpointPolicy:  modelprofile.FieldForbidden,
			SecretRefPolicy: modelprofile.FieldForbidden,
			Options:         environmentModelPricingOptions(),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("%w: demo model catalog is invalid", ErrInvalidConfig)
		}
		backendCatalog, err := newEnvironmentBackendCatalog(config.runtimeStorage)
		if err != nil {
			return nil, nil, err
		}
		return modelCatalog, backendCatalog, nil
	}
	if config.modelProvider != defaultModelProvider {
		return nil, nil, fmt.Errorf("%w: model provider %q is unsupported", ErrInvalidConfig, config.modelProvider)
	}
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider:        config.modelProvider,
		Models:          config.modelNames,
		EndpointPolicy:  modelprofile.FieldOptional,
		EndpointSchemes: []string{"https"},
		EndpointHosts:   config.endpointHosts,
		SecretRefPolicy: modelprofile.FieldRequired,
		Options:         environmentModelPricingOptions(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: model catalog is invalid", ErrInvalidConfig)
	}
	backendCatalog, err := newEnvironmentBackendCatalog(config.runtimeStorage)
	if err != nil {
		return nil, nil, err
	}
	return modelCatalog, backendCatalog, nil
}

func environmentModelPricingOptions() map[string]modelprofile.OptionSpec {
	min, max := int64(0), int64(1_000_000_000_000)
	return map[string]modelprofile.OptionSpec{
		runtimebudget.InputCostOption:  {Kind: modelprofile.OptionInteger, MinInteger: &min, MaxInteger: &max},
		runtimebudget.OutputCostOption: {Kind: modelprofile.OptionInteger, MinInteger: &min, MaxInteger: &max},
	}
}

func newEnvironmentBackendCatalog(runtimeStorage string) (*backend.ProviderCatalog, error) {
	inMemory := backend.ProviderSpec{
		Provider:        "inmemory",
		Capabilities:    []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit},
		EndpointPolicy:  backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden,
		Options:         map[string]backend.OptionSpec{},
	}
	if runtimeStorage == "redis" {
		backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
			Provider:        "redis",
			Capabilities:    []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory},
			EndpointPolicy:  backend.FieldRequired,
			EndpointSchemes: []string{"redis"},
			SecretRefPolicy: backend.FieldOptional,
			Options:         map[string]backend.OptionSpec{},
		}, s3BackendProviderSpec(), inMemory)
		if err != nil {
			return nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
		}
		return backendCatalog, nil
	}
	backendCatalog, err := backend.NewProviderCatalog(s3BackendProviderSpec(), inMemory)
	if err != nil {
		return nil, fmt.Errorf("%w: backend catalog is invalid", ErrInvalidConfig)
	}
	return backendCatalog, nil
}

func s3BackendProviderSpec() backend.ProviderSpec {
	return backend.ProviderSpec{
		Provider:        "s3",
		Capabilities:    []backend.Capability{backend.CapabilityArtifact},
		EndpointPolicy:  backend.FieldRequired,
		EndpointSchemes: []string{"http", "https"},
		SecretRefPolicy: backend.FieldRequired,
		Options: map[string]backend.OptionSpec{
			"bucket":             {Kind: backend.OptionString, Required: true},
			"region":             {Kind: backend.OptionString, DefaultValue: stringOption("us-east-1")},
			"path_style":         {Kind: backend.OptionBoolean, DefaultValue: stringOption("false")},
			"allow_insecure":     {Kind: backend.OptionBoolean, DefaultValue: stringOption("false")},
			"max_bytes":          {Kind: backend.OptionInteger, DefaultValue: stringOption("33554432"), MinInteger: int64Option(1), MaxInteger: int64Option(1073741824)},
			"connect_timeout_ms": {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
			"read_timeout_ms":    {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
			"write_timeout_ms":   {Kind: backend.OptionInteger, DefaultValue: stringOption("15000"), MinInteger: int64Option(1), MaxInteger: int64Option(300000)},
		},
		ValidateBinding: validEnvironmentS3Binding,
	}
}

func validEnvironmentS3Binding(binding backend.CapabilityBinding) bool {
	return validEnvironmentS3Endpoint(binding.Endpoint, binding.Options["allow_insecure"] == "true") && validS3Bucket(binding.Options["bucket"])
}

func stringOption(value string) *string { return &value }
func int64Option(value int64) *int64    { return &value }

func environmentBool(name string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%w: %s must be true or false", ErrInvalidConfig, name)
	}
	return parsed, nil
}

func requiredEnvironment(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", ErrInvalidConfig, name)
	}
	return value, nil
}

func environmentOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func environmentList(name, value string, lowercase bool) ([]string, error) {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if lowercase {
			part = strings.ToLower(part)
		}
		if part == "" {
			return nil, fmt.Errorf("%w: %s contains an empty item", ErrInvalidConfig, name)
		}
		result = append(result, part)
	}
	return result, nil
}

// parseEnvironmentAPIIdentities accepts comma-separated token|tenant|app|subject
// entries. Tokens are used only as map keys and never included in errors.
func parseEnvironmentAPIIdentities(value string) (map[string]gateway.APIIdentity, error) {
	result := make(map[string]gateway.APIIdentity)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.Split(entry, "|")
		if len(parts) != 4 {
			return nil, fmt.Errorf("%w: %s must use token|tenant|app|subject entries", ErrInvalidConfig, envAPIIdentities)
		}
		token := strings.TrimSpace(parts[0])
		identity := gateway.APIIdentity{TenantID: strings.TrimSpace(parts[1]), AppID: strings.TrimSpace(parts[2]), SubjectID: strings.TrimSpace(parts[3])}
		if token == "" || identity.TenantID == "" || identity.AppID == "" || identity.SubjectID == "" {
			return nil, fmt.Errorf("%w: %s contains an incomplete identity", ErrInvalidConfig, envAPIIdentities)
		}
		if _, exists := result[token]; exists {
			return nil, fmt.Errorf("%w: %s contains duplicate tokens", ErrInvalidConfig, envAPIIdentities)
		}
		result[token] = identity
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: %s is empty", ErrInvalidConfig, envAPIIdentities)
	}
	return result, nil
}
