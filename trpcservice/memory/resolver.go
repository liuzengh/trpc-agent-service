// Package memory resolves one tenant-scoped tRPC-Agent-Go Memory service from
// an immutable execution profile. It owns only backend selection and scope
// enforcement; memory semantics remain in the upstream implementations.
package memory

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	agentmemoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	agentmemorymem0 "trpc.group/trpc-go/trpc-agent-go/memory/mem0"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const memoryDomain = "memory"

// BackendProfileReader is intentionally read-only: resolving a running turn
// must not mutate the tenant control plane.
type BackendProfileReader interface {
	GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error)
}

// Builder functions make provider construction independently testable without
// starting PostgreSQL or Redis. They return the public upstream contract.
type PostgresBuilder func(string) (agentmemory.Service, error)
type RedisBuilder func(string, string) (agentmemory.Service, error)

// Mem0Connection is deployment-owned endpoint configuration.  API keys are
// intentionally absent: cloud credentials are resolved from the tenant's
// versioned SecretRef with backend_connect scope.
type Mem0Connection struct {
	Host          string `json:"host"`
	SelfHostedOSS bool   `json:"self_hosted_oss"`
}

type Mem0Builder func(Mem0Connection, string) (agentmemory.Service, error)

// Resolver maps an exact memory BackendBinding to a deployment-owned
// connection. Connection material is never stored in a tenant profile.
type Resolver struct {
	Profiles            BackendProfileReader
	PostgresConnections map[string]string
	RedisConnections    map[string]string
	Mem0Connections     map[string]Mem0Connection
	Secrets             secrets.Provider
	Subject             string
	AllowInMemory       bool
	BuildPostgres       PostgresBuilder
	BuildRedis          RedisBuilder
	BuildMem0           Mem0Builder
	Decorator           ServiceDecorator
	Telemetry           telemetry.Provider
}

// ServiceDecorator is a narrow Worker-composition extension point. It keeps
// normal backend selection independent from optional online-migration logic.
type ServiceDecorator interface {
	Decorate(context.Context, profile.ExecutionProfileSnapshot, agentmemory.Service) (agentmemory.Service, error)
}

// Resolve creates a service for the immutable Bundle currently being built.
// BundleManager owns caching and closes it only after its final in-flight
// lease; this resolver deliberately has no second, competing cache.
func (r Resolver) Resolve(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (agentmemory.Service, error) {
	if ctx == nil || r.Profiles == nil || snapshot.Key.TenantID == "" || snapshot.Key.AgentAppID == "" ||
		snapshot.Key.ConfigVersion < 1 || snapshot.AppName != appName(snapshot.Key.TenantID, snapshot.Key.AgentAppID) {
		return nil, runtime.ErrInvariantViolation
	}
	binding, ok := snapshot.BackendBindingFor(memoryDomain)
	if !ok || len(binding.Required) == 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	backend, err := r.Profiles.GetBackend(ctx, snapshot.Key.TenantID, binding.BackendProfileID, binding.BackendVersion)
	if err != nil {
		return nil, err
	}
	if backend.TenantID != snapshot.Key.TenantID || backend.ProfileID != binding.BackendProfileID || backend.Version != binding.BackendVersion {
		return nil, runtime.ErrTenantScope
	}
	if backend.Status != "active" || !bindingCapabilitiesSatisfied(binding.Required, backend.Capabilities) {
		return nil, runtime.ErrCapabilityUnsupported
	}

	var service agentmemory.Service
	switch backend.Provider {
	case "postgres":
		if backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if backend.SchemaVersion != 1 && backend.SchemaVersion != 2 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		connectionID := "default"
		if backend.SchemaVersion == 2 {
			connectionID = backend.Configuration["connection_id"]
		}
		dsn, ok := r.PostgresConnections[connectionID]
		if !ok || !validConnection(connectionID, dsn) || r.BuildPostgres == nil {
			return nil, fmt.Errorf("%w: memory postgres connection %q is not deployed", runtime.ErrCapabilityUnsupported, connectionID)
		}
		service, err = r.BuildPostgres(dsn)
	case "redis-memory":
		if backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if backend.SchemaVersion != 1 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		connectionID := backend.Configuration["connection_id"]
		redisURL, ok := r.RedisConnections[connectionID]
		if !ok || !validConnection(connectionID, redisURL) || r.BuildRedis == nil {
			return nil, fmt.Errorf("%w: memory redis connection %q is not deployed", runtime.ErrCapabilityUnsupported, connectionID)
		}
		// The prefix is process-owned. Tenant and app isolation is additionally
		// carried by the upstream UserKey.AppName, enforced below.
		service, err = r.BuildRedis(redisURL, "trpc-memory")
	case "inmemory-memory":
		if backend.SchemaVersion != 1 || backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 ||
			!backend.Capabilities["single_node_only"] || !r.AllowInMemory {
			return nil, runtime.ErrCapabilityUnsupported
		}
		service = agentmemoryinmemory.NewMemoryService()
	case "mem0-memory":
		if backend.SchemaVersion != 1 || r.BuildMem0 == nil || strings.TrimSpace(r.Subject) != r.Subject || r.Subject == "" {
			return nil, runtime.ErrCapabilityUnsupported
		}
		connectionID := backend.Configuration["connection_id"]
		connection, ok := r.Mem0Connections[connectionID]
		if !ok || !validMem0Connection(connectionID, connection) {
			return nil, fmt.Errorf("%w: mem0 connection %q is not deployed", runtime.ErrCapabilityUnsupported, connectionID)
		}
		apiKey, resolveErr := r.resolveMem0Credential(ctx, snapshot, backend, connection)
		if resolveErr != nil {
			return nil, resolveErr
		}
		defer clearBytes(apiKey)
		service, err = r.BuildMem0(connection, string(apiKey))
	default:
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, runtime.ErrInvariantViolation
	}
	scoped := agentmemory.Service(scopedService{app: snapshot.AppName, inner: service})
	if telemetry.Enabled(r.Telemetry) {
		scoped = telemetryService{inner: scoped, provider: r.Telemetry}
	}
	if r.Decorator == nil {
		return scoped, nil
	}
	// A Bundle can outlive a migration control event for a short period. Keep
	// the primary service immutable but consult the decorator at each write,
	// so a cached Bundle cannot silently bypass newly enabled dual-write.
	return migrationAwareService{snapshot: snapshot, primary: scoped, decorator: r.Decorator}, nil
}

func bindingCapabilitiesSatisfied(required []string, available provider.CapabilitySet) bool {
	for _, value := range required {
		if !available[value] {
			return false
		}
	}
	return true
}

func (r Resolver) resolveMem0Credential(ctx context.Context, snapshot profile.ExecutionProfileSnapshot, backend provider.BackendProfileSnapshot, connection Mem0Connection) ([]byte, error) {
	if connection.SelfHostedOSS {
		if backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		return nil, nil
	}
	if r.Secrets == nil || backend.CredentialRef.Ref == "" || backend.CredentialRef.Version < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	value, err := r.Secrets.Resolve(ctx, secrets.Scope{TenantID: snapshot.Key.TenantID, Subject: r.Subject,
		Purpose: secrets.PurposeBackendConnect, ResourceID: backend.ProfileID, ResourceVersion: backend.Version}, backend.CredentialRef)
	if err != nil {
		return nil, err
	}
	if value.Version != backend.CredentialRef.Version || len(value.Bytes) == 0 || strings.TrimSpace(string(value.Bytes)) != string(value.Bytes) || strings.ContainsAny(string(value.Bytes), "\x00\r\n") {
		clearBytes(value.Bytes)
		return nil, runtime.ErrVersionMismatch
	}
	return value.Bytes, nil
}

func validMem0Connection(id string, connection Mem0Connection) bool {
	if !validConnection(id, connection.Host) {
		return false
	}
	endpoint, err := url.Parse(connection.Host)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return false
	}
	return endpoint.Scheme == "https" || (connection.SelfHostedOSS && endpoint.Scheme == "http")
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func appName(tenantID, agentAppID string) string { return tenantID + "/" + agentAppID }

func validConnection(id, value string) bool {
	return id != "" && len(id) <= 128 && strings.TrimSpace(id) == id && value != "" &&
		strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

// scopedService denies any accidental use of a different AppName before the
// request reaches an upstream backend. This is a service isolation boundary,
// not a substitute for backend-side tenant partitioning.
type scopedService struct {
	app   string
	inner agentmemory.Service
}

func (s scopedService) ReadMemories(ctx context.Context, key agentmemory.UserKey, limit int) ([]*agentmemory.Entry, error) {
	if !s.userKeyAllowed(key) {
		return nil, runtime.ErrTenantScope
	}
	return s.inner.ReadMemories(ctx, key, limit)
}
func (s scopedService) SearchMemories(ctx context.Context, key agentmemory.UserKey, query string, opts ...agentmemory.SearchOption) ([]*agentmemory.Entry, error) {
	if !s.userKeyAllowed(key) {
		return nil, runtime.ErrTenantScope
	}
	return s.inner.SearchMemories(ctx, key, query, opts...)
}
func (s scopedService) AddMemory(ctx context.Context, key agentmemory.UserKey, value string, topics []string, opts ...agentmemory.AddOption) error {
	if !s.userKeyAllowed(key) {
		return runtime.ErrTenantScope
	}
	return s.inner.AddMemory(ctx, key, value, topics, opts...)
}
func (s scopedService) UpdateMemory(ctx context.Context, key agentmemory.Key, value string, topics []string, opts ...agentmemory.UpdateOption) error {
	if !s.memoryKeyAllowed(key) {
		return runtime.ErrTenantScope
	}
	return s.inner.UpdateMemory(ctx, key, value, topics, opts...)
}
func (s scopedService) DeleteMemory(ctx context.Context, key agentmemory.Key) error {
	if !s.memoryKeyAllowed(key) {
		return runtime.ErrTenantScope
	}
	return s.inner.DeleteMemory(ctx, key)
}
func (s scopedService) ClearMemories(ctx context.Context, key agentmemory.UserKey) error {
	if !s.userKeyAllowed(key) {
		return runtime.ErrTenantScope
	}
	return s.inner.ClearMemories(ctx, key)
}
func (s scopedService) Tools() []tool.Tool { return s.inner.Tools() }
func (s scopedService) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) error {
	if value == nil || value.AppName != s.app {
		return runtime.ErrTenantScope
	}
	return s.inner.EnqueueAutoMemoryJob(ctx, value)
}
func (s scopedService) Close() error { return s.inner.Close() }
func (s scopedService) userKeyAllowed(key agentmemory.UserKey) bool {
	return s.inner != nil && key.AppName == s.app
}
func (s scopedService) memoryKeyAllowed(key agentmemory.Key) bool {
	return s.inner != nil && key.AppName == s.app
}

var _ agentmemory.Service = scopedService{}

// migrationAwareService provides a per-write authority projection while
// preserving an inexpensive, immutable primary service for reads. It does not
// close services returned by Decorate because decorators are wrappers around
// the same primary resource, not independently owned clients.
type migrationAwareService struct {
	snapshot  profile.ExecutionProfileSnapshot
	primary   agentmemory.Service
	decorator ServiceDecorator
}

func (s migrationAwareService) ReadMemories(ctx context.Context, key agentmemory.UserKey, limit int) ([]*agentmemory.Entry, error) {
	return s.primary.ReadMemories(ctx, key, limit)
}
func (s migrationAwareService) SearchMemories(ctx context.Context, key agentmemory.UserKey, query string, opts ...agentmemory.SearchOption) ([]*agentmemory.Entry, error) {
	return s.primary.SearchMemories(ctx, key, query, opts...)
}
func (s migrationAwareService) AddMemory(ctx context.Context, key agentmemory.UserKey, value string, topics []string, opts ...agentmemory.AddOption) error {
	return s.write(ctx, func(service agentmemory.Service) error { return service.AddMemory(ctx, key, value, topics, opts...) })
}
func (s migrationAwareService) UpdateMemory(ctx context.Context, key agentmemory.Key, value string, topics []string, opts ...agentmemory.UpdateOption) error {
	return s.write(ctx, func(service agentmemory.Service) error { return service.UpdateMemory(ctx, key, value, topics, opts...) })
}
func (s migrationAwareService) DeleteMemory(ctx context.Context, key agentmemory.Key) error {
	return s.write(ctx, func(service agentmemory.Service) error { return service.DeleteMemory(ctx, key) })
}
func (s migrationAwareService) ClearMemories(ctx context.Context, key agentmemory.UserKey) error {
	return s.write(ctx, func(service agentmemory.Service) error { return service.ClearMemories(ctx, key) })
}
func (s migrationAwareService) Tools() []tool.Tool { return s.primary.Tools() }
func (s migrationAwareService) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) error {
	return s.write(ctx, func(service agentmemory.Service) error { return service.EnqueueAutoMemoryJob(ctx, value) })
}
func (s migrationAwareService) Close() error { return s.primary.Close() }
func (s migrationAwareService) write(ctx context.Context, operation func(agentmemory.Service) error) error {
	if s.primary == nil || s.decorator == nil || operation == nil {
		return runtime.ErrInvariantViolation
	}
	service, err := s.decorator.Decorate(ctx, s.snapshot, s.primary)
	if err != nil {
		return err
	}
	if service == nil {
		return runtime.ErrInvariantViolation
	}
	return operation(service)
}

var _ agentmemory.Service = migrationAwareService{}

// newMem0Service adapts the upstream ingest-first Mem0 integration to the
// runner's memory.Service contract.  Mem0 deliberately exposes read-only
// tools: writes occur only through durable session ingestion, so callers
// cannot claim unsupported update/delete/clear semantics.
// NewMem0Service is the sole adapter boundary between the upstream ingest
// reader and runner memory.Service.  Worker composition calls it through the
// Resolver builder so tests can still substitute a fake without HTTP.
func NewMem0Service(connection Mem0Connection, apiKey string) (agentmemory.Service, error) {
	options := []agentmemorymem0.ServiceOpt{agentmemorymem0.WithHost(connection.Host)}
	if connection.SelfHostedOSS {
		options = append(options, agentmemorymem0.WithSelfHostedOSS())
	} else {
		options = append(options, agentmemorymem0.WithAPIKey(apiKey))
	}
	inner, err := agentmemorymem0.NewService(options...)
	if err != nil {
		return nil, err
	}
	return mem0Service{inner: inner}, nil
}

type mem0Service struct{ inner *agentmemorymem0.Service }

func (s mem0Service) ReadMemories(ctx context.Context, key agentmemory.UserKey, limit int) ([]*agentmemory.Entry, error) {
	return s.inner.ReadMemories(ctx, key, limit)
}
func (s mem0Service) SearchMemories(ctx context.Context, key agentmemory.UserKey, query string, opts ...agentmemory.SearchOption) ([]*agentmemory.Entry, error) {
	return s.inner.SearchMemories(ctx, key, query, opts...)
}
func (s mem0Service) AddMemory(context.Context, agentmemory.UserKey, string, []string, ...agentmemory.AddOption) error {
	return runtime.ErrCapabilityUnsupported
}
func (s mem0Service) UpdateMemory(context.Context, agentmemory.Key, string, []string, ...agentmemory.UpdateOption) error {
	return runtime.ErrCapabilityUnsupported
}
func (s mem0Service) DeleteMemory(context.Context, agentmemory.Key) error {
	return runtime.ErrCapabilityUnsupported
}
func (s mem0Service) ClearMemories(context.Context, agentmemory.UserKey) error {
	return runtime.ErrCapabilityUnsupported
}
func (s mem0Service) Tools() []tool.Tool { return s.inner.Tools() }
func (s mem0Service) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) error {
	return s.inner.IngestSession(ctx, value)
}
func (s mem0Service) Close() error { return s.inner.Close() }

var _ agentmemory.Service = mem0Service{}
