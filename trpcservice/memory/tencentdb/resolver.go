// Package tencentdb wires TencentDB Agent Memory into tenant-scoped runners.
package tencentdb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory/tencentdb"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

const providerName = "tencentdb"

// GatewayResolver resolves an operator-controlled TencentDB Agent Memory
// gateway by logical backend name. It must not consume tenant-provided URLs.
type GatewayResolver interface {
	ResolveTencentDBGateway(context.Context, string) (string, error)
}

// Resolver creates and owns TencentDB Agent Memory services selected by
// immutable application configuration versions.
type Resolver struct {
	secrets  platformsecret.SecretProvider
	gateways GatewayResolver

	mu       sync.Mutex
	closed   bool
	services map[string]*frameworkmemory.Service
}

// NewResolver creates a TencentDB Agent Memory resolver.
func NewResolver(secrets platformsecret.SecretProvider, gateways GatewayResolver) (*Resolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if gateways == nil {
		return nil, errors.New("tencentdb gateway resolver is required")
	}
	return &Resolver{
		secrets:  secrets,
		gateways: gateways,
		services: make(map[string]*frameworkmemory.Service),
	}, nil
}

// ResolveSessionIngestor returns an ingestor only when the exact config
// version selects the TencentDB Memory provider.
func (r *Resolver) ResolveSessionIngestor(
	ctx context.Context,
	exec worker.Execution,
) (frameworksession.Ingestor, error) {
	if r == nil || r.secrets == nil || r.gateways == nil {
		return nil, errors.New("tencentdb memory resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Memory
	if ref.IsZero() {
		return nil, nil
	}
	if exec.Tenant.SessionPrincipalID != exec.Tenant.UserID {
		// A shared group Session contains messages from multiple senders. The
		// framework ingestor receives its full transcript without per-message
		// sender identity, so attributing it to the current sender would leak
		// other members' content into that sender's Memory scope.
		return nil, nil
	}
	if err := ValidateBackend(ref); err != nil {
		return nil, err
	}
	scope := exec.Tenant.Scope()
	if exec.Tenant.UserID == "" {
		return nil, errors.New("memory user_id is required")
	}
	serviceKey, err := scope.Key("memory", exec.Tenant.ConfigVersion)
	if err != nil {
		return nil, err
	}
	gatewayURL, err := r.gateways.ResolveTencentDBGateway(ctx, ref.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve tencentdb memory gateway: %w", err)
	}
	if err := validateGatewayURL(gatewayURL); err != nil {
		return nil, err
	}
	service, err := r.resolveService(ctx, serviceKey, scope, ref, gatewayURL)
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (r *Resolver) resolveService(
	ctx context.Context,
	key string,
	scope tenant.Scope,
	ref tenant.BackendRef,
	gatewayURL string,
) (*frameworkmemory.Service, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("tencentdb memory resolver is closed")
	}
	if service := r.services[key]; service != nil {
		r.mu.Unlock()
		return service, nil
	}
	r.mu.Unlock()

	apiKey, err := r.secrets.ResolveSecret(ctx, scope, ref.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("resolve memory api key: %w", err)
	}
	if apiKey == "" {
		return nil, errors.New("memory api key is required")
	}
	options := []frameworkmemory.Option{
		frameworkmemory.WithAPIKey(apiKey),
		frameworkmemory.WithSessionKeyFunc(memorySessionKey(scope)),
		frameworkmemory.WithGatewayURL(gatewayURL),
	}
	service, err := frameworkmemory.NewService(options...)
	if err != nil {
		return nil, fmt.Errorf("create tencentdb memory service: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		closedErr := errors.New("tencentdb memory resolver is closed")
		if closeErr := service.Close(); closeErr != nil {
			return nil, errors.Join(closedErr, fmt.Errorf("close tencentdb memory service: %w", closeErr))
		}
		return nil, closedErr
	}
	if existing := r.services[key]; existing != nil {
		if closeErr := service.Close(); closeErr != nil {
			return nil, fmt.Errorf("close duplicate tencentdb memory service: %w", closeErr)
		}
		return existing, nil
	}
	r.services[key] = service
	return service, nil
}

func memorySessionKey(scope tenant.Scope) frameworkmemory.SessionKeyFunc {
	return func(session *frameworksession.Session) string {
		if session == nil {
			return ""
		}
		key, err := scope.Key("memory", session.UserID, session.ID)
		if err != nil {
			return ""
		}
		return key
	}
}

// ValidateBackend checks that ref selects the TencentDB Memory adapter.
func ValidateBackend(ref tenant.BackendRef) error {
	if ref.Kind != tenant.BackendExternal {
		return errors.New("tencentdb memory backend kind must be external")
	}
	if ref.Provider != providerName {
		return fmt.Errorf("unsupported memory provider %q", ref.Provider)
	}
	if ref.Name == "" {
		return errors.New("tencentdb memory backend name is required")
	}
	if ref.SecretRef == (tenant.SecretRef{}) {
		return errors.New("tencentdb memory backend secret_ref is required")
	}
	return nil
}

func validateGatewayURL(value string) error {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("parse tencentdb memory gateway url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("tencentdb memory gateway url must be an absolute http(s) URL")
	}
	return nil
}

// Close stops every owned memory service after draining its queued captures.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := r.services
	r.services = nil
	r.mu.Unlock()

	var errs []error
	for _, service := range services {
		if err := service.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
