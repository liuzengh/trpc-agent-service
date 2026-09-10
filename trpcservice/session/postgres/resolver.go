// Package postgres wires tenant-scoped PostgreSQL Session services.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
)

const defaultSessionSchema = "agent"

var postgresIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SessionResolver caches framework PostgreSQL session services selected by an
// immutable application configuration. Session concurrency is owned by the
// Redis lease at the worker boundary, not by this provider.
type SessionResolver struct {
	secrets    platformsecret.SecretProvider
	defaultDSN string
	mu         sync.Mutex
	closed     bool
	services   map[string]session.Service
}

// NewSessionResolver creates a PostgreSQL session resolver backed by a scoped
// secret provider.
func NewSessionResolver(secrets platformsecret.SecretProvider, defaultDSNs ...string) (*SessionResolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if len(defaultDSNs) > 1 {
		return nil, errors.New("only one default postgres dsn is supported")
	}
	defaultDSN := ""
	if len(defaultDSNs) == 1 {
		defaultDSN = strings.TrimSpace(defaultDSNs[0])
	}
	return &SessionResolver{secrets: secrets, defaultDSN: defaultDSN, services: make(map[string]session.Service)}, nil
}

// ResolveSession returns the configured framework PostgreSQL session service.
func (r *SessionResolver) ResolveSession(ctx context.Context, exec worker.Execution) (session.Service, error) {
	if r == nil || r.secrets == nil {
		return nil, errors.New("postgres session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Session
	if ref.Kind != tenant.BackendSQL || ref.Provider != "postgres" {
		return nil, fmt.Errorf("session backend %q must use postgres provider", ref.Name)
	}
	schema, err := sessionSchema(ref)
	if err != nil {
		return nil, err
	}
	key, err := r.sessionCacheKey(exec)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("postgres session resolver is closed")
	}
	if value := r.services[key]; value != nil {
		return value, nil
	}
	dsn, err := r.resolveSessionDSN(ctx, exec)
	if err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx, dsn, schema); err != nil {
		return nil, err
	}
	service, err := sessionpostgres.NewService(sessionpostgres.WithPostgresClientDSN(dsn), sessionpostgres.WithSchema(schema))
	if err != nil {
		return nil, fmt.Errorf("create postgres session service: %w", err)
	}
	r.services[key] = service
	return service, nil
}

func (r *SessionResolver) resolveSessionDSN(ctx context.Context, exec worker.Execution) (string, error) {
	ref := exec.Config.BackendConfig.Session
	dsn := r.defaultDSN
	if ref.SecretRef != (tenant.SecretRef{}) {
		var err error
		dsn, err = r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref.SecretRef)
		if err != nil {
			return "", fmt.Errorf("resolve session dsn: %w", err)
		}
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", errors.New("session dsn is required")
	}
	return dsn, nil
}

func ensureSchema(ctx context.Context, dsn, schema string) (err error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres session backend: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(context.Background()); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close postgres session backend: %w", closeErr))
		}
	}()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
		return fmt.Errorf("create postgres session schema: %w", err)
	}
	return nil
}

// Close closes all cached session services.
func (r *SessionResolver) Close() error {
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
func sessionSchema(ref tenant.BackendRef) (string, error) {
	schema := ref.Options["schema"]
	if schema == "" {
		schema = defaultSessionSchema
	}
	if !postgresIdentifier.MatchString(schema) {
		return "", errors.New("session backend schema is invalid")
	}
	return schema, nil
}
func (r *SessionResolver) sessionCacheKey(exec worker.Execution) (string, error) {
	ref := exec.Config.BackendConfig.Session
	schema, err := sessionSchema(ref)
	if err != nil {
		return "", err
	}
	parts := []string{exec.Tenant.ConfigVersion, exec.Config.BackendConfig.Name, ref.Name, schema}
	if secretRef := ref.SecretRef; secretRef != (tenant.SecretRef{}) {
		parts = append(parts, secretRef.Name, secretRef.Version)
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}
