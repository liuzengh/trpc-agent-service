// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const maxBackendProfileManifestBytes = 64 << 10

const backendProfileHealthCheckTimeout = 2 * time.Second

var (
	// ErrBackendProfileManifestInvalid identifies malformed operator profile metadata.
	ErrBackendProfileManifestInvalid = errors.New("storage backend profile manifest is invalid")
	// ErrBackendProfileNotFound identifies a tenant reference to an unknown operator profile.
	ErrBackendProfileNotFound = errors.New("storage backend profile was not found")
	// ErrBackendProfileUnavailable identifies a profile whose secret material cannot be resolved.
	ErrBackendProfileUnavailable = errors.New("storage backend profile is unavailable")
	// ErrBackendProfileTypeMismatch identifies a profile used for a different backend type.
	ErrBackendProfileTypeMismatch = errors.New("storage backend profile type does not match")
	// ErrBackendEndpointInvalid identifies an invalid operator-owned connection endpoint.
	ErrBackendEndpointInvalid = errors.New("storage backend endpoint is invalid")
	// ErrBackendOptionInvalid identifies a tenant-controlled backend option outside the allowlist.
	ErrBackendOptionInvalid = errors.New("storage backend option is invalid")
	// ErrBackendInitialization identifies a third-party backend constructor failure.
	ErrBackendInitialization = errors.New("storage backend initialization failed")
	// ErrBackendProfileHealthCheckFailed identifies a failed live profile probe
	// without exposing a connection string or provider error in the response.
	ErrBackendProfileHealthCheckFailed = errors.New("storage backend profile health check failed")
)

var (
	profileIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	envNamePattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
)

// BackendProfile is operator-owned connection material. ConnectionString is a
// secret and must never be logged, serialized into tenant state, or returned
// through an API.
type BackendProfile struct {
	Backend          string
	ConnectionString string
	AllowInsecure    bool
}

// BackendProfileResolver allows the composition root to substitute a Secret
// Manager or KMS-backed resolver without changing tenant or adapter code.
type BackendProfileResolver interface {
	ResolveBackendProfile(profileID string) (BackendProfile, error)
}

// TenantBackendProfileResolver is the scoped form used by the production
// adapter. A profile may be shared by several tenants, but an operator can
// also bind it to an explicit allowlist. Keeping the tenant in this call
// prevents a caller from accidentally turning a profile ID into a global
// capability when a control-plane policy requires isolation.
type TenantBackendProfileResolver interface {
	ResolveBackendProfileForTenant(tenantID, profileID string) (BackendProfile, error)
}

// BackendProfileHealthChecker is an optional readiness seam for remote
// profiles. It keeps provider-specific probing out of the tenant adapter while
// allowing production composition to fail closed when a live check is needed.
type BackendProfileHealthChecker interface {
	HealthCheckBackendProfile(context.Context, string, string) error
}

// BackendProfileCatalog is an immutable environment-backed profile resolver.
type BackendProfileCatalog struct {
	profiles        map[string]BackendProfile
	tenantAllowlist map[string]map[string]struct{}
}

// BackendProfileValidator holds only the non-secret part of an operator-owned
// backend profile manifest. Control-plane and queue processes use it to reject
// unknown, mistyped, or unauthorized profile references without receiving the
// Session/Memory connection material needed exclusively by Workers.
type BackendProfileValidator struct {
	profiles        map[string]backendProfileDefinition
	tenantAllowlist map[string]map[string]struct{}
}

type backendProfileDefinition struct {
	ID            string   `json:"id"`
	Backend       string   `json:"backend"`
	ConnectionEnv string   `json:"connectionEnv"`
	AllowInsecure bool     `json:"allowInsecure,omitempty"`
	TenantIDs     []string `json:"tenantIds,omitempty"`
}

// LoadBackendProfileValidator parses and validates public profile metadata.
// It deliberately does not call a credential lookup function. Use this in
// Gateway, Admin, Consumer, and Delivery so their pods do not need tenant
// Session/Memory credentials merely to validate a Tenant configuration.
func LoadBackendProfileValidator(manifest string) (*BackendProfileValidator, error) {
	if len(manifest) == 0 || len(manifest) > maxBackendProfileManifestBytes {
		return nil, ErrBackendProfileManifestInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(manifest))
	decoder.DisallowUnknownFields()
	var definitions []backendProfileDefinition
	if err := decoder.Decode(&definitions); err != nil || len(definitions) == 0 {
		return nil, ErrBackendProfileManifestInvalid
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, ErrBackendProfileManifestInvalid
	}

	profiles := make(map[string]backendProfileDefinition, len(definitions))
	allowlists := make(map[string]map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if !profileIDPattern.MatchString(definition.ID) ||
			!envNamePattern.MatchString(definition.ConnectionEnv) ||
			(definition.Backend != "redis" && definition.Backend != "postgres") {
			return nil, ErrBackendProfileManifestInvalid
		}
		if _, duplicate := profiles[definition.ID]; duplicate {
			return nil, ErrBackendProfileManifestInvalid
		}
		if len(definition.TenantIDs) > 0 {
			allowed := make(map[string]struct{}, len(definition.TenantIDs))
			for _, tenantID := range definition.TenantIDs {
				if !validProfileTenantID(tenantID) {
					return nil, ErrBackendProfileManifestInvalid
				}
				if _, duplicate := allowed[tenantID]; duplicate {
					return nil, ErrBackendProfileManifestInvalid
				}
				allowed[tenantID] = struct{}{}
			}
			allowlists[definition.ID] = allowed
		}
		profiles[definition.ID] = definition
	}
	return &BackendProfileValidator{profiles: profiles, tenantAllowlist: allowlists}, nil
}

// LoadBackendProfiles resolves the secret connection material for the Worker
// data plane after validating the public manifest. Non-Worker processes should
// use LoadBackendProfileValidator instead.
func LoadBackendProfiles(
	manifest string,
	lookup func(string) (string, bool),
) (*BackendProfileCatalog, error) {
	if lookup == nil {
		return nil, ErrBackendProfileManifestInvalid
	}
	validator, err := LoadBackendProfileValidator(manifest)
	if err != nil {
		return nil, err
	}
	profiles := make(map[string]BackendProfile, len(validator.profiles))
	for profileID, definition := range validator.profiles {
		connectionString, ok := lookup(definition.ConnectionEnv)
		if !ok || connectionString == "" {
			return nil, fmt.Errorf("%w: profile %q", ErrBackendProfileUnavailable, profileID)
		}
		profile := BackendProfile{
			Backend:          definition.Backend,
			ConnectionString: connectionString,
			AllowInsecure:    definition.AllowInsecure,
		}
		if err := validateBackendEndpoint(profile); err != nil {
			return nil, fmt.Errorf("%w: profile %q", ErrBackendEndpointInvalid, profileID)
		}
		profiles[profileID] = profile
	}
	return &BackendProfileCatalog{
		profiles:        profiles,
		tenantAllowlist: validator.tenantAllowlist,
	}, nil
}

// ValidateTenantStorage checks that a Tenant refers to installed public
// profile metadata. Endpoint reachability and connection-string validation
// remain a Worker startup responsibility because this validator holds no
// credentials.
func (v *BackendProfileValidator) ValidateTenantStorage(tenantID string, config tenant.StorageConfig) error {
	if v == nil {
		return ErrBackendProfileNotFound
	}
	if err := validateTenantProfileMetadata(v, tenantID, config.SessionBackend, config.SessionProfile, "session"); err != nil {
		return err
	}
	return validateTenantProfileMetadata(v, tenantID, config.MemoryBackend, config.MemoryProfile, "memory")
}

func validateTenantProfileMetadata(v *BackendProfileValidator, tenantID, backend, profileID, domain string) error {
	switch backend {
	case "", "inmemory":
		if profileID != "" {
			return fmt.Errorf("%w: %s in-memory backend cannot use profile %q", ErrBackendOptionInvalid, domain, profileID)
		}
		return nil
	case "redis", "postgres":
		if !profileIDPattern.MatchString(profileID) {
			return fmt.Errorf("%w: %s profile is required", ErrBackendOptionInvalid, domain)
		}
		definition, ok := v.profiles[profileID]
		if !ok || !profileAllowedForTenant(v.tenantAllowlist, tenantID, profileID) {
			return fmt.Errorf("%s profile %q: %w", domain, profileID, ErrBackendProfileNotFound)
		}
		if definition.Backend != backend {
			return fmt.Errorf("%w: %s profile %q is %s", ErrBackendProfileTypeMismatch, domain, profileID, definition.Backend)
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported %s backend %q", ErrBackendOptionInvalid, domain, backend)
	}
}

func profileAllowedForTenant(allowlists map[string]map[string]struct{}, tenantID, profileID string) bool {
	if tenantID == "" || !validProfileTenantID(tenantID) {
		return false
	}
	allowed, scoped := allowlists[profileID]
	if !scoped {
		return true
	}
	_, ok := allowed[tenantID]
	return ok
}

// ResolveBackendProfile resolves a profile without copying or exposing the
// surrounding catalog.
func (c *BackendProfileCatalog) ResolveBackendProfile(profileID string) (BackendProfile, error) {
	if c == nil {
		return BackendProfile{}, ErrBackendProfileNotFound
	}
	profile, ok := c.profiles[profileID]
	if !ok {
		return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendProfileNotFound, profileID)
	}
	return profile, nil
}

// ResolveBackendProfileForTenant applies an optional operator allowlist. An
// empty allowlist deliberately means an operator-managed shared profile; a
// non-empty list is strict and fails closed for every other tenant.
func (c *BackendProfileCatalog) ResolveBackendProfileForTenant(tenantID, profileID string) (BackendProfile, error) {
	if tenantID == "" || !validProfileTenantID(tenantID) {
		return BackendProfile{}, ErrBackendProfileNotFound
	}
	profile, err := c.ResolveBackendProfile(profileID)
	if err != nil {
		return BackendProfile{}, err
	}
	if allowed, scoped := c.tenantAllowlist[profileID]; scoped {
		if _, ok := allowed[tenantID]; !ok {
			return BackendProfile{}, ErrBackendProfileNotFound
		}
	}
	return profile, nil
}

// HealthCheckBackendProfile performs a live, short-lived probe using the
// operator-owned profile. Probe failures deliberately omit provider error text
// because driver errors can echo credentials or connection details.
func (c *BackendProfileCatalog) HealthCheckBackendProfile(ctx context.Context, tenantID, profileID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	profile, err := c.ResolveBackendProfileForTenant(tenantID, profileID)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, backendProfileHealthCheckTimeout)
	defer cancel()
	switch profile.Backend {
	case "redis":
		options, err := redis.ParseURL(profile.ConnectionString)
		if err != nil {
			return fmt.Errorf("%w: profile %q", ErrBackendProfileHealthCheckFailed, profileID)
		}
		client := redis.NewClient(options)
		defer client.Close()
		if err := client.Ping(probeCtx).Err(); err != nil {
			if probeCtx.Err() != nil {
				return probeCtx.Err()
			}
			return fmt.Errorf("%w: profile %q", ErrBackendProfileHealthCheckFailed, profileID)
		}
	case "postgres":
		db, err := sql.Open("postgres", profile.ConnectionString)
		if err != nil {
			return fmt.Errorf("%w: profile %q", ErrBackendProfileHealthCheckFailed, profileID)
		}
		defer db.Close()
		if err := db.PingContext(probeCtx); err != nil {
			if probeCtx.Err() != nil {
				return probeCtx.Err()
			}
			return fmt.Errorf("%w: profile %q", ErrBackendProfileHealthCheckFailed, profileID)
		}
	default:
		return fmt.Errorf("%w: profile %q", ErrBackendProfileHealthCheckFailed, profileID)
	}
	return nil
}

// ValidateTenantStorage verifies profile references before a tenant mutation
// is committed. Worker startup performs the same check before constructing a
// backend, but admission-time validation prevents an operator from persisting
// a configuration that can only fail on the first user request.
func (c *BackendProfileCatalog) ValidateTenantStorage(tenantID string, config tenant.StorageConfig) error {
	if c == nil {
		return ErrBackendProfileNotFound
	}
	if err := validateTenantProfileReference(c, tenantID, config.SessionBackend, config.SessionProfile, "session"); err != nil {
		return err
	}
	return validateTenantProfileReference(c, tenantID, config.MemoryBackend, config.MemoryProfile, "memory")
}

func validateTenantProfileReference(c *BackendProfileCatalog, tenantID, backend, profileID, domain string) error {
	switch backend {
	case "", "inmemory":
		if profileID != "" {
			return fmt.Errorf("%w: %s in-memory backend cannot use profile %q", ErrBackendOptionInvalid, domain, profileID)
		}
		return nil
	case "redis", "postgres":
		if !profileIDPattern.MatchString(profileID) {
			return fmt.Errorf("%w: %s profile is required", ErrBackendOptionInvalid, domain)
		}
		profile, err := c.ResolveBackendProfileForTenant(tenantID, profileID)
		if err != nil {
			return fmt.Errorf("%s profile %q: %w", domain, profileID, err)
		}
		if profile.Backend != backend {
			return fmt.Errorf("%w: %s profile %q is %s", ErrBackendProfileTypeMismatch, domain, profileID, profile.Backend)
		}
		if err := validateBackendEndpoint(profile); err != nil {
			return fmt.Errorf("%w: %s profile %q", ErrBackendEndpointInvalid, domain, profileID)
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported %s backend %q", ErrBackendOptionInvalid, domain, backend)
	}
}

func validProfileTenantID(value string) bool {
	if value == "" || len(value) > 64 || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validateBackendEndpoint(profile BackendProfile) error {
	connectionString := profile.ConnectionString
	if connectionString == "" || len(connectionString) > 4096 ||
		strings.ContainsAny(connectionString, "\x00\r\n") {
		return ErrBackendEndpointInvalid
	}
	parsed, err := url.Parse(connectionString)
	if err != nil || parsed.Opaque != "" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Fragment != "" {
		return ErrBackendEndpointInvalid
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch profile.Backend {
	case "redis":
		if scheme != "redis" && scheme != "rediss" {
			return ErrBackendEndpointInvalid
		}
		if !profile.AllowInsecure && scheme != "rediss" {
			return ErrBackendEndpointInvalid
		}
	case "postgres":
		if scheme != "postgres" && scheme != "postgresql" {
			return ErrBackendEndpointInvalid
		}
		if parsed.EscapedPath() == "" || parsed.EscapedPath() == "/" {
			return ErrBackendEndpointInvalid
		}
		if !profile.AllowInsecure {
			modes := parsed.Query()["sslmode"]
			if len(modes) != 1 {
				return ErrBackendEndpointInvalid
			}
			if modes[0] != "verify-full" {
				return ErrBackendEndpointInvalid
			}
		}
	default:
		return ErrBackendEndpointInvalid
	}
	return nil
}

// ValidateServicePostgresURL validates the operator-owned PostgreSQL URL used
// by platform control-plane and reliable-pipeline processes. Production callers
// must pass false so a connection without server identity verification cannot
// become an accidental service-wide trust boundary. Local integration stacks
// may opt in explicitly with a process-level development setting.
func ValidateServicePostgresURL(connectionString string, allowInsecure bool) error {
	return validateBackendEndpoint(BackendProfile{
		Backend:          "postgres",
		ConnectionString: connectionString,
		AllowInsecure:    allowInsecure,
	})
}

type rejectingBackendProfileResolver struct{}

func (rejectingBackendProfileResolver) ResolveBackendProfile(profileID string) (BackendProfile, error) {
	return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendProfileNotFound, profileID)
}
