// Package runtimeplane composes the tenant-scoped Knowledge and Artifact data
// planes used by Agent Workers. Tenant state contains only profile IDs; endpoint
// and credential material remains operator-owned.
package runtimeplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const maxProfileManifestBytes = 128 << 10

var (
	ErrProfileManifestInvalid   = errors.New("runtime data-plane profile manifest is invalid")
	ErrProfileNotFound          = errors.New("runtime data-plane profile was not found")
	ErrProfileTypeMismatch      = errors.New("runtime data-plane profile type does not match")
	ErrProfileSecretUnavailable = errors.New("runtime data-plane profile secret is unavailable")
	ErrDataPlaneUnavailable     = errors.New("runtime data plane is unavailable")

	profileIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	envNamePattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	modelNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
)

type profileDefinition struct {
	ID            string   `json:"id"`
	Backend       string   `json:"backend"`
	Endpoint      string   `json:"endpoint"`
	TLS           bool     `json:"tls"`
	AllowInsecure bool     `json:"allowInsecure,omitempty"`
	TenantIDs     []string `json:"tenantIds,omitempty"`

	Collection         string `json:"collection,omitempty"`
	Dimension          int    `json:"dimension,omitempty"`
	APIKeyEnv          string `json:"apiKeyEnv,omitempty"`
	EmbeddingEndpoint  string `json:"embeddingEndpoint,omitempty"`
	EmbeddingModel     string `json:"embeddingModel,omitempty"`
	EmbeddingAPIKeyEnv string `json:"embeddingAPIKeyEnv,omitempty"`

	Bucket          string `json:"bucket,omitempty"`
	Region          string `json:"region,omitempty"`
	AccessKeyEnv    string `json:"accessKeyEnv,omitempty"`
	SecretKeyEnv    string `json:"secretKeyEnv,omitempty"`
	SessionTokenEnv string `json:"sessionTokenEnv,omitempty"`
	CreateBucket    bool   `json:"createBucket,omitempty"`
	MaxBytes        int64  `json:"maxBytes,omitempty"`
}

type resolvedProfile struct {
	definition      profileDefinition
	apiKey          string
	embeddingAPIKey string
	accessKey       string
	secretKey       string
	sessionToken    string
}

// ProfileValidator contains only non-secret profile metadata and is safe for
// Admin, Gateway and queue processes.
type ProfileValidator struct {
	definitions map[string]profileDefinition
	allowlists  map[string]map[string]struct{}
}

// Catalog is the Worker-only profile catalog with resolved secret material.
// Its fields deliberately remain private so callers cannot serialize it.
type Catalog struct {
	validator *ProfileValidator
	profiles  map[string]resolvedProfile
}

func LoadProfileValidator(manifest string) (*ProfileValidator, error) {
	definitions, err := parseProfileManifest(manifest)
	if err != nil {
		return nil, err
	}
	validator := &ProfileValidator{
		definitions: make(map[string]profileDefinition, len(definitions)),
		allowlists:  make(map[string]map[string]struct{}, len(definitions)),
	}
	for _, definition := range definitions {
		if _, duplicate := validator.definitions[definition.ID]; duplicate {
			return nil, ErrProfileManifestInvalid
		}
		if err := validateDefinition(definition); err != nil {
			return nil, ErrProfileManifestInvalid
		}
		if len(definition.TenantIDs) > 0 {
			allowed := make(map[string]struct{}, len(definition.TenantIDs))
			for _, tenantID := range definition.TenantIDs {
				if err := tenant.ValidateTenantID(tenantID); err != nil {
					return nil, ErrProfileManifestInvalid
				}
				if _, duplicate := allowed[tenantID]; duplicate {
					return nil, ErrProfileManifestInvalid
				}
				allowed[tenantID] = struct{}{}
			}
			validator.allowlists[definition.ID] = allowed
		}
		validator.definitions[definition.ID] = definition
	}
	return validator, nil
}

func LoadProfiles(manifest string, lookup func(string) (string, bool)) (*Catalog, error) {
	if lookup == nil {
		return nil, ErrProfileManifestInvalid
	}
	validator, err := LoadProfileValidator(manifest)
	if err != nil {
		return nil, err
	}
	profiles := make(map[string]resolvedProfile, len(validator.definitions))
	for id, definition := range validator.definitions {
		profile := resolvedProfile{definition: definition}
		switch definition.Backend {
		case "qdrant":
			if definition.APIKeyEnv != "" {
				profile.apiKey, err = requiredSecret(lookup, definition.APIKeyEnv)
				if err != nil {
					return nil, fmt.Errorf("%w: profile %q", err, id)
				}
			}
			profile.embeddingAPIKey, err = requiredSecret(lookup, definition.EmbeddingAPIKeyEnv)
		case "s3":
			profile.accessKey, err = requiredSecret(lookup, definition.AccessKeyEnv)
			if err == nil {
				profile.secretKey, err = requiredSecret(lookup, definition.SecretKeyEnv)
			}
			if err == nil && definition.SessionTokenEnv != "" {
				profile.sessionToken, err = requiredSecret(lookup, definition.SessionTokenEnv)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%w: profile %q", err, id)
		}
		profiles[id] = profile
	}
	return &Catalog{validator: validator, profiles: profiles}, nil
}

func requiredSecret(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || value == "" || len(value) > 8192 || strings.ContainsAny(value, "\x00\r\n") {
		return "", ErrProfileSecretUnavailable
	}
	return value, nil
}

func parseProfileManifest(manifest string) ([]profileDefinition, error) {
	if manifest == "" || len(manifest) > maxProfileManifestBytes {
		return nil, ErrProfileManifestInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(manifest))
	decoder.DisallowUnknownFields()
	var definitions []profileDefinition
	if err := decoder.Decode(&definitions); err != nil || definitions == nil {
		return nil, ErrProfileManifestInvalid
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, ErrProfileManifestInvalid
	}
	return definitions, nil
}

func validateDefinition(definition profileDefinition) error {
	if !profileIDPattern.MatchString(definition.ID) || strings.TrimSpace(definition.Endpoint) != definition.Endpoint {
		return ErrProfileManifestInvalid
	}
	switch definition.Backend {
	case "qdrant":
		if definition.Bucket != "" || definition.Region != "" || definition.AccessKeyEnv != "" ||
			definition.SecretKeyEnv != "" || definition.SessionTokenEnv != "" || definition.CreateBucket || definition.MaxBytes != 0 ||
			!validOptionalEnv(definition.APIKeyEnv) || !envNamePattern.MatchString(definition.EmbeddingAPIKeyEnv) ||
			!modelNamePattern.MatchString(definition.EmbeddingModel) {
			return ErrProfileManifestInvalid
		}
		host, port, err := splitEndpoint(definition.Endpoint)
		if err != nil {
			return err
		}
		if err := (knowledgeplane.QdrantConfig{
			Host: host, Port: port, TLS: definition.TLS, AllowInsecure: definition.AllowInsecure,
			Collection: definition.Collection, Dimension: definition.Dimension,
		}).Validate(); err != nil {
			return err
		}
		return validateEmbeddingEndpoint(definition.EmbeddingEndpoint, definition.AllowInsecure)
	case "s3":
		if definition.Collection != "" || definition.Dimension != 0 || definition.APIKeyEnv != "" ||
			definition.EmbeddingEndpoint != "" || definition.EmbeddingModel != "" || definition.EmbeddingAPIKeyEnv != "" ||
			!envNamePattern.MatchString(definition.AccessKeyEnv) || !envNamePattern.MatchString(definition.SecretKeyEnv) ||
			!validOptionalEnv(definition.SessionTokenEnv) {
			return ErrProfileManifestInvalid
		}
		return (artifactplane.MinIOConfig{
			Endpoint: definition.Endpoint, AccessKey: "profile", SecretKey: "profile", Bucket: definition.Bucket,
			Region: definition.Region, Secure: definition.TLS, AllowInsecure: definition.AllowInsecure,
			CreateBucket: definition.CreateBucket, MaxBytes: definition.MaxBytes,
		}).Validate()
	default:
		return ErrProfileManifestInvalid
	}
}

func validOptionalEnv(value string) bool { return value == "" || envNamePattern.MatchString(value) }

func splitEndpoint(endpoint string) (string, int, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return "", 0, ErrProfileManifestInvalid
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, ErrProfileManifestInvalid
	}
	return host, port, nil
}

func validateEmbeddingEndpoint(endpoint string, allowInsecure bool) error {
	if endpoint == "" || len(endpoint) > 2048 || strings.ContainsAny(endpoint, "\x00\r\n") {
		return ErrProfileManifestInvalid
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Opaque != "" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return ErrProfileManifestInvalid
	}
	if parsed.Scheme == "http" && (!allowInsecure || !isLoopbackHost(parsed.Hostname())) {
		return ErrProfileManifestInvalid
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func (v *ProfileValidator) ValidateTenantStorage(tenantID string, config tenant.StorageConfig) error {
	if v == nil {
		return ErrProfileNotFound
	}
	if err := v.validateBinding(tenantID, config.KnowledgeBackend, config.KnowledgeProfile, "qdrant"); err != nil {
		return err
	}
	return v.validateBinding(tenantID, config.ArtifactBackend, config.ArtifactProfile, "s3")
}

func (v *ProfileValidator) validateBinding(tenantID, backend, profileID, expected string) error {
	if backend == "" && profileID == "" {
		return nil
	}
	if backend != expected || !profileIDPattern.MatchString(profileID) {
		return ErrProfileTypeMismatch
	}
	definition, ok := v.definitions[profileID]
	if !ok || !v.allowed(tenantID, profileID) {
		return ErrProfileNotFound
	}
	if definition.Backend != backend {
		return ErrProfileTypeMismatch
	}
	return nil
}

func (v *ProfileValidator) allowed(tenantID, profileID string) bool {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return false
	}
	allowed, scoped := v.allowlists[profileID]
	if !scoped {
		return true
	}
	_, ok := allowed[tenantID]
	return ok
}

func (c *Catalog) ValidateTenantStorage(tenantID string, config tenant.StorageConfig) error {
	if c == nil || c.validator == nil {
		return ErrProfileNotFound
	}
	return c.validator.ValidateTenantStorage(tenantID, config)
}

func (c *Catalog) resolve(tenantID, profileID, backend string) (resolvedProfile, error) {
	if c == nil || c.validator == nil || !c.validator.allowed(tenantID, profileID) {
		return resolvedProfile{}, ErrProfileNotFound
	}
	profile, ok := c.profiles[profileID]
	if !ok {
		return resolvedProfile{}, ErrProfileNotFound
	}
	if profile.definition.Backend != backend {
		return resolvedProfile{}, ErrProfileTypeMismatch
	}
	return profile, nil
}
