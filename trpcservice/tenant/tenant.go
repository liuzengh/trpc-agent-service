// Package tenant models multi-tenant isolation and agent configuration.
package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

var (
	ErrInvalidArgument      = errors.New("tenant: invalid argument")
	ErrTenantScope          = errors.New("tenant: resource is outside tenant scope")
	ErrNotFound             = errors.New("tenant: resource not found")
	ErrAlreadyExists        = errors.New("tenant: resource already exists")
	ErrTenantInactive       = errors.New("tenant: tenant is not active")
	ErrNoPublishedRevision  = errors.New("tenant: no published revision")
	ErrRevisionNotPublished = errors.New("tenant: revision is not published")
)

// ErrConfigIntegrity reports that a stored revision config no longer matches
// the digest recorded when it was created. It means the row was changed by
// something other than a Repository — a manual UPDATE, a partial restore, a
// corrupt write — so it is a fault in the stored data, not in the request.
//
// It is deliberately not ErrInvalidArgument. The caller cannot cause this and
// cannot fix it by changing the request, so answering "bad request" would be
// both wrong and useless: the same request fails identically forever. It has to
// be reported as a server fault. The admin API has no case for it and falls
// through to its default, which answers 500; do not add a 4xx case for it.
var ErrConfigIntegrity = errors.New("tenant: stored config does not match its digest")

var (
	slugPattern       = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	resourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// maxRevisionNo is the largest revision number a revision may carry.
//
// RevisionNo is a uint64, but a revision number has to survive a round trip
// through a signed 64-bit column, and a value above this would be stored as a
// negative number and read back as a different revision. The bound lives here,
// with the rest of the domain rules, rather than in the one implementation that
// happens to have the column: an id that the reference implementation accepts
// and a production implementation rejects is not a valid id, it is drift.
const maxRevisionNo = uint64(math.MaxInt64)

// TenantContext is the mandatory tenant scope for tenant-owned resources.
type TenantContext struct {
	TenantID string
}

// Validate rejects requests that did not resolve an explicit tenant.
func (c TenantContext) Validate() error {
	return ValidateResourceID("tenant_id", c.TenantID)
}

// ValidateResourceID prevents ambiguous session namespaces and cache keys.
func ValidateResourceID(field string, value string) error {
	if !resourceIDPattern.MatchString(value) {
		return fmt.Errorf("%w: invalid %s", ErrInvalidArgument, field)
	}
	return nil
}

type Status string

const (
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusDeleting  Status = "deleting"
)

type Quota struct {
	MaxConcurrentRuns int   `json:"max_concurrent_runs,omitempty"`
	MonthlyTokenLimit int64 `json:"monthly_token_limit,omitempty"`
	MonthlyCostCents  int64 `json:"monthly_cost_cents,omitempty"`
}

type AuditPolicy struct {
	RetainContent bool `json:"retain_content"`
	RetentionDays int  `json:"retention_days,omitempty"`
}

// Tenant is the highest isolation boundary in the platform.
type Tenant struct {
	ID          string      `json:"id"`
	Slug        string      `json:"slug"`
	Name        string      `json:"name"`
	Status      Status      `json:"status"`
	Quota       Quota       `json:"quota"`
	AuditPolicy AuditPolicy `json:"audit_policy"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// Validate reports whether this tenant is well formed. It is exported because
// every Repository implementation, in this package or not, has to reject the
// same input before it reaches storage; a second implementation that reimplemented
// these rules would drift from this one on the first change.
func (t Tenant) Validate() error {
	if err := ValidateResourceID("tenant id", t.ID); err != nil {
		return err
	}
	if !slugPattern.MatchString(t.Slug) {
		return fmt.Errorf("%w: tenant slug must match %s", ErrInvalidArgument, slugPattern)
	}
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("%w: tenant name is required", ErrInvalidArgument)
	}
	switch t.Status {
	case StatusActive, StatusSuspended, StatusDeleting:
		return nil
	default:
		return fmt.Errorf("%w: unsupported tenant status %q", ErrInvalidArgument, t.Status)
	}
}

type AppStatus string

const (
	AppStatusActive   AppStatus = "active"
	AppStatusDisabled AppStatus = "disabled"
)

// RevisionRoute reserves the routing shape used by future gray releases.
type RevisionRoute struct {
	RevisionID string `json:"revision_id"`
	Weight     uint32 `json:"weight"`
}

type RoutingPolicy struct {
	DefaultRevisionID string          `json:"default_revision_id,omitempty"`
	Routes            []RevisionRoute `json:"routes,omitempty"`
}

// AgentApp is the stable identity whose sessions survive revision changes.
type AgentApp struct {
	ID             string        `json:"id"`
	TenantID       string        `json:"tenant_id"`
	Name           string        `json:"name"`
	Status         AppStatus     `json:"status"`
	RoutingVersion uint64        `json:"routing_version"`
	RoutingPolicy  RoutingPolicy `json:"routing_policy"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

// Validate reports whether this app is well formed and belongs to scope.
// Fields the Repository owns (RoutingVersion, RoutingPolicy) are not checked
// here: a Repository overwrites them rather than trusting the caller.
func (a AgentApp) Validate(scope TenantContext) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if a.TenantID != scope.TenantID {
		return fmt.Errorf("%w: app tenant %q does not match %q", ErrTenantScope, a.TenantID, scope.TenantID)
	}
	if err := ValidateResourceID("app id", a.ID); err != nil {
		return err
	}
	if strings.TrimSpace(a.Name) == "" {
		return fmt.Errorf("%w: app name is required", ErrInvalidArgument)
	}
	switch a.Status {
	case AppStatusActive, AppStatusDisabled:
		return nil
	default:
		return fmt.Errorf("%w: unsupported app status %q", ErrInvalidArgument, a.Status)
	}
}

type RevisionStatus string

const (
	RevisionStatusDraft     RevisionStatus = "draft"
	RevisionStatusPublished RevisionStatus = "published"
	RevisionStatusRetired   RevisionStatus = "retired"
)

type ModelConfig struct {
	Provider string `json:"provider"`
	Name     string `json:"name"`
	// BaseURL is the endpoint a remote provider talks to. It is omitempty so a
	// revision written before this field existed marshals to exactly the same
	// bytes as before, and therefore keeps the same ConfigDigest: revisions are
	// immutable and their stored digest is re-checked on read, so a field that
	// added bytes to old configs would report every one of them as corrupt.
	BaseURL string `json:"base_url,omitempty"`
	// SecretRef names a credential; it never carries the value itself. The
	// runtime resolves it when it builds the model.
	SecretRef   string   `json:"secret_ref,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	MaxTokens   int      `json:"max_tokens,omitempty"`
}

// RevisionConfig contains references only; secret values are resolved later.
type RevisionConfig struct {
	AgentName        string      `json:"agent_name"`
	Description      string      `json:"description,omitempty"`
	Instruction      string      `json:"instruction,omitempty"`
	Model            ModelConfig `json:"model"`
	ToolRefs         []string    `json:"tool_refs,omitempty"`
	SkillRefs        []string    `json:"skill_refs,omitempty"`
	KnowledgeRefs    []string    `json:"knowledge_refs,omitempty"`
	PolicyRefs       []string    `json:"policy_refs,omitempty"`
	BackendProfileID string      `json:"backend_profile_id,omitempty"`
}

// Validate reports whether this config can be stored and later executed.
func (c RevisionConfig) Validate() error {
	if strings.TrimSpace(c.AgentName) == "" {
		return fmt.Errorf("%w: agent name is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(c.Model.Provider) == "" || strings.TrimSpace(c.Model.Name) == "" {
		return fmt.Errorf("%w: model provider and name are required", ErrInvalidArgument)
	}
	if c.Model.MaxTokens < 0 {
		return fmt.Errorf("%w: max_tokens cannot be negative", ErrInvalidArgument)
	}
	if c.Model.Temperature != nil && (*c.Model.Temperature < 0 || *c.Model.Temperature > 2) {
		return fmt.Errorf("%w: temperature must be between 0 and 2", ErrInvalidArgument)
	}
	// The empty id means "this process's default store" and is the overwhelming
	// majority of revisions, so it stays legal. Anything else is a resource id
	// and is held to the same rules as every other one: it becomes a lookup key,
	// a cache key and a singleflight key in storagebundle, and the only place it
	// can be refused once and for all is before it is stored.
	//
	// Not TrimSpace'd first. A config carrying "  " is carrying a non-empty id
	// that no store will ever answer for, and treating it as absent here would
	// silently serve the default to a revision that asked for something else.
	//
	// A revision already stored with an illegal id now fails this check on the
	// way back out, which is the intended direction: it was never servable —
	// storagebundle refuses it at resolve time — so the failure moves earlier
	// rather than appearing where it did not exist before.
	if c.BackendProfileID != "" {
		if err := ValidateResourceID("backend profile id", c.BackendProfileID); err != nil {
			return err
		}
	}
	return nil
}

// Digest returns the immutable configuration fingerprint.
func (c RevisionConfig) Digest() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w: encode revision config: %v", ErrInvalidArgument, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// AgentRevision is an immutable runtime configuration snapshot. Lifecycle
// metadata may advance, but Config and ConfigDigest never change after create.
type AgentRevision struct {
	ID           string         `json:"id"`
	TenantID     string         `json:"tenant_id"`
	AgentAppID   string         `json:"agent_app_id"`
	RevisionNo   uint64         `json:"revision_no"`
	Config       RevisionConfig `json:"config"`
	ConfigDigest string         `json:"config_digest"`
	Status       RevisionStatus `json:"status"`
	CreatedBy    string         `json:"created_by"`
	CreatedAt    time.Time      `json:"created_at"`
	PublishedAt  *time.Time     `json:"published_at,omitempty"`
}

// ValidateForCreate reports whether this revision is acceptable as a new
// revision of scope's app. It is create-only: it requires the draft status, so
// it must not be used to re-check a revision loaded back from storage.
func (r AgentRevision) ValidateForCreate(scope TenantContext) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if r.TenantID != scope.TenantID {
		return fmt.Errorf("%w: revision tenant %q does not match %q", ErrTenantScope, r.TenantID, scope.TenantID)
	}
	if err := ValidateResourceID("revision id", r.ID); err != nil {
		return err
	}
	if err := ValidateResourceID("app id", r.AgentAppID); err != nil {
		return err
	}
	if r.RevisionNo == 0 {
		return fmt.Errorf("%w: revision number must be positive", ErrInvalidArgument)
	}
	if r.RevisionNo > maxRevisionNo {
		return fmt.Errorf(
			"%w: revision number must be at most %d", ErrInvalidArgument, maxRevisionNo)
	}
	if strings.TrimSpace(r.CreatedBy) == "" {
		return fmt.Errorf("%w: created_by is required", ErrInvalidArgument)
	}
	if r.Status != RevisionStatusDraft {
		return fmt.Errorf("%w: a new revision must be draft", ErrInvalidArgument)
	}
	return r.Config.Validate()
}
