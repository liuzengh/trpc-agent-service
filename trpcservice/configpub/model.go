// Package configpub implements the durable, tenant-safe configuration
// publication boundary (P1-08): immutable config revisions, validated state
// transitions, canary rollout assignment, rollback, and runtime snapshot
// resolution. PostgreSQL is the single authoritative fact source; Redis is
// never consulted for configuration facts; in-process caches are not used for
// config facts so a publication or rollback is observable by every subsequent
// request.
//
// Security invariants enforced by this package:
//   - revision content is immutable after insert (frozen by database trigger);
//   - secret material may only appear as bounded server-resolvable SecretRefs;
//   - the vector backend of a published revision can never be enabled (the
//     production vector lifecycle stays governed by P1-06 and stays disabled);
//   - every caller-supplied identifier is validated and tenant-scoped
//     server-side; callers cannot override tenant, active revision or rollout
//     scope through payloads;
//   - errors and telemetry never carry raw config content, SecretRef values,
//     DSNs, endpoints, tenant IDs or raw backend errors.
package configpub

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Status is the bounded revision state machine. Transitions are additionally
// enforced by a database trigger; this mirror exists for fail-closed checks
// before any durable write is attempted.
type Status string

const (
	StatusDraft      Status = "draft"
	StatusValidated  Status = "validated"
	StatusPublished  Status = "published"
	StatusSuperseded Status = "superseded"
	StatusRecalled   Status = "recalled"
	StatusRejected   Status = "rejected"
)

// RuntimeReadable reports whether a revision may serve runtime traffic. A
// draft or rejected revision never serves requests; previously published
// revisions remain readable so in-flight jobs keep their immutable version.
func (s Status) RuntimeReadable() bool {
	switch s {
	case StatusPublished, StatusSuperseded, StatusRecalled:
		return true
	default:
		return false
	}
}

// Document is the bounded, secret-free runtime configuration snapshot stored
// inside an immutable revision. It carries identities and references only:
// no prompt text, no webhook bodies, no credentials, no raw diffs.
type Document struct {
	SchemaVersion int                  `json:"schema_version"`
	BackendPolicy tenant.BackendPolicy `json:"backend_policy"`
	Agent         AgentDocument        `json:"agent"`
	Bindings      []BindingDocument    `json:"bindings,omitempty"`
}

// AgentDocument pins the agent release identity carried by a revision. The
// revision version doubles as the agent release version, matching the
// AgentRefDTO/ConfigVersion invariant used across ingress, queue, worker,
// completion and outbox.
type AgentDocument struct {
	AgentAppID     string `json:"agent_app_id"`
	ModelConfigRef string `json:"model_config_ref,omitempty"`
	ToolPolicyRef  string `json:"tool_policy_ref,omitempty"`
	GuardrailRef   string `json:"guardrail_ref,omitempty"`
}

// BindingDocument pins a channel binding reference. Secret material is only
// ever a SecretRef (env:// or secret:// scheme); values resolve server-side.
type BindingDocument struct {
	BindingID      string `json:"binding_id"`
	Channel        string `json:"channel"`
	ExternalAppID  string `json:"external_app_id"`
	SecretRef      string `json:"secret_ref,omitempty"`
	VerifyTokenRef string `json:"verify_token_ref,omitempty"`
	TargetType     string `json:"target_type,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
}

const (
	documentSchemaVersion = 1
	maxDocumentBytes      = 32 * 1024
	maxBindings           = 8
	maxBoundedRef         = 256
	maxBoundedID          = 128
)

// ValidationError carries a bounded, telemetry-safe category describing why a
// revision was rejected. The category is the only externally visible detail.
type ValidationError struct {
	Category string
}

func (e *ValidationError) Error() string { return "configpub: revision validation failed" }

// Bounded validation categories (allowlist). Nothing else is ever reported.
const (
	CategorySyntacticInvalid   = "syntactic_invalid"
	CategorySchemaInvalid      = "schema_invalid"
	CategoryUnsupportedBackend = "unsupported_backend"
	CategoryMissingSetting     = "missing_required_setting"
	CategoryForbiddenProdDep   = "forbidden_production_dependency"
	CategoryInvalidSecretRef   = "invalid_secret_ref"
	CategoryCrossTenantRef     = "cross_tenant_reference"
	CategoryUnknownRef         = "unknown_reference"
	CategoryIncompatibleVer    = "incompatible_config_version"
)

func validationError(category string) error {
	return &ValidationError{Category: category}
}

// CategoryOf returns the bounded category of a validation failure, or "".
func CategoryOf(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Category
	}
	return ""
}

// supportedBackends is the server-owned allowlist for backend policy values.
// The vector backend is pinned to "none": enabling derived-vector traffic is
// outside the P1-08 boundary and stays fail-closed (P1-06).
var supportedBackends = map[string][]string{
	"session": {"postgres", "memory"},
	"memory":  {"postgres", "memory"},
	"vector":  {"none"},
	"object":  {"none", "s3"},
}

// DecodeDocument parses and syntactically validates a stored snapshot.
func DecodeDocument(raw []byte) (Document, error) {
	if len(raw) == 0 {
		return Document{}, validationError(CategorySyntacticInvalid)
	}
	if len(raw) > maxDocumentBytes {
		return Document{}, validationError(CategorySchemaInvalid)
	}
	var doc Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Document{}, validationError(CategorySyntacticInvalid)
	}
	if err := doc.validateShape(); err != nil {
		return Document{}, err
	}
	return doc, nil
}

// EncodeDocument serializes a validated snapshot for durable storage.
func EncodeDocument(doc Document) ([]byte, error) {
	if err := doc.validateShape(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, validationError(CategorySyntacticInvalid)
	}
	if len(raw) > maxDocumentBytes {
		return nil, validationError(CategorySchemaInvalid)
	}
	return raw, nil
}

// validateShape enforces the structural schema: bounded fields, valid IDs,
// SecretRef-shaped references and a supported schema version.
func (d Document) validateShape() error {
	if d.SchemaVersion != documentSchemaVersion {
		return validationError(CategoryIncompatibleVer)
	}
	if err := validateBackendShape(d.BackendPolicy); err != nil {
		return err
	}
	if !validID(d.Agent.AgentAppID) {
		return validationError(CategorySchemaInvalid)
	}
	if err := validateRef(d.Agent.ModelConfigRef, true); err != nil {
		return err
	}
	if err := validateRef(d.Agent.ToolPolicyRef, true); err != nil {
		return err
	}
	if err := validateRef(d.Agent.GuardrailRef, true); err != nil {
		return err
	}
	if len(d.Bindings) > maxBindings {
		return validationError(CategorySchemaInvalid)
	}
	seen := make(map[string]struct{}, len(d.Bindings))
	for _, binding := range d.Bindings {
		if !validID(binding.BindingID) || !validID(binding.ExternalAppID) {
			return validationError(CategorySchemaInvalid)
		}
		if binding.Channel != tenant.ChannelLark && binding.Channel != tenant.ChannelTelegram {
			return validationError(CategorySchemaInvalid)
		}
		key := binding.Channel + "\x00" + binding.ExternalAppID
		if _, exists := seen[key]; exists {
			return validationError(CategorySchemaInvalid)
		}
		seen[key] = struct{}{}
		if err := validateSecretRefShape(binding.SecretRef); err != nil {
			return err
		}
		if err := validateSecretRefShape(binding.VerifyTokenRef); err != nil {
			return err
		}
		if err := validateRef(binding.TargetID, true); err != nil {
			return err
		}
		switch binding.TargetType {
		case "", "message", "user", "chat":
		default:
			return validationError(CategorySchemaInvalid)
		}
		if binding.TargetType == "message" && binding.TargetID != "" {
			return validationError(CategorySchemaInvalid)
		}
	}
	return nil
}

func validateBackendShape(policy tenant.BackendPolicy) error {
	for name, allowed := range map[string][]string{
		"session": supportedBackends["session"], "memory": supportedBackends["memory"],
		"vector": supportedBackends["vector"], "object": supportedBackends["object"],
	} {
		value := map[string]string{"session": policy.Session, "memory": policy.Memory, "vector": policy.Vector, "object": policy.Object}[name]
		if strings.TrimSpace(value) == "" {
			return validationError(CategoryMissingSetting)
		}
		ok := false
		for _, candidate := range allowed {
			if value == candidate {
				ok = true
				break
			}
		}
		if !ok {
			return validationError(CategoryUnsupportedBackend)
		}
	}
	return nil
}

// validateRef bounds a non-secret reference field. Empty is allowed only when
// optional is true.
func validateRef(value string, optional bool) error {
	if value == "" {
		if optional {
			return nil
		}
		return validationError(CategoryMissingSetting)
	}
	if len(value) > maxBoundedRef || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
		return validationError(CategorySchemaInvalid)
	}
	return nil
}

// validateSecretRefShape rejects plaintext material: a binding secret field
// must either be empty or carry a bounded env:// or secret:// reference.
func validateSecretRefShape(ref string) error {
	if ref == "" {
		return nil
	}
	if err := tenant.ValidateSecretRef(ref); err != nil {
		return validationError(CategoryInvalidSecretRef)
	}
	return nil
}

func validID(id string) bool {
	if len(id) < 1 || len(id) > maxBoundedID {
		return false
	}
	for _, r := range id {
		switch {
		case r == '-' || r == '_' || r == '.':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// Fingerprint derives the bounded server-owned content fingerprint stored in
// the durable checksum column. It is not reversible and never exposes content.
func Fingerprint(doc Document) (string, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", validationError(CategorySyntacticInvalid)
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])[:16], nil
}

// Revision is the durable state of one immutable configuration revision.
type Revision struct {
	TenantID       string
	Version        int64
	Status         Status
	Document       Document
	Fingerprint    string
	ActorCategory  string
	ReasonCategory string
	CreatedAt      time.Time
	ValidatedAt    time.Time
	PublishedAt    time.Time
	SupersededAt   time.Time
	RecalledAt     time.Time
	RejectedAt     time.Time
}

// RolloutState is the durable per-tenant assignment state. baselineVersion 0
// means "no baseline revision exists".
type RolloutState struct {
	TenantID        string
	ActiveVersion   int64
	BaselineVersion int64
	Percentage      int
	UpdatedAt       time.Time
}

// Operation kinds (bounded).
const (
	OperationValidate = "validate"
	OperationPublish  = "publish"
	OperationRollout  = "rollout"
	OperationRollback = "rollback"
)

// Operation outcomes (bounded).
const (
	OutcomeCommitted    = "committed"
	OutcomeRejected     = "rejected"
	OutcomeStaleVersion = "stale_version"
	OutcomeConflict     = "conflict"
)

// OperationRecord is the durable idempotency record for one publication
// operation. Only committed operations are recorded: a committed operation
// whose response was lost converges on retry, while failed operations may be
// retried with the same key after the operator fixes the cause.
type OperationRecord struct {
	TenantID              string
	OperationID           string
	Kind                  string
	TargetVersion         int64
	ExpectedActiveVersion int64
	RequestedPercentage   int
	ResultActiveVersion   int64
	Outcome               string
	ReasonCategory        string
	ActorCategory         string
	CreatedAt             time.Time
}

// Safe, stable errors. Callers map them to bounded categories; none of them
// ever wraps a raw backend error message.
var (
	ErrInvalidArgument      = errors.New("configpub: invalid argument")
	ErrTenantMismatch       = errors.New("configpub: tenant mismatch")
	ErrUnavailable          = errors.New("configpub: configuration fact source unavailable")
	ErrRevisionNotFound     = errors.New("configpub: revision not found")
	ErrRevisionState        = errors.New("configpub: revision state does not allow the operation")
	ErrStaleExpectedVersion = errors.New("configpub: stale expected active version")
	ErrDuplicateContent     = errors.New("configpub: identical revision content already exists")
	ErrInvalidRollout       = errors.New("configpub: rollout assignment is invalid")
	ErrOperationMismatch    = errors.New("configpub: operation id already used for a different request")
	ErrNotManaged           = errors.New("configpub: tenant is not under publication management")
)

// Bounded reason categories recorded durably and reported on failures.
const (
	ReasonValidationFailed = "validation_failed"
	ReasonWrongStatus      = "wrong_status"
	ReasonMissingRevision  = "missing_revision"
	ReasonInvalidRollout   = "invalid_rollout"
	ReasonStaleVersion     = "stale_version"
	ReasonDuplicateContent = "duplicate_content"
	ReasonTenantMismatch   = "tenant_mismatch"
)

func fmtE(format string, args ...any) error { return fmt.Errorf("configpub: "+format, args...) }
