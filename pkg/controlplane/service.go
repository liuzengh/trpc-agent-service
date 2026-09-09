package controlplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// VersionSnapshotPreparer applies operator-owned policy and immutable runtime
// limits before a version can enter the control-plane database. Publishing
// invokes it again as a preflight and rejects any mutation, so a policy change
// cannot silently rewrite a draft while changing its release semantics.
type VersionSnapshotPreparer func(context.Context, string, *VersionSnapshot) error

// Service owns transactional Agent app/version/deployment lifecycle changes.
type Service struct {
	db              *sql.DB
	prepareSnapshot VersionSnapshotPreparer
}

// These errors separate caller-visible control-plane outcomes from internal
// database and policy failures. HTTP callers can map them without parsing
// driver messages or exposing topology/credential details.
var (
	ErrInvalidControlPlaneRequest = errors.New("invalid control-plane request")
	ErrControlPlaneNotFound       = errors.New("control-plane resource not found")
	ErrControlPlaneConflict       = errors.New("control-plane state conflict")
	ErrTenantInactive             = errors.New("tenant is not active")
)

// NewService creates a control-plane service. Version creation fails closed
// unless prepareSnapshot is configured, so non-HTTP callers cannot bypass
// tenant policy or operator model-catalog pinning.
func NewService(db *sql.DB, prepareSnapshot VersionSnapshotPreparer) *Service {
	return &Service{db: db, prepareSnapshot: prepareSnapshot}
}

type AgentApp struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenantId"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

type AgentVersion struct {
	ID            string `json:"id"`
	AgentAppID    string `json:"agentAppId"`
	Status        string `json:"status"`
	ConfigHash    string `json:"configHash"`
	VersionNumber int64  `json:"versionNumber"`
}

type DeploymentSet struct {
	StableID string `json:"stableId"`
	CanaryID string `json:"canaryId,omitempty"`
}

func (s *Service) CreateApp(ctx context.Context, tenantID, name, description, actor string) (*AgentApp, error) {
	ctx = normalizeContext(ctx)
	if err := validateControlIdentity(tenantID, name, actor); err != nil {
		return nil, err
	}
	if len(description) > 4<<10 || !utf8.ValidString(description) || strings.ContainsRune(description, 0) {
		return nil, fmt.Errorf("%w: description must be valid UTF-8 without NUL and at most 4096 bytes", ErrInvalidControlPlaneRequest)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("control-plane database is not configured")
	}
	app := &AgentApp{ID: uuid.NewString(), TenantID: tenantID, Name: name, Description: description, Status: "active"}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin agent app: %w", err)
	}
	defer tx.Rollback()
	if err := requireActiveTenant(ctx, tx, tenantID); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_apps (id,tenant_id,name,description,status)
		VALUES ($1,$2,$3,$4,'active')`, app.ID, app.TenantID, app.Name, app.Description)
	if err != nil {
		return nil, wrapControlPlaneDBError("create agent app", err)
	}
	if err := writeControlAudit(ctx, tx, tenantID, actor, "agent_app.create", "agent_app", app.ID, map[string]interface{}{
		"name": name,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit agent app: %w", err)
	}
	return app, nil
}

// CreateVersion stores a canonical, secret-free immutable draft. App-row
// locking serializes version numbers across Admin replicas.
func (s *Service) CreateVersion(ctx context.Context, tenantID, appName, actor string, snapshot VersionSnapshot) (*AgentVersion, error) {
	ctx = normalizeContext(ctx)
	if err := validateControlIdentity(tenantID, appName, actor); err != nil {
		return nil, err
	}
	if s == nil || s.prepareSnapshot == nil {
		return nil, fmt.Errorf("version snapshot preparer is required")
	}
	if err := s.prepareSnapshot(ctx, tenantID, &snapshot); err != nil {
		return nil, fmt.Errorf("prepare version snapshot: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidControlPlaneRequest, err)
	}
	if snapshot.Agent.Name != appName {
		return nil, fmt.Errorf("%w: version snapshot agent %q does not match agent app %q", ErrInvalidControlPlaneRequest, snapshot.Agent.Name, appName)
	}
	if s.db == nil {
		return nil, fmt.Errorf("control-plane database is not configured")
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("encode version snapshot: %w", err)
	}
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin version: %w", err)
	}
	defer tx.Rollback()
	if err := requireActiveTenant(ctx, tx, tenantID); err != nil {
		return nil, err
	}
	var appID string
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM agent_apps WHERE tenant_id=$1 AND name=$2 AND status='active' FOR UPDATE`,
		tenantID, appName).Scan(&appID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: agent app %q", ErrControlPlaneNotFound, appName)
		}
		return nil, fmt.Errorf("resolve agent app: %w", err)
	}
	var number int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version_number),0)+1 FROM agent_versions WHERE agent_app_id=$1`, appID).Scan(&number); err != nil {
		return nil, fmt.Errorf("allocate version number: %w", err)
	}
	version := &AgentVersion{ID: uuid.NewString(), AgentAppID: appID, Status: "draft", ConfigHash: hash, VersionNumber: number}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_versions (id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by)
		VALUES ($1,$2,$3,$4,$5,'draft',$6)`, version.ID, appID, number, data, hash, actor); err != nil {
		return nil, wrapControlPlaneDBError("create agent version", err)
	}
	if err := writeControlAudit(ctx, tx, tenantID, actor, "agent_version.create", "agent_version", version.ID, map[string]interface{}{
		"agent_app_id": appID,
		"config_hash":  hash,
		"version":      number,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit version: %w", err)
	}
	return version, nil
}

func (s *Service) PublishVersion(ctx context.Context, tenantID, versionID, actor string) error {
	ctx = normalizeContext(ctx)
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControlPlaneRequest, err)
	}
	if err := validateOpaqueID("version ID", versionID, 64); err != nil {
		return err
	}
	if err := validateActor(actor); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("control-plane database is not configured")
	}
	if s.prepareSnapshot == nil {
		return fmt.Errorf("version snapshot preparer is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin publish version: %w", err)
	}
	defer tx.Rollback()
	if err := requireActiveTenant(ctx, tx, tenantID); err != nil {
		return err
	}
	var status, appName, configHash string
	var configSnapshot []byte
	err = tx.QueryRowContext(ctx, `
		SELECT av.status, av.config_snapshot, av.config_hash, aa.name
		FROM agent_versions av
		JOIN agent_apps aa ON aa.id=av.agent_app_id
		WHERE av.id=$1 AND aa.tenant_id=$2 AND aa.status='active'
		FOR UPDATE OF av`, versionID, tenantID).Scan(&status, &configSnapshot, &configHash, &appName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: agent version", ErrControlPlaneNotFound)
		}
		return fmt.Errorf("load version for publish: %w", err)
	}
	if status != "draft" {
		return fmt.Errorf("%w: agent version is already %s", ErrControlPlaneConflict, status)
	}
	if err := s.preflightDraftSnapshot(ctx, tenantID, appName, configSnapshot, configHash); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_versions
		SET status='published', published_at=now()
		WHERE id=$1 AND status='draft'`, versionID)
	if err != nil {
		return fmt.Errorf("publish version: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("publish version rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: agent version changed while publishing", ErrControlPlaneConflict)
	}
	if err := writeControlAudit(ctx, tx, tenantID, actor, "agent_version.publish", "agent_version", versionID, nil); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish version: %w", err)
	}
	return nil
}

// preflightDraftSnapshot makes publishing a real admission boundary. The
// persisted snapshot is checked for integrity, validated against the active
// operator policy, and compared after preflight so an old draft cannot be
// silently changed during publication.
func (s *Service) preflightDraftSnapshot(
	ctx context.Context,
	tenantID string,
	appName string,
	rawSnapshot []byte,
	storedHash string,
) error {
	ctx = normalizeContext(ctx)
	var snapshot VersionSnapshot
	if err := json.Unmarshal(rawSnapshot, &snapshot); err != nil {
		return fmt.Errorf("%w: decode persisted version snapshot", ErrInvalidControlPlaneRequest)
	}
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("%w: validate persisted version snapshot: %v", ErrInvalidControlPlaneRequest, err)
	}
	if snapshot.Agent.Name != appName {
		return fmt.Errorf("%w: version snapshot agent %q does not match agent app %q", ErrInvalidControlPlaneRequest, snapshot.Agent.Name, appName)
	}
	canonicalSnapshot, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("%w: encode persisted version snapshot", ErrInvalidControlPlaneRequest)
	}
	digest := sha256.Sum256(canonicalSnapshot)
	if storedHash == "" || !strings.EqualFold(storedHash, hex.EncodeToString(digest[:])) {
		return fmt.Errorf("%w: persisted version snapshot hash does not match", ErrInvalidControlPlaneRequest)
	}

	var prepared VersionSnapshot
	if err := json.Unmarshal(canonicalSnapshot, &prepared); err != nil {
		return fmt.Errorf("%w: copy persisted version snapshot", ErrInvalidControlPlaneRequest)
	}
	if err := s.prepareSnapshot(ctx, tenantID, &prepared); err != nil {
		return fmt.Errorf("revalidate draft version snapshot: %w", err)
	}
	if err := prepared.Validate(); err != nil {
		return fmt.Errorf("%w: validate prepared version snapshot: %v", ErrInvalidControlPlaneRequest, err)
	}
	if prepared.Agent.Name != appName {
		return fmt.Errorf("%w: prepared version snapshot agent %q does not match agent app %q", ErrInvalidControlPlaneRequest, prepared.Agent.Name, appName)
	}
	preparedSnapshot, err := json.Marshal(prepared)
	if err != nil {
		return fmt.Errorf("%w: encode prepared version snapshot", ErrInvalidControlPlaneRequest)
	}
	if !bytes.Equal(canonicalSnapshot, preparedSnapshot) {
		return fmt.Errorf("%w: draft version no longer matches current operator policy; create a new version", ErrInvalidControlPlaneRequest)
	}
	return nil
}

// Deploy atomically replaces the active stable/canary set. Passing no canary
// performs a rollback/cutover to the requested stable version.
func (s *Service) Deploy(ctx context.Context, tenantID, appName, stableVersionID, canaryVersionID string, canaryBPS int, actor string) (*DeploymentSet, error) {
	ctx = normalizeContext(ctx)
	if err := validateControlIdentity(tenantID, appName, actor); err != nil {
		return nil, err
	}
	if err := validateOpaqueID("stable version ID", stableVersionID, 64); err != nil {
		return nil, err
	}
	if canaryVersionID != "" {
		if canaryVersionID == stableVersionID {
			return nil, fmt.Errorf("%w: stable and canary versions must differ", ErrInvalidControlPlaneRequest)
		}
		if err := validateOpaqueID("canary version ID", canaryVersionID, 64); err != nil {
			return nil, err
		}
	}
	if (canaryVersionID == "" && canaryBPS != 0) || (canaryVersionID != "" && (canaryBPS <= 0 || canaryBPS >= 10000)) {
		return nil, fmt.Errorf("%w: canary traffic must be 1..9999 bps when a canary version is set", ErrInvalidControlPlaneRequest)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("control-plane database is not configured")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin deployment: %w", err)
	}
	defer tx.Rollback()
	if err := requireActiveTenant(ctx, tx, tenantID); err != nil {
		return nil, err
	}
	var appID string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM agent_apps WHERE tenant_id=$1 AND name=$2 AND status='active' FOR UPDATE`, tenantID, appName).Scan(&appID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: agent app %q", ErrControlPlaneNotFound, appName)
		}
		return nil, fmt.Errorf("resolve deployment app: %w", err)
	}
	for _, versionID := range []string{stableVersionID, canaryVersionID} {
		if versionID == "" {
			continue
		}
		var status string
		if err := tx.QueryRowContext(ctx, `
			SELECT status
			FROM agent_versions
			WHERE id=$1 AND agent_app_id=$2
			FOR UPDATE`, versionID, appID).Scan(&status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: agent version", ErrControlPlaneNotFound)
			}
			return nil, fmt.Errorf("load deployment version %s: %w", versionID, err)
		}
		if status != "published" {
			return nil, fmt.Errorf("%w: agent version is %s, not published", ErrControlPlaneConflict, status)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE deployments SET status='completed',updated_at=now() WHERE tenant_id=$1 AND agent_app_id=$2 AND status='active'`, tenantID, appID); err != nil {
		return nil, fmt.Errorf("retire active deployments: %w", err)
	}
	result := &DeploymentSet{StableID: uuid.NewString()}
	if _, err := tx.ExecContext(ctx, `INSERT INTO deployments (id,tenant_id,agent_app_id,agent_version_id,kind,traffic_bps,status,created_by) VALUES ($1,$2,$3,$4,'stable',10000,'active',$5)`, result.StableID, tenantID, appID, stableVersionID, actor); err != nil {
		return nil, wrapControlPlaneDBError("create stable deployment", err)
	}
	if canaryVersionID != "" {
		result.CanaryID = uuid.NewString()
		if _, err := tx.ExecContext(ctx, `INSERT INTO deployments (id,tenant_id,agent_app_id,agent_version_id,kind,traffic_bps,status,created_by) VALUES ($1,$2,$3,$4,'canary',$5,'active',$6)`, result.CanaryID, tenantID, appID, canaryVersionID, canaryBPS, actor); err != nil {
			return nil, wrapControlPlaneDBError("create canary deployment", err)
		}
	}
	if err := writeControlAudit(ctx, tx, tenantID, actor, "deployment.activate", "agent_app", appID, map[string]interface{}{
		"stable_version_id": stableVersionID,
		"canary_version_id": canaryVersionID,
		"canary_bps":        canaryBPS,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit deployment: %w", err)
	}
	return result, nil
}

func validateControlIdentity(tenantID, appName, actor string) error {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControlPlaneRequest, err)
	}
	if err := tenant.ValidateAgentAppName(appName); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControlPlaneRequest, err)
	}
	return validateActor(actor)
}

// requireActiveTenant serializes business lifecycle writes with tenant
// suspension/deletion. The row lock is held until the surrounding transaction
// commits, so a status change cannot race between admission and the resource
// mutation. Recovery, deletion and audit operations intentionally do not call
// this helper because they must remain available while a tenant is suspended.
func requireActiveTenant(ctx context.Context, tx *sql.Tx, tenantID string) error {
	var status string
	err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM tenants
		WHERE id=$1
		FOR UPDATE`, tenantID).Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: tenant", ErrControlPlaneNotFound)
		}
		return fmt.Errorf("lock tenant for control-plane write: %w", err)
	}
	if status != string(tenant.TenantStatusActive) {
		return fmt.Errorf("%w: tenant status is %s", ErrTenantInactive, status)
	}
	return nil
}

func validateActor(value string) error {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: actor must be 1..256 bytes of trimmed UTF-8 text", ErrInvalidControlPlaneRequest)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return fmt.Errorf("%w: actor cannot contain control characters", ErrInvalidControlPlaneRequest)
		}
	}
	return nil
}

func validateOpaqueID(kind, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must be 1..%d bytes", ErrInvalidControlPlaneRequest, kind, maxBytes)
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_') {
			return fmt.Errorf("%w: %s contains unsupported characters", ErrInvalidControlPlaneRequest, kind)
		}
	}
	return nil
}

// wrapControlPlaneDBError translates stable PostgreSQL constraint classes into
// API-safe outcomes while preserving ordinary driver errors for 503 handling.
// The operation text is intentionally generic; callers must never receive a
// raw PostgreSQL message because it may contain hostnames or credential hints.
func wrapControlPlaneDBError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code.Class() {
		case "23": // integrity constraint violation (unique/FK/check)
			return fmt.Errorf("%w: %s", ErrControlPlaneConflict, operation)
		case "22": // data exception, normally caused by an invalid request
			return fmt.Errorf("%w: %s", ErrInvalidControlPlaneRequest, operation)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func writeControlAudit(ctx context.Context, tx *sql.Tx, tenantID, actor, action, resourceType, resourceID string, details interface{}) error {
	ctx = normalizeContext(ctx)
	payload, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("encode control-plane audit: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO control_plane_audit
			(tenant_id,actor,action,resource_type,resource_id,details)
		VALUES ($1,$2,$3,$4,$5,$6)`, tenantID, actor, action, resourceType, resourceID, payload); err != nil {
		return fmt.Errorf("write control-plane audit: %w", err)
	}
	return nil
}
