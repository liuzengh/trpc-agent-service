// Package controlplane resolves immutable Agent versions for execution.
package controlplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// DeploymentKind distinguishes the stable and canary routes.
type DeploymentKind string

const (
	DeploymentStable DeploymentKind = "stable"
	DeploymentCanary DeploymentKind = "canary"
)

// ErrNoActiveDeployment allows Worker to distinguish legacy tenant
// configuration from a broken control-plane lookup.
var ErrNoActiveDeployment = errors.New("no active agent deployment")

// ErrVersionNotAvailable means an asynchronous job cannot safely reconstruct
// the immutable runtime it was pinned to. Draft versions and inactive tenant
// or app records are intentionally not executable.
var ErrVersionNotAvailable = errors.New("pinned agent version is not available")

// Typed execution errors let the Worker distinguish a terminal idempotency
// conflict from a transient same-request race. Callers must not retry the
// latter by starting a second model invocation while the first is RUNNING.
var (
	ErrPayloadConflict           = errors.New("execution request payload conflicts with its idempotency key")
	ErrExecutionInProgress       = errors.New("execution for this idempotency key is already running")
	ErrExecutionAlreadySucceeded = errors.New("execution for this idempotency key already succeeded")
	ErrInvocationBindingMissing  = errors.New("invocation binding is missing")
	ErrVersionBindingConflict    = errors.New("execution version does not match the pinned invocation binding")
	ErrRequestIdentityConflict   = errors.New("idempotency key is bound to a different request identity")
	ErrExecutionFenceMismatch    = errors.New("execution attempt token does not match")
	ErrExecutionRecordMissing    = errors.New("execution record is missing")
	ErrExecutionTerminalConflict = errors.New("execution record has a different terminal status")
	ErrExecutionRetryUnsafe      = errors.New("failed execution is not safe to retry automatically")
	ErrExecutionOutcomeUnknown   = errors.New("abandoned execution outcome is unknown")
	ErrExecutionLeaseExpired     = errors.New("execution attempt lease has expired")
	// ErrLegacyExecutionAPI is returned by Start, the pre-fencing entry point.
	// Keeping the method as a hard failure preserves source compatibility for
	// callers while making it impossible to create an execution that has no
	// token or session admission guard.
	ErrLegacyExecutionAPI            = errors.New("legacy execution API is disabled; use StartWithRequest")
	ErrSessionExecutionInProgress    = errors.New("another execution for this session is already running")
	ErrSessionReconciliationRequired = errors.New("session execution requires operator reconciliation")
	ErrReconciliationNotAllowed      = errors.New("execution is not eligible for reconciliation")
	ErrReconciliationConflict        = errors.New("reconciliation already exists with different evidence")
	// ErrInvalidReconciliationRequest marks caller-controlled validation
	// failures. The Admin API may map this to 400 without exposing internal
	// database or driver diagnostics that use the same operation prefix.
	ErrInvalidReconciliationRequest = errors.New("invalid reconciliation request")
)

// VersionSnapshot is immutable once an AgentVersion is published.
type VersionSnapshot struct {
	Agent                        tenant.AgentConfig `json:"agent"`
	Model                        tenant.ModelConfig `json:"model"`
	ModelCatalogRevision         string             `json:"modelCatalogRevision,omitempty"`
	ModelContextWindow           int                `json:"modelContextWindow,omitempty"`
	RuntimeCapabilityFingerprint string             `json:"runtimeCapabilityFingerprint,omitempty"`
}

// Validate prevents immutable deployment snapshots from becoming a second
// secret store. Credentials stay in the encrypted tenant configuration and are
// injected by Worker after a version is resolved.
func (s VersionSnapshot) Validate() error {
	if err := tenant.ValidateAgentModel(s.Agent, s.Model); err != nil {
		return err
	}
	if s.Model.APIKey != "" {
		return fmt.Errorf("version snapshot must not contain a model API key")
	}
	if (s.ModelCatalogRevision == "") != (s.ModelContextWindow == 0) || s.ModelContextWindow < 0 {
		return fmt.Errorf("version model catalog revision and context window must be set together")
	}
	if s.RuntimeCapabilityFingerprint != "" {
		if len(s.RuntimeCapabilityFingerprint) != sha256.Size*2 {
			return fmt.Errorf("version runtime capability fingerprint has invalid length")
		}
		if _, err := hex.DecodeString(s.RuntimeCapabilityFingerprint); err != nil {
			return fmt.Errorf("version runtime capability fingerprint is invalid")
		}
	}
	return nil
}

// ResolvedDeployment binds an invocation to exact immutable configuration.
type ResolvedDeployment struct {
	TenantID      string
	AgentAppID    string
	AgentAppName  string
	VersionID     string
	VersionNumber int64
	DeploymentID  string
	Kind          DeploymentKind
	TrafficBPS    int
	Snapshot      VersionSnapshot
}

// ResolvedVersion is the deployment-independent immutable configuration used
// by derived asynchronous work such as Summary generation.
type ResolvedVersion struct {
	TenantID      string
	AgentAppID    string
	AgentAppName  string
	VersionID     string
	VersionNumber int64
	Snapshot      VersionSnapshot
}

// Resolver selects a deployment using a stable tenant/session hash.
type Resolver interface {
	Resolve(ctx context.Context, tenantID, appName, routingKey string) (*ResolvedDeployment, error)
}

type rowsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// PostgresResolver reads active immutable versions from PostgreSQL.
type PostgresResolver struct {
	db *sql.DB
}

// NewPostgresResolver creates a resolver over an existing connection pool.
func NewPostgresResolver(db *sql.DB) *PostgresResolver {
	return &PostgresResolver{db: db}
}

// LoadVersion resolves exactly the version pinned by a durable asynchronous
// job. Retired versions remain valid for deterministic retries; drafts and
// suspended tenant/app records fail closed.
func (r *PostgresResolver) LoadVersion(
	ctx context.Context,
	tenantID string,
	agentAppID string,
	versionID string,
) (*ResolvedVersion, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("load pinned version: control-plane database is not configured")
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("load pinned version: %w", err)
	}
	if err := validateOpaqueID("agent app ID", agentAppID, 64); err != nil {
		return nil, fmt.Errorf("load pinned version: %w", err)
	}
	if err := validateOpaqueID("version ID", versionID, 64); err != nil {
		return nil, fmt.Errorf("load pinned version: %w", err)
	}
	var encoded []byte
	resolved := &ResolvedVersion{TenantID: tenantID}
	err := r.db.QueryRowContext(normalizeContext(ctx), `
		SELECT aa.id, aa.name, av.id, av.version_number, av.config_snapshot
		FROM agent_apps aa
		JOIN tenants t ON t.id=aa.tenant_id
		JOIN agent_versions av ON av.agent_app_id=aa.id
		WHERE t.id=$1 AND t.status='active'
		  AND aa.id=$2 AND aa.status='active'
		  AND av.id=$3 AND av.status IN ('published','retired')`,
		tenantID, agentAppID, versionID,
	).Scan(
		&resolved.AgentAppID, &resolved.AgentAppName, &resolved.VersionID,
		&resolved.VersionNumber, &encoded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVersionNotAvailable
	}
	if err != nil {
		return nil, fmt.Errorf("load pinned version: %w", err)
	}
	if err := json.Unmarshal(encoded, &resolved.Snapshot); err != nil {
		return nil, fmt.Errorf("decode pinned version %s snapshot: %w", versionID, err)
	}
	if err := resolved.Snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate pinned version %s snapshot: %w", versionID, err)
	}
	if err := validateResolvedAgentIdentity(&ResolvedDeployment{
		AgentAppName: resolved.AgentAppName,
		VersionID:    resolved.VersionID,
		Snapshot:     resolved.Snapshot,
	}); err != nil {
		return nil, err
	}
	return resolved, nil
}

// Resolve returns canary when the stable hash bucket is inside its configured
// basis-point allocation; otherwise it returns stable.
func (r *PostgresResolver) Resolve(ctx context.Context, tenantID, appName, routingKey string) (*ResolvedDeployment, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("resolve agent deployment: control-plane database is not configured")
	}
	ctx = normalizeContext(ctx)
	return resolveActive(ctx, r.db, tenantID, appName, routingKey)
}

func resolveActive(ctx context.Context, queryer rowsQuerier, tenantID, appName, routingKey string) (*ResolvedDeployment, error) {
	ctx = normalizeContext(ctx)
	const query = `
		SELECT aa.id, aa.name, av.id, av.version_number, d.id,
		       d.kind, d.traffic_bps, av.config_snapshot
		FROM agent_apps aa
		JOIN deployments d ON d.agent_app_id=aa.id
		JOIN agent_versions av ON av.id=d.agent_version_id
		WHERE aa.tenant_id=$1 AND aa.name=$2 AND aa.status='active'
		  AND EXISTS (
		      SELECT 1 FROM tenants t
		      WHERE t.id=aa.tenant_id AND t.status='active'
		  )
		  AND d.status='active' AND av.status='published'
		  AND d.kind IN ('stable','canary')
		ORDER BY CASE d.kind WHEN 'stable' THEN 0 ELSE 1 END`
	rows, err := queryer.QueryContext(ctx, query, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("resolve agent deployment: %w", err)
	}
	defer rows.Close()

	var stable, canary *ResolvedDeployment
	for rows.Next() {
		resolved := &ResolvedDeployment{TenantID: tenantID}
		var snapshot []byte
		if err := rows.Scan(
			&resolved.AgentAppID, &resolved.AgentAppName, &resolved.VersionID,
			&resolved.VersionNumber, &resolved.DeploymentID, &resolved.Kind,
			&resolved.TrafficBPS, &snapshot,
		); err != nil {
			return nil, fmt.Errorf("scan agent deployment: %w", err)
		}
		if err := json.Unmarshal(snapshot, &resolved.Snapshot); err != nil {
			return nil, fmt.Errorf("decode version %s snapshot: %w", resolved.VersionID, err)
		}
		if err := resolved.Snapshot.Validate(); err != nil {
			return nil, fmt.Errorf("validate version %s snapshot: %w", resolved.VersionID, err)
		}
		if err := validateResolvedAgentIdentity(resolved); err != nil {
			return nil, err
		}
		switch resolved.Kind {
		case DeploymentStable:
			if stable != nil {
				return nil, fmt.Errorf("multiple active stable deployments for app %s", resolved.AgentAppID)
			}
			stable = resolved
		case DeploymentCanary:
			if canary != nil {
				return nil, fmt.Errorf("multiple active canary deployments for app %s", resolved.AgentAppID)
			}
			canary = resolved
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent deployments: %w", err)
	}
	if stable == nil {
		return nil, ErrNoActiveDeployment
	}
	if canary != nil && InTrafficBucket(tenantID, stable.AgentAppID, routingKey, canary.TrafficBPS) {
		return canary, nil
	}
	return stable, nil
}

// ResolvePinned returns the immutable version previously assigned to an
// idempotency key, or atomically pins the currently routed deployment before
// model execution. The Agent App share lock serializes this decision with an
// Admin deployment cutover, so a retry cannot cross a rollout boundary.
func (r *PostgresResolver) ResolvePinned(
	ctx context.Context,
	tenantID string,
	appName string,
	routingKey string,
	idempotencyKey string,
) (*ResolvedDeployment, error) {
	return r.resolvePinned(ctx, tenantID, appName, routingKey, idempotencyKey, "")
}

// ResolvePinnedWithPayload pins the deployment and binds the canonical request
// payload hash in the same transaction. A later reuse of the provider
// idempotency key with different content is rejected before model execution.
func (r *PostgresResolver) ResolvePinnedWithPayload(
	ctx context.Context,
	tenantID string,
	appName string,
	routingKey string,
	idempotencyKey string,
	payloadHash string,
) (*ResolvedDeployment, error) {
	ctx = normalizeContext(ctx)
	if !validPayloadHash(payloadHash) {
		return nil, fmt.Errorf("resolve pinned: payload hash must be a SHA-256 hex digest")
	}
	return r.resolvePinned(ctx, tenantID, appName, routingKey, idempotencyKey, strings.ToLower(payloadHash))
}

func (r *PostgresResolver) resolvePinned(
	ctx context.Context,
	tenantID string,
	appName string,
	routingKey string,
	idempotencyKey string,
	payloadHash string,
) (*ResolvedDeployment, error) {
	ctx = normalizeContext(ctx)
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("resolve pinned: control-plane database is not configured")
	}
	if tenantID == "" || appName == "" || routingKey == "" || idempotencyKey == "" {
		return nil, fmt.Errorf("tenant, app, routing and idempotency keys are required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin invocation binding: %w", err)
	}
	defer tx.Rollback()

	pinned, err := loadPinned(ctx, tx, tenantID, idempotencyKey)
	if err == nil {
		if err := bindRequestIdentity(ctx, tx, tenantID, idempotencyKey, routingKey, payloadHash); err != nil {
			return nil, err
		}
		if pinned.AgentAppName != appName {
			return nil, fmt.Errorf("idempotency key is already bound to agent app %q", pinned.AgentAppName)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit invocation binding read: %w", err)
		}
		return pinned, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	var appID string
	if err := tx.QueryRowContext(ctx, `
		SELECT aa.id FROM agent_apps aa
		JOIN tenants t ON t.id=aa.tenant_id AND t.status='active'
		WHERE aa.tenant_id=$1 AND aa.name=$2 AND aa.status='active'
		FOR SHARE`, tenantID, appName).Scan(&appID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoActiveDeployment
		}
		return nil, fmt.Errorf("lock agent app for routing: %w", err)
	}
	resolved, err := resolveActive(ctx, tx, tenantID, appName, routingKey)
	if err != nil {
		return nil, err
	}
	if resolved.AgentAppID != appID {
		return nil, fmt.Errorf("resolved agent app changed during routing")
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO invocation_bindings (
			tenant_id, idempotency_key, agent_app_id, agent_version_id,
			deployment_id, session_id, payload_hash
		) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''))
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		tenantID, idempotencyKey, resolved.AgentAppID, resolved.VersionID,
		resolved.DeploymentID, routingKey, payloadHash,
	)
	if err != nil {
		return nil, fmt.Errorf("pin invocation deployment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("pin invocation deployment rows: %w", err)
	}
	if rows == 0 {
		resolved, err = loadPinned(ctx, tx, tenantID, idempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("load concurrent invocation binding: %w", err)
		}
		if resolved.AgentAppName != appName {
			return nil, fmt.Errorf("idempotency key is already bound to agent app %q", resolved.AgentAppName)
		}
	}
	if err := bindRequestIdentity(ctx, tx, tenantID, idempotencyKey, routingKey, payloadHash); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit invocation binding: %w", err)
	}
	return resolved, nil
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func loadPinned(ctx context.Context, tx *sql.Tx, tenantID, idempotencyKey string) (*ResolvedDeployment, error) {
	ctx = normalizeContext(ctx)
	row := tx.QueryRowContext(ctx, `
		SELECT aa.id, aa.name, av.id, av.version_number, d.id,
		       d.kind, d.traffic_bps, av.config_snapshot
		FROM invocation_bindings ib
		JOIN agent_apps aa ON aa.id=ib.agent_app_id AND aa.tenant_id=ib.tenant_id
		JOIN agent_versions av ON av.id=ib.agent_version_id AND av.agent_app_id=ib.agent_app_id
		JOIN deployments d ON d.id=ib.deployment_id AND d.tenant_id=ib.tenant_id
		WHERE ib.tenant_id=$1 AND ib.idempotency_key=$2
		  AND EXISTS (
		      SELECT 1 FROM tenants t
		      WHERE t.id=ib.tenant_id AND t.status='active'
		  )`, tenantID, idempotencyKey)
	return scanResolved(row, tenantID)
}

func scanResolved(row rowScanner, tenantID string) (*ResolvedDeployment, error) {
	resolved := &ResolvedDeployment{TenantID: tenantID}
	var snapshot []byte
	if err := row.Scan(
		&resolved.AgentAppID,
		&resolved.AgentAppName,
		&resolved.VersionID,
		&resolved.VersionNumber,
		&resolved.DeploymentID,
		&resolved.Kind,
		&resolved.TrafficBPS,
		&snapshot,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(snapshot, &resolved.Snapshot); err != nil {
		return nil, fmt.Errorf("decode pinned version %s snapshot: %w", resolved.VersionID, err)
	}
	if err := resolved.Snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate pinned version %s snapshot: %w", resolved.VersionID, err)
	}
	if err := validateResolvedAgentIdentity(resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

// validateResolvedAgentIdentity prevents a malformed or incorrectly joined
// control-plane row from running a version snapshot under another app name.
// Agent.Name is the immutable runtime identity; it must agree with the
// database-owned AgentApp name before the deployment can be returned.
func validateResolvedAgentIdentity(resolved *ResolvedDeployment) error {
	if resolved == nil {
		return fmt.Errorf("resolved deployment is required")
	}
	if resolved.AgentAppName == "" || resolved.Snapshot.Agent.Name == "" {
		return fmt.Errorf("resolved deployment has an incomplete agent identity")
	}
	if resolved.Snapshot.Agent.Name != resolved.AgentAppName {
		return fmt.Errorf("version %s snapshot agent %q does not match agent app %q", resolved.VersionID, resolved.Snapshot.Agent.Name, resolved.AgentAppName)
	}
	return nil
}

// InTrafficBucket uses 10,000 buckets so percentage changes preserve stable
// session affinity and support sub-percent canaries.
func InTrafficBucket(tenantID, appID, routingKey string, trafficBPS int) bool {
	if trafficBPS <= 0 {
		return false
	}
	if trafficBPS >= 10000 {
		return true
	}
	digest := sha256.Sum256([]byte(tenantID + "\x00" + appID + "\x00" + routingKey))
	bucket := int(binary.BigEndian.Uint64(digest[:8]) % 10000)
	return bucket < trafficBPS
}

// ExecutionRecorder persists the exact resolved version before model work.
type ExecutionRecorder struct {
	db              *sql.DB
	exec            func(context.Context, string, ...interface{}) (sql.Result, error)
	leaseTTL        time.Duration
	advisoryFencing bool
}

// ExecutionHandle identifies one append-only execution attempt. Token proves
// ownership when committing that attempt's terminal state. Generation is the
// session admission generation; neither value fences tRPC Session/Memory
// writes because those upstream interfaces do not accept write authority.
type ExecutionHandle struct {
	ID         int64
	Token      string
	Generation int64
}

// Failure classifies a failed attempt and whether replay is known to be safe.
// SafeToRetry must remain false once model or tool execution may have started.
type Failure struct {
	Code        string
	SafeToRetry bool
}

const (
	maxExecutionErrorBytes   = 512
	DefaultExecutionLeaseTTL = 15 * time.Minute
)

// NewExecutionRecorder creates a recorder over an existing connection pool.
func NewExecutionRecorder(db *sql.DB) *ExecutionRecorder {
	recorder := &ExecutionRecorder{db: db, leaseTTL: DefaultExecutionLeaseTTL}
	if db != nil {
		recorder.exec = db.ExecContext
	}
	return recorder
}

// NewExecutionRecorderWithAdvisoryFencing enables the production admission
// protocol. StartWithRequest takes the same PostgreSQL advisory lock that
// fenced Session/Memory adapters hold while an operation is in flight. The
// legacy constructor intentionally remains compatible with sqlmock/unit
// callers; production composition must use this constructor when it enables
// the corresponding adapter capability.
func NewExecutionRecorderWithAdvisoryFencing(db *sql.DB) *ExecutionRecorder {
	recorder := NewExecutionRecorder(db)
	recorder.advisoryFencing = true
	return recorder
}

// NewExecutionRecorderWithLeaseTTL creates a recorder whose attempts must be
// renewed before leaseTTL elapses.
func NewExecutionRecorderWithLeaseTTL(db *sql.DB, leaseTTL time.Duration) (*ExecutionRecorder, error) {
	if leaseTTL < time.Minute || leaseTTL > 24*time.Hour {
		return nil, fmt.Errorf("execution lease TTL must be between 1m and 24h")
	}
	recorder := NewExecutionRecorder(db)
	recorder.leaseTTL = leaseTTL
	return recorder, nil
}

// NewExecutionRecorderWithLeaseTTLAndAdvisoryFencing is the strict
// production constructor combining bounded attempts with session admission
// fencing.
func NewExecutionRecorderWithLeaseTTLAndAdvisoryFencing(db *sql.DB, leaseTTL time.Duration) (*ExecutionRecorder, error) {
	if leaseTTL < time.Minute || leaseTTL > 24*time.Hour {
		return nil, fmt.Errorf("execution lease TTL must be between 1m and 24h")
	}
	recorder := NewExecutionRecorderWithAdvisoryFencing(db)
	recorder.leaseTTL = leaseTTL
	return recorder, nil
}

// Start is the pre-fencing execution API and is intentionally disabled.
//
// Deprecated: use StartWithRequest. The old signature cannot return an
// execution token or admission generation, so accepting it would let a
// caller bypass session fencing and write an unowned RUNNING row.
func (r *ExecutionRecorder) Start(ctx context.Context, tenantID, sessionID string, resolved *ResolvedDeployment) (int64, error) {
	return 0, ErrLegacyExecutionAPI
}

// StartWithRequest opens one token-fenced attempt for an identity already
// pinned by ResolvePinnedWithPayload. Successful result persistence and the
// terminal transition are committed atomically by resultcache.CommitSuccess.
func (r *ExecutionRecorder) StartWithRequest(
	ctx context.Context,
	tenantID, sessionID, idempotencyKey, payloadHash string,
	resolved *ResolvedDeployment,
) (ExecutionHandle, error) {
	ctx = normalizeContext(ctx)
	if r == nil || r.db == nil || resolved == nil {
		return ExecutionHandle{}, fmt.Errorf("record execution: resolved deployment is required")
	}
	if tenantID == "" || sessionID == "" || idempotencyKey == "" {
		return ExecutionHandle{}, fmt.Errorf("record execution: tenant, session and idempotency key are required")
	}
	if len(idempotencyKey) > 256 || strings.ContainsAny(idempotencyKey, "\x00\r\n") {
		return ExecutionHandle{}, fmt.Errorf("record execution: idempotency key is invalid")
	}
	if !validPayloadHash(payloadHash) {
		return ExecutionHandle{}, fmt.Errorf("record execution: payload hash must be a SHA-256 hex digest")
	}
	payloadHash = strings.ToLower(payloadHash)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecutionHandle{}, fmt.Errorf("record execution begin: %w", err)
	}
	defer tx.Rollback()
	if r.advisoryFencing {
		if _, err := tx.ExecContext(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			fence.ScopeFor(tenantID, resolved.AgentAppID, sessionID)); err != nil {
			return ExecutionHandle{}, fmt.Errorf("record execution acquire session fence: %w", err)
		}
	}

	var boundSession, boundHash sql.NullString
	var boundAppID, boundVersionID, boundDeploymentID string
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id
		FROM invocation_bindings ib
		JOIN tenants t ON t.id=ib.tenant_id AND t.status='active'
		WHERE ib.tenant_id=$1 AND ib.idempotency_key=$2
		FOR UPDATE OF ib`, tenantID, idempotencyKey).Scan(
		&boundSession, &boundHash, &boundAppID, &boundVersionID, &boundDeploymentID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionHandle{}, ErrInvocationBindingMissing
		}
		return ExecutionHandle{}, fmt.Errorf("record execution load invocation binding: %w", err)
	}
	if !boundSession.Valid || boundSession.String == "" || boundSession.String != sessionID {
		return ExecutionHandle{}, ErrRequestIdentityConflict
	}
	if !boundHash.Valid || boundHash.String == "" || !strings.EqualFold(boundHash.String, payloadHash) {
		return ExecutionHandle{}, ErrPayloadConflict
	}
	if boundAppID != resolved.AgentAppID || boundVersionID != resolved.VersionID || boundDeploymentID != resolved.DeploymentID {
		return ExecutionHandle{}, ErrVersionBindingConflict
	}

	var (
		lastID, attemptNumber                                                         int64
		lastStatus, lastSession, lastHash, lastAppID, lastVersionID, lastDeploymentID string
		retrySafe, reconciled                                                         bool
	)
	err = tx.QueryRowContext(ctx, `
		SELECT e.id, e.status, e.session_id, COALESCE(e.payload_hash,''), e.agent_app_id,
		       e.agent_version_id, e.deployment_id, e.attempt_number, e.retry_safe,
		       EXISTS (
		           SELECT 1 FROM execution_reconciliations reconciliation
		           WHERE reconciliation.execution_id=e.id
		             AND reconciliation.tenant_id=e.tenant_id
		             AND reconciliation.decision='SAFE_TO_RETRY'
		       )
		FROM execution_records e
		WHERE e.tenant_id=$1 AND e.idempotency_key=$2
		ORDER BY e.id DESC
		LIMIT 1
		FOR UPDATE OF e`, tenantID, idempotencyKey).Scan(
		&lastID, &lastStatus, &lastSession, &lastHash, &lastAppID,
		&lastVersionID, &lastDeploymentID, &attemptNumber, &retrySafe, &reconciled,
	)
	if err == nil {
		if lastSession != sessionID {
			return ExecutionHandle{}, ErrRequestIdentityConflict
		}
		if lastHash != "" && !strings.EqualFold(lastHash, payloadHash) {
			return ExecutionHandle{}, ErrPayloadConflict
		}
		if lastAppID != resolved.AgentAppID || lastVersionID != resolved.VersionID || lastDeploymentID != resolved.DeploymentID {
			return ExecutionHandle{}, ErrVersionBindingConflict
		}
		switch lastStatus {
		case "RUNNING":
			return ExecutionHandle{}, ErrExecutionInProgress
		case "SUCCEEDED":
			return ExecutionHandle{}, ErrExecutionAlreadySucceeded
		case "FAILED":
			if !retrySafe && !reconciled {
				return ExecutionHandle{}, ErrExecutionRetryUnsafe
			}
			attemptNumber++
		case "ABANDONED":
			if !reconciled {
				return ExecutionHandle{}, ErrExecutionOutcomeUnknown
			}
			attemptNumber++
		default:
			return ExecutionHandle{}, fmt.Errorf("record execution: unsupported previous status %q", lastStatus)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ExecutionHandle{}, fmt.Errorf("record execution load previous attempt: %w", err)
	} else {
		attemptNumber = 1
	}
	if err := requireReadySessionGuard(
		ctx, tx, tenantID, resolved.AgentAppID, sessionID,
	); err != nil {
		return ExecutionHandle{}, err
	}

	handle := ExecutionHandle{Token: uuid.NewString()}
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO execution_records (
			tenant_id, session_id, agent_app_id, agent_version_id,
			deployment_id, idempotency_key, payload_hash, attempt_number,
			execution_token, status, heartbeat_at, lease_until
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,'RUNNING',clock_timestamp(),
			clock_timestamp() + ($10 * INTERVAL '1 millisecond')
		)
		RETURNING id`, tenantID, sessionID, resolved.AgentAppID, resolved.VersionID,
		resolved.DeploymentID, idempotencyKey, payloadHash, attemptNumber,
		handle.Token, r.leaseTTL.Milliseconds()).Scan(&handle.ID); err != nil {
		return ExecutionHandle{}, fmt.Errorf("record execution start: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		UPDATE session_execution_guards
		SET status='RUNNING', generation=generation+1,
		    current_execution_id=$4, blocked_reason='', updated_at=now()
		WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
		  AND status='READY' AND current_execution_id IS NULL
		RETURNING generation`, tenantID, resolved.AgentAppID, sessionID, handle.ID).
		Scan(&handle.Generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionHandle{}, ErrSessionReconciliationRequired
		}
		return ExecutionHandle{}, fmt.Errorf("record execution claim session guard: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ExecutionHandle{}, fmt.Errorf("record execution start commit: %w", err)
	}
	return handle, nil
}

func requireReadySessionGuard(
	ctx context.Context,
	tx *sql.Tx,
	tenantID, appID, sessionID string,
) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_execution_guards (
			tenant_id, agent_app_id, session_id, status
		) VALUES ($1,$2,$3,'READY')
		ON CONFLICT (tenant_id, agent_app_id, session_id) DO NOTHING`,
		tenantID, appID, sessionID,
	); err != nil {
		return fmt.Errorf("record execution initialize session guard: %w", err)
	}
	var status string
	var currentExecutionID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT status, current_execution_id
		FROM session_execution_guards
		WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
		FOR UPDATE`, tenantID, appID, sessionID).Scan(&status, &currentExecutionID); err != nil {
		return fmt.Errorf("record execution lock session guard: %w", err)
	}
	switch status {
	case "READY":
		if currentExecutionID.Valid {
			return ErrSessionReconciliationRequired
		}
		return nil
	case "RUNNING":
		return ErrSessionExecutionInProgress
	case "BLOCKED":
		return ErrSessionReconciliationRequired
	default:
		return fmt.Errorf("record execution: unsupported session guard status %q", status)
	}
}

// bindRequestIdentity establishes the immutable session and payload identity
// for a post-migration binding. The row lock prevents two replicas from
// backfilling different identities concurrently.
func bindRequestIdentity(
	ctx context.Context,
	tx *sql.Tx,
	tenantID, idempotencyKey, sessionID, payloadHash string,
) error {
	if sessionID == "" || len(sessionID) > 512 || !utf8.ValidString(sessionID) ||
		strings.ContainsAny(sessionID, "\x00\r\n") {
		return fmt.Errorf("resolve pinned: session ID is invalid")
	}
	if payloadHash != "" && !validPayloadHash(payloadHash) {
		return fmt.Errorf("resolve pinned: payload hash must be a SHA-256 hex digest")
	}
	var storedSession, storedHash sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id, payload_hash FROM invocation_bindings
		WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE`, tenantID, idempotencyKey).
		Scan(&storedSession, &storedHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInvocationBindingMissing
		}
		return fmt.Errorf("resolve pinned load request binding: %w", err)
	}
	if storedSession.Valid && storedSession.String != "" && storedSession.String != sessionID {
		return ErrRequestIdentityConflict
	}
	if payloadHash != "" && storedHash.Valid && storedHash.String != "" &&
		!strings.EqualFold(storedHash.String, payloadHash) {
		return ErrPayloadConflict
	}
	if !storedSession.Valid || storedSession.String == "" ||
		(payloadHash != "" && (!storedHash.Valid || storedHash.String == "")) {
		if _, err := tx.ExecContext(ctx, `
			UPDATE invocation_bindings
			SET session_id=CASE WHEN session_id IS NULL OR session_id='' THEN $3 ELSE session_id END,
			    payload_hash=CASE
			        WHEN $4='' THEN payload_hash
			        WHEN payload_hash IS NULL OR payload_hash='' THEN $4
			        ELSE payload_hash
			    END
			WHERE tenant_id=$1 AND idempotency_key=$2
			  AND (session_id IS NULL OR session_id='' OR session_id=$3)
			  AND ($4='' OR payload_hash IS NULL OR payload_hash='' OR LOWER(payload_hash)=LOWER($4))`,
			tenantID, idempotencyKey, sessionID, strings.ToLower(payloadHash)); err != nil {
			return fmt.Errorf("resolve pinned bind request identity: %w", err)
		}
	}
	return nil
}

// Finish supports legacy, non-idempotent execution rows only. Fenced request
// executions use the token-bound failure path or resultcache.CommitSuccess.
func (r *ExecutionRecorder) Finish(ctx context.Context, id int64, status, errorMessage string) error {
	ctx = normalizeContext(ctx)
	if status != "SUCCEEDED" && status != "FAILED" {
		return fmt.Errorf("record execution finish: invalid status %q", status)
	}
	if r == nil || r.exec == nil {
		return fmt.Errorf("record execution finish: database is not configured")
	}
	errorMessage = sanitizeExecutionError(errorMessage)
	result, err := r.exec(ctx, `
		UPDATE execution_records
		SET status=$2, error_message=$3, completed_at=now()
		WHERE id=$1 AND execution_token='' AND status='RUNNING'`, id, status, errorMessage)
	if err != nil {
		return fmt.Errorf("record execution finish: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record execution rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("record execution finish: record %d is not running", id)
	}
	return nil
}

// Fail records a fenced failure. Retry safety is durable state consumed by
// StartWithRequest; it is not inferred later from an error string.
func (r *ExecutionRecorder) Fail(ctx context.Context, handle ExecutionHandle, failure Failure) error {
	ctx = normalizeContext(ctx)
	if r == nil || r.db == nil {
		return fmt.Errorf("record execution failure: database is not configured")
	}
	if handle.ID <= 0 || handle.Token == "" || len(handle.Token) > 64 ||
		strings.ContainsAny(handle.Token, "\x00\r\n") {
		return fmt.Errorf("record execution failure: execution handle is invalid")
	}
	failure.Code = sanitizeExecutionError(failure.Code)
	if failure.Code == "" {
		return fmt.Errorf("record execution failure: failure code is required")
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE execution_records
		SET status='FAILED', retry_safe=$3, error_message=$4,
		    completed_at=clock_timestamp(), lease_until=clock_timestamp()
		WHERE id=$1 AND execution_token=$2 AND status='RUNNING'
		  AND lease_until > clock_timestamp()`,
		handle.ID, handle.Token, failure.SafeToRetry, failure.Code)
	if err != nil {
		return fmt.Errorf("record execution failure: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record execution failure rows: %w", err)
	}
	if rows == 1 {
		return nil
	}
	if rows != 0 {
		return fmt.Errorf("record execution failure: updated %d records", rows)
	}

	var storedToken, storedStatus string
	var storedRetrySafe bool
	var leaseUntil time.Time
	var leaseValid bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT execution_token, status, retry_safe, lease_until,
		       (lease_until > clock_timestamp())
		FROM execution_records
		WHERE id=$1`, handle.ID).Scan(&storedToken, &storedStatus, &storedRetrySafe, &leaseUntil, &leaseValid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrExecutionRecordMissing
		}
		return fmt.Errorf("record execution failure state: %w", err)
	}
	if storedToken != handle.Token {
		return ErrExecutionFenceMismatch
	}
	if storedStatus == "RUNNING" && !leaseValid {
		return ErrExecutionLeaseExpired
	}
	if storedStatus == "FAILED" && storedRetrySafe == failure.SafeToRetry {
		return nil
	}
	return fmt.Errorf(
		"%w: record=%d status=%s retry_safe=%t",
		ErrExecutionTerminalConflict, handle.ID, storedStatus, storedRetrySafe,
	)
}

// RenewLease extends a live attempt only when both its token and its current
// lease are valid. An expired attempt cannot resurrect itself before the
// reconciler observes it.
func (r *ExecutionRecorder) RenewLease(ctx context.Context, handle ExecutionHandle) error {
	ctx = normalizeContext(ctx)
	if r == nil || r.db == nil || r.leaseTTL <= 0 {
		return fmt.Errorf("renew execution lease: recorder is not configured")
	}
	if handle.ID <= 0 || handle.Token == "" || len(handle.Token) > 64 ||
		strings.ContainsAny(handle.Token, "\x00\r\n") {
		return fmt.Errorf("renew execution lease: execution handle is invalid")
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE execution_records
		SET heartbeat_at=clock_timestamp(),
		    lease_until=clock_timestamp() + ($3 * INTERVAL '1 millisecond')
		WHERE id=$1 AND execution_token=$2 AND status='RUNNING'
		  AND lease_until > clock_timestamp()`, handle.ID, handle.Token, r.leaseTTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("renew execution lease: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("renew execution lease rows: %w", err)
	}
	if rows == 1 {
		return nil
	}
	if rows != 0 {
		return fmt.Errorf("renew execution lease: updated %d records", rows)
	}

	var storedToken, status string
	var leaseUntil time.Time
	var leaseValid bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT execution_token, status, lease_until,
		       (lease_until > clock_timestamp())
		FROM execution_records
		WHERE id=$1`, handle.ID).Scan(&storedToken, &status, &leaseUntil, &leaseValid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrExecutionRecordMissing
		}
		return fmt.Errorf("renew execution lease state: %w", err)
	}
	if storedToken != handle.Token {
		return ErrExecutionFenceMismatch
	}
	if status != "RUNNING" {
		return fmt.Errorf("%w: record=%d status=%s", ErrExecutionTerminalConflict, handle.ID, status)
	}
	if !leaseValid {
		return ErrExecutionLeaseExpired
	}
	return fmt.Errorf("renew execution lease: update rejected")
}

// RunHeartbeat renews one attempt until ctx is cancelled. The first failed
// renewal ends the loop so the caller can cancel model and tool execution.
func (r *ExecutionRecorder) RunHeartbeat(
	ctx context.Context,
	handle ExecutionHandle,
	interval time.Duration,
) error {
	if r == nil || r.leaseTTL <= 0 || interval <= 0 || interval >= r.leaseTTL/2 {
		return fmt.Errorf("execution heartbeat interval must be positive and less than half the lease TTL")
	}
	ctx = normalizeContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.RenewLease(ctx, handle); err != nil {
				// Stopping a completed invocation can cancel an in-flight SQL
				// renewal. CommitSuccess still validates the database lease.
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func validPayloadHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sanitizeExecutionError(value string) string {
	value = strings.Map(func(r rune) rune {
		switch r {
		case '\r', '\n', '\t', 0:
			return ' '
		default:
			return r
		}
	}, value)
	value = strings.TrimSpace(value)
	if len(value) > maxExecutionErrorBytes {
		value = value[:maxExecutionErrorBytes]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}

// ReconcileExpired marks only lease-expired RUNNING records ABANDONED. A
// session guard is the admission serialization seam, so one pass must never
// update two attempts from the same session in one SQL statement: the
// row-level guard trigger transitions the guard after the first attempt. The
// database path therefore locks execution rows first, deduplicates by session
// in memory, then locks each guard and updates one attempt at a time. This
// preserves the execution->guard lock order used by terminal writers and
// lets a later pass drain legacy unresolved attempts safely.
func (r *ExecutionRecorder) ReconcileExpired(ctx context.Context, now time.Time, limit int) (int64, error) {
	ctx = normalizeContext(ctx)
	if r == nil {
		return 0, fmt.Errorf("reconcile executions: database is not configured")
	}
	if now.IsZero() || limit <= 0 || limit > 10000 {
		return 0, fmt.Errorf("reconcile executions: current time and limit 1..10000 are required")
	}
	if r.db != nil {
		return r.reconcileExpiredTx(ctx, now.UTC(), limit)
	}
	if r.exec == nil {
		return 0, fmt.Errorf("reconcile executions: database is not configured")
	}
	// Keep the injectable executor used by package-level tests and embedders.
	// Production recorders always have db set and use reconcileExpiredTx above.
	result, err := r.exec(ctx, `
		WITH stale AS (
			SELECT DISTINCT ON (e.tenant_id, e.agent_app_id, e.session_id) e.id
			FROM execution_records e
			JOIN session_execution_guards g
			  ON g.tenant_id=e.tenant_id
			 AND g.agent_app_id=e.agent_app_id
			 AND g.session_id=e.session_id
			 AND (g.status='READY' OR g.status='BLOCKED' OR g.current_execution_id=e.id)
			WHERE e.status='RUNNING' AND e.idempotency_key IS NOT NULL
			  AND e.idempotency_key <> '' AND e.lease_until < $1
			  AND e.lease_until < clock_timestamp()
			ORDER BY e.tenant_id, e.agent_app_id, e.session_id,
			         e.lease_until, e.id
			LIMIT $2
		)
		UPDATE execution_records execution
		SET status='ABANDONED', retry_safe=FALSE,
		    error_message='expired_execution_lease', completed_at=now(), lease_until=now()
		FROM stale
		WHERE execution.id=stale.id AND execution.status='RUNNING'
		  AND execution.lease_until < $1
		  AND execution.lease_until < clock_timestamp()`, now.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("reconcile executions: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("reconcile executions rows: %w", err)
	}
	return rows, nil
}

type staleExecutionCandidate struct {
	id        int64
	tenantID  string
	appID     string
	sessionID string
}

// reconcileExpiredTx performs one bounded pass with explicit row-by-row
// transitions. PostgreSQL row triggers run once per affected row; updating a
// multi-row batch for one session would make the second trigger observe a
// guard that the first trigger already moved to BLOCKED and abort the whole
// statement. Candidates are locked in execution order, deduplicated by
// tenant/app/session, and then paired with the guard under the same
// transaction. A BLOCKED guard is temporarily re-pointed to the stale row so
// the existing strict trigger can make the transition and leave the session
// blocked until every uncertain attempt is reconciled.
func (r *ExecutionRecorder) reconcileExpiredTx(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("reconcile executions begin: %w", err)
	}
	defer tx.Rollback()

	candidateQuery := `
		SELECT e.id, e.tenant_id, e.agent_app_id, e.session_id
		FROM execution_records e
		JOIN session_execution_guards g
		  ON g.tenant_id=e.tenant_id
		 AND g.agent_app_id=e.agent_app_id
		 AND g.session_id=e.session_id
		 AND (g.status='READY' OR g.status='BLOCKED' OR
		      (g.status='RUNNING' AND g.current_execution_id=e.id))
		WHERE e.status='RUNNING' AND e.idempotency_key IS NOT NULL
			  AND e.idempotency_key <> '' AND e.lease_until < $1
			  AND e.lease_until < clock_timestamp()
		ORDER BY e.tenant_id, e.agent_app_id, e.session_id,
		         e.lease_until, e.id
		LIMIT $2`
	// Strict production reconciliation must acquire the same advisory lock as
	// StartWithRequest and the fenced Session/Memory services before it takes
	// any row lock. The legacy sqlmock-compatible path retains SKIP LOCKED;
	// it is not used by the production worker constructor.
	if !r.advisoryFencing {
		candidateQuery += ` FOR UPDATE OF e SKIP LOCKED`
	}
	rows, err := tx.QueryContext(ctx, candidateQuery, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("reconcile executions list stale attempts: %w", err)
	}
	candidates := make([]staleExecutionCandidate, 0, limit)
	for rows.Next() {
		var candidate staleExecutionCandidate
		if err := rows.Scan(&candidate.id, &candidate.tenantID, &candidate.appID, &candidate.sessionID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("reconcile executions scan stale attempt: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("reconcile executions iterate stale attempts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("reconcile executions close stale attempts: %w", err)
	}

	seenSessions := make(map[string]struct{}, len(candidates))
	var reconciled int64
	for _, candidate := range candidates {
		sessionKey := candidate.tenantID + "\x00" + candidate.appID + "\x00" + candidate.sessionID
		if _, seen := seenSessions[sessionKey]; seen {
			continue
		}
		seenSessions[sessionKey] = struct{}{}

		if r.advisoryFencing {
			if err := acquireSessionAdvisoryXact(ctx, tx,
				candidate.tenantID, candidate.appID, candidate.sessionID); err != nil {
				return 0, err
			}
			// The candidate list is deliberately unlocked in strict mode. Re-read
			// the attempt after the advisory lock so a concurrent finisher or
			// heartbeat can invalidate the stale snapshot before we lock rows.
			var (
				currentTenant, currentApp, currentSession, currentStatus string
				currentLease                                             time.Time
			)
			if err := tx.QueryRowContext(ctx, `
				SELECT tenant_id, agent_app_id, session_id, status, lease_until
				FROM execution_records
				WHERE id=$1 AND status='RUNNING' AND lease_until < clock_timestamp()
				FOR UPDATE`, candidate.id).Scan(
				&currentTenant, &currentApp, &currentSession, &currentStatus, &currentLease,
			); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return 0, fmt.Errorf("reconcile executions reload stale attempt %d: %w", candidate.id, err)
			}
			if currentTenant != candidate.tenantID || currentApp != candidate.appID ||
				currentSession != candidate.sessionID || currentStatus != "RUNNING" ||
				!currentLease.Before(cutoff) {
				continue
			}
		}

		var guardStatus string
		var currentExecutionID sql.NullInt64
		if err := tx.QueryRowContext(ctx, `
			SELECT status, current_execution_id
			FROM session_execution_guards
			WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
			FOR UPDATE`, candidate.tenantID, candidate.appID, candidate.sessionID).
			Scan(&guardStatus, &currentExecutionID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// A guarded execution must never be finalized without a guard.
				return 0, fmt.Errorf("reconcile executions: guard missing for execution %d", candidate.id)
			}
			return 0, fmt.Errorf("reconcile executions lock session guard: %w", err)
		}

		switch guardStatus {
		case "RUNNING":
			if !currentExecutionID.Valid || currentExecutionID.Int64 != candidate.id {
				// The guard changed after the candidate snapshot. The execution
				// row remains locked; leave it for a later pass rather than
				// weakening the trigger's ownership check.
				continue
			}
		case "BLOCKED":
			// Migration 019 can leave more than one unresolved RUNNING row
			// for a session. Re-pointing the guard while it is locked lets the
			// strict trigger process one such row without accepting stale
			// terminal writes from a worker.
			result, err := tx.ExecContext(ctx, `
				UPDATE session_execution_guards
				SET status='RUNNING', current_execution_id=$4,
				    blocked_reason='', updated_at=now()
				WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
				  AND status='BLOCKED'`,
				candidate.tenantID, candidate.appID, candidate.sessionID, candidate.id)
			if err != nil {
				return 0, fmt.Errorf("reconcile executions claim blocked guard: %w", err)
			}
			guardRows, err := result.RowsAffected()
			if err != nil {
				return 0, fmt.Errorf("reconcile executions claim guard rows: %w", err)
			}
			if guardRows != 1 {
				return 0, fmt.Errorf("reconcile executions: guard changed while claiming execution %d", candidate.id)
			}
		default:
			// READY with a stale RUNNING row is an invariant violation. Fail
			// closed and leave both rows untouched for operator inspection.
			return 0, fmt.Errorf("reconcile executions: stale execution %d has guard status %q", candidate.id, guardStatus)
		}

		result, err := tx.ExecContext(ctx, `
			UPDATE execution_records
			SET status='ABANDONED', retry_safe=FALSE,
			    error_message='expired_execution_lease', completed_at=now(), lease_until=now()
				WHERE id=$1 AND tenant_id=$2 AND status='RUNNING'
				  AND lease_until < $3 AND lease_until < clock_timestamp()`, candidate.id, candidate.tenantID, cutoff)
		if err != nil {
			return 0, fmt.Errorf("reconcile executions abandon attempt %d: %w", candidate.id, err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("reconcile executions abandon rows: %w", err)
		}
		if rowsAffected != 1 {
			return 0, fmt.Errorf("reconcile executions: attempt %d changed while locked", candidate.id)
		}
		reconciled++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("reconcile executions commit: %w", err)
	}
	return reconciled, nil
}

// acquireSessionAdvisoryXact is the database-side serialization seam shared
// by strict admission, reconciliation, and backend-native Session/Memory
// fences. Callers must acquire it before execution/guard row locks; keeping a
// single lock order prevents a stale reconciler from racing a live worker.
func acquireSessionAdvisoryXact(ctx context.Context, tx *sql.Tx, tenantID, appID, sessionID string) error {
	ctx = normalizeContext(ctx)
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fence.ScopeFor(tenantID, appID, sessionID)); err != nil {
		return fmt.Errorf("reconcile executions acquire session fence: %w", err)
	}
	return nil
}

// ReconcileForRetry records an operator's decision that one uncertain
// execution may be retried. The execution row, reconciliation record, audit
// event, and session admission guard are updated in one transaction. A
// reconciliation never overwrites an existing decision: repeating the exact
// request is idempotent, while different evidence is a conflict.
func (r *ExecutionRecorder) ReconcileForRetry(
	ctx context.Context,
	tenantID string,
	executionID int64,
	actor, reason, evidence string,
) error {
	ctx = normalizeContext(ctx)
	if r == nil || r.db == nil {
		return fmt.Errorf("reconcile execution: database is not configured")
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: tenant ID is invalid", ErrInvalidReconciliationRequest)
	}
	if executionID <= 0 {
		return fmt.Errorf("%w: execution ID must be positive", ErrInvalidReconciliationRequest)
	}
	if err := validateReconciliationText("actor", actor, 256, true); err != nil {
		return err
	}
	if err := validateReconciliationText("reason", reason, 512, true); err != nil {
		return err
	}
	if err := validateReconciliationText("evidence", evidence, 2048, false); err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reconcile execution begin: %w", err)
	}
	defer tx.Rollback()

	var (
		sessionID, appID, status, idempotencyKey string
		retrySafe                                bool
	)
	loadExecutionQuery := `
		SELECT session_id, agent_app_id, status, retry_safe,
		       COALESCE(idempotency_key,'')
		FROM execution_records
		WHERE tenant_id=$1 AND id=$2`
	if !r.advisoryFencing {
		loadExecutionQuery += ` FOR UPDATE`
	}
	err = tx.QueryRowContext(ctx, loadExecutionQuery, tenantID, executionID).Scan(
		&sessionID, &appID, &status, &retrySafe, &idempotencyKey,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrExecutionRecordMissing
	}
	if err != nil {
		return fmt.Errorf("reconcile execution load attempt: %w", err)
	}
	if idempotencyKey == "" || (status != "ABANDONED" && !(status == "FAILED" && !retrySafe)) {
		return ErrReconciliationNotAllowed
	}
	if r.advisoryFencing {
		// The first read only discovers the lock scope. Acquire the same
		// session advisory fence used by admission and backend operations, then
		// lock and re-read the execution row. This preserves advisory ->
		// execution -> guard ordering and prevents an operator decision from
		// racing a live Session/Memory operation.
		if err := acquireSessionAdvisoryXact(ctx, tx, tenantID, appID, sessionID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `
			SELECT session_id, agent_app_id, status, retry_safe,
			       COALESCE(idempotency_key,'')
			FROM execution_records
			WHERE tenant_id=$1 AND id=$2
			FOR UPDATE`, tenantID, executionID).Scan(
			&sessionID, &appID, &status, &retrySafe, &idempotencyKey,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrExecutionRecordMissing
			}
			return fmt.Errorf("reconcile execution reload attempt: %w", err)
		}
		if idempotencyKey == "" || (status != "ABANDONED" && !(status == "FAILED" && !retrySafe)) {
			return ErrReconciliationNotAllowed
		}
	}

	var existingActor, existingReason, existingEvidence, decision string
	newDecision := false
	err = tx.QueryRowContext(ctx, `
		SELECT decision, actor, reason, evidence
		FROM execution_reconciliations
		WHERE tenant_id=$1 AND execution_id=$2
		FOR UPDATE`, tenantID, executionID).Scan(
		&decision, &existingActor, &existingReason, &existingEvidence,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("reconcile execution load decision: %w", err)
	}
	if err == nil {
		if decision != "SAFE_TO_RETRY" || existingActor != actor ||
			existingReason != reason || existingEvidence != evidence {
			return ErrReconciliationConflict
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO execution_reconciliations
				(execution_id, tenant_id, decision, actor, reason, evidence)
			VALUES ($1,$2,'SAFE_TO_RETRY',$3,$4,$5)`,
			executionID, tenantID, actor, reason, evidence); err != nil {
			return fmt.Errorf("reconcile execution record decision: %w", err)
		}
		newDecision = true
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_execution_guards
			(tenant_id, agent_app_id, session_id, status)
		VALUES ($1,$2,$3,'READY')
		ON CONFLICT (tenant_id, agent_app_id, session_id) DO NOTHING`,
		tenantID, appID, sessionID); err != nil {
		return fmt.Errorf("reconcile execution initialize session guard: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT status, current_execution_id
		FROM session_execution_guards
		WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
		FOR UPDATE`, tenantID, appID, sessionID).Scan(new(string), new(sql.NullInt64)); err != nil {
		return fmt.Errorf("reconcile execution lock session guard: %w", err)
	}
	var guardStatus string
	var currentExecutionID sql.NullInt64
	// The guard is now held, so a concurrent StartWithRequest cannot commit a
	// new owner while this snapshot is being calculated. The read is
	// deliberately non-locking to preserve the execution->guard lock order used
	// by the finisher trigger and expiry reconciler.
	// The guard lock prevents a new StartWithRequest from admitting work while
	// this snapshot is calculated. This read intentionally does not take an
	// execution lock: all pre-existing rows were locked above, and a concurrent
	// finisher may safely publish its terminal state through the guard trigger.
	unresolvedRows, err := tx.QueryContext(ctx, `
		SELECT e.id, e.status
		FROM execution_records e
		WHERE e.tenant_id=$1 AND e.agent_app_id=$2 AND e.session_id=$3
		  AND e.idempotency_key IS NOT NULL AND e.idempotency_key <> ''
		  AND (
			 e.status='RUNNING'
			 OR e.status='ABANDONED'
			 OR (e.status='FAILED' AND e.retry_safe=FALSE)
		  )
		  AND NOT EXISTS (
			 SELECT 1 FROM execution_reconciliations reconciliation
			 WHERE reconciliation.tenant_id=e.tenant_id
			   AND reconciliation.execution_id=e.id
		  )
		ORDER BY CASE WHEN e.status='RUNNING' THEN 0 ELSE 1 END, e.id DESC`,
		tenantID, appID, sessionID)
	if err != nil {
		return fmt.Errorf("reconcile execution list unresolved attempts: %w", err)
	}
	count := 0
	hasRunning := false
	var latestUnresolvedID, latestRunningID int64
	for unresolvedRows.Next() {
		var unresolvedID int64
		var unresolvedStatus string
		if err := unresolvedRows.Scan(&unresolvedID, &unresolvedStatus); err != nil {
			unresolvedRows.Close()
			return fmt.Errorf("reconcile execution scan unresolved attempt: %w", err)
		}
		count++
		if count == 1 {
			latestUnresolvedID = unresolvedID
		}
		if unresolvedStatus == "RUNNING" {
			hasRunning = true
			if latestRunningID == 0 {
				latestRunningID = unresolvedID
			}
		}
	}
	if err := unresolvedRows.Err(); err != nil {
		unresolvedRows.Close()
		return fmt.Errorf("reconcile execution iterate unresolved attempts: %w", err)
	}
	if err := unresolvedRows.Close(); err != nil {
		return fmt.Errorf("reconcile execution close unresolved attempts: %w", err)
	}
	if count == 0 {
		guardStatus = "READY"
		currentExecutionID = sql.NullInt64{}
	} else if hasRunning {
		guardStatus = "RUNNING"
		currentExecutionID = sql.NullInt64{Int64: latestRunningID, Valid: true}
	} else {
		guardStatus = "BLOCKED"
		currentExecutionID = sql.NullInt64{Int64: latestUnresolvedID, Valid: true}
	}
	blockedReason := ""
	if guardStatus == "BLOCKED" {
		blockedReason = "unresolved_execution"
	}
	var guardExecution interface{}
	if currentExecutionID.Valid {
		guardExecution = currentExecutionID.Int64
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE session_execution_guards
		SET status=$4, current_execution_id=$5, blocked_reason=$6, updated_at=now()
		WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`,
		tenantID, appID, sessionID, guardStatus, guardExecution, blockedReason)
	if err != nil {
		return fmt.Errorf("reconcile execution update session guard: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("reconcile execution guard rows: %w", err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("reconcile execution guard row is missing")
	}

	details := map[string]interface{}{
		"execution_id": executionID,
		"session_id":   sessionID,
		"decision":     "SAFE_TO_RETRY",
		"reason":       reason,
		"evidence":     evidence,
		"guard_status": guardStatus,
	}
	if newDecision {
		if err := writeControlAudit(ctx, tx, tenantID, actor,
			"execution.reconcile", "execution", strconv.FormatInt(executionID, 10), details); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("reconcile execution commit: %w", err)
	}
	return nil
}

func validateReconciliationText(field, value string, maxBytes int, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidReconciliationRequest, field)
	}
	if strings.TrimSpace(value) != value || len(value) > maxBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n\t") {
		return fmt.Errorf("%w: %s is invalid or exceeds %d bytes", ErrInvalidReconciliationRequest, field, maxBytes)
	}
	return nil
}

// RunReconciler continues after transient database errors and stops only when
// ctx is cancelled.
func (r *ExecutionRecorder) RunReconciler(
	ctx context.Context,
	interval time.Duration,
	batchSize int,
	onError func(error),
) error {
	return r.RunReconcilerWithObserver(ctx, interval, batchSize, nil, onError)
}

// RunReconcilerWithObserver exposes successful row counts for production
// metrics without coupling the control-plane package to a metrics backend.
func (r *ExecutionRecorder) RunReconcilerWithObserver(
	ctx context.Context,
	interval time.Duration,
	batchSize int,
	onReconcile func(int64),
	onError func(error),
) error {
	if interval <= 0 || batchSize <= 0 || batchSize > 10000 {
		return fmt.Errorf("execution reconciler requires a positive interval and batch size 1..10000")
	}
	ctx = normalizeContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			rows, err := r.ReconcileExpired(ctx, now.UTC(), batchSize)
			if err != nil {
				if ctx.Err() == nil && onError != nil {
					onError(err)
				}
				continue
			}
			if onReconcile != nil {
				onReconcile(rows)
			}
		}
	}
}
