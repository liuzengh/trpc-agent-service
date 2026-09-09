package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/session"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// Replay modes. Dry-run is the default and never writes a single row.
const (
	ReplayModeDryRun = "dry-run"
	ReplayModeRepair = "repair"
)

// Stable replay violation categories. None of them may silently pass and
// none of them may ever trigger a row modification outside the single
// permitted monotonic watermark repair.
const (
	ViolationSequenceGap      = "sequence_gap"
	ViolationEventTypeUnknown = "event_type_unknown"
	ViolationPayloadInvalid   = "payload_invalid"
	ViolationFieldInvalid     = "field_invalid"
	ViolationIdentityMismatch = "identity_mismatch"
	ViolationParentMissing    = "parent_missing"
	ViolationParentFuture     = "parent_future"
	ViolationEventIDDuplicate = "event_id_duplicate"
	ViolationWatermarkHigh    = "watermark_high"
	ViolationWatermarkLow     = "watermark_low"
	ViolationEventLimit       = "session_event_limit"
	ViolationDecodeFailed     = "payload_decode_failed"
)

// ReplayConfig is the operator input of one bounded replay run.
type ReplayConfig struct {
	// OwnerURL is the recovery-owner connection used only to discover the
	// tenant list (explicit offline privilege; no runtime cross-tenant
	// function is added).
	OwnerURL string
	// RuntimeURL is the NOBYPASSRLS runtime role connection used for every
	// per-tenant transaction through the P2-01 tenant context protocol.
	RuntimeURL string
	Mode       string
	// RepairAllowed must be set by the command entrypoint only when the
	// explicit operator gate (flag + environment) is present; the library
	// refuses repair without it.
	RepairAllowed     bool
	BatchSize         int
	SessionEventLimit int
}

// ReplayResult is the deterministic, content-free replay summary: counts,
// violation categories and a digest that never exposes tenant, session or
// event identifiers or payload content.
type ReplayResult struct {
	Mode       string           `json:"mode"`
	Tenants    int              `json:"tenants"`
	Sessions   int64            `json:"sessions"`
	Events     int64            `json:"events"`
	MaxSeq     int64            `json:"max_sequence"`
	Violations map[string]int64 `json:"violations,omitempty"`
	Digest     string           `json:"digest"`
	Repaired   int64            `json:"repaired,omitempty"`
}

// RunReplay performs the bounded session-event replay: keyset-paginated,
// stable (tenant, session, sequence) order, no OFFSET scans, no unbounded
// memory, zero external side effects. Dry-run writes nothing; the only
// permitted write of repair mode is the monotonic raise of a verified-low
// session watermark on a fully valid session.
func RunReplay(ctx context.Context, cfg ReplayConfig) (ReplayResult, error) {
	result := ReplayResult{Mode: cfg.Mode}
	if ctx.Err() != nil {
		return result, fmt.Errorf("%w: replay context already done", ErrTimeoutOrCancelled)
	}
	if cfg.OwnerURL == "" || cfg.RuntimeURL == "" {
		return result, fmt.Errorf("%w: replay requires owner and runtime dsn", ErrInvalidConfig)
	}
	if cfg.Mode != ReplayModeDryRun && cfg.Mode != ReplayModeRepair {
		return result, fmt.Errorf("%w: replay mode unsupported", ErrInvalidConfig)
	}
	if cfg.Mode == ReplayModeRepair && !cfg.RepairAllowed {
		return result, fmt.Errorf("%w: repair requires the explicit operator gate", ErrInvalidConfig)
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 10000 {
		cfg.BatchSize = 500
	}
	if cfg.SessionEventLimit <= 0 {
		cfg.SessionEventLimit = 100000
	}

	ownerPool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: cfg.OwnerURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		if ctx.Err() != nil {
			return result, fmt.Errorf("%w: replay owner pool", ErrTimeoutOrCancelled)
		}
		return result, fmt.Errorf("%w: replay owner pool", ErrDependencyUnavailable)
	}
	defer ownerPool.Close()
	if err := requirePrivilegedRole(ctx, ownerPool, "trpc_runtime"); err != nil {
		return result, err
	}
	runtimePool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: cfg.RuntimeURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		if ctx.Err() != nil {
			return result, fmt.Errorf("%w: replay runtime pool", ErrTimeoutOrCancelled)
		}
		return result, fmt.Errorf("%w: replay runtime pool", ErrDependencyUnavailable)
	}
	defer runtimePool.Close()
	if err := tenantctx.EnsureRuntimeRoleLimits(ctx, runtimePool); err != nil {
		return result, fmt.Errorf("%w: replay runtime role limits", ErrForbiddenState)
	}

	tenantIDs, err := discoverTenants(ctx, ownerPool)
	if err != nil {
		return result, err
	}
	result.Tenants = len(tenantIDs)

	overall := sha256.New()
	for _, tenantID := range tenantIDs {
		err := tenantctx.WithTenantContext(ctx, runtimePool, tenantID, "replay", func(ctx context.Context, tx pgx.Tx) error {
			return replayTenant(ctx, tx, tenantID, cfg, &result, overall)
		})
		if err != nil {
			return result, err
		}
	}
	result.Digest = hex.EncodeToString(overall.Sum(nil))
	if len(result.Violations) > 0 {
		return result, fmt.Errorf("%w: replay integrity violations recorded", ErrReplayViolation)
	}
	return result, nil
}

// discoverTenants lists tenant identifiers with the recovery owner. Values
// are hashed into the digest but never logged or returned.
func discoverTenants(ctx context.Context, pool pgxQuery) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT tenant_id FROM tenant ORDER BY tenant_id`)
	if err != nil {
		return nil, classifyQueryError("tenant discovery", err)
	}
	defer rows.Close()
	var tenants []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, classifyQueryError("tenant discovery scan", err)
		}
		tenants = append(tenants, id)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyQueryError("tenant discovery iterate", err)
	}
	return tenants, nil
}

// replayTenant walks one tenant's sessions in stable keyset order inside the
// tenant transaction and feeds the deterministic overall digest.
func replayTenant(ctx context.Context, tx pgx.Tx, tenantID string, cfg ReplayConfig, result *ReplayResult, overall hash.Hash) error {
	tenantHash := sha256.Sum256([]byte("p2-02|tenant|" + tenantID))
	overall.Write(tenantHash[:])
	lastSession := ""
	for {
		rows, err := tx.Query(ctx, `SELECT session_id, last_event_seq FROM session
			WHERE tenant_id = $1 AND session_id > $2 ORDER BY session_id LIMIT $3`,
			tenantID, lastSession, cfg.BatchSize)
		if err != nil {
			return classifyQueryError("replay session page", err)
		}
		var ids []string
		var watermarks []int64
		for rows.Next() {
			var id string
			var watermark int64
			if err := rows.Scan(&id, &watermark); err != nil {
				rows.Close()
				return classifyQueryError("replay session scan", err)
			}
			ids = append(ids, id)
			watermarks = append(watermarks, watermark)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return classifyQueryError("replay session iterate", err)
		}
		rows.Close()
		if len(ids) == 0 {
			break
		}
		for i, id := range ids {
			if err := replaySession(ctx, tx, tenantID, id, watermarks[i], cfg, result, overall); err != nil {
				return err
			}
			result.Sessions++
			lastSession = id
		}
	}
	return nil
}

// sessionReplay carries the bounded per-session state: a streaming digest
// hasher, the event-id/sequence map for parent validation and the unresolved
// parent backlog.
type sessionReplay struct {
	hasher       hash.Hash
	eventIDs     map[string]int64
	unresolved   map[string]int64
	expectedSeq  int64
	hasViolation bool
}

func newSessionReplay() *sessionReplay {
	return &sessionReplay{
		hasher:      sha256.New(),
		eventIDs:    map[string]int64{},
		unresolved:  map[string]int64{},
		expectedSeq: 1,
	}
}

func replaySession(ctx context.Context, tx pgx.Tx, tenantID, sessionID string, watermark int64, cfg ReplayConfig, result *ReplayResult, overall hash.Hash) error {
	state := newSessionReplay()
	lastSeq := int64(0)
	for {
		rows, err := tx.Query(ctx, `SELECT sequence, event_id, event_type, attempt,
				parent_event_id, message_id, execution_id, trace_id, payload, created_at, tenant_id
			FROM session_event
			WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
			ORDER BY sequence LIMIT $4`, tenantID, sessionID, lastSeq, cfg.BatchSize)
		if err != nil {
			return classifyQueryError("replay event page", err)
		}
		var batch []replaySessionEventRow
		for rows.Next() {
			var row replaySessionEventRow
			if err := rows.Scan(&row.sequence, &row.eventID, &row.eventType, &row.attempt,
				&row.parent, &row.messageID, &row.executionID, &row.traceID, &row.payload,
				&row.createdAt, &row.rowTenant); err != nil {
				rows.Close()
				return classifyQueryError("replay event scan", err)
			}
			batch = append(batch, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return classifyQueryError("replay event iterate", err)
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			if len(state.eventIDs) >= cfg.SessionEventLimit {
				recordViolation(result, state, ViolationEventLimit)
				break
			}
			validateEventRow(tenantID, sessionID, row, state, result)
			result.Events++
			if row.sequence > result.MaxSeq {
				result.MaxSeq = row.sequence
			}
			lastSeq = row.sequence
		}
		if state.hasViolation && len(state.eventIDs) >= cfg.SessionEventLimit {
			break
		}
	}
	// Parents referenced before they appeared: if the identifier exists
	// later in the session it is a future parent, otherwise it is missing.
	// Both are stable fail-closed categories.
	for id := range state.unresolved {
		if _, exists := state.eventIDs[id]; exists {
			recordViolation(result, state, ViolationParentFuture)
		} else {
			recordViolation(result, state, ViolationParentMissing)
		}
	}
	// Watermark comparison against the observed maximum sequence.
	switch {
	case watermark > lastSeq:
		recordViolation(result, state, ViolationWatermarkHigh)
	case watermark < lastSeq:
		if cfg.Mode == ReplayModeRepair && !state.hasViolation {
			tag, err := tx.Exec(ctx, `UPDATE session SET last_event_seq = $1
				WHERE tenant_id = $2 AND session_id = $3 AND last_event_seq = $4 AND last_event_seq < $1`,
				lastSeq, tenantID, sessionID, watermark)
			if err != nil {
				return classifyQueryError("watermark repair", err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("%w: watermark repair lost the monotonic race", ErrForbiddenState)
			}
			result.Repaired += tag.RowsAffected()
		} else {
			recordViolation(result, state, ViolationWatermarkLow)
		}
	}
	// Feed the completed session into the overall digest. The hash chain
	// binds the tenant, the session and every event line without exposing
	// any identifier or payload content.
	sessionHash := sha256.Sum256(state.hasher.Sum(nil))
	overall.Write(sessionHash[:])
	return nil
}

// validateEventRow applies the current release event contract to one row and
// records stable violation categories. It never mutates any row.
// replaySessionEventRow mirrors one session_event row.
type replaySessionEventRow struct {
	sequence    int64
	eventID     string
	eventType   string
	attempt     int
	parent      *string
	messageID   *string
	executionID *string
	traceID     *string
	payload     []byte
	createdAt   time.Time
	rowTenant   string
}

func validateEventRow(tenantID, sessionID string, row replaySessionEventRow, state *sessionReplay, result *ReplayResult) {
	if row.rowTenant != tenantID || sessionID == "" {
		recordViolation(result, state, ViolationIdentityMismatch)
	}
	if row.sequence < 1 || row.attempt < 1 {
		recordViolation(result, state, ViolationFieldInvalid)
	}
	if !validEventTypeValue(row.eventType) {
		recordViolation(result, state, ViolationEventTypeUnknown)
	}
	if !validEventIDValue(row.eventID) {
		recordViolation(result, state, ViolationFieldInvalid)
	}
	for _, value := range []*string{row.parent, row.messageID, row.executionID, row.traceID} {
		if value != nil && !validEventIDValue(*value) {
			recordViolation(result, state, ViolationFieldInvalid)
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(row.payload, &payload); err != nil {
		recordViolation(result, state, ViolationDecodeFailed)
	} else if !reflect.DeepEqual(session.RedactPayload(payload), payload) {
		recordViolation(result, state, ViolationPayloadInvalid)
	}
	// Strict sequence continuity from 1.
	if row.sequence != state.expectedSeq {
		recordViolation(result, state, ViolationSequenceGap)
	}
	state.expectedSeq = row.sequence + 1
	// event_id uniqueness within the session (the primary key makes this
	// impossible; the check is defensive and deterministic).
	if previous, exists := state.eventIDs[row.eventID]; exists {
		recordViolation(result, state, ViolationEventIDDuplicate)
		_ = previous
	} else {
		state.eventIDs[row.eventID] = row.sequence
	}
	// Parent rules: same session, must exist, must be earlier.
	if row.parent != nil {
		if parentSeq, ok := state.eventIDs[*row.parent]; ok {
			if parentSeq >= row.sequence {
				recordViolation(result, state, ViolationParentFuture)
			}
		} else if *row.parent != row.eventID {
			state.unresolved[*row.parent]++
		}
	}
	// Streaming digest line: bounded, deterministic, content-free outside
	// the hash.
	payloadHash := sha256.Sum256(row.payload)
	fmt.Fprintf(state.hasher, "%d|%s|%s|%d|%s|%s|%d\n",
		row.sequence, row.eventID, row.eventType, row.attempt,
		derefOrEmpty(row.parent), hex.EncodeToString(payloadHash[:]), row.createdAt.UnixNano())
}

func recordViolation(result *ReplayResult, state *sessionReplay, category string) {
	if result.Violations == nil {
		result.Violations = map[string]int64{}
	}
	result.Violations[category]++
	state.hasViolation = true
}

func validEventTypeValue(value string) bool {
	switch session.EventType(value) {
	case session.EventUserReceived, session.EventAgentStarted, session.EventToolStarted,
		session.EventToolCompleted, session.EventToolFailed, session.EventAssistantCompleted,
		session.EventExecutionFailed, session.EventExecutionCanceled:
		return true
	default:
		return false
	}
}

// validEventIDValue mirrors the session domain identifier rule: non-empty,
// at most 256 bytes, no surrounding whitespace and no separator characters.
func validEventIDValue(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if r == '|' || r == '\\' || r == '/' || r == 0 || r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return false
		}
	}
	return true
}

func derefOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
