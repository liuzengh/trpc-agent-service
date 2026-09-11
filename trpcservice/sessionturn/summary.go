package sessionturn

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	ErrSummaryBoundary = errors.New("sessionturn: summary boundary is invalid")
	ErrSummaryConflict = errors.New("sessionturn: summary content conflicts with an existing record")
	ErrSummaryJobLost  = errors.New("sessionturn: summary job lease lost")
)

const (
	defaultSummaryGenerator = "trpc-summary-v1"
	defaultSummaryPrompt    = "default"
	defaultSummaryLease     = 30 * time.Second
	maxSummaryAttempts      = 5
)

// SummaryWrite is the complete claim made by one summarizer invocation. The
// Events field is used for source-hash validation and is never persisted as
// raw transcript content in the summary table.
type SummaryWrite struct {
	TenantID               string
	Key                    session.Key
	FilterKey              string
	CoveredFromSequence    int64
	CoveredThroughSequence int64
	LastEventID            string
	SessionVersion         int64
	SummaryVersion         int64
	BoundaryVersion        int
	GeneratorVersion       string
	ModelVersion           string
	PromptVersion          string
	SourceSHA256           string
	SummaryText            string
	Topics                 []string
	Events                 []event.Event
}

type summaryRecord struct {
	TenantID               string
	FilterKey              string
	CoveredFromSequence    int64
	CoveredThroughSequence int64
	LastEventID            string
	SessionVersion         int64
	SummaryVersion         int64
	BoundaryVersion        int
	GeneratorVersion       string
	ModelVersion           string
	PromptVersion          string
	SourceSHA256           string
	SummarySHA256          string
	SummaryText            string
	Topics                 []string
	CreatedAt              time.Time
}

type summaryJob struct {
	JobID                    string
	TenantID                 string
	Key                      session.Key
	FilterKey                string
	RequestedFromSequence    int64
	RequestedThroughSequence int64
	SessionVersion           int64
	BoundaryVersion          int
	LastEventID              string
	SourceSHA256             string
	GeneratorVersion         string
	PromptVersion            string
	Attempts                 int
}

func (p *Postgres) PutSummary(ctx context.Context, req SummaryWrite) (err error) {
	ctx, finish := observability.StartStorage(ctx, "summary.write", "postgres", req.TenantID, req.GeneratorVersion)
	defer func() { finish(err) }()
	if err := validateSummaryWrite(req); err != nil {
		return err
	}
	ctx = normalizeContext(ctx)
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sessionturn: begin summary write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentSequence, currentVersion int64
	if err := tx.QueryRow(ctx, `
		SELECT last_event_sequence, version
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		FOR SHARE`, req.Key.AppName, req.Key.UserID, req.Key.SessionID).
		Scan(&currentSequence, &currentVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: session does not exist", ErrSummaryBoundary)
		}
		return fmt.Errorf("sessionturn: lock summary session: %w", err)
	}
	if req.CoveredThroughSequence > currentSequence || req.SessionVersion > currentVersion {
		return fmt.Errorf("%w: coverage exceeds current session version", ErrSummaryBoundary)
	}
	rows, err := tx.Query(ctx, `
		SELECT sequence, event_id, event_data
		FROM session_turn_events
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		  AND sequence BETWEEN $4 AND $5
		ORDER BY sequence`, req.Key.AppName, req.Key.UserID, req.Key.SessionID,
		req.CoveredFromSequence, req.CoveredThroughSequence)
	if err != nil {
		return fmt.Errorf("sessionturn: query summary source events: %w", err)
	}
	decoded, err := scanSummaryEvents(rows)
	if err != nil {
		return err
	}
	if len(decoded) != int(req.CoveredThroughSequence-req.CoveredFromSequence+1) {
		return fmt.Errorf("%w: event range is incomplete", ErrSummaryBoundary)
	}
	if decoded[len(decoded)-1].id != req.LastEventID {
		return fmt.Errorf("%w: last event id does not match coverage", ErrSummaryBoundary)
	}
	if got := summarySourceHash(decoded, req.FilterKey); got != req.SourceSHA256 {
		return fmt.Errorf("%w: source hash mismatch", ErrSummaryBoundary)
	}
	summaryHash := summaryTextHash(req.SummaryText)
	topicValues := req.Topics
	if topicValues == nil {
		topicValues = []string{}
	}
	topics, err := json.Marshal(topicValues)
	if err != nil {
		return fmt.Errorf("sessionturn: encode summary topics: %w", err)
	}
	var existingSource, existingSummary, existingText string
	err = tx.QueryRow(ctx, `
		SELECT source_sha256, summary_sha256, summary_text
		FROM session_turn_summaries
		WHERE tenant_id = $1 AND app_name = $2 AND user_id = $3 AND session_id = $4
		  AND filter_key = $5 AND covered_from_sequence = $6
		  AND covered_through_sequence = $7 AND generator_version = $8
		  AND summary_version = $9`,
		req.TenantID, req.Key.AppName, req.Key.UserID, req.Key.SessionID, req.FilterKey,
		req.CoveredFromSequence, req.CoveredThroughSequence, req.GeneratorVersion,
		req.SummaryVersion).Scan(&existingSource, &existingSummary, &existingText)
	if err == nil {
		if existingSource != req.SourceSHA256 || existingSummary != summaryHash || existingText != req.SummaryText {
			return ErrSummaryConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("sessionturn: check summary idempotency: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO session_turn_summaries
			(tenant_id, app_name, user_id, session_id, filter_key,
			 covered_from_sequence, covered_through_sequence, last_event_id,
			 session_version, summary_version, boundary_version,
			 generator_version, model_version, prompt_version,
			 source_sha256, summary_sha256, summary_text, topics)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18::jsonb)`,
		req.TenantID, req.Key.AppName, req.Key.UserID, req.Key.SessionID, req.FilterKey,
		req.CoveredFromSequence, req.CoveredThroughSequence, req.LastEventID,
		req.SessionVersion, req.SummaryVersion, req.BoundaryVersion,
		req.GeneratorVersion, req.ModelVersion, req.PromptVersion,
		req.SourceSHA256, summaryHash, req.SummaryText, topics); err != nil {
		return fmt.Errorf("sessionturn: insert summary: %w", err)
	}
	return tx.Commit(ctx)
}

type summarySourceEvent struct {
	sequence int64
	id       string
	event    event.Event
}

func scanSummaryEvents(rows pgx.Rows) ([]summarySourceEvent, error) {
	defer rows.Close()
	result := make([]summarySourceEvent, 0)
	for rows.Next() {
		var item summarySourceEvent
		var raw []byte
		if err := rows.Scan(&item.sequence, &item.id, &raw); err != nil {
			return nil, fmt.Errorf("sessionturn: scan summary source event: %w", err)
		}
		if err := json.Unmarshal(raw, &item.event); err != nil {
			return nil, fmt.Errorf("%w: summary source event: %v", ErrCorruptData, err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionturn: iterate summary source events: %w", err)
	}
	return result, nil
}

func summarySourceHash(events []summarySourceEvent, filterKey string) string {
	hash := sha256.New()
	for _, item := range events {
		if !item.event.Filter(filterKey) {
			continue
		}
		encoded, _ := json.Marshal(item.event)
		_, _ = hash.Write([]byte(strconv.FormatInt(item.sequence, 10)))
		_, _ = hash.Write([]byte("\x1f"))
		_, _ = hash.Write([]byte(item.id))
		_, _ = hash.Write([]byte("\x1f"))
		_, _ = hash.Write(encoded)
		_, _ = hash.Write([]byte("\x1e"))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func summarySourceHashSnapshot(events []event.Event, from, through int64, filterKey string) string {
	items := make([]summarySourceEvent, 0, len(events))
	for index, item := range events {
		sequence := int64(index + 1)
		if sequence < from || sequence > through {
			continue
		}
		items = append(items, summarySourceEvent{sequence: sequence, id: item.ID, event: item})
	}
	return summarySourceHash(items, filterKey)
}

func validateSummaryWrite(req SummaryWrite) error {
	if err := validateKey(req.Key); err != nil {
		return err
	}
	if req.CoveredFromSequence <= 0 || req.CoveredThroughSequence < req.CoveredFromSequence || req.LastEventID == "" {
		return fmt.Errorf("%w: event coverage is invalid", ErrSummaryBoundary)
	}
	if req.SessionVersion < 0 || req.SummaryVersion <= 0 || req.BoundaryVersion != session.SummaryBoundaryVersion {
		return fmt.Errorf("%w: unsupported summary version", ErrSummaryBoundary)
	}
	if req.GeneratorVersion == "" || req.SourceSHA256 == "" || req.SummaryText == "" {
		return fmt.Errorf("%w: summary metadata is incomplete", ErrSummaryBoundary)
	}
	if req.SourceSHA256 != summarySourceHashSnapshot(req.Events, req.CoveredFromSequence, req.CoveredThroughSequence, req.FilterKey) {
		return fmt.Errorf("%w: caller source hash mismatch", ErrSummaryBoundary)
	}
	return nil
}

func (p *Postgres) loadSummaryRecords(ctx context.Context, tenantID string, key session.Key, filterKey string) (result []summaryRecord, err error) {
	ctx, finish := observability.StartStorage(ctx, "summary.read", "postgres", tenantID, "")
	defer func() { finish(err) }()
	rows, err := p.pool.Query(normalizeContext(ctx), `
		SELECT tenant_id, filter_key, covered_from_sequence, covered_through_sequence, last_event_id,
		       session_version, summary_version, boundary_version, generator_version,
		       model_version, prompt_version, source_sha256, summary_sha256,
		       summary_text, topics, created_at
		FROM session_turn_summaries
		WHERE tenant_id = $1 AND app_name = $2 AND user_id = $3 AND session_id = $4 AND filter_key = $5
		ORDER BY covered_through_sequence DESC, summary_version DESC, created_at DESC`,
		tenantID, key.AppName, key.UserID, key.SessionID, filterKey)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: query summaries: %w", err)
	}
	defer rows.Close()
	result = make([]summaryRecord, 0)
	for rows.Next() {
		var item summaryRecord
		var topics []byte
		if err := rows.Scan(&item.TenantID, &item.FilterKey, &item.CoveredFromSequence, &item.CoveredThroughSequence,
			&item.LastEventID, &item.SessionVersion, &item.SummaryVersion, &item.BoundaryVersion,
			&item.GeneratorVersion, &item.ModelVersion, &item.PromptVersion, &item.SourceSHA256,
			&item.SummarySHA256, &item.SummaryText, &topics, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("sessionturn: scan summary: %w", err)
		}
		if err := json.Unmarshal(topics, &item.Topics); err != nil {
			return nil, fmt.Errorf("%w: summary topics: %v", ErrCorruptData, err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func compatibleSummary(record summaryRecord, snapshot *Snapshot) bool {
	if snapshot == nil || record.BoundaryVersion != session.SummaryBoundaryVersion ||
		record.SummaryVersion <= 0 || record.CoveredFromSequence <= 0 ||
		record.CoveredThroughSequence < record.CoveredFromSequence ||
		record.CoveredThroughSequence > snapshot.LastEventSequence ||
		record.SessionVersion > snapshot.Version ||
		record.CoveredThroughSequence > int64(len(snapshot.Events)) {
		return false
	}
	last := snapshot.Events[record.CoveredThroughSequence-1]
	if last.ID != record.LastEventID {
		return false
	}
	if summarySourceHashSnapshot(snapshot.Events, record.CoveredFromSequence, record.CoveredThroughSequence, record.FilterKey) != record.SourceSHA256 {
		return false
	}
	return summaryTextHash(record.SummaryText) == record.SummarySHA256
}

func summaryTextHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func (p *Postgres) Summary(ctx context.Context, key session.Key, snapshot *Snapshot, filterKey string) (*session.Summary, bool) {
	return p.summary(ctx, "", key, snapshot, filterKey)
}

func (p *Postgres) SummaryForTenant(ctx context.Context, tenantID string, key session.Key, snapshot *Snapshot, filterKey string) (*session.Summary, bool) {
	return p.summary(ctx, tenantID, key, snapshot, filterKey)
}

func (p *Postgres) summary(ctx context.Context, tenantID string, key session.Key, snapshot *Snapshot, filterKey string) (*session.Summary, bool) {
	if snapshot == nil {
		return nil, false
	}
	records, err := p.loadSummaryRecords(ctx, tenantID, key, filterKey)
	if err != nil {
		return nil, false
	}
	for _, record := range records {
		if !compatibleSummary(record, snapshot) {
			continue
		}
		cutoff := snapshot.Events[record.CoveredThroughSequence-1].Timestamp
		return &session.Summary{
			Summary: record.SummaryText, Topics: append([]string(nil), record.Topics...),
			UpdatedAt: record.CreatedAt,
			Boundary:  session.NewSummaryBoundaryWithEventID(record.FilterKey, cutoff, record.LastEventID),
		}, true
	}
	return nil, false
}

func (p *Postgres) AllSummaries(ctx context.Context, key session.Key, snapshot *Snapshot) (map[string]*session.Summary, error) {
	return p.allSummaries(ctx, "", key, snapshot)
}

func (p *Postgres) AllSummariesForTenant(ctx context.Context, tenantID string, key session.Key, snapshot *Snapshot) (map[string]*session.Summary, error) {
	return p.allSummaries(ctx, tenantID, key, snapshot)
}

func (p *Postgres) allSummaries(ctx context.Context, tenantID string, key session.Key, snapshot *Snapshot) (map[string]*session.Summary, error) {
	if snapshot == nil {
		return nil, nil
	}
	// The table is queried per filter because filter keys are user-defined and
	// the compatibility check must run before any summary is exposed.
	rows, err := p.pool.Query(normalizeContext(ctx), `
		SELECT DISTINCT filter_key FROM session_turn_summaries
		WHERE tenant_id = $1 AND app_name = $2 AND user_id = $3 AND session_id = $4`,
		tenantID, key.AppName, key.UserID, key.SessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	filters := make([]string, 0)
	for rows.Next() {
		var filter string
		if err := rows.Scan(&filter); err != nil {
			return nil, err
		}
		filters = append(filters, filter)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make(map[string]*session.Summary)
	for _, filter := range filters {
		if value, ok := p.summary(ctx, tenantID, key, snapshot, filter); ok {
			result[filter] = value
		}
	}
	return result, nil
}

func (p *Postgres) EnqueueSummaryJob(ctx context.Context, job summaryJob) (err error) {
	ctx, finish := observability.StartStorage(ctx, "summary.job.enqueue", "postgres", job.TenantID, job.GeneratorVersion)
	defer func() { finish(err) }()
	if job.JobID == "" {
		job.JobID = "summary-" + uuid.NewString()
	}
	_, err = p.pool.Exec(normalizeContext(ctx), `
		INSERT INTO session_summary_jobs
			(job_id, tenant_id, app_name, user_id, session_id, filter_key,
			 requested_from_sequence, requested_through_sequence, session_version,
			 boundary_version, last_event_id, source_sha256, generator_version,
			 prompt_version, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'pending')
		ON CONFLICT (app_name, user_id, session_id, filter_key,
		             tenant_id,
		             requested_from_sequence, requested_through_sequence,
		             generator_version, source_sha256) DO NOTHING`,
		job.JobID, job.TenantID, job.Key.AppName, job.Key.UserID, job.Key.SessionID, job.FilterKey,
		job.RequestedFromSequence, job.RequestedThroughSequence, job.SessionVersion,
		job.BoundaryVersion, job.LastEventID, job.SourceSHA256, job.GeneratorVersion,
		job.PromptVersion)
	return err
}

func (p *Postgres) claimSummaryJob(ctx context.Context, owner string, lease time.Duration) (*summaryJob, error) {
	if lease <= 0 {
		lease = defaultSummaryLease
	}
	var job summaryJob
	err := p.pool.QueryRow(normalizeContext(ctx), `
		WITH candidate AS (
			SELECT job_id FROM session_summary_jobs
			WHERE (status = 'pending' OR (status = 'running' AND lease_expires_at < clock_timestamp()))
			  AND next_attempt_at <= clock_timestamp()
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE session_summary_jobs j
		SET status = 'running', lease_owner = $1,
		    lease_expires_at = clock_timestamp() + $2::interval,
		    attempts = j.attempts + 1, updated_at = clock_timestamp()
		FROM candidate c
		WHERE j.job_id = c.job_id
			RETURNING j.job_id, j.tenant_id, j.app_name, j.user_id, j.session_id, j.filter_key,
		          j.requested_from_sequence, j.requested_through_sequence,
		          j.session_version, j.boundary_version, j.last_event_id,
		          j.source_sha256, j.generator_version, j.prompt_version, j.attempts`,
		owner, fmt.Sprintf("%f seconds", lease.Seconds())).Scan(
		&job.JobID, &job.TenantID, &job.Key.AppName, &job.Key.UserID, &job.Key.SessionID, &job.FilterKey,
		&job.RequestedFromSequence, &job.RequestedThroughSequence, &job.SessionVersion,
		&job.BoundaryVersion, &job.LastEventID, &job.SourceSHA256, &job.GeneratorVersion,
		&job.PromptVersion, &job.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (p *Postgres) completeSummaryJob(ctx context.Context, jobID, owner string) error {
	tag, err := p.pool.Exec(normalizeContext(ctx), `
		UPDATE session_summary_jobs
		SET status = 'succeeded', lease_owner = '', lease_expires_at = NULL,
		    updated_at = clock_timestamp()
		WHERE job_id = $1 AND status = 'running' AND lease_owner = $2`, jobID, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrSummaryJobLost
	}
	return nil
}

func (p *Postgres) failSummaryJob(ctx context.Context, jobID, owner string, cause error) error {
	_, err := p.pool.Exec(normalizeContext(ctx), `
		UPDATE session_summary_jobs
		SET status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'pending' END,
		    lease_owner = '', lease_expires_at = NULL,
		    next_attempt_at = CASE WHEN attempts >= $3 THEN clock_timestamp()
		                            ELSE clock_timestamp() + interval '1 second' END,
		    last_error = $4, updated_at = clock_timestamp()
		WHERE job_id = $1 AND status = 'running' AND lease_owner = $2`,
		jobID, owner, maxSummaryAttempts, safeSummaryError(cause))
	return err
}

func safeSummaryError(err error) string {
	if err == nil {
		return ""
	}
	return observability.ErrorCategory(err)
}
