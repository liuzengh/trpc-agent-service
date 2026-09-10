package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	platformadmin "github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

const workerHeartbeatTTL = 15 * time.Second

// OperationsSummary returns durable control-plane counters. Redis liveness is
// added by the gateway adapter because this Store intentionally owns only
// PostgreSQL resources.
func (s *Store) OperationsSummary(ctx context.Context) (platformadmin.OperationsSummary, error) {
	return s.operationsSummary(ctx, nil)
}

// OperationsSummaryForTenants returns the same operational counters limited to
// an authenticated tenant allowlist. Infrastructure readiness remains global;
// workload counters are tenant-scoped so operator and auditor views cannot
// infer another tenant's traffic or backlog.
func (s *Store) OperationsSummaryForTenants(ctx context.Context, tenantIDs []string) (platformadmin.OperationsSummary, error) {
	return s.operationsSummary(ctx, tenantIDs)
}

func (s *Store) operationsSummary(ctx context.Context, tenantIDs []string) (platformadmin.OperationsSummary, error) {
	if err := s.validate(); err != nil {
		return platformadmin.OperationsSummary{}, err
	}
	args := []any{intervalLiteral(workerHeartbeatTTL)}
	workloadScope := ""
	if tenantIDs != nil {
		args = append(args, tenantIDs)
		workloadScope = " AND tenant_id = ANY($2)"
	}
	var activeExecutions, queueBacklog, retryBacklog, replyBacklog int64
	var pendingApprovals, activeMigrations, stuckMigrations, workerCount, workerCapacity int64
	var migrationProgress float64
	var channelReadiness string
	query := fmt.Sprintf(`
SELECT
    (SELECT count(*) FROM platform.execution WHERE status = 'RUNNING'%[1]s),
    (SELECT count(*) FROM platform.dispatch_outbox WHERE status IN ('PENDING', 'PUBLISHING')%[1]s),
    (SELECT count(*) FROM platform.execution WHERE status = 'PENDING' AND attempt > 0%[1]s),
    (SELECT count(*) FROM platform.reply_outbox WHERE status IN ('PENDING', 'SENDING')%[1]s),
    (SELECT count(*) FROM platform.tool_approval WHERE status = 'PENDING'%[1]s),
    (SELECT count(*) FROM platform.data_migration WHERE status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING')%[1]s),
    (SELECT count(*) FROM platform.data_migration
       WHERE status IN ('DRAINING', 'COPYING', 'VERIFYING')
         AND updated_at < clock_timestamp() - interval '5 minutes'%[1]s),
    (SELECT COALESCE(avg(
        CASE
            WHEN total_sessions <= 0 THEN 0
            WHEN status = 'VERIFYING' THEN verify_progress::double precision / total_sessions
            ELSE copy_progress::double precision / total_sessions
     END
    ) FILTER (WHERE status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING')), 0)
     FROM platform.data_migration WHERE TRUE%[1]s),
    (SELECT count(*) FROM platform.worker_heartbeat
	   WHERE status = 'READY'
	   AND last_seen_at >= clock_timestamp() - $1::interval),
    (SELECT COALESCE(sum(concurrency), 0) FROM platform.worker_heartbeat
       WHERE status = 'READY'
         AND last_seen_at >= clock_timestamp() - $1::interval),
    (SELECT CASE
        WHEN count(*) FILTER (WHERE status = 'ACTIVE') = 0 THEN 'NOT_READY'
        WHEN count(*) FILTER (WHERE status = 'ACTIVE' AND connection_status <> 'READY') = 0 THEN 'READY'
        ELSE 'DEGRADED'
     END
     FROM platform.channel_binding
     WHERE TRUE%[1]s)`, workloadScope)
	if err := s.pool.QueryRow(ctx, query, args...).Scan(
		&activeExecutions, &queueBacklog, &retryBacklog, &replyBacklog,
		&pendingApprovals, &activeMigrations, &stuckMigrations, &migrationProgress,
		&workerCount, &workerCapacity,
		&channelReadiness,
	); err != nil {
		if tenantIDs == nil && s.metrics != nil {
			s.metrics.SetBackendReadiness("postgres", false)
		}
		return platformadmin.OperationsSummary{}, fmt.Errorf("read operations counters: %w", err)
	}
	recentErrors, err := s.recentOperationalErrors(ctx, tenantIDs)
	if err != nil {
		if tenantIDs == nil && s.metrics != nil {
			s.metrics.SetBackendReadiness("postgres", false)
		}
		return platformadmin.OperationsSummary{}, err
	}
	workerReadiness := "NOT_READY"
	if workerCount > 0 {
		workerReadiness = "READY"
	}
	workerUtilization := 0.0
	if workerCapacity > 0 {
		workerUtilization = float64(activeExecutions) / float64(workerCapacity)
	}
	result := platformadmin.OperationsSummary{
		GatewayReadiness:  "READY",
		WorkerReadiness:   workerReadiness,
		WorkerCount:       int(workerCount),
		WorkerCapacity:    int(workerCapacity),
		WorkerUtilization: workerUtilization,
		ActiveExecutions:  int(activeExecutions),
		QueueBacklog:      int(queueBacklog),
		RetryBacklog:      int(retryBacklog),
		ReplyBacklog:      int(replyBacklog),
		PendingApprovals:  int(pendingApprovals),
		ActiveMigrations:  int(activeMigrations),
		StuckMigrations:   int(stuckMigrations),
		MigrationProgress: migrationProgress,
		// Audit writes are synchronous with their owning transaction; there is
		// no separate audit delivery queue in this service.
		AuditBacklog:     0,
		ChannelReadiness: channelReadiness,
		RecentErrors:     recentErrors,
		Backends: []platformadmin.BackendReadiness{{
			Name: "postgres", Provider: "postgres", Status: "READY",
		}},
		GeneratedAt: time.Now().UTC(),
	}
	if tenantIDs == nil && s.metrics != nil {
		s.metrics.SetBackendReadiness("postgres", true)
		s.metrics.SetOperationsSnapshot(platformmetrics.OperationsSnapshot{
			ActiveExecutions:  activeExecutions,
			WorkerCount:       workerCount,
			WorkerCapacity:    workerCapacity,
			QueueBacklog:      queueBacklog,
			RetryBacklog:      retryBacklog,
			ReplyBacklog:      replyBacklog,
			PendingApprovals:  pendingApprovals,
			ActiveMigrations:  activeMigrations,
			StuckMigrations:   stuckMigrations,
			MigrationProgress: migrationProgress,
			AuditBacklog:      0,
		})
	}
	return result, nil
}

func (s *Store) recentOperationalErrors(ctx context.Context, tenantIDs []string) ([]platformadmin.RecentError, error) {
	where := "error_type <> ''"
	args := make([]any, 0, 1)
	if tenantIDs != nil {
		args = append(args, tenantIDs)
		where += " AND tenant_id = ANY($1)"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT tenant_id, app_id, event_type, error_type, trace_id, created_at
FROM platform.audit_event
WHERE %s
ORDER BY created_at DESC, audit_event_id DESC
LIMIT 5`, where), args...)
	if err != nil {
		return nil, fmt.Errorf("read recent operational errors: %w", err)
	}
	defer rows.Close()
	values := make([]platformadmin.RecentError, 0, 5)
	for rows.Next() {
		var value platformadmin.RecentError
		if err := rows.Scan(
			&value.TenantID, &value.AppID, &value.EventType,
			&value.ErrorType, &value.TraceID, &value.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan recent operational error: %w", err)
		}
		value.ErrorType = platformlog.SafeError(errors.New(value.ErrorType))
		value.OccurredAt = value.OccurredAt.UTC()
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent operational errors: %w", err)
	}
	return values, nil
}

// UpsertWorkerHeartbeat records liveness without storing execution payloads,
// leases, or credentials.
func (s *Store) UpsertWorkerHeartbeat(
	ctx context.Context,
	workerID, status string,
	concurrency int,
	startedAt time.Time,
	lastError string,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if workerID == "" || concurrency <= 0 || startedAt.IsZero() {
		return errors.New("worker heartbeat identity, concurrency, and start time are required")
	}
	if status != "READY" && status != "NOT_READY" {
		return errors.New("worker heartbeat status is invalid")
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO platform.worker_heartbeat (worker_id, status, concurrency, started_at, last_seen_at, last_error)
VALUES ($1, $2, $3, $4, clock_timestamp(), $5)
ON CONFLICT (worker_id) DO UPDATE SET
    status = EXCLUDED.status,
    concurrency = EXCLUDED.concurrency,
    started_at = EXCLUDED.started_at,
    last_seen_at = clock_timestamp(),
    last_error = EXCLUDED.last_error`, workerID, status, concurrency, startedAt.UTC(), lastError)
	if err != nil {
		return fmt.Errorf("upsert worker heartbeat: %w", err)
	}
	return nil
}

func (s *Store) DeleteWorkerHeartbeat(ctx context.Context, workerID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if workerID == "" {
		return errors.New("worker_id is required")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM platform.worker_heartbeat WHERE worker_id = $1`, workerID); err != nil {
		return fmt.Errorf("delete worker heartbeat: %w", err)
	}
	return nil
}
