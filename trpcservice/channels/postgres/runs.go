package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const runColumns = `tenant_id, run_id, request_id, inbox_id, channel, channel_binding_id, ` +
	`agent_app_id, principal_id, session_id, accept_sequence, status, attempt, max_attempts, ` +
	`max_run_duration_ms, recovery_grace_ms, next_attempt_at, last_dispatched_at, ` +
	`claim_token, claimed_by, claimed_at, execute_deadline_at, recover_after, ` +
	`execution_started_at, first_execution_started_at, revision_id, error_type, ` +
	`event_count, output_parts, ` +
	`execution_millis, finished_at, created_at, updated_at`

const (
	selectRunSQL = `SELECT ` + runColumns + ` FROM channel_agent_runs
		WHERE tenant_id = $1 AND run_id = $2`

	selectRunForUpdateSQL = selectRunSQL + ` FOR UPDATE`

	selectSessionRunsSQL = `SELECT ` + runColumns + ` FROM channel_agent_runs
		WHERE tenant_id = $1 AND agent_app_id = $2 AND principal_id = $3 AND session_id = $4
		ORDER BY accept_sequence
		LIMIT $5`

	// The Session's earliest unfinished Run, locked. Nothing else is a
	// candidate: strict per-Session ordering means a later message may not be
	// answered before this one, however long this one takes.
	selectEarliestPendingRunSQL = `SELECT ` + runColumns + ` FROM channel_agent_runs
		WHERE tenant_id = $1 AND agent_app_id = $2 AND principal_id = $3 AND session_id = $4
		  AND status IN ('accepted', 'running')
		ORDER BY accept_sequence
		LIMIT 1
		FOR UPDATE`

	// The claim restates every predicate the row was chosen on, including the
	// attempt it was read at. Under the FOR UPDATE above that is belt and
	// braces, but it is what makes the state check and the transition one
	// statement rather than two, so no other path can produce a claim that
	// skipped the check.
	claimRunSQL = `UPDATE channel_agent_runs
		SET status = 'running',
		    attempt = attempt + 1,
		    claim_token = $3,
		    claimed_by = $4,
		    claimed_at = $5,
		    execute_deadline_at = $6,
		    recover_after = $7,
		    execution_started_at = NULL,
		    updated_at = $5
		WHERE tenant_id = $1 AND run_id = $2
		  AND status = 'accepted' AND attempt = $8
		  AND attempt < max_attempts AND next_attempt_at <= $5
		RETURNING ` + runColumns

	// Terminating an accepted Run whose attempts are spent. This is the path
	// that stops an undeliverable message from blocking its Session forever.
	terminateAcceptedRunSQL = `UPDATE channel_agent_runs
		SET status = 'failed', error_type = $3, finished_at = $4, updated_at = $4
		WHERE tenant_id = $1 AND run_id = $2 AND status = 'accepted' AND attempt = $5`

	// Terminating a claimed Run. The claim token is part of the predicate, so a
	// Worker whose claim was already recovered cannot terminate the Run the new
	// owner is executing.
	terminateClaimedRunSQL = `UPDATE channel_agent_runs
		SET status = 'failed', error_type = $3, finished_at = $4, updated_at = $4,
		    claim_token = '', claimed_by = '', claimed_at = NULL,
		    execute_deadline_at = NULL, recover_after = NULL, execution_started_at = NULL
		WHERE tenant_id = $1 AND run_id = $2
		  AND status = 'running' AND claim_token = $5 AND attempt = $6`

	// last_dispatched_at is cleared because the row is going back into the queue
	// on a new generation; see the in-memory requeueRunLocked for why keeping it
	// would suppress the new attempt for the dispatch scan's staleness window
	// rather than for its backoff. first_execution_started_at is deliberately
	// absent: it outlives every claim.
	requeueClaimedRunSQL = `UPDATE channel_agent_runs
		SET status = 'accepted', error_type = $3, next_attempt_at = $4, updated_at = $4,
		    claim_token = '', claimed_by = '', claimed_at = NULL,
		    execute_deadline_at = NULL, recover_after = NULL, execution_started_at = NULL,
		    last_dispatched_at = NULL
		WHERE tenant_id = $1 AND run_id = $2
		  AND status = 'running' AND claim_token = $5 AND attempt = $6`

	// COALESCE makes this idempotent under one claim: a retried write finds the
	// start time already set and leaves it, rather than moving the instant the
	// execution deadline is judged from.
	//
	// Both marks are written here, under the same claim-token CAS, so a Run
	// cannot be executing with only one of them set. Every column reference on
	// the right reads the row as it was before this statement, which is what
	// makes the second COALESCE mean "the time this attempt started, or now if
	// this is the attempt starting" rather than collapsing into the first.
	markRunStartedSQL = `UPDATE channel_agent_runs
		SET execution_started_at = COALESCE(execution_started_at, $3::timestamptz),
		    first_execution_started_at = COALESCE(
		        first_execution_started_at, execution_started_at, $3::timestamptz),
		    updated_at = CASE WHEN execution_started_at IS NULL THEN $3::timestamptz
		                      ELSE updated_at END
		WHERE tenant_id = $1 AND run_id = $2 AND status = 'running' AND claim_token = $4
		RETURNING 1`

	recordRunRevisionSQL = `UPDATE channel_agent_runs
		SET revision_id = $3, updated_at = $4
		WHERE tenant_id = $1 AND run_id = $2 AND status = 'running' AND claim_token = $5
		RETURNING 1`

	finishRunSQL = `UPDATE channel_agent_runs
		SET status = $3,
		    error_type = $4,
		    revision_id = CASE WHEN $5::text = '' THEN revision_id ELSE $5::text END,
		    event_count = $6, output_parts = $7, execution_millis = $8,
		    finished_at = $9, updated_at = $9,
		    claim_token = '', claimed_by = '', claimed_at = NULL,
		    execute_deadline_at = NULL, recover_after = NULL, execution_started_at = NULL
		WHERE tenant_id = $1 AND run_id = $2 AND status = 'running' AND claim_token = $10
		RETURNING ` + runColumns

	// SKIP LOCKED so two recovery scanners divide the work instead of queueing
	// behind each other. The C collation makes the order a plain byte
	// comparison, which is what the in-memory Store sorts by; a database whose
	// default collation ordered text differently would otherwise return a
	// different page.
	//
	// scopeFilterSQL binds $1 and $2; see scope.go. It sits before LIMIT and
	// before FOR UPDATE, so a scoped scanner neither reads nor locks a row
	// outside its binding, and rows it may not touch cannot crowd its own work
	// out of the page.
	selectExpiredRunsSQL = `SELECT ` + runColumns + ` FROM channel_agent_runs
		WHERE status = 'running' AND recover_after <= $3` + scopeFilterSQL + `
		ORDER BY tenant_id COLLATE "C", run_id COLLATE "C"
		LIMIT $4
		FOR UPDATE SKIP LOCKED`

	// The attempt comes back with the reference and is what the mark below is
	// fenced on; see channels.RunDispatch.
	selectDispatchableRunsSQL = `SELECT tenant_id, run_id, attempt FROM channel_agent_runs
		WHERE status = 'accepted' AND next_attempt_at <= $3
		  AND (last_dispatched_at IS NULL OR last_dispatched_at <= $4)` + scopeFilterSQL + `
		ORDER BY tenant_id COLLATE "C", run_id COLLATE "C"
		LIMIT $5`

	// Three predicates beyond the reference, each covering a way the row can
	// stop being the one that was announced:
	//
	// status = 'accepted' — it has been claimed or finished since the scan.
	// attempt = d.attempt — it was claimed and requeued, so this stamp belongs
	// to a generation that is over.
	// next_attempt_at <= $1 — it is not due yet, which happens when a caller
	// publishes for a row mid-backoff; stamping it would hide it from the scan
	// until the staleness window expired instead of until it came due.
	//
	// GREATEST keeps the stamp monotonic, so an older mark still in flight
	// cannot undo a newer one. It ignores NULLs, which is what makes it also
	// the right expression for a row that has never been dispatched. The row
	// still counts as affected when GREATEST changes nothing, which is
	// deliberate: the caller asked whether the CAS found its row.
	markRunsDispatchedSQL = `UPDATE channel_agent_runs AS r
		SET last_dispatched_at = GREATEST(r.last_dispatched_at, $1::timestamptz)
		FROM unnest($2::text[], $3::text[], $4::integer[]) AS d(tenant_id, run_id, attempt)
		WHERE r.tenant_id = d.tenant_id AND r.run_id = d.run_id
		  AND r.status = 'accepted' AND r.attempt = d.attempt
		  AND r.next_attempt_at <= $1::timestamptz`
)

// GetRun returns one Run.
func (s *Store) GetRun(
	ctx context.Context,
	scope tenant.TenantContext,
	runID string,
) (channels.Run, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.Run{}, err
	}
	if err := scope.Validate(); err != nil {
		return channels.Run{}, err
	}
	if err := tenant.ValidateResourceID("run id", runID); err != nil {
		return channels.Run{}, err
	}
	run, err := scanRun("read run", s.pool.QueryRow(ctx, selectRunSQL, scope.TenantID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.Run{}, notFound("run", runID)
	}
	if err != nil {
		return channels.Run{}, storageError(ctx, "read run", err)
	}
	return run, nil
}

// ListSessionRuns returns a Session's Runs in accept order.
func (s *Store) ListSessionRuns(
	ctx context.Context,
	scope tenant.TenantContext,
	key sessiondir.Key,
	request channels.ListRequest,
) ([]channels.Run, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := validateSessionScope(scope, key); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(
		ctx, selectSessionRunsSQL,
		key.TenantID, key.AppID, key.PrincipalID, key.SessionID, int32(request.Limit))
	if err != nil {
		return nil, storageError(ctx, "list session runs", err)
	}
	defer rows.Close()

	runs := make([]channels.Run, 0)
	for rows.Next() {
		run, err := scanRun("list session runs", rows)
		if err != nil {
			return nil, storageError(ctx, "list session runs", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(ctx, "list session runs", err)
	}
	return runs, nil
}

// ClaimNextRun claims the earliest executable Run of one Session.
func (s *Store) ClaimNextRun(
	ctx context.Context,
	scope tenant.TenantContext,
	key sessiondir.Key,
	request channels.ClaimRunRequest,
) (channels.RunClaim, bool, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.RunClaim{}, false, err
	}
	if err := validateSessionScope(scope, key); err != nil {
		return channels.RunClaim{}, false, err
	}
	if err := request.Validate(); err != nil {
		return channels.RunClaim{}, false, err
	}
	now := channels.NormalizeTime(request.Now)

	var (
		claim   channels.RunClaim
		claimed bool
	)
	err := s.withTx(ctx, "claim run", func(tx pgx.Tx) error {
		for sweeps := 0; ; {
			run, err := scanRun("claim run", tx.QueryRow(
				ctx, selectEarliestPendingRunSQL,
				key.TenantID, key.AppID, key.PrincipalID, key.SessionID))
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return storageError(ctx, "claim run", err)
			}
			// This is the Session's earliest unfinished Run. Either it is
			// claimed, or it is terminated because it has no attempts left, or
			// nothing is claimable — never "try the next one".
			if run.Status == channels.RunRunning || run.NextAttemptAt.After(now) {
				return nil
			}
			if run.AttemptsExhausted() {
				if sweeps == channels.MaxExhaustSweep {
					return nil
				}
				sweeps++
				if _, err := tx.Exec(
					ctx, terminateAcceptedRunSQL,
					run.TenantID, run.RunID,
					string(channels.ErrorAttemptsExhausted), now, run.Attempt,
				); err != nil {
					return storageError(ctx, "terminate exhausted run", err)
				}
				continue
			}

			deadline := now.Add(run.Policy.MaxRunDuration)
			recoverAfter := deadline.Add(run.Policy.RecoveryGrace)
			updated, err := scanRun("claim run", tx.QueryRow(
				ctx, claimRunSQL,
				run.TenantID, run.RunID,
				request.ClaimToken, request.ClaimedBy, now,
				deadline, recoverAfter, run.Attempt))
			if errors.Is(err, pgx.ErrNoRows) {
				// Unreachable while the row is locked, but a claim that cannot
				// prove it won reports no claim rather than inventing one.
				return nil
			}
			if err != nil {
				return storageError(ctx, "claim run", err)
			}

			message, err := scanInbox("claim run", tx.QueryRow(
				ctx, selectInboxSQL, updated.TenantID, updated.InboxID))
			if errors.Is(err, pgx.ErrNoRows) {
				return storageFailure("claim run", errors.New("run has no inbox message"))
			}
			if err != nil {
				return storageError(ctx, "claim run", err)
			}

			claim = channels.RunClaim{
				Run:                      updated,
				Message:                  message.Message,
				DeliveryTarget:           message.DeliveryTarget,
				RemainingExecutionBudget: deadline.Sub(now),
			}
			claimed = true
			return nil
		}
	})
	if err != nil {
		return channels.RunClaim{}, false, err
	}
	return claim, claimed, nil
}

// MarkRunStarted records that the Runner is about to be called.
func (s *Store) MarkRunStarted(
	ctx context.Context,
	scope tenant.TenantContext,
	token channels.RunToken,
	now time.Time,
) error {
	if err := s.validateCall(ctx); err != nil {
		return err
	}
	if err := validateTokenCall(scope, token); err != nil {
		return err
	}
	if now.IsZero() {
		return requiredTimeError("now")
	}
	return s.casRun(ctx, "mark run started", markRunStartedSQL,
		scope.TenantID, token.RunID, channels.NormalizeTime(now), token.ClaimToken)
}

// RecordRunRevision stores the revision that is answering this Run.
func (s *Store) RecordRunRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	token channels.RunToken,
	revisionID string,
	now time.Time,
) error {
	if err := s.validateCall(ctx); err != nil {
		return err
	}
	if err := validateTokenCall(scope, token); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("revision id", revisionID); err != nil {
		return err
	}
	if now.IsZero() {
		return requiredTimeError("now")
	}
	return s.casRun(ctx, "record run revision", recordRunRevisionSQL,
		scope.TenantID, token.RunID, revisionID,
		channels.NormalizeTime(now), token.ClaimToken)
}

// casRun runs a single-statement compare-and-set that returns one row on a hit
// and none on a miss.
func (s *Store) casRun(ctx context.Context, operation, statement string, args ...any) error {
	var hit int
	err := s.pool.QueryRow(ctx, statement, args...).Scan(&hit)
	if errors.Is(err, pgx.ErrNoRows) {
		// The Run is not running under this token any more. Whether it was
		// recovered, finished by someone else, or never existed is deliberately
		// not distinguished: in every case this caller must write nothing.
		return channels.ErrStaleClaim
	}
	if err != nil {
		return storageError(ctx, operation, err)
	}
	return nil
}

// YieldRun returns a claimed Run to the queue before execution began.
func (s *Store) YieldRun(
	ctx context.Context,
	scope tenant.TenantContext,
	token channels.RunToken,
	request channels.YieldRunRequest,
) (channels.RequeueOutcome, error) {
	if err := s.validateCall(ctx); err != nil {
		return "", err
	}
	if err := validateTokenCall(scope, token); err != nil {
		return "", err
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	now := channels.NormalizeTime(request.Now)

	var outcome channels.RequeueOutcome
	err := s.withTx(ctx, "yield run", func(tx pgx.Tx) error {
		run, err := scanRun("yield run", tx.QueryRow(
			ctx, selectRunForUpdateSQL, scope.TenantID, token.RunID))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("run", token.RunID)
		}
		if err != nil {
			return storageError(ctx, "yield run", err)
		}
		if run.Status != channels.RunRunning || run.ClaimToken != token.ClaimToken {
			return channels.ErrStaleClaim
		}
		if run.ExecutionStartedAt != nil {
			return channels.ErrExecutionStarted
		}
		outcome, err = requeueRun(ctx, tx, run, request.ErrorType, now)
		return err
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// requeueRun returns a claimed Run to the queue, or terminates it when there is
// nothing left to retry with. It is shared by YieldRun and the recovery scanner
// so that both make the same decision about a spent attempt budget.
func requeueRun(
	ctx context.Context,
	tx pgx.Tx,
	run channels.Run,
	errorType channels.ErrorType,
	now time.Time,
) (channels.RequeueOutcome, error) {
	if run.AttemptsExhausted() {
		if _, err := tx.Exec(
			ctx, terminateClaimedRunSQL,
			run.TenantID, run.RunID, string(channels.ErrorAttemptsExhausted), now,
			run.ClaimToken, run.Attempt,
		); err != nil {
			return "", storageError(ctx, "terminate exhausted run", err)
		}
		return channels.RequeueExhausted, nil
	}
	nextAttemptAt := channels.NormalizeTime(
		now.Add(channels.Backoff(run.Attempt, run.RunID)))
	if _, err := tx.Exec(
		ctx, requeueClaimedRunSQL,
		run.TenantID, run.RunID, string(errorType), nextAttemptAt,
		run.ClaimToken, run.Attempt,
	); err != nil {
		return "", storageError(ctx, "requeue run", err)
	}
	return channels.RequeueRetried, nil
}

// FinishRun terminates a Run and writes its answer in one transaction.
func (s *Store) FinishRun(
	ctx context.Context,
	scope tenant.TenantContext,
	request channels.FinishRunRequest,
) (channels.FinishRunResult, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.FinishRunResult{}, err
	}
	if err := scope.Validate(); err != nil {
		return channels.FinishRunResult{}, err
	}
	if err := request.Validate(); err != nil {
		return channels.FinishRunResult{}, err
	}
	now := channels.NormalizeTime(request.Now)

	var result channels.FinishRunResult
	err := s.withTx(ctx, "finish run", func(tx pgx.Tx) error {
		run, err := scanRun("finish run", tx.QueryRow(
			ctx, finishRunSQL,
			scope.TenantID, request.Token.RunID,
			string(request.Status), string(request.ErrorType), request.RevisionID,
			request.Stats.EventCount, request.Stats.OutputParts, request.Stats.ExecutionMillis,
			now, request.Token.ClaimToken))
		if errors.Is(err, pgx.ErrNoRows) {
			// The claim is gone, so this execution is not the one that owns the
			// Run any more. Rolling back here is what keeps its answer out of
			// the Outbox: the new owner will produce its own.
			return channels.ErrStaleClaim
		}
		if err != nil {
			return storageError(ctx, "finish run", err)
		}

		parts := make([]channels.OutboxPart, 0, len(request.Outbox))
		for _, draft := range request.Outbox {
			part, err := insertOutboxPart(ctx, tx, run, draft, now)
			if err != nil {
				return err
			}
			parts = append(parts, part)
		}
		sortOutboxParts(parts)
		result = channels.FinishRunResult{Run: run, Outbox: parts}
		return nil
	})
	if err != nil {
		return channels.FinishRunResult{}, err
	}
	return result, nil
}

// RecoverRuns requeues or terminates Runs whose recovery deadline passed.
func (s *Store) RecoverRuns(
	ctx context.Context,
	request channels.RecoverRequest,
) ([]channels.RunRecovery, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	now := channels.NormalizeTime(request.Now)

	var recovered []channels.RunRecovery
	err := s.withTx(ctx, "recover runs", func(tx pgx.Tx) error {
		tenantArg, bindingArg := scopeArgs(request.Scope)
		rows, err := tx.Query(
			ctx, selectExpiredRunsSQL,
			tenantArg, bindingArg, now, int32(request.Limit))
		if err != nil {
			return storageError(ctx, "recover runs", err)
		}
		expired := make([]channels.Run, 0)
		for rows.Next() {
			run, err := scanRun("recover runs", rows)
			if err != nil {
				rows.Close()
				return storageError(ctx, "recover runs", err)
			}
			expired = append(expired, run)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return storageError(ctx, "recover runs", err)
		}

		recovered = make([]channels.RunRecovery, 0, len(expired))
		for _, run := range expired {
			// The Worker that held this claim is unreachable or dead. A
			// timeout, not a failure: nothing here knows whether it produced
			// anything, which is why the next attempt has to reconcile.
			outcome, err := requeueRun(ctx, tx, run, channels.ErrorRunTimeout, now)
			if err != nil {
				return err
			}
			recovered = append(recovered, channels.RunRecovery{
				RunRef:  channels.RunRef{TenantID: run.TenantID, RunID: run.RunID},
				Outcome: outcome,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recovered, nil
}

// ListDispatchableRuns returns due Runs whose wakeup looks lost.
func (s *Store) ListDispatchableRuns(
	ctx context.Context,
	request channels.DispatchScanRequest,
) ([]channels.RunDispatch, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	now := channels.NormalizeTime(request.Now)
	tenantArg, bindingArg := scopeArgs(request.Scope)
	rows, err := s.pool.Query(
		ctx, selectDispatchableRunsSQL,
		tenantArg, bindingArg, now, now.Add(-request.StaleAfter), int32(request.Limit))
	if err != nil {
		return nil, storageError(ctx, "list dispatchable runs", err)
	}
	defer rows.Close()

	candidates := make([]channels.RunDispatch, 0)
	for rows.Next() {
		var candidate channels.RunDispatch
		if err := rows.Scan(
			&candidate.TenantID, &candidate.RunID, &candidate.Attempt,
		); err != nil {
			return nil, storageError(ctx, "list dispatchable runs", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(ctx, "list dispatchable runs", err)
	}
	return candidates, nil
}

// MarkRunsDispatched records that a wakeup was published, for the rows still on
// the generation it was published for.
func (s *Store) MarkRunsDispatched(
	ctx context.Context,
	dispatches []channels.RunDispatch,
	at time.Time,
) (int, error) {
	if err := s.validateCall(ctx); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, requiredTimeError("dispatched at")
	}
	if len(dispatches) > channels.MaxListLimit {
		return 0, tooManyRefsError("runs")
	}
	tenantIDs := make([]string, 0, len(dispatches))
	runIDs := make([]string, 0, len(dispatches))
	attempts := make([]int32, 0, len(dispatches))
	for _, dispatch := range dispatches {
		if err := dispatch.Validate(); err != nil {
			return 0, err
		}
		tenantIDs = append(tenantIDs, dispatch.TenantID)
		runIDs = append(runIDs, dispatch.RunID)
		attempts = append(attempts, dispatch.Attempt)
	}
	if len(dispatches) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(
		ctx, markRunsDispatchedSQL,
		channels.NormalizeTime(at), tenantIDs, runIDs, attempts,
	)
	if err != nil {
		return 0, storageError(ctx, "mark runs dispatched", err)
	}
	return int(tag.RowsAffected()), nil
}

// scanRun reads one Run row.
//
// It returns pgx.ErrNoRows unwrapped so the caller can turn it into the right
// domain error — which for a claim is "nothing to do" and for a CAS is a stale
// claim, two very different answers that only the caller can choose between.
func scanRun(operation string, row pgx.Row) (channels.Run, error) {
	var (
		item           channels.Run
		channel        string
		status         string
		errorType      string
		maxDurationMS  int64
		recoveryMS     int64
		executionMS    int64
		attempt        int32
		maxAttempts    int32
		eventCount     int32
		outputParts    int32
		lastDispatched *time.Time
		claimedAt      *time.Time
		executeBy      *time.Time
		recoverAfter   *time.Time
		startedAt      *time.Time
		firstStartedAt *time.Time
		finishedAt     *time.Time
	)
	if err := row.Scan(
		&item.TenantID, &item.RunID, &item.RequestID, &item.InboxID, &channel,
		&item.ChannelBindingID, &item.AgentAppID, &item.PrincipalID, &item.SessionID,
		&item.AcceptSequence, &status, &attempt, &maxAttempts,
		&maxDurationMS, &recoveryMS, &item.NextAttemptAt, &lastDispatched,
		&item.ClaimToken, &item.ClaimedBy, &claimedAt, &executeBy, &recoverAfter,
		&startedAt, &firstStartedAt, &item.RevisionID, &errorType, &eventCount,
		&outputParts, &executionMS, &finishedAt, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return channels.Run{}, err
	}
	item.Channel = channels.ChannelType(channel)
	item.Status = channels.RunStatus(status)
	item.ErrorType = channels.ErrorType(errorType)
	item.Attempt = attempt
	item.Policy = channels.RunPolicy{
		MaxAttempts:    maxAttempts,
		MaxRunDuration: durationFromMillis(maxDurationMS),
		RecoveryGrace:  durationFromMillis(recoveryMS),
	}
	item.Stats = channels.RunStats{
		EventCount:      eventCount,
		OutputParts:     outputParts,
		ExecutionMillis: executionMS,
	}
	item.NextAttemptAt = item.NextAttemptAt.UTC()
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	item.LastDispatchedAt = nullableTime(lastDispatched)
	item.ClaimedAt = nullableTime(claimedAt)
	item.ExecuteDeadlineAt = nullableTime(executeBy)
	item.RecoverAfter = nullableTime(recoverAfter)
	item.ExecutionStartedAt = nullableTime(startedAt)
	item.FirstExecutionStartedAt = nullableTime(firstStartedAt)
	item.FinishedAt = nullableTime(finishedAt)

	// A status or error class outside the vocabulary means somebody wrote this
	// row without going through this package. Reporting it as a storage fault
	// rather than handing it back is the same call checkStoredPin makes in the
	// session directory: a value that fails the writer's own validation did not
	// come from the writer.
	if err := item.Status.Validate(); err != nil {
		return channels.Run{}, integrityError(operation, "status")
	}
	if err := item.ErrorType.Validate(); err != nil {
		return channels.Run{}, integrityError(operation, "error_type")
	}
	return item, nil
}

// validateSessionScope checks a tenant scope and a Session key together.
func validateSessionScope(scope tenant.TenantContext, key sessiondir.Key) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if key.TenantID != scope.TenantID {
		return tenantMismatchError("session key", key.TenantID, scope.TenantID)
	}
	return nil
}

// validateTokenCall checks a tenant scope and an attempt token together.
func validateTokenCall(scope tenant.TenantContext, token channels.RunToken) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return token.Validate()
}
