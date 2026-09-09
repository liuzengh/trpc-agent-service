package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const outboxColumns = `tenant_id, outbox_id, run_id, request_id, channel, ` +
	`channel_binding_id, session_id, part_no, idempotency_key, client_message_id, status, ` +
	`attempt, max_attempts, next_attempt_at, last_dispatched_at, send_token, sent_by, ` +
	`send_deadline_at, duplicate_risk, error_type, external_message_id, sent_at, body, ` +
	`delivery_channel, delivery_version, delivery_payload, created_at, updated_at`

const (
	selectOutboxSQL = `SELECT ` + outboxColumns + ` FROM channel_outbox_messages
		WHERE tenant_id = $1 AND outbox_id = $2`

	selectOutboxByIdempotencySQL = `SELECT ` + outboxColumns + ` FROM channel_outbox_messages
		WHERE tenant_id = $1 AND channel_binding_id = $2 AND idempotency_key = $3`

	selectRunOutboxSQL = `SELECT ` + outboxColumns + ` FROM channel_outbox_messages
		WHERE tenant_id = $1 AND run_id = $2
		ORDER BY part_no, outbox_id COLLATE "C"
		LIMIT $3`

	// DO NOTHING rather than DO UPDATE: when a re-executed Run produces the
	// same part again, the row that already exists is the one that counts. It
	// may already have been claimed, sent, or marked at risk of duplication,
	// and overwriting it with a fresh draft would throw all of that away and
	// send the answer a second time.
	insertOutboxSQL = `INSERT INTO channel_outbox_messages (
		tenant_id, outbox_id, run_id, request_id, channel, channel_binding_id, session_id,
		part_no, idempotency_key, client_message_id, status, attempt, max_attempts,
		next_attempt_at, body, delivery_channel, delivery_version, delivery_payload,
		created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
		        $18, $19, $20)
		ON CONFLICT ON CONSTRAINT channel_outbox_messages_idempotency_key DO NOTHING
		RETURNING ` + outboxColumns

	// unsentPredecessorSQL is the eligibility rule the whole delivery side turns
	// on: one answer's parts go out in order, so a part is sendable only once
	// every lower-numbered part of its Run has been sent. It is spelled once and
	// pasted into both the claim and the dispatch scan, because the two disagreeing
	// would mean announcing wakeups for parts nothing is allowed to claim.
	//
	// `o` is the outer row in both users.
	unsentPredecessorSQL = `EXISTS (
			SELECT 1 FROM channel_outbox_messages p
			WHERE p.tenant_id = o.tenant_id AND p.run_id = o.run_id
			  AND p.part_no < o.part_no AND p.status <> 'sent')`

	// FOR UPDATE names o explicitly: the predecessor subquery reads rows this
	// transaction has no business locking, and locking them would turn two
	// dispatchers working on two different answers into a queue.
	selectClaimableOutboxSQL = `SELECT ` + outboxColumns + ` FROM channel_outbox_messages AS o
		WHERE o.tenant_id = $1 AND o.status = 'pending' AND o.next_attempt_at <= $2
		  AND ($3::text = '' OR o.outbox_id = $3::text)
		  AND NOT ` + unsentPredecessorSQL + `
		ORDER BY o.next_attempt_at, o.outbox_id COLLATE "C"
		LIMIT 1
		FOR UPDATE OF o SKIP LOCKED`

	// failSuccessorOutboxSQL closes the parts that will never be reachable once
	// an earlier one has terminally failed. Only pending rows are touched: a part
	// already sending owns its own outcome, and a part already sent or failed has
	// one. It runs in the transaction that wrote the failure, so no claim can
	// slip between the two.
	//
	// last_dispatched_at is deliberately left where it is, exactly as
	// terminatePendingOutboxSQL leaves it. The mark exists to keep the dispatch
	// scan from re-announcing a row it has already announced, and that scan reads
	// pending rows only — so on a terminal row the column is inert, and clearing
	// it would be a write the memory Store does not make.
	failSuccessorOutboxSQL = `UPDATE channel_outbox_messages
		SET status = 'failed', error_type = $4, updated_at = $5
		WHERE tenant_id = $1 AND run_id = $2 AND part_no > $3 AND status = 'pending'`

	claimOutboxSQL = `UPDATE channel_outbox_messages
		SET status = 'sending', attempt = attempt + 1, send_token = $3, sent_by = $4,
		    send_deadline_at = $5, updated_at = $6
		WHERE tenant_id = $1 AND outbox_id = $2
		  AND status = 'pending' AND attempt = $7
		  AND attempt < max_attempts AND next_attempt_at <= $6
		RETURNING ` + outboxColumns

	terminatePendingOutboxSQL = `UPDATE channel_outbox_messages
		SET status = 'failed', error_type = $3, updated_at = $4
		WHERE tenant_id = $1 AND outbox_id = $2 AND status = 'pending' AND attempt = $5`

	terminateSendingOutboxSQL = `UPDATE channel_outbox_messages
		SET status = 'failed', error_type = $3, updated_at = $4,
		    duplicate_risk = duplicate_risk OR $5,
		    send_token = '', sent_by = '', send_deadline_at = NULL
		WHERE tenant_id = $1 AND outbox_id = $2
		  AND status = 'sending' AND send_token = $6 AND attempt = $7`

	// last_dispatched_at is cleared for the same reason as in
	// requeueClaimedRunSQL: the part re-enters the queue on a new generation and
	// must be announced when its backoff expires, not when the dispatch scan's
	// staleness window does.
	requeueOutboxSQL = `UPDATE channel_outbox_messages
		SET status = 'pending', error_type = $3, next_attempt_at = $4, updated_at = $4,
		    duplicate_risk = duplicate_risk OR $5,
		    send_token = '', sent_by = '', send_deadline_at = NULL,
		    last_dispatched_at = NULL
		WHERE tenant_id = $1 AND outbox_id = $2
		  AND status = 'sending' AND send_token = $6 AND attempt = $7`

	markOutboxSentSQL = `UPDATE channel_outbox_messages
		SET status = 'sent', error_type = '', external_message_id = $3, sent_at = $4,
		    updated_at = $4, send_token = '', sent_by = '', send_deadline_at = NULL
		WHERE tenant_id = $1 AND outbox_id = $2 AND status = 'sending' AND send_token = $5`

	// scopeFilterSQL binds $1 and $2 and needs no alias here; see scope.go.
	selectExpiredOutboxSQL = `SELECT ` + outboxColumns + ` FROM channel_outbox_messages
		WHERE status = 'sending' AND send_deadline_at <= $3` + scopeFilterSQL + `
		ORDER BY tenant_id COLLATE "C", outbox_id COLLATE "C"
		LIMIT $4
		FOR UPDATE SKIP LOCKED`

	// The scope filter is the aliased spelling: an unqualified column here would
	// still resolve to o, but the predecessor subquery beside it makes the
	// difference between an outer and an inner reference worth stating.
	selectDispatchableOutboxSQL = `SELECT o.tenant_id, o.outbox_id, o.channel, o.attempt
		FROM channel_outbox_messages AS o
		WHERE o.status = 'pending' AND o.next_attempt_at <= $3
		  AND (o.last_dispatched_at IS NULL OR o.last_dispatched_at <= $4)
		  AND NOT ` + unsentPredecessorSQL + aliasedScopeFilterSQL + `
		ORDER BY o.tenant_id COLLATE "C", o.outbox_id COLLATE "C"
		LIMIT $5`

	// Fenced, due-guarded and monotonic exactly like markRunsDispatchedSQL,
	// which carries the reasoning for each predicate.
	markOutboxDispatchedSQL = `UPDATE channel_outbox_messages AS o
		SET last_dispatched_at = GREATEST(o.last_dispatched_at, $1::timestamptz)
		FROM unnest($2::text[], $3::text[], $4::integer[]) AS d(tenant_id, outbox_id, attempt)
		WHERE o.tenant_id = d.tenant_id AND o.outbox_id = d.outbox_id
		  AND o.status = 'pending' AND o.attempt = d.attempt
		  AND o.next_attempt_at <= $1::timestamptz`
)

// insertOutboxPart writes one part of an answer, or returns the part a previous
// attempt of the same Run already wrote for it.
func insertOutboxPart(
	ctx context.Context,
	tx pgx.Tx,
	run channels.Run,
	draft channels.OutboxDraft,
	now time.Time,
) (channels.OutboxPart, error) {
	// An answer must go back to the channel the question came from. A draft
	// that names a different one is refused rather than rewritten, because the
	// caller and the platform disagree about where this answer belongs.
	if draft.DeliveryTarget.Channel != run.Channel {
		return channels.OutboxPart{}, fmt.Errorf(
			"%w: outbox delivery target channel does not match the run",
			tenant.ErrInvalidArgument)
	}
	body, err := encodeJSON("finish run", draft.Message)
	if err != nil {
		return channels.OutboxPart{}, err
	}
	idempotencyKey := channels.IdempotencyKeyFor(run.RequestID, draft.PartNo)

	args := []any{
		run.TenantID, draft.OutboxID, run.RunID, run.RequestID, string(run.Channel),
		run.ChannelBindingID, run.SessionID, draft.PartNo, idempotencyKey,
		draft.ClientMessageID, string(channels.OutboxPending), int32(0), draft.MaxAttempts,
		now, string(body),
	}
	args = append(args, deliveryArguments(draft.DeliveryTarget)...)
	args = append(args, now, now)

	part, err := scanOutbox("finish run", tx.QueryRow(ctx, insertOutboxSQL, args...))
	switch {
	case err == nil:
		return part, nil
	case errors.Is(err, pgx.ErrNoRows):
		// The idempotency key was already taken, which is the retry case: the
		// original part, with whatever send state it has accumulated, wins.
		existing, err := scanOutbox("finish run", tx.QueryRow(
			ctx, selectOutboxByIdempotencySQL,
			run.TenantID, run.ChannelBindingID, idempotencyKey))
		if errors.Is(err, pgx.ErrNoRows) {
			return channels.OutboxPart{}, storageFailure(
				"finish run", errors.New("conflicting outbox part disappeared"))
		}
		if err != nil {
			return channels.OutboxPart{}, storageError(ctx, "finish run", err)
		}
		return existing, nil
	default:
		return channels.OutboxPart{}, constraintError(
			ctx, "insert outbox part", err, map[string]error{
				"channel_outbox_messages_pkey":     alreadyExists("outbox id"),
				"channel_outbox_messages_part_key": alreadyExists("outbox part number"),
			})
	}
}

// GetOutboxPart returns one part.
func (s *Store) GetOutboxPart(
	ctx context.Context,
	scope tenant.TenantContext,
	outboxID string,
) (channels.OutboxPart, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.OutboxPart{}, err
	}
	if err := scope.Validate(); err != nil {
		return channels.OutboxPart{}, err
	}
	if err := tenant.ValidateResourceID("outbox id", outboxID); err != nil {
		return channels.OutboxPart{}, err
	}
	part, err := scanOutbox(
		"read outbox part", s.pool.QueryRow(ctx, selectOutboxSQL, scope.TenantID, outboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.OutboxPart{}, notFound("outbox part", outboxID)
	}
	if err != nil {
		return channels.OutboxPart{}, storageError(ctx, "read outbox part", err)
	}
	return part, nil
}

// ListRunOutbox returns a Run's answer parts in order.
func (s *Store) ListRunOutbox(
	ctx context.Context,
	scope tenant.TenantContext,
	runID string,
	request channels.ListRequest,
) ([]channels.OutboxPart, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := tenant.ValidateResourceID("run id", runID); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(
		ctx, selectRunOutboxSQL, scope.TenantID, runID, int32(request.Limit))
	if err != nil {
		return nil, storageError(ctx, "list run outbox", err)
	}
	defer rows.Close()

	parts := make([]channels.OutboxPart, 0)
	for rows.Next() {
		part, err := scanOutbox("list run outbox", rows)
		if err != nil {
			return nil, storageError(ctx, "list run outbox", err)
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(ctx, "list run outbox", err)
	}
	return parts, nil
}

// ClaimOutbox claims one due part for sending.
func (s *Store) ClaimOutbox(
	ctx context.Context,
	scope tenant.TenantContext,
	request channels.ClaimOutboxRequest,
) (channels.OutboxClaim, bool, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.OutboxClaim{}, false, err
	}
	if err := scope.Validate(); err != nil {
		return channels.OutboxClaim{}, false, err
	}
	if err := request.Validate(); err != nil {
		return channels.OutboxClaim{}, false, err
	}
	now := channels.NormalizeTime(request.Now)

	var (
		claim   channels.OutboxClaim
		claimed bool
	)
	err := s.withTx(ctx, "claim outbox part", func(tx pgx.Tx) error {
		for sweeps := 0; ; {
			part, err := scanOutbox("claim outbox part", tx.QueryRow(
				ctx, selectClaimableOutboxSQL,
				scope.TenantID, now, request.OutboxID))
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return storageError(ctx, "claim outbox part", err)
			}
			// An exhausted part is terminated where it is found, so a dead row is
			// not selected by every scan from now on. It is a terminal failure
			// like any other, so it closes the rest of the answer with it.
			if part.AttemptsExhausted() {
				if sweeps == channels.MaxExhaustSweep {
					return nil
				}
				sweeps++
				if _, err := tx.Exec(
					ctx, terminatePendingOutboxSQL,
					part.TenantID, part.OutboxID,
					string(channels.ErrorAttemptsExhausted), now, part.Attempt,
				); err != nil {
					return storageError(ctx, "terminate exhausted outbox part", err)
				}
				if err := failOutboxSuccessors(ctx, tx, part, now); err != nil {
					return err
				}
				continue
			}
			// See channels.ClaimOutboxRequest.ExpectedChannel: the wakeup named a
			// channel, the row is the authority, and a disagreement writes
			// nothing at all — no status, no attempt, no token.
			if request.ExpectedChannel != "" && part.Channel != request.ExpectedChannel {
				return nil
			}

			// Normalized before it is stored, so the value the Sender's budget is
			// derived from is exactly the value that comes back out of the column.
			deadline := channels.NormalizeTime(now.Add(request.SendTimeout))
			updated, err := scanOutbox("claim outbox part", tx.QueryRow(
				ctx, claimOutboxSQL,
				part.TenantID, part.OutboxID, request.SendToken, request.SentBy,
				deadline, now, part.Attempt))
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return storageError(ctx, "claim outbox part", err)
			}
			claim = channels.OutboxClaim{Part: updated}
			claimed = true
			return nil
		}
	})
	if err != nil {
		return channels.OutboxClaim{}, false, err
	}
	return claim, claimed, nil
}

// CompleteOutbox closes a send attempt.
func (s *Store) CompleteOutbox(
	ctx context.Context,
	scope tenant.TenantContext,
	request channels.CompleteOutboxRequest,
) (channels.OutboxStatus, error) {
	if err := s.validateCall(ctx); err != nil {
		return "", err
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	now := channels.NormalizeTime(request.Now)

	var status channels.OutboxStatus
	err := s.withTx(ctx, "complete outbox part", func(tx pgx.Tx) error {
		part, err := scanOutbox("complete outbox part", tx.QueryRow(
			ctx, selectOutboxSQL+` FOR UPDATE`, scope.TenantID, request.Token.OutboxID))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("outbox part", request.Token.OutboxID)
		}
		if err != nil {
			return storageError(ctx, "complete outbox part", err)
		}
		// A send whose deadline passed has already been treated as an unknown
		// outcome by the recovery scanner, which cleared this token. The late
		// Sender therefore misses the CAS and must not overwrite what recovery
		// decided.
		if part.Status != channels.OutboxSending || part.SendToken != request.Token.SendToken {
			return channels.ErrStaleClaim
		}

		switch request.Result.Outcome {
		case channels.SendSucceeded:
			if _, err := tx.Exec(
				ctx, markOutboxSentSQL,
				part.TenantID, part.OutboxID, request.Result.ExternalMessageID,
				now, part.SendToken,
			); err != nil {
				return storageError(ctx, "mark outbox part sent", err)
			}
			status = channels.OutboxSent
			return nil
		case channels.SendPermanent:
			if _, err := tx.Exec(
				ctx, terminateSendingOutboxSQL,
				part.TenantID, part.OutboxID, string(request.Result.ErrorType), now,
				false, part.SendToken, part.Attempt,
			); err != nil {
				return storageError(ctx, "fail outbox part", err)
			}
			if err := failOutboxSuccessors(ctx, tx, part, now); err != nil {
				return err
			}
			status = channels.OutboxFailed
			return nil
		default:
			// Retryable and unknown differ only in whether the attempt may have
			// been delivered. The mark is applied even when the requeue turns
			// out to be a termination, because the risk does not depend on
			// whether there are attempts left.
			atRisk := request.Result.Outcome == channels.SendUnknown
			outcome, err := requeueOutboxPart(
				ctx, tx, part, request.Result.ErrorType, atRisk, now)
			if err != nil {
				return err
			}
			if outcome == channels.RequeueExhausted {
				status = channels.OutboxFailed
			} else {
				status = channels.OutboxPending
			}
			return nil
		}
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

// requeueOutboxPart returns a part to the queue, or terminates it when there is
// nothing left to retry with. It is shared by CompleteOutbox and the recovery
// scanner so both make the same decision about a spent send budget.
func requeueOutboxPart(
	ctx context.Context,
	tx pgx.Tx,
	part channels.OutboxPart,
	errorType channels.ErrorType,
	atRisk bool,
	now time.Time,
) (channels.RequeueOutcome, error) {
	if part.AttemptsExhausted() {
		if _, err := tx.Exec(
			ctx, terminateSendingOutboxSQL,
			part.TenantID, part.OutboxID, string(channels.ErrorAttemptsExhausted), now,
			atRisk, part.SendToken, part.Attempt,
		); err != nil {
			return "", storageError(ctx, "terminate exhausted outbox part", err)
		}
		if err := failOutboxSuccessors(ctx, tx, part, now); err != nil {
			return "", err
		}
		return channels.RequeueExhausted, nil
	}
	nextAttemptAt := channels.NormalizeTime(
		now.Add(channels.Backoff(part.Attempt, part.OutboxID)))
	if _, err := tx.Exec(
		ctx, requeueOutboxSQL,
		part.TenantID, part.OutboxID, string(errorType), nextAttemptAt,
		atRisk, part.SendToken, part.Attempt,
	); err != nil {
		return "", storageError(ctx, "requeue outbox part", err)
	}
	return channels.RequeueRetried, nil
}

// failOutboxSuccessors closes the Run's later parts that have not been sent,
// after one of its parts has terminally failed.
//
// It is called from inside the transaction that wrote the failure, never on its
// own, so a dispatcher cannot claim a part whose predecessor is already dead.
// See channels.ErrorPredecessorFailed for why the alternative — leaving them
// pending — is worse than closing them.
func failOutboxSuccessors(
	ctx context.Context,
	tx pgx.Tx,
	failed channels.OutboxPart,
	now time.Time,
) error {
	if _, err := tx.Exec(
		ctx, failSuccessorOutboxSQL,
		failed.TenantID, failed.RunID, failed.PartNo,
		string(channels.ErrorPredecessorFailed), now,
	); err != nil {
		return storageError(ctx, "close unreachable outbox parts", err)
	}
	return nil
}

// RecoverOutbox requeues or terminates parts whose send deadline passed.
func (s *Store) RecoverOutbox(
	ctx context.Context,
	request channels.RecoverRequest,
) ([]channels.OutboxRecovery, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	now := channels.NormalizeTime(request.Now)

	var recovered []channels.OutboxRecovery
	err := s.withTx(ctx, "recover outbox parts", func(tx pgx.Tx) error {
		tenantArg, bindingArg := scopeArgs(request.Scope)
		rows, err := tx.Query(
			ctx, selectExpiredOutboxSQL,
			tenantArg, bindingArg, now, int32(request.Limit))
		if err != nil {
			return storageError(ctx, "recover outbox parts", err)
		}
		expired := make([]channels.OutboxPart, 0)
		for rows.Next() {
			part, err := scanOutbox("recover outbox parts", rows)
			if err != nil {
				rows.Close()
				return storageError(ctx, "recover outbox parts", err)
			}
			expired = append(expired, part)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return storageError(ctx, "recover outbox parts", err)
		}

		recovered = make([]channels.OutboxRecovery, 0, len(expired))
		for _, part := range expired {
			// An expired send is an unknown outcome, never a failure: the
			// request left this process and may well have been delivered.
			outcome, err := requeueOutboxPart(
				ctx, tx, part, channels.ErrorOutcomeUnknown, true, now)
			if err != nil {
				return err
			}
			recovered = append(recovered, channels.OutboxRecovery{
				OutboxRef: channels.OutboxRef{
					TenantID: part.TenantID,
					OutboxID: part.OutboxID,
				},
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

// ListDispatchableOutbox returns due parts whose wakeup looks lost.
func (s *Store) ListDispatchableOutbox(
	ctx context.Context,
	request channels.DispatchScanRequest,
) ([]channels.OutboxDispatch, error) {
	if err := s.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	now := channels.NormalizeTime(request.Now)
	tenantArg, bindingArg := scopeArgs(request.Scope)
	rows, err := s.pool.Query(
		ctx, selectDispatchableOutboxSQL,
		tenantArg, bindingArg, now, now.Add(-request.StaleAfter), int32(request.Limit))
	if err != nil {
		return nil, storageError(ctx, "list dispatchable outbox parts", err)
	}
	defer rows.Close()

	candidates := make([]channels.OutboxDispatch, 0)
	for rows.Next() {
		var (
			candidate channels.OutboxDispatch
			channel   string
		)
		if err := rows.Scan(
			&candidate.TenantID, &candidate.OutboxID, &channel, &candidate.Attempt,
		); err != nil {
			return nil, storageError(ctx, "list dispatchable outbox parts", err)
		}
		candidate.Channel = channels.ChannelType(channel)
		// The column is written by this package and constrained by the domain,
		// but a scan that returned an unroutable channel would publish a wakeup
		// no consumer could act on, so it is checked on the way out like any
		// other stored enum.
		if err := candidate.Channel.Validate(); err != nil {
			return nil, integrityError("list dispatchable outbox parts", "channel")
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(ctx, "list dispatchable outbox parts", err)
	}
	return candidates, nil
}

// MarkOutboxDispatched records that a wakeup was published, for the parts still
// on the generation it was published for.
func (s *Store) MarkOutboxDispatched(
	ctx context.Context,
	dispatches []channels.OutboxDispatch,
	at time.Time,
) (int, error) {
	if err := s.validateCall(ctx); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, requiredTimeError("dispatched at")
	}
	if len(dispatches) > channels.MaxListLimit {
		return 0, tooManyRefsError("outbox parts")
	}
	tenantIDs := make([]string, 0, len(dispatches))
	outboxIDs := make([]string, 0, len(dispatches))
	attempts := make([]int32, 0, len(dispatches))
	for _, dispatch := range dispatches {
		if err := dispatch.Validate(); err != nil {
			return 0, err
		}
		tenantIDs = append(tenantIDs, dispatch.TenantID)
		outboxIDs = append(outboxIDs, dispatch.OutboxID)
		attempts = append(attempts, dispatch.Attempt)
	}
	if len(dispatches) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(
		ctx, markOutboxDispatchedSQL,
		channels.NormalizeTime(at), tenantIDs, outboxIDs, attempts,
	)
	if err != nil {
		return 0, storageError(ctx, "mark outbox parts dispatched", err)
	}
	return int(tag.RowsAffected()), nil
}

// scanOutbox reads one Outbox row.
func scanOutbox(operation string, row pgx.Row) (channels.OutboxPart, error) {
	var (
		item            channels.OutboxPart
		channel         string
		status          string
		errorType       string
		partNo          int32
		attempt         int32
		maxAttempts     int32
		lastDispatched  *time.Time
		sendDeadline    *time.Time
		sentAt          *time.Time
		body            []byte
		deliveryChannel string
		deliveryVersion int32
		deliveryPayload []byte
	)
	if err := row.Scan(
		&item.TenantID, &item.OutboxID, &item.RunID, &item.RequestID, &channel,
		&item.ChannelBindingID, &item.SessionID, &partNo, &item.IdempotencyKey,
		&item.ClientMessageID, &status, &attempt, &maxAttempts, &item.NextAttemptAt,
		&lastDispatched, &item.SendToken, &item.SentBy, &sendDeadline, &item.DuplicateRisk,
		&errorType, &item.ExternalMessageID, &sentAt, &body,
		&deliveryChannel, &deliveryVersion, &deliveryPayload,
		&item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return channels.OutboxPart{}, err
	}
	item.Channel = channels.ChannelType(channel)
	item.Status = channels.OutboxStatus(status)
	item.ErrorType = channels.ErrorType(errorType)
	item.PartNo = partNo
	item.Attempt = attempt
	item.MaxAttempts = maxAttempts
	item.NextAttemptAt = item.NextAttemptAt.UTC()
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	item.LastDispatchedAt = nullableTime(lastDispatched)
	item.SendDeadlineAt = nullableTime(sendDeadline)
	item.SentAt = nullableTime(sentAt)

	if err := decodeJSON(operation, "body", body, &item.Message); err != nil {
		return channels.OutboxPart{}, err
	}
	target, err := decodeDeliveryTarget(
		operation, deliveryChannel, deliveryVersion, deliveryPayload)
	if err != nil {
		return channels.OutboxPart{}, err
	}
	item.DeliveryTarget = target

	if err := item.Status.Validate(); err != nil {
		return channels.OutboxPart{}, integrityError(operation, "status")
	}
	if err := item.ErrorType.Validate(); err != nil {
		return channels.OutboxPart{}, integrityError(operation, "error_type")
	}
	return item, nil
}

// sortOutboxParts orders parts the way every list in this package returns them.
func sortOutboxParts(parts []channels.OutboxPart) {
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].PartNo != parts[j].PartNo {
			return parts[i].PartNo < parts[j].PartNo
		}
		return parts[i].OutboxID < parts[j].OutboxID
	})
}
