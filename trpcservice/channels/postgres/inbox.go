package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const inboxColumns = `tenant_id, inbox_id, channel, channel_binding_id, agent_app_id, ` +
	`principal_id, session_id, external_event_id, received_at, accepted_at, message, ` +
	`delivery_channel, delivery_version, delivery_payload`

const (
	selectInboxSQL = `SELECT ` + inboxColumns + ` FROM channel_inbox_messages
		WHERE tenant_id = $1 AND inbox_id = $2`

	selectInboxByEventSQL = `SELECT inbox_id FROM channel_inbox_messages
		WHERE tenant_id = $1 AND channel_binding_id = $2 AND external_event_id = $3`

	// The duplicate path answers with the state of the rows that already exist,
	// which spans both tables: the Run holds the generation a caller has to
	// fence its wakeup bookkeeping with, and the inbox message holds the
	// acceptance time of the delivery that won, which is the one that counts.
	selectRunByInboxSQL = `SELECT r.run_id, r.request_id, r.accept_sequence, r.attempt,
		    i.accepted_at
		FROM channel_agent_runs AS r
		JOIN channel_inbox_messages AS i
		  ON i.tenant_id = r.tenant_id AND i.inbox_id = r.inbox_id
		WHERE r.tenant_id = $1 AND r.inbox_id = $2`

	selectNextSequenceSQL = `SELECT COALESCE(MAX(accept_sequence), 0) + 1
		FROM channel_agent_runs
		WHERE tenant_id = $1 AND agent_app_id = $2 AND principal_id = $3 AND session_id = $4`

	insertInboxSQL = `INSERT INTO channel_inbox_messages (` + inboxColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

	insertRunSQL = `INSERT INTO channel_agent_runs (
		tenant_id, run_id, request_id, inbox_id, channel, channel_binding_id,
		agent_app_id, principal_id, session_id, accept_sequence, status, attempt,
		max_attempts, max_run_duration_ms, recovery_grace_ms, next_attempt_at,
		created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`
)

// errDuplicateEvent is internal: it marks the narrow race in which two accepts
// of one external event pass the duplicate check because they hold locks on
// different Session keys. It never escapes Accept.
var errDuplicateEvent = errors.New("channels/postgres: external event already accepted")

// Accept records one external event and the Run that will answer it.
func (s *Store) Accept(
	ctx context.Context,
	scope tenant.TenantContext,
	request channels.AcceptRequest,
) (channels.AcceptResult, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.AcceptResult{}, err
	}
	if err := request.Validate(scope); err != nil {
		return channels.AcceptResult{}, err
	}

	// Two passes at most. The first can lose the race described on
	// errDuplicateEvent; by the second the winner has committed, so the
	// duplicate check inside acceptOnce finds it and returns its identifiers.
	for attempt := 0; attempt < 2; attempt++ {
		result, err := s.acceptOnce(ctx, scope, request)
		if errors.Is(err, errDuplicateEvent) {
			continue
		}
		return result, err
	}
	return channels.AcceptResult{}, storageFailure(
		"accept event", errors.New("external event neither inserted nor found"))
}

func (s *Store) acceptOnce(
	ctx context.Context,
	scope tenant.TenantContext,
	request channels.AcceptRequest,
) (channels.AcceptResult, error) {
	envelope := request.Envelope
	now := channels.NormalizeTime(request.Now)
	key := envelope.SessionKey()

	body, err := encodeJSON("accept event", envelope.Message)
	if err != nil {
		return channels.AcceptResult{}, err
	}

	var result channels.AcceptResult
	err = s.withTx(ctx, "accept event", func(tx pgx.Tx) error {
		// The ordering lock is taken before anything is read, and is held to
		// the end of the transaction. It is what makes accept_sequence a real
		// order: without it two concurrent accepts for one conversation both
		// read the same MAX and both claim the same position. It also serialises
		// the duplicate check against a concurrent insert of the same event.
		if _, err := tx.Exec(
			ctx, acquireSessionLockSQL,
			sessionOrderingLockClass, sessionLockObject(key),
		); err != nil {
			return storageError(ctx, "acquire session ordering lock", err)
		}

		var existingInboxID string
		err := tx.QueryRow(
			ctx, selectInboxByEventSQL,
			scope.TenantID, envelope.ChannelBindingID, envelope.ExternalEventID,
		).Scan(&existingInboxID)
		switch {
		case err == nil:
			duplicate, err := duplicateResult(ctx, tx, scope.TenantID, existingInboxID)
			if err != nil {
				return err
			}
			result = duplicate
			return nil
		case errors.Is(err, pgx.ErrNoRows):
			// Fall through to the insert.
		default:
			return storageError(ctx, "look up accepted event", err)
		}

		var sequence int64
		if err := tx.QueryRow(
			ctx, selectNextSequenceSQL,
			key.TenantID, key.AppID, key.PrincipalID, key.SessionID,
		).Scan(&sequence); err != nil {
			return storageError(ctx, "assign accept sequence", err)
		}

		if _, err := tx.Exec(
			ctx, insertInboxSQL,
			scope.TenantID,
			request.IDs.InboxID,
			string(envelope.Channel),
			envelope.ChannelBindingID,
			envelope.AgentAppID,
			envelope.PrincipalID,
			envelope.SessionID,
			envelope.ExternalEventID,
			channels.NormalizeTime(envelope.ReceivedAt),
			now,
			string(body),
			string(envelope.DeliveryTarget.Channel),
			int32(envelope.DeliveryTarget.Version),
			string(envelope.DeliveryTarget.Payload),
		); err != nil {
			return constraintError(ctx, "insert inbox message", err, map[string]error{
				"channel_inbox_messages_event_key": errDuplicateEvent,
				"channel_inbox_messages_pkey":      alreadyExists("inbox id"),
			})
		}

		if _, err := tx.Exec(
			ctx, insertRunSQL,
			scope.TenantID,
			request.IDs.RunID,
			request.IDs.RequestID,
			request.IDs.InboxID,
			string(envelope.Channel),
			envelope.ChannelBindingID,
			envelope.AgentAppID,
			envelope.PrincipalID,
			envelope.SessionID,
			sequence,
			string(channels.RunAccepted),
			int32(0),
			request.Policy.MaxAttempts,
			millisFromDuration(request.Policy.MaxRunDuration),
			millisFromDuration(request.Policy.RecoveryGrace),
			// A freshly accepted Run is due immediately: the first attempt has
			// nothing to back off from.
			now,
			now,
			now,
		); err != nil {
			return constraintError(ctx, "insert run", err, map[string]error{
				"channel_agent_runs_pkey":        alreadyExists("run id"),
				"channel_agent_runs_run_id_key":  alreadyExists("run id"),
				"channel_agent_runs_request_key": alreadyExists("request id"),
				"channel_agent_runs_inbox_key":   alreadyExists("inbox id"),
			})
		}

		result = channels.AcceptResult{
			InboxID:        request.IDs.InboxID,
			RunID:          request.IDs.RunID,
			RequestID:      request.IDs.RequestID,
			AcceptSequence: sequence,
			Attempt:        0,
			AcceptedAt:     now,
		}
		return nil
	})
	if err != nil {
		return channels.AcceptResult{}, err
	}
	return result, nil
}

// duplicateResult reports what a previous Accept stored.
//
// Nothing here is taken from the call that is asking. The attempt is the Run's
// generation as it stands now — not zero, once the Run has been claimed — and
// the acceptance time is the original one, so a redelivered event does not look
// newer every time it arrives.
func duplicateResult(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	inboxID string,
) (channels.AcceptResult, error) {
	var (
		runID      string
		requestID  string
		sequence   int64
		attempt    int32
		acceptedAt time.Time
	)
	err := tx.QueryRow(ctx, selectRunByInboxSQL, tenantID, inboxID).
		Scan(&runID, &requestID, &sequence, &attempt, &acceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// The caller reached this only after finding the inbox message in this
		// transaction, and Accept writes both rows together, so the join can
		// only come up empty if the Run is missing. If one is, the honest
		// answer is a failure rather than a result with an empty run id.
		return channels.AcceptResult{}, storageFailure(
			"resolve duplicate event", fmt.Errorf("accepted message has no run"))
	}
	if err != nil {
		return channels.AcceptResult{}, storageError(ctx, "resolve duplicate event", err)
	}
	return channels.AcceptResult{
		InboxID:        inboxID,
		RunID:          runID,
		RequestID:      requestID,
		AcceptSequence: sequence,
		Attempt:        attempt,
		AcceptedAt:     acceptedAt.UTC(),
		Duplicate:      true,
	}, nil
}

// GetInbox returns one accepted event.
func (s *Store) GetInbox(
	ctx context.Context,
	scope tenant.TenantContext,
	inboxID string,
) (channels.InboxMessage, error) {
	if err := s.validateCall(ctx); err != nil {
		return channels.InboxMessage{}, err
	}
	if err := scope.Validate(); err != nil {
		return channels.InboxMessage{}, err
	}
	if err := tenant.ValidateResourceID("inbox id", inboxID); err != nil {
		return channels.InboxMessage{}, err
	}
	message, err := scanInbox(
		"read inbox message", s.pool.QueryRow(ctx, selectInboxSQL, scope.TenantID, inboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.InboxMessage{}, notFound("inbox message", inboxID)
	}
	if err != nil {
		return channels.InboxMessage{}, storageError(ctx, "read inbox message", err)
	}
	return message, nil
}

// scanInbox reads one inbox row.
//
// It returns pgx.ErrNoRows unwrapped so the caller can turn it into the right
// domain error; every other failure is already a storage error.
func scanInbox(operation string, row pgx.Row) (channels.InboxMessage, error) {
	var (
		item            channels.InboxMessage
		channel         string
		body            []byte
		deliveryChannel string
		deliveryVersion int32
		deliveryPayload []byte
	)
	if err := row.Scan(
		&item.TenantID, &item.InboxID, &channel, &item.ChannelBindingID, &item.AgentAppID,
		&item.PrincipalID, &item.SessionID, &item.ExternalEventID, &item.ReceivedAt,
		&item.AcceptedAt, &body, &deliveryChannel, &deliveryVersion, &deliveryPayload,
	); err != nil {
		return channels.InboxMessage{}, err
	}
	item.Channel = channels.ChannelType(channel)
	item.ReceivedAt = item.ReceivedAt.UTC()
	item.AcceptedAt = item.AcceptedAt.UTC()
	if err := decodeJSON(operation, "message", body, &item.Message); err != nil {
		return channels.InboxMessage{}, err
	}
	target, err := decodeDeliveryTarget(operation, deliveryChannel, deliveryVersion, deliveryPayload)
	if err != nil {
		return channels.InboxMessage{}, err
	}
	item.DeliveryTarget = target
	return item, nil
}

// decodeDeliveryTarget rebuilds a delivery target from its three columns.
//
// The payload is handed back as the bytes that were stored, without being
// re-encoded: an adapter's blob is its own, and normalising it here would mean
// the value an adapter reads is not the value it wrote.
func decodeDeliveryTarget(
	operation string,
	channel string,
	version int32,
	payload []byte,
) (channels.DeliveryTarget, error) {
	narrowed, err := uint16FromColumn(operation, "delivery_version", version)
	if err != nil {
		return channels.DeliveryTarget{}, err
	}
	if len(payload) == 0 || !json.Valid(payload) {
		return channels.DeliveryTarget{}, integrityError(operation, "delivery_payload")
	}
	return channels.DeliveryTarget{
		Channel: channels.ChannelType(channel),
		Version: narrowed,
		Payload: json.RawMessage(payload),
	}, nil
}

// deliveryArguments lays a delivery target out in the order every statement
// binds it.
func deliveryArguments(target channels.DeliveryTarget) []any {
	return []any{string(target.Channel), int32(target.Version), string(target.Payload)}
}
