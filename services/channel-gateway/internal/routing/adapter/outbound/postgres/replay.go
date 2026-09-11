package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

func (store *Store) BeginReplay(ctx context.Context, source domain.ReplaySource) error {
	if source.ObservedAt.IsZero() {
		source.ObservedAt = time.Now().UTC()
	}
	sourceErr := source.Validate()
	if sourceErr != nil && !errors.Is(sourceErr, domain.ErrHistoryGap) {
		return sourceErr
	}
	if store.pool == nil {
		return domain.ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(tx) }()
	created, err := tx.Exec(ctx, `INSERT INTO gateway_route_replay_state(singleton,stream_name,stream_id,target_sequence,highest_sequence,last_observed_at,apply_lag_since)
 VALUES(true,$1,$2,$3,$3,LEAST($4,clock_timestamp()),CASE WHEN $3::bigint>0 THEN clock_timestamp() END)
 ON CONFLICT(singleton) DO NOTHING`, source.StreamName, source.StreamID, int64(source.LastSequence), source.ObservedAt)
	if err != nil {
		return fmt.Errorf("initialize replay state: %w", err)
	}
	health, err := readHealth(ctx, tx, true)
	if err != nil {
		return err
	}
	reason := domain.QuarantineReason("")
	if health.StreamName != source.StreamName || health.StreamID != source.StreamID {
		reason = domain.QuarantineSourceChanged
	} else if sourceErr != nil {
		reason = domain.QuarantineHistoryGap
	}
	if reason != "" {
		if err = insertQuarantine(ctx, tx, domain.StreamPosition{StreamName: source.StreamName, StreamID: source.StreamID}, reason, ""); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", domain.ErrProjectionBlocked, reason)
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_route_replay_state SET
 target_sequence=GREATEST(target_sequence,$1),highest_sequence=GREATEST(highest_sequence,$1),
 apply_lag_since=CASE WHEN GREATEST(highest_sequence,$1)>contiguous_sequence
   THEN COALESCE(apply_lag_since,clock_timestamp()) ELSE NULL END,
 last_observed_at=GREATEST(last_observed_at,LEAST($2,clock_timestamp())),updated_at=now()
 WHERE singleton`, int64(source.LastSequence), source.ObservedAt)
	if err != nil {
		return err
	}
	if err = auditProjections(ctx, tx, health); err != nil {
		return err
	}
	if created.RowsAffected() == 1 {
		var oldFacts bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_route_projections) OR EXISTS(SELECT 1 FROM gateway_route_receipts)`).Scan(&oldFacts); err != nil {
			return err
		}
		current, readErr := readHealth(ctx, tx, false)
		if readErr != nil {
			return readErr
		}
		if oldFacts && current.BlockedReason == "" {
			if err = insertQuarantine(ctx, tx, domain.StreamPosition{StreamName: source.StreamName, StreamID: source.StreamID}, domain.QuarantineUnboundProjection, ""); err != nil {
				return err
			}
		}
	}
	health, err = readHealth(ctx, tx, false)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if health.BlockedReason != "" {
		return domain.ErrProjectionBlocked
	}
	return nil
}

func (store *Store) ApplyFromStream(ctx context.Context, position domain.StreamPosition, event domain.RouteEvent) error {
	if err := position.Validate(); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		if qerr := store.Quarantine(ctx, position, domain.QuarantineInvalidSchema, event.ReceiptDigest()); qerr != nil {
			return qerr
		}
		return fmt.Errorf("%w: %w", domain.ErrProjectionBlocked, err)
	}
	if store.pool == nil {
		return domain.ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(tx) }()
	health, err := readHealth(ctx, tx, true)
	if err != nil {
		return err
	}
	if health.StreamName == "" {
		return domain.ErrNotInitialized
	}
	if health.StreamName != position.StreamName || health.StreamID != position.StreamID {
		return quarantineAndCommit(ctx, tx, position, domain.QuarantineSourceChanged, event.ReceiptDigest())
	}
	var previous, status string
	err = tx.QueryRow(ctx, `SELECT digest,status FROM gateway_route_stream_receipts WHERE stream_name=$1 AND stream_id=$2 AND sequence=$3`, position.StreamName, position.StreamID, int64(position.Sequence)).Scan(&previous, &status)
	if err == nil {
		if previous != event.ReceiptDigest() {
			return quarantineAndCommit(ctx, tx, position, domain.QuarantineSequenceConflict, event.ReceiptDigest())
		}
		if status != "APPLIED" {
			return domain.ErrProjectionBlocked
		}
		if health.BlockedReason != "" {
			return domain.ErrProjectionBlocked
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if health.BlockedReason != "" {
		return domain.ErrProjectionBlocked
	}
	// SAVEPOINT rolls back a rejected event receipt while retaining the outer
	// transaction in which the quarantine is durably recorded.
	change, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	err = applyEvent(ctx, change, event)
	if err != nil {
		if rollbackErr := rollbackTransaction(change); rollbackErr != nil {
			return rollbackErr
		}
		if errors.Is(err, domain.ErrGenerationConflict) || errors.Is(err, domain.ErrInvalidEvent) {
			return quarantineAndCommit(ctx, tx, position, domain.QuarantineRouteConflict, event.ReceiptDigest())
		}
		return err
	}
	if err = change.Commit(ctx); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO gateway_route_stream_receipts(stream_name,stream_id,sequence,event_id,digest,status) VALUES($1,$2,$3,$4,$5,'APPLIED')`, position.StreamName, position.StreamID, int64(position.Sequence), event.EventID, event.ReceiptDigest())
	if err != nil {
		return err
	}
	checkpoint, err := advanceCheckpoint(ctx, tx, position, health.ContiguousSequence)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_route_replay_state SET
 contiguous_sequence=$1,highest_sequence=GREATEST(highest_sequence,$2),
 apply_lag_since=CASE WHEN GREATEST(highest_sequence,$2)>$1
   THEN COALESCE(apply_lag_since,clock_timestamp()) ELSE NULL END,updated_at=now()
 WHERE singleton`, int64(checkpoint), int64(position.Sequence))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func advanceCheckpoint(ctx context.Context, tx pgx.Tx, p domain.StreamPosition, checkpoint uint64) (uint64, error) {
	rows, err := tx.Query(ctx, `SELECT sequence FROM gateway_route_stream_receipts WHERE stream_name=$1 AND stream_id=$2 AND sequence>$3 AND status='APPLIED' ORDER BY sequence`, p.StreamName, p.StreamID, int64(checkpoint))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var sequence uint64
		if err = rows.Scan(&sequence); err != nil {
			return 0, err
		}
		if sequence != checkpoint+1 {
			break
		}
		checkpoint = sequence
	}
	return checkpoint, rows.Err()
}

// Quarantine persists only a typed reason and payload digest, never raw invalid
// JSON or credentials. It does not take account locks, so an integrity alarm can
// be recorded after the caller has released its read transaction.
func (store *Store) Quarantine(ctx context.Context, p domain.StreamPosition, reason domain.QuarantineReason, digest string) error {
	if err := reason.Validate(); err != nil {
		return err
	}
	if store.pool == nil {
		return domain.ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(tx) }()
	if err = insertQuarantine(ctx, tx, p, reason, digest); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertQuarantine(ctx context.Context, tx pgx.Tx, p domain.StreamPosition, reason domain.QuarantineReason, digest string) error {
	if err := reason.Validate(); err != nil {
		return err
	}
	if p.StreamName == "" {
		p.StreamName = "unknown"
	}
	if p.StreamID == "" {
		p.StreamID = "unknown"
	}
	if len(p.StreamName) > 128 || len(p.StreamID) > 128 || p.Sequence > uint64(^uint64(0)>>1) {
		return domain.ErrInvalidEvent
	}
	if digest != "" && (len(digest) != 71 || digest[:7] != "sha256:") {
		return domain.ErrInvalidEvent
	}
	_, err := tx.Exec(ctx, `INSERT INTO gateway_route_quarantines(stream_name,stream_id,sequence,reason,digest) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, p.StreamName, p.StreamID, int64(p.Sequence), string(reason), digest)
	return err
}
func quarantineAndCommit(ctx context.Context, tx pgx.Tx, p domain.StreamPosition, reason domain.QuarantineReason, digest string) error {
	if err := insertQuarantine(ctx, tx, p, reason, digest); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO gateway_route_stream_receipts(stream_name,stream_id,sequence,digest,status,reason) VALUES($1,$2,$3,$4,'QUARANTINED',$5) ON CONFLICT DO NOTHING`, p.StreamName, p.StreamID, int64(p.Sequence), digest, string(reason))
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", domain.ErrProjectionBlocked, reason)
}

func (store *Store) QueryProjectionHealth(ctx context.Context) (domain.ProjectionHealth, error) {
	if store.pool == nil {
		return domain.ProjectionHealth{}, domain.ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return domain.ProjectionHealth{}, err
	}
	defer func() { _ = rollbackTransaction(tx) }()
	health, err := readHealth(ctx, tx, false)
	if err != nil {
		return health, err
	}
	if err = auditProjections(ctx, tx, health); err != nil {
		return health, err
	}
	health, err = readHealth(ctx, tx, false)
	if err != nil {
		return health, err
	}
	if err = tx.Commit(ctx); err != nil {
		return health, err
	}
	return health, nil
}

func readHealth(ctx context.Context, tx pgx.Tx, exclusive bool) (domain.ProjectionHealth, error) {
	var h domain.ProjectionHealth
	query := `SELECT stream_name,stream_id,target_sequence,contiguous_sequence,highest_sequence,last_observed_at,
 (last_observed_at < clock_timestamp() - ($1::double precision * interval '1 second')),
 apply_lag_since,COALESCE(apply_lag_since <= clock_timestamp() - ($2::double precision * interval '1 second'),false)
 FROM gateway_route_replay_state WHERE singleton`
	if exclusive {
		query += " FOR UPDATE"
	}
	err := tx.QueryRow(ctx, query, domain.MaxProjectionObservationAge.Seconds(), domain.MaxProjectionApplyLag.Seconds()).Scan(&h.StreamName, &h.StreamID, &h.TargetSequence, &h.ContiguousSequence, &h.HighestSequence, &h.LastObservedAt, &h.Stale, &h.ApplyLagSince, &h.ApplyLagExceeded)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return h, err
	}
	reasonErr := tx.QueryRow(ctx, `SELECT reason FROM gateway_route_quarantines ORDER BY quarantine_id LIMIT 1`).Scan(&h.BlockedReason)
	if reasonErr != nil && !errors.Is(reasonErr, pgx.ErrNoRows) {
		return h, reasonErr
	}
	h.Initialized = err == nil && h.ContiguousSequence >= h.TargetSequence && h.BlockedReason == "" && !h.Stale && !h.ApplyLagExceeded
	if err != nil && h.BlockedReason == "" {
		h.BlockedReason = "projection_not_initialized"
	}
	return h, nil
}

func auditProjections(ctx context.Context, tx pgx.Tx, health domain.ProjectionHealth) error {
	rows, err := tx.Query(ctx, `SELECT provider,account_id,generation,enabled,snapshot,digest FROM gateway_route_projections`)
	if err != nil {
		return err
	}
	corrupt := false
	for rows.Next() {
		var provider, account, digest string
		var generation int64
		var enabled bool
		var snapshot []byte
		if err = rows.Scan(&provider, &account, &generation, &enabled, &snapshot, &digest); err != nil {
			rows.Close()
			return err
		}
		if _, err = decodeProjection(provider, account, generation, enabled, snapshot, digest); err != nil {
			corrupt = true
			break
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if corrupt {
		return insertQuarantine(ctx, tx, domain.StreamPosition{StreamName: health.StreamName, StreamID: health.StreamID}, domain.QuarantineCorruptProjection, "")
	}
	return nil
}

// ObserveSource refreshes transport observation, not business projection time.
// It never moves the startup target, and validates history/instance before
// extending the grace window. Transient transport failures do not call this.
func (store *Store) ObserveSource(ctx context.Context, source domain.ReplaySource) error {
	if source.ObservedAt.IsZero() {
		source.ObservedAt = time.Now().UTC()
	}
	if store.pool == nil {
		return domain.ErrUnavailable
	}
	sourceErr := source.Validate()
	if sourceErr != nil && !errors.Is(sourceErr, domain.ErrHistoryGap) {
		return sourceErr
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(tx) }()
	health, err := readHealth(ctx, tx, true)
	if err != nil {
		return err
	}
	if health.StreamName == "" {
		return domain.ErrNotInitialized
	}
	reason := domain.QuarantineReason("")
	if health.StreamName != source.StreamName || health.StreamID != source.StreamID {
		reason = domain.QuarantineSourceChanged
	} else if sourceErr != nil {
		reason = domain.QuarantineHistoryGap
	}
	if reason != "" {
		if err = insertQuarantine(ctx, tx, domain.StreamPosition{StreamName: source.StreamName, StreamID: source.StreamID}, reason, ""); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		return domain.ErrProjectionBlocked
	}
	if health.BlockedReason != "" {
		return domain.ErrProjectionBlocked
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_route_replay_state SET highest_sequence=GREATEST(highest_sequence,$1),
 apply_lag_since=CASE WHEN GREATEST(highest_sequence,$1)>contiguous_sequence
   THEN COALESCE(apply_lag_since,clock_timestamp()) ELSE NULL END,
 last_observed_at=GREATEST(last_observed_at,LEAST($2,clock_timestamp())),updated_at=now()
 WHERE singleton`, int64(source.LastSequence), source.ObservedAt)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
