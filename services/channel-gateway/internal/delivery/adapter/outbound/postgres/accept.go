package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"go.opentelemetry.io/otel/trace"
)

type rowReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func find(ctx context.Context, q rowReader, id string) (domain.Receipt, string, bool, error) {
	var r domain.Receipt
	var digest string
	var carrier tracecontext.Carrier
	err := q.QueryRow(ctx, `SELECT intent_id,run_id,part_count,digest,COALESCE(traceparent,''),COALESCE(tracestate,'') FROM gateway_delivery_intents WHERE intent_id=$1`, id).Scan(&r.IntentID, &r.RunID, &r.PartCount, &digest, &carrier.Traceparent, &carrier.Tracestate)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, "", false, nil
	}
	if err != nil {
		return r, "", false, databaseError(ctx, err)
	}
	linkCarrier(ctx, carrier)
	return r, digest, true, nil
}
func (s *Store) Find(ctx context.Context, id string) (domain.Receipt, string, bool, error) {
	if id == "" || len(id) > 128 {
		return domain.Receipt{}, "", false, domain.ErrInvalid
	}
	return find(ctx, s.pool, id)
}
func (s *Store) Accept(ctx context.Context, p domain.Prepared) (domain.Receipt, error) {
	if err := p.Validate(); err != nil {
		return domain.Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Receipt{}, databaseError(ctx, err)
	}
	defer rollback(tx)
	// All new Final admissions share the capacity lock. It also serializes absent
	// intent/run identities, without depending on an existing row to lock.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004286)`); err != nil {
		return domain.Receipt{}, databaseError(ctx, err)
	}
	old, digest, found, err := find(ctx, tx, p.Intent.ID)
	if err != nil {
		return old, err
	}
	if found {
		if digest != p.Digest {
			return old, domain.ErrConflict
		}
		return old, databaseError(ctx, tx.Commit(ctx))
	}
	var barrier bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_delivery_intents WHERE run_id=$1)`, p.Intent.RunID).Scan(&barrier); err != nil {
		return old, databaseError(ctx, err)
	}
	if barrier {
		return old, domain.ErrConflict
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return old, err
	}
	if !now.Before(p.Intent.Deadline) {
		return old, domain.ErrExpired
	}
	var intents, parts int64
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_delivery_intents),(SELECT count(*) FROM gateway_delivery_parts)`).Scan(&intents, &parts); err != nil {
		return old, databaseError(ctx, err)
	}
	if intents >= int64(s.options.MaxIntents) || parts+int64(len(p.Parts)) > int64(s.options.MaxParts) {
		return old, domain.ErrCapacity
	}
	ir, _ := json.Marshal(p.Intent)
	tr, _ := json.Marshal(p.Target)
	carrier := tracecontext.Capture(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO gateway_delivery_intents(intent_id,run_id,digest,intent,target,provider,account_id,deadline,part_count,traceparent,tracestate) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),NULLIF($11,''))`, p.Intent.ID, p.Intent.RunID, p.Digest, ir, tr, p.Target.Provider, p.Target.AccountID, p.Intent.Deadline, len(p.Parts), carrier.Traceparent, carrier.Tracestate)
	if err != nil {
		return old, databaseError(ctx, err)
	}
	for index, body := range p.Parts {
		if _, err = tx.Exec(ctx, `INSERT INTO gateway_delivery_parts(part_id,intent_id,part_index,body,state) VALUES($1,$2,$3,$4,'PENDING')`, domain.PartID(p.Intent.ID, index), p.Intent.ID, index, body); err != nil {
			return old, databaseError(ctx, err)
		}
	}
	r := domain.Receipt{IntentID: p.Intent.ID, RunID: p.Intent.RunID, PartCount: len(p.Parts)}
	if err = tx.Commit(ctx); err != nil {
		return domain.Receipt{}, databaseError(ctx, err)
	}
	return r, nil
}
func (s *Store) Get(ctx context.Context, id string) (domain.Snapshot, error) {
	r, _, found, err := s.Find(ctx, id)
	if err != nil {
		return domain.Snapshot{}, err
	}
	if !found {
		return domain.Snapshot{}, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT part_id,intent_id,part_index,body,state,attempt_number FROM gateway_delivery_parts WHERE intent_id=$1 ORDER BY part_index`, id)
	if err != nil {
		return domain.Snapshot{}, databaseError(ctx, err)
	}
	defer rows.Close()
	out := domain.Snapshot{Receipt: r}
	for rows.Next() {
		var p domain.Part
		if err = rows.Scan(&p.ID, &p.IntentID, &p.Index, &p.Text, &p.State, &p.AttemptNumber); err != nil {
			return out, databaseError(ctx, err)
		}
		out.Parts = append(out.Parts, p)
	}
	if err = rows.Err(); err != nil {
		return out, databaseError(ctx, err)
	}
	if len(out.Parts) != r.PartCount {
		return out, domain.ErrUnavailable
	}
	return out, nil
}

func linkCarrier(ctx context.Context, carrier tracecontext.Carrier) {
	sc := trace.SpanContextFromContext(carrier.Restore(context.Background()))
	ambient := trace.SpanContextFromContext(ctx)
	if sc.IsValid() && (sc.TraceID() != ambient.TraceID() || sc.SpanID() != ambient.SpanID()) {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc})
	}
}
